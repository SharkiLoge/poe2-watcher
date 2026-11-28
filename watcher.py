#!/usr/bin/env python3
"""Divine News Radar — сторож новостей Path of Exile 2 -> Telegram.

Один прогон = проверить все источники, отправить срочные пуши, раз в день в 21:00 (Рим)
отправить дайджест, сохранить state.json. Запускается из GitHub Actions каждые 10 минут.

Локально:  python watcher.py --dry-run            (ничего не шлёт, state не пишет)
           python watcher.py --dry-run --no-seed  (всё найденное считается новым — пример на живых данных)
           python watcher.py --test-message       (тестовое сообщение в Telegram)
"""
import argparse
import datetime as dt
import html
import json
import os
import re
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import feedparser
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
UTC = dt.timezone.utc
SOURCES = ("ggg", "reddit", "youtube", "twitch", "poe2db", "vaal")

# Сколько дней помнить уже виденное (по источнику дедупа).
SEEN_TTL_DAYS = {"ggg": 400, "ggg_dev": 30, "poe2site": 400, "poe2db": 400, "vaal_tools": 400,
                 "reddit_post": 7, "reddit_urgent": 7, "yt_video": 60,
                 "yt_search_en": 10, "yt_search_ru": 10, "twitch_clip": 3, "twitch_star": 3}

SECTION_TITLES = [
    ("ggg", "🏛 GGG"),
    ("twitch_new", "🟣 Twitch — новые стримеры в PoE 2"),
    ("twitch_clips", "🎬 Twitch — клипы"),
    ("reddit", "👽 Reddit r/PathOfExile2"),
    ("yt_list", "▶️ YouTube — каналы из списка (по просмотрам)"),
    ("yt_ext", "📺 YouTube — каналы не из списка"),
    ("yt_comp", "🎞 YouTube — компиляции клипов"),
    ("yt_live", "🔴 YouTube — стримы каналов из списка"),
]


# ---------------------------------------------------------------- время
def now_utc():
    return dt.datetime.now(UTC)


def iso_z(t):
    return t.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(UTC)


def rome_time(t):
    """Европейское летнее время: с последнего воскресенья марта 01:00 UTC до последнего воскресенья октября."""
    def last_sunday(month):
        d = dt.datetime(t.year, month, 31, 1, 0, tzinfo=UTC)
        while d.weekday() != 6:
            d -= dt.timedelta(days=1)
        return d
    offset = 2 if last_sunday(3) <= t < last_sunday(10) else 1
    return t + dt.timedelta(hours=offset)


def pacific_day(t):
    # Квота YouTube сбрасывается в полночь по Тихоокеанскому времени (берём UTC-8, точность до часа не важна).
    return (t - dt.timedelta(hours=8)).strftime("%Y-%m-%d")


def fmt_num(n):
    n = int(n or 0)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M".replace(".0M", "M")
    if n >= 1000:
        return f"{n / 1000:.1f}K".replace(".0K", "K")
    return str(n)


def esc(s):
    return html.escape(str(s), quote=False)


def load_env():
    path = os.path.join(HERE, ".env")
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.strip().strip('"').strip("'")
            if v:
                os.environ.setdefault(k.strip(), v)


def kw_regex(words):
    parts = [r"(?<![\w])" + re.escape(w).replace(r"\ ", r"[\s\-]") + r"(?![\w])" for w in words]
    return re.compile("|".join(parts), re.IGNORECASE)


# ---------------------------------------------------------------- контекст прогона
class Run:
    def __init__(self, cfg, state, args):
        self.cfg, self.state, self.args = cfg, state, args
        self.t = now_utc()
        self.ts = int(self.t.timestamp())
        self.th = cfg["thresholds"]
        self.dry = args.dry_run
        self.urgent = []          # тексты срочных пушей этого прогона
        self.http = requests.Session()
        self.http.headers["User-Agent"] = cfg.get("user_agent", "poe2-watcher/1.0 (PoE 2 news monitor)")
        self.reddit_headers = {"User-Agent": cfg["reddit"]["user_agent"]}
        st = state
        st.setdefault("seen", {})
        st.setdefault("digest", {})
        st.setdefault("outbox", [])
        st.setdefault("errors", {})
        st.setdefault("runs", 0)
        if "sent" not in st:
            st["sent"] = seed_sent(st)
        self._err_counted = set()  # источник со сбоем считается в дайджесте один раз за прогон
        self._feeds = {}           # RSS каналов YouTube за этот прогон: общий для основного модуля и clips.py
        self._feed_lock = threading.Lock()
        self._feed_hard_fails = 0  # каналов, не ответивших и после повторов (много — YouTube лежит, не ждём)
        known_sources = set(st["seen"].keys())
        self.seeding = set() if args.no_seed else None   # None = решаем по наличию источника в state
        self._known_sources = known_sources
        self.kw_ggg = kw_regex(cfg["keywords"]["ggg_urgent"])
        self.kw_drama = kw_regex(cfg["keywords"]["drama"])
        self.kw_poe2 = kw_regex(cfg["keywords"]["poe2_title"])
        self.kw_poe1 = kw_regex(cfg["keywords"].get("poe1_only", []) or ["path of exile 1"])

    # --- дедуп
    def is_seeding(self, source):
        if self.seeding is not None:
            return False
        return source not in self._known_sources

    def mark_undated(self, source, key):
        """Как mark(), но для источников без дат (poe2db, сайт PoE 2): при --no-seed тоже молча запоминаем —
        «всё как новое» тут означало бы сотни пушей обо всём, что лежит на сайте годами."""
        bucket = self.state["seen"].setdefault(source, {})
        new = key not in bucket
        bucket[key] = self.ts
        return new and source in self._known_sources

    def poe1_only(self, text):
        """Текст явно про PoE 1 и ни слова про PoE 2."""
        return bool((poe1_marker(text) or self.kw_poe1.search(text)) and not self.kw_poe2.search(text))

    def mark(self, source, key):
        """True, если ключ новый и источник не в режиме первичного заполнения."""
        bucket = self.state["seen"].setdefault(source, {})
        new = key not in bucket
        bucket[key] = self.ts
        return new and not self.is_seeding(source)

    def was_seen(self, source, key):
        return key in self.state["seen"].get(source, {})

    # --- «одно видео — один пуш»: ключи yt:<id> / tw:<id> всего, что ушло в Telegram (или разобрано как реакция)
    def was_sent(self, key):
        return key in self.state["sent"]

    def mark_sent(self, key):
        self.state["sent"][key] = self.ts

    def yt_kind(self, vid, title, desc, promo=()):
        return yt_kind(vid, title, desc, promo)

    def yt_promo(self, entries):
        return yt_promo(entries)

    def yt_gate(self, vid, cid, name, title, kind, url):
        return yt_gate(self, vid, cid, name, title, kind, url)

    def yt_feed(self, ch):
        return yt_feed(self, ch)

    # --- вывод
    def push(self, emoji, line, source, url):
        text = f"{emoji} {esc(line)}\n<i>{esc(source)}</i> · {esc(url)}"
        self.urgent.append(text)

    def drop(self, rule, text):
        """Запомнить отсеянное фильтром — в конце прогона печатается сводка (лог Actions / dry-run)."""
        self.__dict__.setdefault("dropped", {}).setdefault(rule, []).append(text)

    def digest(self, section, key, line, url, score=0, **extra):
        """extra — поля для фильтров build_digest (title, cid, desc у YouTube)."""
        d = self.state["digest"]
        d[f"{section}|{key}"] = {"sec": section, "text": line, "url": url, "score": score, "ts": self.ts, **extra}

    def error(self, source, msg):
        """В лог — каждый сбой; в счётчик дайджеста — источник один раз за прогон («×N» = N прогонов со сбоем)."""
        print(f"[warn] {source}: {msg}")
        if source in self._err_counted:
            return
        self._err_counted.add(source)
        day = rome_time(self.t).strftime("%Y-%m-%d")
        errs = self.state["errors"].setdefault(day, {})
        errs[source] = errs.get(source, 0) + 1

    def get(self, source, url, **kw):
        """GET без утечки ключей в лог: при ошибке печатаем только хост и путь."""
        kw.setdefault("timeout", 20)
        safe = url.split("?")[0]
        for attempt in range(2):
            try:
                r = self.http.get(url, **kw)
            except requests.RequestException as e:
                if attempt:
                    self.error(source, f"{safe}: {type(e).__name__}")
                    return None
                time.sleep(2)
                continue
            if r.status_code >= 500 and not attempt:
                time.sleep(2)
                continue
            if not r.ok:
                self.error(source, f"{safe}: HTTP {r.status_code}")
                return None
            return r
        return None


# ---------------------------------------------------------------- GGG
def poe1_marker(title):
    return bool(re.search(r"\b3\.\d{2}\b", title) or re.search(r"path of exile\s*(1\b|:)|\bpoe\s*1\b", title, re.I))


def thread_key(url):
    return re.sub(r"^https?://(www\.)?", "", url).split("#")[0].rstrip("/")


def forum_threads(page):
    """[(url, title, дата первого поста | None)] из списка тем форума pathofexile.com."""
    out = []
    for row in page.split('<td class="thread">')[1:]:
        m = re.search(r'href="(/forum/view-thread/\d+)"[^>]*>\s*([^<]{3,200}?)\s*<', row)
        if not m:
            continue
        pub = None
        d = re.search(r'class="post_date">\s*,?\s*([A-Z][a-z]{2} \d{1,2}, \d{4}, \d{1,2}:\d{2}:\d{2} [AP]M)', row)
        if d:
            try:
                pub = dt.datetime.strptime(d.group(1), "%b %d, %Y, %I:%M:%S %p").replace(tzinfo=UTC)
            except ValueError:
                pass
        out.append(("https://www.pathofexile.com" + m.group(1), html.unescape(m.group(2).strip()), pub))
    return out


def ggg_thread_game(run, url, title):
    """К какой игре относится новость GGG: 'poe2', 'poe1' или None (страницу проверить не удалось).
    RSS новостей — это форум «Announcements» PoE 1, туда же GGG кладёт общие новости обеих игр."""
    if run.kw_poe2.search(title):
        return "poe2"
    if run.poe1_only(title):
        return "poe1"
    r = run.get("ggg", url)
    if r is None:
        return None
    m = re.search(r"<title>([^<]*)", r.text)
    page_title = html.unescape(m.group(1)) if m else ""
    if page_title.startswith(("Early Access", "Path of Exile 2")):
        return "poe2"
    body = re.sub(r"<[^>]+>", " ", r.text)
    return "poe2" if run.kw_poe2.search(body) else "poe1"


def check_ggg(run):
    """Все открытые ленты GGG; каждая отдельно, чтобы сбой одной не глушил остальные."""
    for name, fn in (("ggg", check_ggg_forums), ("ggg_dev", check_ggg_dev), ("poe2site", check_poe2_site)):
        try:
            fn(run)
        except Exception as e:
            run.error(name, f"{type(e).__name__}: {str(e)[:200]}")


def ggg_max_age_h(run):
    """Темы GGG старше этого (по дате темы) — ни пуша, ни дайджеста. config.json → ggg.digest_max_age_hours."""
    return run.cfg["ggg"].get("digest_max_age_hours", 36)


def check_ggg_forums(run):
    cfg = run.cfg["ggg"]
    items = []   # (url, title, источник, дата, все темы срочно, надо ли выяснять игру)
    for forum in cfg["forums"]:
        r = run.get("ggg", forum["url"])
        if r is None:
            continue
        threads = forum_threads(r.text)
        if not threads:
            run.error("ggg", f"{forum['name']}: темы не найдены — разметка форума поменялась?")
        for url, title, pub in threads:
            items.append((url, title, f"форум: {forum['name']}", pub, forum.get("all_urgent", False), False))
    r = run.get("ggg", cfg["news_rss"])
    if r is not None:
        for e in feedparser.parse(r.content).entries:
            pub = None
            if e.get("published_parsed"):
                pub = dt.datetime(*e.published_parsed[:6], tzinfo=UTC)
            items.append((e.get("link", ""), html.unescape(e.get("title", "").strip()), "pathofexile.com/news", pub,
                          cfg.get("news_poe2_all_urgent", True), True))
    for url, title, src, pub, all_urgent, classify in items:
        if not url or not title:
            continue
        key = thread_key(url)
        if not run.mark("ggg", key):
            continue
        game = ggg_thread_game(run, url, title) if classify else "poe2"
        if game == "poe1":
            continue                      # только про PoE 1 — не шлём вообще, даже в дайджест
        if pub is not None and (run.t - pub).total_seconds() > ggg_max_age_h(run) * 3600:
            run.drop("GGG старше 36 ч", f"{title} ({pub:%d.%m.%Y})")
            continue                      # старая тема (анонсы прошлых месяцев) — только запомнили, никуда не шлём
        if (all_urgent and game == "poe2") or run.kw_ggg.search(title):
            run.push("🔥", f"GGG: {title}", src, url)
        else:
            tag = " (игра не определена)" if game is None else ""
            run.digest("ggg", key, f"{title}{tag}", url, score=int(pub.timestamp()) if pub else run.ts)


def check_ggg_dev(run):
    """Дев-трекер gggtracker.com (собирает посты сотрудников GGG с форума). Берём только форумы PoE 2:
    новая тема от GGG — 🔥, ответ сотрудника в теме обсуждений/фидбэка PoE 2 — 💬."""
    cfg = run.cfg["ggg"].get("dev_tracker")
    if not cfg:
        return
    r = run.get("ggg_dev", cfg["url"])
    if r is None:
        return
    try:
        acts = r.json().get("activity", [])
    except ValueError:
        run.error("ggg_dev", "ответ не JSON")
        return
    poe2_ids = set(cfg["poe2_forum_ids"])
    reply_ids = set(cfg["reply_forum_ids"])
    replies = {}
    for a in acts:
        d = a.get("data") or {}
        if a.get("type") != "forum_post" or d.get("forum_id") not in poe2_ids:
            continue
        if not run.mark("ggg_dev", str(d.get("id"))):
            continue
        try:
            t = dt.datetime.fromisoformat(d["time"]).astimezone(UTC)
        except (KeyError, ValueError):
            t = run.t
        if (run.t - t).total_seconds() > 3 * 86400:
            continue
        host = d.get("host") or "www.pathofexile.com"
        turl = f"https://{host}/forum/view-thread/{d['thread_id']}"
        title = html.unescape(d.get("thread_title", "")).strip()
        poster = d.get("poster", "GGG")
        if d.get("forum_id") not in reply_ids:
            # Форумы, где темы создаёт сама GGG (анонсы, патчноуты, пресс-релизы): новая тема — новость.
            # Ответы в них не дублируем — сами темы ловит проверка форумов.
            if run.mark("ggg", thread_key(turl)):
                run.push("🔥", f"GGG: {title}", f"дев-трекер · форум: {d.get('forum_name', '')} · {poster}", turl)
            continue
        rep = replies.setdefault(turl, {"title": title, "forum": d.get("forum_name", ""), "who": [],
                                        "url": f"https://{host}/forum/view-post/{d['id']}"})
        if poster not in rep["who"]:
            rep["who"].append(poster)
    for rep in replies.values():
        run.push("💬", f"GGG ответили в теме «{rep['title']}» — {', '.join(rep['who'])}",
                 f"дев-трекер · форум: {rep['forum']}", rep["url"])


def check_poe2_site(run):
    """pathofexile2.com — сайт-приложение; его разделы (страницы анонса, классы, лига) лежат в content.json.
    Новый раздел или блок на сайте = GGG что-то открыли (тизер, раскрытие)."""
    url = run.cfg["ggg"].get("poe2_site_content")
    if not url:
        return
    r = run.get("poe2site", url)
    if r is None:
        return
    try:
        data = r.json()
    except ValueError:
        run.error("poe2site", "ответ не JSON")
        return
    site = "https://pathofexile2.com"
    found = [(f"block:{c}", f"на сайте включён блок «{c}»", site) for c in data.get("availableContent", [])]
    la = (data.get("data") or {}).get("league-announcement") or {}
    slug = la.get("slug")
    if slug:
        label = (la.get("label") or {}).get("en_US") or slug
        found.append((f"page:{slug}", f"страница анонса «{label}»", f"{site}/{slug}"))
        for s in la.get("subRoutes", []):
            name = s if isinstance(s, str) else (s.get("path") or s.get("name") or "")
            if name:
                found.append((f"page:{slug}/{name}", f"раздел «{name}» на странице «{label}»", f"{site}/{slug}/{name}"))
    for rt in (data.get("routes") or {}).values():
        if isinstance(rt, dict) and rt.get("path"):
            name = (rt.get("title") or {}).get("en_US") or rt["path"]
            found.append((f"route:{rt['path']}", f"страница «{name}»", f"{site}/{rt['path']}"))
    if not found:
        run.error("poe2site", "в content.json нет разделов — формат поменялся?")
    for key, line, link in found:
        if run.mark_undated("poe2site", key):
            run.push("🆕", f"pathofexile2.com: {line}", "сайт PoE 2", link)


# ---------------------------------------------------------------- Reddit
def reddit_posts(run):
    """Список (feed, id, title, url, created_dt|None, score|None). None — Reddit недоступен в этот прогон."""
    sub = run.cfg["reddit"]["subreddit"]
    cid, secret = os.environ.get("REDDIT_CLIENT_ID"), os.environ.get("REDDIT_CLIENT_SECRET")
    posts = []
    if cid and secret:
        try:
            tok = run.http.post("https://www.reddit.com/api/v1/access_token",
                                data={"grant_type": "client_credentials"}, auth=(cid, secret), timeout=20,
                                headers=run.reddit_headers)
        except requests.RequestException as e:
            run.error("reddit", f"oauth: {type(e).__name__}")
            return None
        if not tok.ok:
            run.error("reddit", f"oauth: HTTP {tok.status_code}")
            return None
        h = {**run.reddit_headers, "Authorization": "bearer " + tok.json().get("access_token", "")}
        for feed, path in (("new", f"/r/{sub}/new?limit=100"), ("top", f"/r/{sub}/top?t=day&limit=50")):
            r = run.get("reddit", "https://oauth.reddit.com" + path, headers=h)
            if r is None:
                return None
            for c in r.json()["data"]["children"]:
                d = c["data"]
                meta = {"author": d.get("author") or "", "flair": d.get("link_flair_text") or "",
                        "author_flair": d.get("author_flair_text") or ""}
                posts.append((feed, d["name"], d["title"], "https://www.reddit.com" + d["permalink"],
                              dt.datetime.fromtimestamp(d["created_utc"], UTC), d["score"], meta))
        return posts
    for feed, path in (("new", f"/r/{sub}/new/.rss"), ("top", f"/r/{sub}/top/.rss?t=day")):
        try:
            r = run.http.get("https://www.reddit.com" + path, timeout=20, headers=run.reddit_headers)
        except requests.RequestException as e:
            run.error("reddit", f"{type(e).__name__}")
            return None
        if r.status_code != 200:
            # 429 / 403 / 404 — Reddit режет анонимный доступ; пропускаем прогон, как в брифе.
            run.error("reddit", f"rss {feed}: HTTP {r.status_code} — прогон пропущен")
            return None
        for e in feedparser.parse(r.content).entries:
            created = dt.datetime(*e.updated_parsed[:6], tzinfo=UTC) if e.get("updated_parsed") else None
            meta = {"author": re.sub(r"^/?u/", "", e.get("author", "")), "flair": "", "author_flair": ""}
            posts.append((feed, e.get("id", e.get("link")), e.get("title", ""), e.get("link", ""), created, None, meta))
    return posts


def reddit_official(run, meta):
    """Почему пост — новость без порога апвоутов: автор из GGG или флер News/Information/Announcement. Иначе ''."""
    cfg = run.cfg["reddit"]
    author = meta.get("author", "")
    if author.lower() in {a.lower() for a in cfg.get("ggg_authors", [])} \
            or re.search(r"(^|[-_])GGG($|[-_])", author, re.I) or re.search(r"\bGGG\b", meta.get("author_flair", "")):
        return f"пост GGG: u/{author}"
    flair = meta.get("flair", "").strip()
    if flair and any(f.lower() in flair.lower() for f in cfg.get("news_flairs", [])):
        return f"флер «{flair}»"
    return ""


def check_reddit(run):
    posts = reddit_posts(run)
    if not posts:
        return
    th = run.th
    for rank, (feed, pid, title, url, created, score, meta) in enumerate(posts):
        age_h = (run.t - created).total_seconds() / 3600 if created else None
        official = reddit_official(run, meta)
        if official and (age_h is None or age_h <= 48):
            if run.mark("reddit_urgent", pid):
                run.push("📣", f"Reddit: {title} ({official})", "r/PathOfExile2", url)
            continue
        drama = run.kw_drama.search(title)
        hot = False
        if score is not None and age_h is not None and age_h <= 12:
            window = th["reddit_urgent_window_hours"]
            hot = score >= th["reddit_urgent_upvotes"] and score / max(age_h, window) * window >= th["reddit_urgent_upvotes"]
        # Слова-триггеры: срочно, если пост уже в топе дня (или набрал ≥50 при OAuth); из /new — в дайджест.
        drama_urgent = drama and (feed == "top" or (score or 0) >= 50)
        if hot or drama_urgent:
            if run.mark("reddit_urgent", pid):
                why = f"{fmt_num(score)} апвоутов" if hot else f"слово «{drama.group(0)}»"
                run.push("🔥", f"Reddit: {title} ({why})", "r/PathOfExile2", url)
            continue
        run.mark("reddit_post", pid)
        if feed == "top":
            if score is not None and score < th["reddit_digest_upvotes"]:
                continue
            label = f" — {fmt_num(score)}⬆" if score is not None else ""
            run.digest("reddit", pid, f"{title}{label}", url, score=score if score is not None else 10_000 - rank)
        elif drama:
            run.digest("reddit", pid, f"{title} — слово «{drama.group(0)}»", url, score=0)


# ---------------------------------------------------------------- YouTube
def yt_api(run, endpoint, params, cost):
    key = os.environ.get("YT_API_KEY")
    if not key:
        return None
    q = run.state.setdefault("yt", {}).setdefault("quota", {})
    day = pacific_day(run.t)
    if q.get("day") != day:
        q.clear()
        q.update(day=day, used=0)
    if q["used"] + cost > run.th["youtube_daily_quota_cap"]:
        run.error("youtube", f"дневной лимит квоты {run.th['youtube_daily_quota_cap']} исчерпан")
        return None
    q["used"] += cost
    r = run.get("youtube", f"https://www.googleapis.com/youtube/v3/{endpoint}", params={**params, "key": key})
    return r.json() if r is not None else None


def yt_feed(run, ch):
    """RSS канала: (entries, None) или (None, "HTTP 503"/имя исключения).
    27.11 04:25–07:55 UTC RSS YouTube отдавал 503 почти на все каналы, а через секунды на тот же адрес — 200.
    Поэтому: до 4 попыток с паузой 2/5/8 с, через раз — та же лента по playlist_id=UU… (другой адрес, те же ролики).
    Если за прогон уже ≥ 5 каналов не ответили и после повторов — YouTube лежит, остальным одна попытка.
    Результат кэшируется на прогон: clips.py берёт те же ленты, а не качает второй раз."""
    cid = ch["id"]
    with run._feed_lock:
        if cid in run._feeds:
            return run._feeds[cid]
    urls = [f"https://www.youtube.com/feeds/videos.xml?channel_id={cid}"]
    if cid.startswith("UC"):
        urls.append(f"https://www.youtube.com/feeds/videos.xml?playlist_id=UU{cid[2:]}")
    tries = 1 if run._feed_hard_fails >= 5 else 4
    res = (None, "—")
    for k in range(tries):
        try:
            r = run.http.get(urls[k % len(urls)], timeout=15)
            if r.status_code == 200:
                entries = feedparser.parse(r.content).entries
                res = (entries, None)
                break
            res = (None, f"HTTP {r.status_code}")
            if r.status_code == 404 and k:      # 404 по обоим адресам — канала нет / неверный ID, ждать нечего
                break
        except requests.RequestException as e:
            res = (None, type(e).__name__)
        if k + 1 < tries:
            time.sleep(2 + 3 * k)
    with run._feed_lock:
        if res[0] is None:
            run._feed_hard_fails += 1
        run._feeds[cid] = res
    return res


def fetch_channel_feed(run, ch):
    entries, err = yt_feed(run, ch)
    return ch, entries, err


def seed_sent(st):
    """Первый запуск с «одно видео — один пуш»: собрать из state то, что уже уходило в Telegram."""
    sent = {}
    cs = st.get("clips", {})
    for vid, rec in cs.get("yt", {}).items():
        if rec.get("p", 0) > 0:
            sent["yt:" + vid] = rec["p"]
    for cl, rec in cs.get("tw", {}).items():
        if rec.get("p", 0) > 0:
            sent["tw:" + cl] = rec["p"]
    for vid, d in st.get("yt", {}).get("videos", {}).items():
        if d.get("a"):                       # 🚀 основного модуля или 🔥 clips.py (он ставит a=1)
            sent.setdefault("yt:" + vid, d.get("p", 0))
    for cl, ts in st.get("seen", {}).get("twitch_clip", {}).items():
        sent.setdefault("tw:" + cl, ts)
    return sent


# ---------------------------------------------------------------- реакции и компиляции
YT_VID_RE = re.compile(r"(?:youtube\.com/(?:watch\?(?:[^\s\"'<>]*?&)?v=|shorts/|live/)|youtu\.be/)([A-Za-z0-9_-]{11})")
REACT_TITLE_RE = re.compile(r"\breact(?:s|ing|ion|ions)?\b|\bwatching\b|реакци", re.IGNORECASE)
TS_NICK_RE = re.compile(r"^[ \t]*(?:\d{1,2}:)?\d{1,2}:\d{2}[ \t]*[-–—:|]?[ \t]*@?([A-Za-z0-9_.\-]{2,30})[ \t]*$", re.MULTILINE)
NOT_NICK = {"intro", "outro", "end", "ending", "start", "credits", "conclusion", "bonus"}


def nick_like(w):
    """Похоже на ник, а не на обычное слово главы («Monk», «Warrior»): цифра, _, заглавная внутри, всё строчными."""
    return bool(re.search(r"\d|_", w) or re.search(r"[a-z][A-Z]", w) or w.islower())


REACT_CUE_RE = re.compile(r"original|interview|footage|react|video by|source|credit|оригинал|интервью|источник|реакц",
                          re.IGNORECASE)


def desc_head_links(desc, title=None):
    """Ссылки на ролики YouTube в первых 3 непустых строках описания. С title — только «ссылки на оригинал»:
    в строке ссылки, строкой выше или в названии есть original/interview/footage/react/… (у GhazzyTV «Check out the
    original interview HERE!», у Zizaran «Original video by»). Без этого слова ссылка в шапке — обычно свой или
    чужой гайд «посмотрите ещё» (Lolcohol → гайд MisoxShiru, Ruetoo, Master T), а не источник."""
    head = [ln for ln in html.unescape(desc or "").splitlines() if ln.strip()][:3]
    out = []
    for i, ln in enumerate(head):
        for v in YT_VID_RE.findall(ln):
            ctx = ln + "\n" + (head[i - 1] if i else "") + "\n" + (title or "")
            if title is None or REACT_CUE_RE.search(ctx):
                out.append(v)
    return out


def yt_promo(entries):
    """Ссылки, которые канал ставит в шапку описания у ≥ 2 роликов из RSS, — постоянная реклама своего гайда,
    а не источник реакции (Lolcohol, SnooBAE85, XTheFarmerX, Kwazi — 27.11)."""
    cnt = {}
    for e in entries or []:
        for v in set(desc_head_links(e.get("summary"))):
            cnt[v] = cnt.get(v, 0) + 1
    return {v for v, n in cnt.items() if n >= 2}


def yt_kind(vid, title, desc, promo=()):
    """["comp", None] — компиляция клипов: в описании таймкоды с ≥ 10 разными метками, из них ≥ 3 похожи на ники,
    и упомянут Twitch/стримеры (Exiled Again, POEClipZ);
    ["react", id|None] — реакция/пересказ: в первых 3 строках описания ссылка на чужой оригинал (desc_head_links, не promo)
    или в названии react/reacts/reacting/reaction/watching/реакция (тогда источник — первая ссылка в описании);
    None — обычный ролик."""
    desc = html.unescape(desc or "")
    labels = {m.group(1) for m in TS_NICK_RE.finditer(desc) if m.group(1).lower() not in NOT_NICK}
    if (len({x.lower() for x in labels}) >= 10 and sum(map(nick_like, labels)) >= 3
            and re.search(r"twitch|streamer|стример", desc, re.IGNORECASE)):
        return ["comp", None]
    links = [v for v in desc_head_links(desc, title or "") if v != vid and v not in promo]
    if links:
        return ["react", links[0]]
    if REACT_TITLE_RE.search(title or ""):
        links = [v for v in YT_VID_RE.findall(desc) if v != vid and v not in promo]
        return ["react", links[0] if links else None]
    return None


def yt_gate(run, vid, cid, name, title, kind, url):
    """Перед пушем про ролик YouTube. True — слать сам ролик. False — не слать: уже присылали, компиляция
    (строка в дайджест) или реакция (вместо неё — 🎙 источник, если его ещё не присылали и ему ≤ 3 дней)."""
    if run.was_sent("yt:" + vid):
        run.drop("повтор: ролик уже присылали", f"{name}: {title}")
        return False
    if kind and kind[0] == "comp":
        run.digest("yt_comp", vid, f"Компиляция {name}", url, score=0)
        run.drop("компиляция клипов → дайджест", f"{name}: {title}")
        run.mark_sent("yt:" + vid)
        return False
    if not (kind and kind[0] == "react"):
        return True
    src = kind[1]
    if not src:
        run.drop("реакция без ссылки на источник — не шлю", f"{name}: {title}")
        run.mark_sent("yt:" + vid)
        return False
    if run.was_sent("yt:" + src):
        run.drop("реакция: источник уже присылали", f"{name}: {title} → {src}")
        run.mark_sent("yt:" + vid)
        return False
    data = yt_api(run, "videos", {"part": "snippet", "id": src}, 1)
    items = (data or {}).get("items", [])
    if data is None:
        run.drop("реакция: YouTube API не ответил — источник не проверить, не шлю", f"{name}: {title} → {src}")
        return False                      # не помечаем: следующий прогон попробует ещё раз
    if not items:
        run.drop("реакция: источник удалён/скрыт — не шлю", f"{name}: {title} → {src}")
        run.mark_sent("yt:" + vid)
        return False
    sn = items[0]["snippet"]
    if sn.get("channelId") == cid and not REACT_TITLE_RE.search(title or ""):
        return True                       # ссылка на свой же ролик («часть 1 тут») — не реакция
    run.mark_sent("yt:" + vid)
    age_h = (run.t - parse_iso(sn["publishedAt"])).total_seconds() / 3600
    if age_h > 72:
        run.drop("реакция: источнику больше 3 дней — не шлю", f"{name}: {title} → {sn.get('title')} ({age_h / 24:.1f} дн.)")
        return False
    src_url = f"https://www.youtube.com/watch?v={src}"
    run.urgent.append(f"🎙 Источник (нашёл через реакцию {esc(name)}): {esc(html.unescape(sn.get('title', '')))} · {esc(src_url)}")
    run.mark_sent("yt:" + src)
    print(f"[react] {name}: «{title}» → источник {src} «{sn.get('title')}» ({sn.get('channelTitle')}, {age_h:.0f} ч)")
    return False


def en_ru_title(title):
    """Все буквы названия — латиница ASCII или кириллица (тайский, корейский, китайский, латиница с диакритикой — нет)."""
    return all(ch.isascii() or "Ѐ" <= ch <= "ӿ" for ch in title if ch.isalpha())


def yt_list_ok(run, title, desc, share):
    """Ролик канала из списка — в дайджест: не PoE 1 и по названию/описанию/каналу про PoE 2."""
    return not run.poe1_only(title) and is_poe2_video(run, title, desc, share)


def yt_ext_ok(run, title):
    """Ролик канала не из списка (поиск) — в дайджест и 📺: не PoE 1, название на английском или русском."""
    return not run.poe1_only(title) and en_ru_title(title)


def is_poe2_video(run, title, desc, channel_poe2_share):
    text = f"{title}\n{desc or ''}"
    if run.kw_poe2.search(text) or re.search(r"\b0\.\d\.\d+[a-z]?\b", title):
        return True
    if re.search(r"\b3\.\d{2}\b", title) or re.search(r"\bpoe\s*1\b|path of exile 1\b", text, re.I):
        return False
    return channel_poe2_share >= 0.5


def check_youtube(run):
    th, yt = run.th, run.state.setdefault("yt", {})
    videos = yt.setdefault("videos", {})
    norms = yt.setdefault("norms", {})
    boot = yt.setdefault("boot", {})
    # Ролики с liveStreamingDetails (идущий стрим, запись стрима, премьера): id -> когда узнали. Их просмотры включают
    # зрителей эфира — с обычными роликами несравнимы: ни 🚀, ни норма канала (ни здесь, ни в clips.py), только
    # строка «Стрим …» в дайджест. Помним 30 дней — столько живут ролики в RSS, из которого строится норма-заготовка.
    live = yt.setdefault("live", {})
    for k in [k for k, ts in live.items() if run.ts - ts > 30 * 86400]:
        del live[k]
    chans = run.cfg["youtube"]["channels"]
    with ThreadPoolExecutor(max_workers=4) as ex:       # 8 потоков = лишний повод для 503 от RSS
        results = list(ex.map(lambda c: fetch_channel_feed(run, c), chans))
    fails = [f"{c['name']} ({err})" for c, entries, err in results if entries is None]
    if fails:
        run.error("youtube_rss", f"{len(fails)} из {len(chans)} каналов не ответили и после повторов: {', '.join(fails[:5])}")
    max_age_h = th["youtube_speed_max_age_hours"]
    for ch, entries, _ in results:
        if not entries:
            continue
        cid = ch["id"]
        promo = yt_promo(entries)
        share = sum(1 for e in entries if run.kw_poe2.search(e.get("title", "") + " " + e.get("summary", ""))) / len(entries)
        yt.setdefault("share", {})[cid] = round(share, 2)   # для фильтра уже накопленного в дайджесте
        # Норма-заготовка, пока не накопились реальные замеры за 6 ч: доля от медианы просмотров роликов старше 3 дней.
        b = boot.get(cid)
        if not b or run.ts - b.get("ts", 0) > 7 * 86400:
            old = []
            for e in entries:
                if e.get("yt_videoid") in live:
                    continue                  # запись стрима / премьера — просмотры эфира, норму не портит
                if e.get("published_parsed") and e.get("media_statistics"):
                    pub = dt.datetime(*e.published_parsed[:6], tzinfo=UTC)
                    if (run.t - pub).days >= 3:
                        old.append(int(e["media_statistics"].get("views", 0)))
            if len(old) >= 3:
                boot[cid] = {"v": int(statistics.median(old[: th["youtube_norm_videos"]]) * th["youtube_norm_bootstrap_fraction"]),
                             "ts": run.ts}
        for e in entries:
            vid = e.get("yt_videoid")
            if not vid or not e.get("published_parsed"):
                continue
            title = e.get("title", "")
            pub = dt.datetime(*e.published_parsed[:6], tzinfo=UTC)
            age_h = (run.t - pub).total_seconds() / 3600
            views = int((e.get("media_statistics") or {}).get("views", 0))
            url = f"https://www.youtube.com/watch?v={vid}"
            if run.mark("yt_video", vid):
                if ch.get("official"):
                    # Официальный канал GGG: любой новый ролик не только про PoE 1 — сразу пуш.
                    # Решаем по названию: ролики PoE 1 GGG подписывает явно («Path of Exile 1: …», лига PoE 1),
                    # а доклады ExileCon про обе игры в описании упоминают PoE 1 — их не теряем.
                    if age_h < 72 and not run.poe1_only(title) and not run.was_sent("yt:" + vid):
                        run.push("🔥", f"GGG на YouTube: {title}", "YouTube Path of Exile", url)
                        run.mark_sent("yt:" + vid)
                elif age_h < 48 and re.search(r"path of exile|\bpoe|пое|wraeclast|exile", title + " " + e.get("summary", ""), re.I):
                    if yt_list_ok(run, title, e.get("summary"), share):
                        run.digest("yt_list", vid, f"{ch['name']}: {title}", url, score=views, title=title, cid=cid, ok=1)
                    else:
                        run.drop("YouTube из списка: PoE 1 / не про PoE 2", f"{ch['name']}: {title}")
            if age_h <= max_age_h and vid not in videos:
                videos[vid] = {"c": cid, "n": ch["name"], "t": title, "p": int(pub.timestamp()), "v": views,
                               "poe2": is_poe2_video(run, title, e.get("summary"), share), "a": 0, "s": 0}
            elif vid in videos:
                videos[vid]["v"] = max(videos[vid]["v"], views)
            if vid in videos:
                videos[vid]["k"] = yt_kind(vid, title, e.get("summary"), promo)   # реакция / компиляция — по описанию
    # Свежие просмотры — videos.list (1 единица квоты на 50 роликов при любом наборе part, поэтому
    # liveStreamingDetails берём тем же вызовом). Без ключа остаются цифры из RSS, и стрим от ролика не отличить.
    ids = [v for v, d in videos.items() if run.ts - d["p"] <= max_age_h * 3600 + 3600]
    for i in range(0, len(ids), 50):
        data = yt_api(run, "videos", {"part": "statistics,liveStreamingDetails", "id": ",".join(ids[i:i + 50]),
                                      "maxResults": 50}, 1)
        for it in (data or {}).get("items", []):
            d = videos[it["id"]]
            d["v"] = int(it.get("statistics", {}).get("viewCount", 0))
            dk = f"yt_list|{it['id']}"
            if "liveStreamingDetails" in it and not d.get("live"):
                d["live"] = 1
                live[it["id"]] = run.ts
                had = run.state["digest"].pop(dk, None)
                if had or (d["poe2"] and not run.poe1_only(d["t"])):
                    run.digest("yt_live", it["id"], f"Стрим {d['n']}: {d['t']}",
                               f"https://www.youtube.com/watch?v={it['id']}", score=d["v"])
                if not d["a"]:
                    run.drop("запись стрима / премьера — не 🚀, строка в дайджест", f"{d['n']}: {d['t']}")
            if dk in run.state["digest"]:
                run.state["digest"][dk]["score"] = d["v"]
            elif f"yt_live|{it['id']}" in run.state["digest"]:
                run.state["digest"][f"yt_live|{it['id']}"]["score"] = d["v"]
    nh = th["youtube_norm_hours"]
    for vid, d in list(videos.items()):
        age_h = (run.ts - d["p"]) / 3600
        if age_h > max_age_h + 2:
            del videos[vid]
            continue
        if d.get("live") or vid in live:
            continue                      # стрим / запись стрима / премьера: ни замера нормы, ни 🚀
        samples = norms.get(d["c"], [])
        if not d["s"] and age_h >= nh:
            d["s"] = 1
            if age_h <= nh + 2 and d["v"] > 0:
                samples = (samples + [int(d["v"] * (nh / age_h) ** 0.6)])[-th["youtube_norm_videos"]:]
                norms[d["c"]] = samples
        norm = statistics.median(samples) if len(samples) >= 3 else (boot.get(d["c"]) or {}).get("v")
        if not norm or d["a"] or not d["poe2"]:
            continue
        if age_h * 60 < th["youtube_speed_min_age_minutes"]:
            continue
        expected = norm * (age_h / nh) ** (0.6 if age_h <= nh else 0.35)
        if d["v"] >= th["youtube_speed_min_views"] and d["v"] >= th["youtube_speed_multiplier"] * expected:
            if not run.yt_gate(vid, d["c"], d["n"], d["t"], d.get("k"), f"https://www.youtube.com/watch?v={vid}"):
                if run.was_sent("yt:" + vid):
                    d["a"] = 1            # разобран (повтор / компиляция / реакция); иначе API не ответил — проверим снова
                continue
            d["a"] = 1
            run.mark_sent("yt:" + vid)
            k = d["v"] / max(expected, 1)
            run.push("🚀", f"YouTube в {k:.0f}× быстрее нормы: {d['n']} — «{d['t']}»: {fmt_num(d['v'])} просмотров "
                           f"за {age_h:.1f} ч (норма канала ~{fmt_num(norm)} за {nh} ч)",
                     "YouTube", f"https://www.youtube.com/watch?v={vid}")
    youtube_search(run)


YT_POE_ANY_RE = re.compile(r"path\s*of\s*exile|\bpoe\s*[12²]?\b|\bpoe[12]\b|\bпое\s*2?\b", re.IGNORECASE)


def yt_channel_poe_history(run, cid, vid):
    """Есть ли среди последних роликов канала (RSS, до 15 шт., без квоты) другой ролик с PoE / Path of Exile
    в названии. True — есть (канал не «впервые»), False — нет, None — RSS не ответил."""
    _, entries, err = fetch_channel_feed(run, {"id": cid, "name": cid})
    if entries is None:
        run.error("youtube_rss", f"история канала {cid}: {err}")
        return None
    return any(YT_POE_ANY_RE.search(e.get("title", "")) for e in entries[:15] if e.get("yt_videoid") != vid)


def youtube_search(run):
    th, yt = run.th, run.state.setdefault("yt", {})
    if not os.environ.get("YT_API_KEY"):
        return
    srch = yt.setdefault("search", {"last": 0, "idx": 0})
    if run.ts - srch["last"] < th["youtube_search_every_minutes"] * 60 - 90:
        return
    queries = run.cfg["youtube"]["search_queries"]
    q = queries[srch["idx"] % len(queries)]
    srch["idx"] += 1
    srch["last"] = run.ts
    after = iso_z(run.t - dt.timedelta(minutes=th["youtube_search_window_minutes"]))
    data = yt_api(run, "search", {"part": "snippet", "q": q["q"], "type": "video", "order": "date",
                                  "publishedAfter": after, "maxResults": 50, "relevanceLanguage": q["lang"]}, 100)
    if not data:
        return
    list_ids = {c["id"] for c in run.cfg["youtube"]["channels"]}
    ext = yt.setdefault("ext", {})          # канал -> {"subs", "ts", "name"}
    poe_ch = yt.setdefault("ext_poe", {})   # каналы не из списка, уже выпускавшие ролик по PoE 2
    hits = []
    for it in data.get("items", []):
        sn, vid = it.get("snippet", {}), it.get("id", {}).get("videoId")
        if not vid or sn.get("channelId") in list_ids:
            continue
        if not run.kw_poe2.search(html.unescape(sn.get("title", ""))):
            continue                      # PoE 2 только в описании/тегах (WoW, Diablo…) — не наш ролик
        if not yt_ext_ok(run, html.unescape(sn.get("title", ""))):
            run.drop("YouTube не из списка: PoE 1 / не en-ru", html.unescape(sn.get("channelTitle", "") + ": " + sn.get("title", "")))
            continue                      # PoE 1 или название не на английском/русском — ни 📺, ни дайджест
        hits.append((vid, sn))
    need = sorted({sn["channelId"] for _, sn in hits
                   if sn["channelId"] not in ext or run.ts - ext[sn["channelId"]]["ts"] > 14 * 86400})
    for i in range(0, len(need), 50):
        cd = yt_api(run, "channels", {"part": "statistics", "id": ",".join(need[i:i + 50])}, 1)
        for c in (cd or {}).get("items", []):
            st = c.get("statistics", {})
            subs = 0 if st.get("hiddenSubscriberCount") else int(st.get("subscriberCount", 0))
            ext[c["id"]] = {"subs": subs, "ts": run.ts}
    source = f"yt_search_{q['lang']}"
    for vid, sn in hits:
        cid = sn["channelId"]
        subs = ext.get(cid, {}).get("subs", 0)
        title = html.unescape(sn.get("title", ""))
        name = html.unescape(sn.get("channelTitle", ""))
        url = f"https://www.youtube.com/watch?v={vid}"
        first_time = cid not in poe_ch
        poe_ch[cid] = run.ts
        if not run.mark(source, vid):
            continue
        unsure = ""
        if first_time and subs >= th["youtube_big_channel_subscribers"]:
            # «Впервые» = бот его раньше не видел. Проверяем по RSS канала (бесплатно): уже снимал PoE — не впервые.
            played = yt_channel_poe_history(run, cid, vid)
            if played:
                first_time = False
                run.drop("📺 не впервые: в RSS канала уже есть PoE", f"{name}: {title}")
            elif played is None:
                unsure = " (историю канала проверить не удалось)"
        if (first_time and subs >= th["youtube_big_channel_subscribers"]
                and run.yt_gate(vid, cid, name, title, yt_kind(vid, title, sn.get("description")), url)):
            run.mark_sent("yt:" + vid)
            run.push("📺", f"Крупный канал впервые про PoE 2: {name} ({fmt_num(subs)} подписчиков) — «{title}»{unsure}",
                     "YouTube", url)
        elif subs >= th["youtube_digest_external_min_subscribers"]:
            run.digest("yt_ext", vid, f"{name} ({fmt_num(subs)} подп.): {title}", url, score=subs, title=title, ok=1)


# ---------------------------------------------------------------- Twitch
POE_VOD_RE = re.compile(r"(?<![\w])(poe\s?2?|path\s+of\s+exile)(?![\w])", re.IGNORECASE)


def twitch_poe_history(run, h, api, uid, gids):
    """Играл ли канал раньше в PoE / PoE 2.
    True — есть клипы из PoE/PoE 2 за год или VOD с PoE в названии; False — следов нет;
    None — проверить не удалось (API не ответил)."""
    params = {"broadcaster_id": uid, "first": 100,
              "started_at": iso_z(run.t - dt.timedelta(days=365)), "ended_at": iso_z(run.t)}
    for _ in range(3):                      # до 300 самых популярных клипов за год
        try:
            r = run.http.get(api + "clips", headers=h, params=params, timeout=20)
        except requests.RequestException:
            return None
        if not r.ok:
            return None
        js = r.json()
        if any(c.get("game_id") in gids for c in js.get("data", [])):
            return True
        cur = (js.get("pagination") or {}).get("cursor")
        if not cur:
            break
        params["after"] = cur
    try:
        r = run.http.get(api + "videos", headers=h, timeout=20,
                         params={"user_id": uid, "type": "archive", "first": 100})
    except requests.RequestException:
        return None
    if not r.ok:
        return None
    return any(POE_VOD_RE.search(v.get("title", "")) for v in r.json().get("data", []))


def check_twitch(run):
    cid, secret = os.environ.get("TWITCH_CLIENT_ID"), os.environ.get("TWITCH_CLIENT_SECRET")
    if not (cid and secret):
        return
    th, tw = run.th, run.state.setdefault("twitch", {})
    try:
        tok = run.http.post("https://id.twitch.tv/oauth2/token", timeout=20,
                            data={"client_id": cid, "client_secret": secret, "grant_type": "client_credentials"})
    except requests.RequestException as e:
        run.error("twitch", f"token: {type(e).__name__}")
        return
    if not tok.ok:
        run.error("twitch", f"token: HTTP {tok.status_code}")
        return
    h = {"Client-Id": cid, "Authorization": "Bearer " + tok.json()["access_token"]}
    api = "https://api.twitch.tv/helix/"
    if not tw.get("game_id"):
        r = run.get("twitch", api + "games", headers=h, params={"name": run.cfg["twitch"]["game_name"]})
        data = (r.json().get("data") if r is not None else None) or []
        if not data:
            run.error("twitch", "игра не найдена")
            return
        tw["game_id"] = data[0]["id"]
    gid = tw["game_id"]
    if not tw.get("poe1_game_id"):
        r = run.get("twitch", api + "games", headers=h, params={"name": run.cfg["twitch"].get("poe1_game_name", "Path of Exile")})
        data = (r.json().get("data") if r is not None else None) or []
        if data:
            tw["poe1_game_id"] = data[0]["id"]
    poe_gids = {gid} | ({tw["poe1_game_id"]} if tw.get("poe1_game_id") else set())
    seeding = "history" not in tw and not run.args.no_seed
    hist = tw.setdefault("history", {})     # login -> [первый раз, последний раз, макс. зрителей]
    known = {s.lower() for s in run.cfg["twitch"]["known_streamers"]}
    stars = [s.lower() for s in run.cfg["twitch"]["stars"]]

    # Звёзды: прямой запрос по никам — пуш, если сейчас в категории PoE 2 (раз на трансляцию).
    star_live = set()
    star_seen = run.state["seen"].setdefault("twitch_star", {})   # без первичного заполнения: звезда в PoE 2 — всегда пуш
    if stars:
        r = run.get("twitch", api + "streams", headers=h, params=[("user_login", s) for s in stars[:100]])
        for s in (r.json().get("data", []) if r is not None else []):
            if s.get("game_id") == gid:
                login = s["user_login"].lower()
                star_live.add(login)
                if s["id"] not in star_seen:
                    run.push("⭐", f"Звезда зашла в PoE 2: {s['user_name']} — {fmt_num(s['viewer_count'])} зрителей, «{s.get('title', '')[:80]}»",
                             "Twitch", f"https://twitch.tv/{login}")
                star_seen[s["id"]] = run.ts

    r = run.get("twitch", api + "streams", headers=h, params={"game_id": gid, "first": 100})
    streams = r.json().get("data", []) if r is not None else []
    followers_ok = True
    for s in streams:
        login, viewers = s["user_login"].lower(), int(s["viewer_count"])
        rec = hist.get(login)
        if rec:
            rec[1], rec[2] = run.ts, max(rec[2], viewers)
            # Крупный по фолловерам канал зашёл с малым числом зрителей — ждём до 12 ч, пока наберёт порог.
            if len(rec) >= 5 and run.ts <= rec[3] and viewers >= th["twitch_new_streamer_followers_min_viewers"]:
                run.push("⚡", f"Впервые стримит PoE 2: {s['user_name']} — {fmt_num(viewers)} зрителей, {fmt_num(rec[4])} фолловеров",
                         "Twitch", f"https://twitch.tv/{login}")
                del rec[3:]
            elif len(rec) >= 5 and run.ts > rec[3]:
                del rec[3:]
            continue
        hist[login] = [run.ts, run.ts, viewers]
        if seeding or login in known or login in star_live:
            continue
        followers = None
        if followers_ok:
            try:
                fr = run.http.get(api + "channels/followers", headers=h, params={"broadcaster_id": s["user_id"]}, timeout=20)
            except requests.RequestException:
                fr = None
            if fr is not None and fr.ok:
                followers = fr.json().get("total")
            else:
                followers_ok = False
                run.error("twitch", f"followers: HTTP {getattr(fr, 'status_code', '—')} — проверяю только зрителей")
        fol_txt = f", {fmt_num(followers)} фолловеров" if followers is not None else ""
        big = (followers or 0) >= th["twitch_new_streamer_followers"]
        min_v = th["twitch_new_streamer_followers_min_viewers"]
        # «Впервые» = бот его раньше не видел. Для тех, о ком сообщим, проверяем: не играл ли канал
        # в PoE / PoE 2 до запуска бота (клипы за год, названия VOD). Играл — молчим.
        unsure = ""
        if big or viewers >= th["twitch_digest_new_streamer_viewers"]:
            played = twitch_poe_history(run, h, api, s["user_id"], poe_gids)
            if played:
                continue
            if played is None:
                unsure = " (историю канала проверить не удалось)"
        if viewers >= th["twitch_new_streamer_viewers"] or (big and viewers >= min_v):
            run.push("⚡", f"Впервые стримит PoE 2: {s['user_name']} — {fmt_num(viewers)} зрителей{fol_txt}{unsure}",
                     "Twitch", f"https://twitch.tv/{login}")
        elif big:
            hist[login] += [run.ts + 12 * 3600, followers]
        if not (viewers >= th["twitch_new_streamer_viewers"] or (big and viewers >= min_v)) \
                and viewers >= th["twitch_digest_new_streamer_viewers"]:
            run.digest("twitch_new", login, f"{s['user_name']} — {fmt_num(viewers)} зрителей{fol_txt}",
                       f"https://twitch.tv/{login}", score=viewers)
    for login in [k for k, v in hist.items() if run.ts - v[1] > 365 * 86400]:
        del hist[login]

    win = th["twitch_clip_window_hours"]
    r = run.get("twitch", api + "clips", headers=h,
                params={"game_id": gid, "first": 100, "started_at": iso_z(run.t - dt.timedelta(hours=win)), "ended_at": iso_z(run.t)})
    for c in (r.json().get("data", []) if r is not None else []):
        views = int(c.get("view_count", 0))
        if views >= th["twitch_clip_urgent_views"]:
            if run.mark("twitch_clip", c["id"]):
                run.push("🔥", f"Клип Twitch {fmt_num(views)} просмотров за {win} ч: {c['broadcaster_name']} — «{c['title']}»",
                         "Twitch", c["url"])
        elif views >= th["twitch_clip_digest_views"]:
            run.digest("twitch_clips", c["id"], f"{c['broadcaster_name']} — «{c['title']}» — {fmt_num(views)} просмотров",
                       c["url"], score=views)


# ---------------------------------------------------------------- poe2db.tw (датамайн)
POE2DB_KINDS = {"UniqueItem": ("unique", "уники"), "GemItem": ("gem", "скиллы/геммы")}
POE2DB_ITEM_CLASS = re.compile(r"\b(UniqueItems?|GemItem|gemitem|whiteitem|\w+Item)\b")


def poe2db_links(page):
    """[(вид, href, название)] предметов на странице poe2db. Классы ссылок разные:
    'UniqueItem', 'UniqueItems UniqueItem', 'GemItem', 'gemitem', 'whiteitem SoulCore', 'CurrencyItem'…"""
    out = []
    for attrs, inner in re.findall(r"<a\b([^>]*)>(.*?)</a>", page, re.S):
        cls = re.search(r'class="([^"]*)"', attrs)
        href = re.search(r'href="(?:/us/)?([^"#?/]+)"', attrs)
        if not (cls and href) or not POE2DB_ITEM_CLASS.search(cls.group(1)):
            continue
        name = html.unescape(re.sub(r"<[^>]+>", "", inner)).strip()
        if not 2 <= len(name) <= 80:
            continue
        c = cls.group(1)
        kind = "unique" if "Unique" in c else "gem" if "gem" in c.lower() else "item"
        out.append((kind, href.group(1), name))
    return out


def check_poe2db(run):
    """poe2db.tw — данные из файлов клиента. Не чаще раза в every_minutes (по умолчанию 30 мин):
    - главная: список версий данных игры (Version_X) и страницы новой лиги (<Лига>_uniques/_gems/_items);
    - страницы лиги: новые уники, геммы, предметы;
    - Ascendancy_class: новые классы и восхождения."""
    cfg = run.cfg.get("poe2db") or {}
    if not cfg.get("enabled", True):
        return
    st = run.state.setdefault("poe2db", {})
    if run.ts - st.get("last", 0) < cfg.get("every_minutes", 30) * 60 - 90:
        return
    st["last"] = run.ts
    base = cfg.get("base", "https://poe2db.tw/us/")
    r = run.get("poe2db", base)
    if r is None:
        return
    home = r.text
    found = []      # (ключ, группа, название, ссылка)
    versions = sorted(set(re.findall(r'href="(?:/us/)?Version_([0-9][0-9A-Za-z.]*)"', home)))
    if not versions:
        run.error("poe2db", "на главной нет списка версий — разметка поменялась?")
    for v in versions:
        found.append((f"version:{v}", "version", f"Version {v}", f"{base}Version_{v}"))
    pages = sorted(set(re.findall(r'href="(?:/us/)?([A-Za-z0-9_\'-]+_(?:uniques|gems|items))"', home)))
    for p in pages:
        found.append((f"page:{p}", "page", p.replace("_", " "), base + p))
        time.sleep(1)
        r = run.get("poe2db", base + p)
        if r is None:
            continue
        got = poe2db_links(r.text)
        if not got:
            run.error("poe2db", f"{p}: предметы не найдены — разметка поменялась?")
        for kind, href, name in got:
            found.append((f"{kind}:{href}", f"{kind}|{p}", name, base + href))
    time.sleep(1)
    r = run.get("poe2db", base + "Ascendancy_class")
    if r is not None:
        asc = re.findall(r'<a class="\w+" data-hover="([^"]*)" href="/us/([^"#?]+)"', r.text)
        asc = [(h, "ascendancy" if "Ascendancy" in hov else "class") for hov, h in asc
               if "Ascendancy" in hov or "Characters_hover" in hov]
        if not asc:
            run.error("poe2db", "Ascendancy_class: восхождения не найдены — разметка поменялась?")
        for href, kind in asc:
            found.append((f"{kind}:{href}", kind, href.replace("_", " "), base + href))
    new, seen_keys = [], set()
    for key, group, name, link in found:
        if key in seen_keys:
            continue
        seen_keys.add(key)
        if run.mark_undated("poe2db", key):
            new.append((group, name, link))
    if "poe2db" not in run._known_sources:
        print(f"[poe2db] первый запуск: молча запомнено {len(seen_keys)} объектов "
              f"({len(versions)} версий, {len(pages)} страниц лиги, уники/геммы/предметы, классы и восхождения)")
    groups = {}
    for group, name, link in new:
        groups.setdefault(group, []).append((name, link))
    labels = {"version": "новая версия данных игры", "page": "новая страница лиги", "class": "новый класс",
              "ascendancy": "новое восхождение"}
    for group, lst in groups.items():
        if group in labels:
            for name, link in lst:
                if group == "page" and any(g.endswith("|" + link[len(base):]) for g in groups):
                    continue              # о новой странице лиги скажет пуш с её содержимым
                run.push("🧬", f"poe2db: {labels[group]} — {name}", "poe2db.tw (датамайн)", link)
            continue
        kind, page = group.split("|", 1)
        what = next((v[1] for v in POE2DB_KINDS.values() if v[0] == kind), "предметы")
        names = ", ".join(n for n, _ in lst[:12]) + (f" и ещё {len(lst) - 12}" if len(lst) > 12 else "")
        link = lst[0][1] if len(lst) == 1 else base + page
        run.push("🧬", f"poe2db: новые {what} ({len(lst)}) — {names}", f"poe2db.tw · {page.replace('_', ' ')}", link)


# ---------------------------------------------------------------- vaal.tools (фан-сайт: раскрытое к 1.0)
VAAL_SUB_RE = re.compile(r'<a href="(?:https://vaal\.tools)?(/release-1-0/([a-z0-9-]+)/)"[^>]*>([^<]{2,120})</a>')


def vaal_parse(page):
    """{категория: [(ключ, название)]} + {slug: (ссылка, название)}.
    Блок: <span class="ri-block-title">Gems</span>; записи — <a|button data-entry="…" class="ri-name…">;
    подзаголовки <li class="ri-class">Changed / After 1.0 (Unconfirmed)</li> помечают записи после них;
    в Sources записей нет — там ссылки на видео/посты (ключ — ссылка)."""
    cats = {}
    for block in re.split(r'<span class="ri-block-title"[^>]*>', page)[1:]:
        cat = html.unescape(block[:block.index("<")]).strip()
        block = block.split("</section>", 1)[0]     # после последнего блока идут карточки классов — не берём
        items, mark = [], ""
        for li_attrs, li in re.findall(r"<ul\b|<li\b([^>]*)>(.*?)</li>", block, re.S):
            if not li_attrs and not li:             # новый ряд (Skills → Supports): пометка не переносится
                mark = ""
                continue
            if "ri-break" in li_attrs:
                mark = ""
                continue
            if 'class="ri-class' in li_attrs:  # подзаголовок: класс предмета, «Changed», «After 1.0 (Unconfirmed)»
                head = html.unescape(re.sub(r"<[^>]+>", "", li)).strip()
                mark = (" (изменено)" if head.startswith("Changed") else
                        " (после 1.0, не подтверждено)" if head.startswith("After 1.0") else "")
                continue
            m = re.search(r'data-entry="([^"]+)"[^>]*class="ri-name[^"]*"[^>]*>(.*?)</(?:a|button)>', li, re.S)
            if m:
                items.append((m.group(1), html.unescape(re.sub(r"<[^>]+>", "", m.group(2))).strip() + mark))
            elif cat == "Sources":
                a = re.search(r'<a href="(https?://[^"]+)"[^>]*>(.*?)</a>', li, re.S)
                if a:
                    items.append(("src:" + html.unescape(a.group(1)), html.unescape(re.sub(r"<[^>]+>", "", a.group(2))).strip()))
        if items:
            cats[cat] = items
    subs = {slug: (href, html.unescape(name).strip()) for href, slug, name in VAAL_SUB_RE.findall(page)}
    return cats, subs


def check_vaal(run):
    """vaal.tools/release-1-0/ — одна страница, не чаще раза в every_minutes. Первый прогон молча запоминает всё."""
    cfg = run.cfg.get("vaal_tools") or {}
    if not cfg.get("enabled", True):
        return
    st = run.state.setdefault("vaal", {})
    if run.ts - st.get("last", 0) < cfg.get("every_minutes", 30) * 60 - 90:
        return
    st["last"] = run.ts
    url = cfg.get("url", "https://vaal.tools/release-1-0/")
    r = run.get("vaal_tools", url)
    if r is None:
        return
    cats, subs = vaal_parse(r.text)
    total = sum(len(v) for v in cats.values())
    if total < 20:                       # сейчас ~340 записей; пусто/мало — разметка поменялась
        run.error("vaal_tools", f"разобрано {total} записей — разметка поменялась?")
        return
    first = "vaal_tools" not in run._known_sources
    base = re.match(r"https?://[^/]+", url).group(0)
    for slug, (href, name) in sorted(subs.items()):
        if run.mark_undated("vaal_tools", "page:" + slug):
            run.push("🧩", f"vaal.tools 1.0: новая страница — {name}", "vaal.tools (фан-сайт, не GGG)", base + href)
    for cat, items in cats.items():
        new = [name for key, name in items if run.mark_undated("vaal_tools", f"{cat}:{key}")]
        if new:
            names = ", ".join(new[:12]) + (f" и ещё {len(new) - 12}" if len(new) > 12 else "")
            run.push("🧩", f"vaal.tools 1.0: новое — {cat} ({len(new)}): {names}", "vaal.tools (фан-сайт, не GGG)", url)
    if first:
        print(f"[vaal] первый запуск: молча запомнено {total} записей в {len(cats)} категориях и {len(subs)} подстраниц")


# ---------------------------------------------------------------- Telegram
def tg_send(run, text, preview=True):
    if run.dry:
        print("\n----- в Telegram ушло бы -----\n" + text)
        return True
    token, chat = os.environ.get("TG_TOKEN"), os.environ.get("TG_CHAT_ID")
    if not (token and chat):
        print("[warn] TG_TOKEN / TG_CHAT_ID не заданы — сообщение не отправлено")
        return False
    for attempt in range(3):
        try:
            r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", timeout=20,
                              json={"chat_id": chat, "text": text, "parse_mode": "HTML",
                                    "disable_web_page_preview": not preview})
        except requests.RequestException as e:
            print(f"[warn] telegram: {type(e).__name__}")
            time.sleep(3)
            continue
        if r.status_code == 429:
            time.sleep(min(int(r.json().get("parameters", {}).get("retry_after", 5)), 30))
            continue
        if not r.ok:
            print(f"[warn] telegram: HTTP {r.status_code} {r.text[:200]}")
            return False
        time.sleep(1.1)
        return True
    return False


def chunks(lines, limit=3900):
    out, cur = [], ""
    for ln in lines:
        if len(cur) + len(ln) + 1 > limit and cur:
            out.append(cur)
            cur = ""
        cur += ln + "\n"
    if cur.strip():
        out.append(cur)
    return out


def digest_drop_reason(run, v):
    """Почему запись дайджеста не показывать (None — показать). Те же правила, что при сборе, —
    чтобы ушло и то, что накопилось в state до правки."""
    if v["sec"] == "ggg":
        if v["score"] < run.ts - ggg_max_age_h(run) * 3600:
            return "GGG старше 36 ч"
        return None
    if v["sec"] == "yt_ext":                 # и у записей с ok=1: правило «PoE 2 в названии» появилось позже
        title = v.get("title") or v["text"].split("подп.): ", 1)[-1]
        if not run.kw_poe2.search(title):
            return "YouTube не из списка: нет PoE 2 в названии"
        if run.poe1_only(title):
            return "YouTube не из списка: PoE 1"
        return None if en_ru_title(title) else "YouTube не из списка: не en/ru"
    if v["sec"] != "yt_list" or v.get("ok"):
        return None
    text = v["text"]
    title, cid = v.get("title"), v.get("cid")
    if title is None:
        title = text
        for ch in run.cfg["youtube"]["channels"]:
            if text.startswith(ch["name"] + ": "):
                title, cid = text[len(ch["name"]) + 2:], ch["id"]
                break
    if run.poe1_only(title):
        return "YouTube из списка: PoE 1"
    share = run.state.get("yt", {}).get("share", {}).get(cid, 0)
    return None if is_poe2_video(run, title, "", share) else "YouTube из списка: канал в основном не про PoE 2"


def build_digest(run):
    d = run.state["digest"]
    rome = rome_time(run.t)
    lines = [f"📌 <b>Дайджест PoE 2 — {rome:%d.%m}</b>"]
    n = 0
    dropped = {}
    for v in d.values():
        why = digest_drop_reason(run, v)
        if why:
            dropped.setdefault(why, []).append(v["text"])
    run.digest_dropped = dropped
    for why, texts in dropped.items():
        print(f"[digest] отсеяно ({why}): {len(texts)} — " + " | ".join(t[:70] for t in texts[:5]))
    for sec, title in SECTION_TITLES:
        items = sorted((v for v in d.values() if v["sec"] == sec and not digest_drop_reason(run, v)),
                       key=lambda v: -v["score"])
        if not items:
            continue
        lines += ["", f"<b>{esc(title)}</b>"]
        for v in items[: run.th["digest_max_per_section"]]:
            if sec == "yt_comp":           # «Компиляция <канал>: <ссылка>» — одной строкой
                lines.append(f"• {esc(v['text'][:150])}: {esc(v['url'])}")
            elif sec == "yt_live":         # «Стрим <канал>: <название> <ссылка>» — одной строкой
                lines.append(f"• {esc(v['text'][:150])} {esc(v['url'])}")
            else:
                lines.append(f"• <a href=\"{esc(v['url'])}\">{esc(v['text'][:150])}</a>")
            n += 1
        if len(items) > run.th["digest_max_per_section"]:
            lines.append(f"  …и ещё {len(items) - run.th['digest_max_per_section']}")
    if not n:
        lines.append("\nЗа день ничего заметного. Сторож работает.")
    errs = dict(run.state["errors"].get(rome.strftime("%Y-%m-%d"), {}))
    if not (os.environ.get("REDDIT_CLIENT_ID") and os.environ.get("REDDIT_CLIENT_SECRET")):
        errs.pop("reddit", None)          # без ключей Reddit режет анонимный доступ всегда — это не сбой
    if errs:
        lines += ["", "⚠️ <i>Сбои источников сегодня (число прогонов со сбоем): " + esc(", ".join(f"{k} ×{v}" for k, v in sorted(errs.items()))) + "</i>"]
    return chunks(lines)


def maybe_digest(run):
    rome = rome_time(run.t)
    day = rome.strftime("%Y-%m-%d")
    if run.state["runs"] == 0 and not run.args.digest_now:
        run.state["last_digest"] = day if rome.hour >= run.th["digest_hour_rome"] else run.state.get("last_digest")
        return
    due = rome.hour >= run.th["digest_hour_rome"] and run.state.get("last_digest") != day
    if not (due or run.args.digest_now):
        return
    ok = all(tg_send(run, part, preview=False) for part in build_digest(run))
    if ok and not run.dry:
        run.state["digest"] = {}
        run.state["last_digest"] = day


def flush_urgent(run):
    queue = run.state["outbox"] + run.urgent
    if len(run.urgent) > 8:   # что-то массовое — одним сообщением, а не очередью пушей
        queue = run.state["outbox"] + ["🔥 <b>Сразу несколько событий</b>\n\n" + "\n\n".join(run.urgent)]
    left = []
    for text in queue:
        if not tg_send(run, text, preview=True):
            left.append(text)
    run.state["outbox"] = [] if run.dry else left[-30:]


# ---------------------------------------------------------------- state
def prune(run):
    st = run.state
    for src, bucket in st["seen"].items():
        ttl = SEEN_TTL_DAYS.get(src, 60) * 86400
        for k in [k for k, v in bucket.items() if run.ts - v > ttl]:
            del bucket[k]
    for k in [k for k, v in st["digest"].items() if run.ts - v["ts"] > 3 * 86400]:
        del st["digest"][k]
    for k in [k for k, v in st["sent"].items() if run.ts - v > 14 * 86400]:
        del st["sent"][k]
    for day in sorted(st["errors"])[:-3]:
        del st["errors"][day]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="ничего не отправлять, печатать сообщения")
    ap.add_argument("--no-seed", action="store_true", help="не заполнять state молча: всё найденное считать новым")
    ap.add_argument("--digest-now", action="store_true", help="собрать дайджест сейчас, не дожидаясь 21:00")
    ap.add_argument("--test-message", nargs="?", const="Divine News Radar на связи ✅", default=None, metavar="ТЕКСТ",
                    help="отправить тестовое сообщение (можно свой текст) и выйти")
    ap.add_argument("--only", default=",".join(SOURCES), help="источники через запятую: " + ",".join(SOURCES))
    ap.add_argument("--state", default=None, help="путь к state.json (по умолчанию рядом со скриптом)")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    load_env()
    cfg = json.load(open(os.path.join(HERE, "config.json"), encoding="utf-8"))
    state_path = args.state or os.path.join(HERE, "state.json")
    state = {}
    if os.path.exists(state_path) and not (args.dry_run and args.no_seed and not args.state):
        with open(state_path, encoding="utf-8") as f:
            state = json.load(f)
    run = Run(cfg, state, args)

    if args.test_message:
        ok = tg_send(run, esc(args.test_message))
        print("отправлено" if ok else "НЕ отправлено")
        sys.exit(0 if ok else 1)

    only = {s.strip() for s in args.only.split(",")}
    for name, fn in (("ggg", check_ggg), ("reddit", check_reddit), ("youtube", check_youtube), ("twitch", check_twitch),
                     ("poe2db", check_poe2db), ("vaal", check_vaal)):
        if name not in only:
            continue
        t0 = time.time()
        try:
            fn(run)
        except Exception as e:  # один упавший источник не должен ронять остальные
            run.error(name, f"{type(e).__name__}: {str(e)[:200]}")
        print(f"[ok] {name}: {time.time() - t0:.1f} с")
    # Модуль «🔥 Клип» (clips.py) — после остальных источников, чтобы не дублировать их пуши про тот же клип.
    if only >= set(SOURCES) or "clips" in only:
        import clips
        t0 = time.time()
        try:
            clips.check_clips(run)
        except Exception as e:
            run.error("clips", f"{type(e).__name__}: {str(e)[:200]}")
        print(f"[ok] clips: {time.time() - t0:.1f} с")
    flush_urgent(run)
    maybe_digest(run)
    prune(run)
    for rule, texts in getattr(run, "dropped", {}).items():
        print(f"[filter] отсеяно ({rule}): {len(texts)} — " + " | ".join(t[:70] for t in texts[:5]))
    state["runs"] = state.get("runs", 0) + 1
    state["last_run"] = iso_z(run.t)
    q = state.get("yt", {}).get("quota", {})
    print(f"[ok] срочных: {len(run.urgent)}, в дайджесте: {len(state['digest'])}, квота YouTube сегодня: {q.get('used', 0)}")
    if not args.dry_run or args.state:
        tmp = state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, state_path)


if __name__ == "__main__":
    main()
