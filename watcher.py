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
import time
from concurrent.futures import ThreadPoolExecutor

import feedparser
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
UTC = dt.timezone.utc
SOURCES = ("ggg", "reddit", "youtube", "twitch")

# Сколько дней помнить уже виденное (по источнику дедупа).
SEEN_TTL_DAYS = {"ggg": 400, "reddit_post": 7, "reddit_urgent": 7, "yt_video": 60,
                 "yt_search_en": 10, "yt_search_ru": 10, "twitch_clip": 3, "twitch_star": 3}

SECTION_TITLES = [
    ("ggg", "🏛 GGG"),
    ("twitch_new", "🟣 Twitch — новые стримеры в PoE 2"),
    ("twitch_clips", "🎬 Twitch — клипы"),
    ("reddit", "👽 Reddit r/PathOfExile2"),
    ("yt_list", "▶️ YouTube — каналы из списка (по просмотрам)"),
    ("yt_ext", "📺 YouTube — каналы не из списка"),
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
        known_sources = set(st["seen"].keys())
        self.seeding = set() if args.no_seed else None   # None = решаем по наличию источника в state
        self._known_sources = known_sources
        self.kw_ggg = kw_regex(cfg["keywords"]["ggg_urgent"])
        self.kw_drama = kw_regex(cfg["keywords"]["drama"])
        self.kw_poe2 = kw_regex(cfg["keywords"]["poe2_title"])

    # --- дедуп
    def is_seeding(self, source):
        if self.seeding is not None:
            return False
        return source not in self._known_sources

    def mark(self, source, key):
        """True, если ключ новый и источник не в режиме первичного заполнения."""
        bucket = self.state["seen"].setdefault(source, {})
        new = key not in bucket
        bucket[key] = self.ts
        return new and not self.is_seeding(source)

    def was_seen(self, source, key):
        return key in self.state["seen"].get(source, {})

    # --- вывод
    def push(self, emoji, line, source, url):
        text = f"{emoji} {esc(line)}\n<i>{esc(source)}</i> · {esc(url)}"
        self.urgent.append(text)

    def digest(self, section, key, line, url, score=0):
        d = self.state["digest"]
        d[f"{section}|{key}"] = {"sec": section, "text": line, "url": url, "score": score, "ts": self.ts}

    def error(self, source, msg):
        print(f"[warn] {source}: {msg}")
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


def check_ggg(run):
    cfg = run.cfg["ggg"]
    items = []
    r = run.get("ggg", cfg["news_rss"])
    if r is not None:
        for e in feedparser.parse(r.content).entries:
            pub = None
            if e.get("published_parsed"):
                pub = dt.datetime(*e.published_parsed[:6], tzinfo=UTC)
            items.append((e.get("link", ""), e.get("title", "").strip(), "pathofexile.com/news", pub))
    for forum in cfg["forums"]:
        r = run.get("ggg", forum["url"])
        if r is None:
            continue
        for path, title in re.findall(r'href="(/forum/view-thread/\d+)"[^>]*>\s*([^<]{3,200}?)\s*<', r.text):
            items.append(("https://www.pathofexile.com" + path, html.unescape(title.strip()),
                          f"форум: {forum['name']}", None))
    for url, title, src, pub in items:
        if not url or not title:
            continue
        key = re.sub(r"^https?://(www\.)?", "", url).rstrip("/")
        if not run.mark("ggg", key):
            continue
        old = pub is not None and (run.t - pub).days > 3
        if run.kw_ggg.search(title) and not poe1_marker(title) and not old:
            run.push("🔥", f"GGG: {title}", src, url)
        else:
            tag = " (PoE 1)" if poe1_marker(title) else ""
            run.digest("ggg", key, f"{title}{tag}", url, score=int(pub.timestamp()) if pub else run.ts)


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
                posts.append((feed, d["name"], d["title"], "https://www.reddit.com" + d["permalink"],
                              dt.datetime.fromtimestamp(d["created_utc"], UTC), d["score"]))
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
            posts.append((feed, e.get("id", e.get("link")), e.get("title", ""), e.get("link", ""), created, None))
    return posts


def check_reddit(run):
    posts = reddit_posts(run)
    if not posts:
        return
    th = run.th
    for rank, (feed, pid, title, url, created, score) in enumerate(posts):
        age_h = (run.t - created).total_seconds() / 3600 if created else None
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


def fetch_channel_feed(run, ch):
    url = f"https://www.youtube.com/feeds/videos.xml?channel_id={ch['id']}"
    try:
        r = run.http.get(url, timeout=20)
    except requests.RequestException as e:
        return ch, None, type(e).__name__
    if r.status_code != 200:
        return ch, None, f"HTTP {r.status_code}"
    return ch, feedparser.parse(r.content).entries, None


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
    chans = run.cfg["youtube"]["channels"]
    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(lambda c: fetch_channel_feed(run, c), chans))
    fails = [f"{c['name']} ({err})" for c, entries, err in results if entries is None]
    if fails:
        run.error("youtube_rss", f"{len(fails)} каналов не ответили: {', '.join(fails[:5])}")
    max_age_h = th["youtube_speed_max_age_hours"]
    for ch, entries, _ in results:
        if not entries:
            continue
        cid = ch["id"]
        share = sum(1 for e in entries if run.kw_poe2.search(e.get("title", "") + " " + e.get("summary", ""))) / len(entries)
        # Норма-заготовка, пока не накопились реальные замеры за 6 ч: доля от медианы просмотров роликов старше 3 дней.
        b = boot.get(cid)
        if not b or run.ts - b.get("ts", 0) > 7 * 86400:
            old = []
            for e in entries:
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
                if ch.get("official") and run.kw_ggg.search(title) and age_h < 72:
                    run.push("🔥", f"GGG на YouTube: {title}", "YouTube Path of Exile", url)
                elif age_h < 48 and re.search(r"path of exile|\bpoe|пое|wraeclast|exile", title + " " + e.get("summary", ""), re.I):
                    run.digest("yt_list", vid, f"{ch['name']}: {title}", url, score=views)
            if age_h <= max_age_h and vid not in videos:
                videos[vid] = {"c": cid, "n": ch["name"], "t": title, "p": int(pub.timestamp()), "v": views,
                               "poe2": is_poe2_video(run, title, e.get("summary"), share), "a": 0, "s": 0}
            elif vid in videos:
                videos[vid]["v"] = max(videos[vid]["v"], views)
    # Свежие просмотры — videos.list (1 единица квоты на 50 роликов). Без ключа остаются цифры из RSS.
    ids = [v for v, d in videos.items() if run.ts - d["p"] <= max_age_h * 3600 + 3600]
    for i in range(0, len(ids), 50):
        data = yt_api(run, "videos", {"part": "statistics", "id": ",".join(ids[i:i + 50]), "maxResults": 50}, 1)
        for it in (data or {}).get("items", []):
            videos[it["id"]]["v"] = int(it.get("statistics", {}).get("viewCount", 0))
            dk = f"yt_list|{it['id']}"
            if dk in run.state["digest"]:
                run.state["digest"][dk]["score"] = videos[it["id"]]["v"]
    nh = th["youtube_norm_hours"]
    for vid, d in list(videos.items()):
        age_h = (run.ts - d["p"]) / 3600
        if age_h > max_age_h + 2:
            del videos[vid]
            continue
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
            d["a"] = 1
            k = d["v"] / max(expected, 1)
            run.push("🚀", f"YouTube в {k:.0f}× быстрее нормы: {d['n']} — «{d['t']}»: {fmt_num(d['v'])} просмотров "
                           f"за {age_h:.1f} ч (норма канала ~{fmt_num(norm)} за {nh} ч)",
                     "YouTube", f"https://www.youtube.com/watch?v={vid}")
    youtube_search(run)


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
        if not re.search(r"path of exile|\bpoe\b|poe2|пое", sn.get("title", "") + " " + sn.get("description", ""), re.I):
            continue
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
        if first_time and subs >= th["youtube_big_channel_subscribers"]:
            run.push("📺", f"Крупный канал впервые про PoE 2: {name} ({fmt_num(subs)} подписчиков) — «{title}»",
                     "YouTube", url)
        elif subs >= th["youtube_digest_external_min_subscribers"]:
            run.digest("yt_ext", vid, f"{name} ({fmt_num(subs)} подп.): {title}", url, score=subs)


# ---------------------------------------------------------------- Twitch
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
        if viewers >= th["twitch_new_streamer_viewers"] or (big and viewers >= min_v):
            run.push("⚡", f"Впервые стримит PoE 2: {s['user_name']} — {fmt_num(viewers)} зрителей{fol_txt}",
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


def build_digest(run):
    d = run.state["digest"]
    rome = rome_time(run.t)
    lines = [f"📌 <b>Дайджест PoE 2 — {rome:%d.%m}</b>"]
    n = 0
    for sec, title in SECTION_TITLES:
        items = sorted((v for v in d.values() if v["sec"] == sec), key=lambda v: -v["score"])
        if not items:
            continue
        lines += ["", f"<b>{esc(title)}</b>"]
        for v in items[: run.th["digest_max_per_section"]]:
            lines.append(f"• <a href=\"{esc(v['url'])}\">{esc(v['text'][:150])}</a>")
            n += 1
        if len(items) > run.th["digest_max_per_section"]:
            lines.append(f"  …и ещё {len(items) - run.th['digest_max_per_section']}")
    if not n:
        lines.append("\nЗа день ничего заметного. Сторож работает.")
    errs = run.state["errors"].get(rome.strftime("%Y-%m-%d"), {})
    if errs:
        lines += ["", "⚠️ <i>Сбои источников сегодня: " + esc(", ".join(f"{k} ×{v}" for k, v in sorted(errs.items()))) + "</i>"]
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
    for day in sorted(st["errors"])[:-3]:
        del st["errors"][day]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="ничего не отправлять, печатать сообщения")
    ap.add_argument("--no-seed", action="store_true", help="не заполнять state молча: всё найденное считать новым")
    ap.add_argument("--digest-now", action="store_true", help="собрать дайджест сейчас, не дожидаясь 21:00")
    ap.add_argument("--test-message", action="store_true", help="отправить тестовое сообщение и выйти")
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
        ok = tg_send(run, "Divine News Radar подключён ✅")
        print("отправлено" if ok else "НЕ отправлено")
        sys.exit(0 if ok else 1)

    only = {s.strip() for s in args.only.split(",")}
    for name, fn in (("ggg", check_ggg), ("reddit", check_reddit), ("youtube", check_youtube), ("twitch", check_twitch)):
        if name not in only:
            continue
        t0 = time.time()
        try:
            fn(run)
        except Exception as e:  # один упавший источник не должен ронять остальные
            run.error(name, f"{type(e).__name__}: {str(e)[:200]}")
        print(f"[ok] {name}: {time.time() - t0:.1f} с")
    flush_urgent(run)
    maybe_digest(run)
    prune(run)
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
