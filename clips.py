#!/usr/bin/env python3
"""Модуль «🔥 Клип» для Divine News Radar: клипы PoE 2, которые прямо сейчас разгоняются (Twitch + YouTube).

Вызывается из watcher.py каждым прогоном: check_clips(run). Пороги — config.json → "clips".
Состояние — state.json → "clips" (снапшоты просмотров, watchlist, недавние пуши).

Отдельно (ничего не шлёт, state не трогает):
  python clips.py --sim24                 какие пуши ушли бы за последние сутки (оценка по живым данным)
  python clips.py --sim24 --set twitch_views=2000 --set youtube_rate_multiplier=4
"""
import datetime as dt
import html
import os
import re
import statistics
import sys
from concurrent.futures import ThreadPoolExecutor

import requests

UTC = dt.timezone.utc
HELIX = "https://api.twitch.tv/helix/"
EVENT_WINDOW = 3600          # «≥N клипов про одно событие за час»
WORD_RE = re.compile(r"[a-zа-яё0-9']{4,}", re.IGNORECASE)
STOP = set("""
that this with what when where from into your yours they them their there here have just like then than
were been being does done dont don't cant can't wont won't will would could should about after before over
under again still only even back down very much more most some what's it's he's she's i'm you're
path exile poe2 poe1 clip clips stream live moment moments best funny highlights highlight episode part
video shorts short today game games gameplay build builds league patch update new first time guys chat
когда этот меня очень просто клип клипы стрим нарезка лучшее моменты момент пое2 сегодня
""".split())


# ---------------------------------------------------------------- формат
def esc(s):
    return html.escape(str(s), quote=False)


def num(n):
    return f"{int(round(n)):,}".replace(",", " ")


def hours(h):
    if h < 1:
        return f"{max(1, round(h * 60))} мин"
    s = f"{h:.1f}".replace(".", ",")
    return (s[:-2] if s.endswith(",0") else s) + " ч"


def dur(sec):
    s = int(round(sec or 0))
    h, rest = divmod(s, 3600)
    m, s = divmod(rest, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def iso_dur(s):
    m = re.fullmatch(r"P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", s or "")
    if not m:
        return 0
    d, h, mi, se = (int(x or 0) for x in m.groups())
    return d * 86400 + h * 3600 + mi * 60 + se


def iso_z(t):
    return t.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(UTC)


def clip_line(emoji, kind, platform, who, views, age_h, speed, title, seconds, url):
    return (f"{emoji} {kind} · {esc(platform)} · {esc(who)} · {num(views)} просм. за {hours(age_h)} "
            f"(+{num(speed)}/ч) · «{esc(title.strip())}» · {dur(seconds)} · {esc(url)}")


def title_words(title):
    return {w.lower() for w in WORD_RE.findall(title or "")} - STOP


# ---------------------------------------------------------------- правила (общие для живого прогона и симуляции)
def tw_trigger(c, views, age_h, speed, in_watch):
    if age_h >= c["twitch_max_age_hours"]:
        return None
    if in_watch and age_h <= c["twitch_watch_first_hours"] and views >= c["twitch_watch_first_hour_views"]:
        return "топ-стример, первый час"
    if views >= c["twitch_views"]:
        return "просмотры"
    if age_h * 60 >= c["twitch_rate_min_age_minutes"] and speed >= c["twitch_views_per_hour"]:
        return "скорость"
    if not in_watch and views >= c["twitch_small_streamer_views"]:
        return "стример вне топа"
    return None


def yt_trigger(c, views, age_h, rate, base):
    if age_h >= c["youtube_max_age_hours"] or age_h * 60 < c["youtube_min_age_minutes"]:
        return None
    if age_h <= c["youtube_fast_hours"] and views >= c["youtube_fast_views"]:
        return "просмотры"
    if base and views >= c["youtube_min_views"] and rate >= c["youtube_rate_multiplier"] * base:
        return f"{rate / base:.0f}× канала"
    return None


def speeds(rec, views, ts):
    """(средняя с рождения, текущая) — просмотров в час. Текущая — по снапшоту 15 мин…3 ч назад."""
    age_h = max((ts - rec["c"]) / 3600, 1 / 6)
    avg = views / age_h
    for t0, v0 in reversed(rec.get("s", [])):
        if ts - t0 >= 15 * 60:
            if ts - t0 <= 3 * 3600:
                return avg, max(0.0, (views - v0) / ((ts - t0) / 3600))
            break
    return avg, avg


def snap(rec, views, ts):
    s = rec.setdefault("s", [])
    if not s or ts - s[-1][0] >= 5 * 60:
        s.append([ts, int(views)])
        del s[:-8]


# ---------------------------------------------------------------- группировка в «событие»
def pick_events(pool, min_n):
    """pool: список dict с ключами new, b (стример/канал), who, words. Жадно: ключ (стример или слово),
    под которым ≥ min_n клипов и есть хотя бы один новый → событие. Возвращает [(label, [items])]."""
    events, left = [], list(pool)
    while True:
        groups = {}
        for it in left:
            groups.setdefault(("b", it["b"]), []).append(it)
            for w in it["words"]:
                groups.setdefault(("w", w), []).append(it)
        best = None
        for k, items in groups.items():
            if len(items) >= min_n and any(i["new"] for i in items):
                score = (len(items), k[0] == "b")
                if best is None or score > best[0]:
                    best = (score, k, items)
        if not best:
            return events
        _, k, items = best
        label = items[0]["who"] if k[0] == "b" else f"«{k[1]}»"
        events.append((label, items))
        ids = {id(i) for i in items}
        left = [i for i in left if id(i) not in ids]


def event_text(label, items):
    lines = [f"🔥 Событие: {esc(label)} — {len(items)} клипа за час"]
    for it in sorted(items, key=lambda i: -i["views"]):
        lines.append(f"• {esc(it['platform'])} · {esc(it['who'])} · {num(it['views'])} просм. · "
                     f"«{esc(it['title'][:90])}» · {esc(it['url'])}")
    return "\n".join(lines)


# ---------------------------------------------------------------- Twitch
def twitch_auth(run):
    cid, secret = os.environ.get("TWITCH_CLIENT_ID"), os.environ.get("TWITCH_CLIENT_SECRET")
    if not (cid and secret):
        return None
    try:
        tok = run.http.post("https://id.twitch.tv/oauth2/token", timeout=20,
                            data={"client_id": cid, "client_secret": secret, "grant_type": "client_credentials"})
    except requests.RequestException as e:
        run.error("clips_twitch", f"token: {type(e).__name__}")
        return None
    if not tok.ok:
        run.error("clips_twitch", f"token: HTTP {tok.status_code}")
        return None
    return {"Client-Id": cid, "Authorization": "Bearer " + tok.json()["access_token"]}


def twitch_game_id(run, cs, h):
    gid = cs.get("gid") or run.state.get("twitch", {}).get("game_id")
    if not gid:
        r = run.get("clips_twitch", HELIX + "games", headers=h, params={"name": run.cfg["twitch"]["game_name"]})
        data = (r.json().get("data") if r is not None else None) or []
        gid = data[0]["id"] if data else None
    if gid:
        cs["gid"] = gid
    return gid


def twitch_clips(run, h, gid, since, pages):
    out, params = [], {"game_id": gid, "first": 100, "started_at": iso_z(since), "ended_at": iso_z(run.t)}
    for _ in range(pages):
        r = run.get("clips_twitch", HELIX + "clips", headers=h, params=params)
        if r is None:
            break
        js = r.json()
        out += js.get("data", [])
        cur = (js.get("pagination") or {}).get("cursor")
        if not cur:
            break
        params["after"] = cur
    return out


def update_watchlist(run, cs, h, gid, streams):
    """Топ-N стримеров PoE 2 по зрителям за 7 дней. Пики копятся каждым прогоном, список пересчитывается раз в сутки.
    Пока наблюдений мало (< 3 дней), добираем стримерами с самыми смотримыми клипами PoE 2 за неделю."""
    c = run.cfg["clips"]
    day = run.t.strftime("%Y-%m-%d")
    peaks = cs.setdefault("tw_peak", {})       # user_id -> {"n": имя, "d": {день: макс. зрителей}}
    for s in streams:
        p = peaks.setdefault(s["user_id"], {"n": s["user_name"], "d": {}})
        p["n"] = s["user_name"]
        p["d"][day] = max(p["d"].get(day, 0), int(s["viewer_count"]))
    cutoff = (run.t - dt.timedelta(days=7)).strftime("%Y-%m-%d")
    for uid in list(peaks):
        peaks[uid]["d"] = {d: v for d, v in peaks[uid]["d"].items() if d > cutoff}
        if not peaks[uid]["d"]:
            del peaks[uid]
    w = cs.setdefault("tw_watch", {})
    if w.get("day") == day and w.get("ids"):
        return set(w["ids"])
    n = c["twitch_watchlist_size"]
    ranked = sorted(peaks, key=lambda u: -max(peaks[u]["d"].values()))[:n]
    days_seen = len({d for p in peaks.values() for d in p["d"]})
    if len(ranked) < n or days_seen < 3:
        totals, names = {}, {}
        for cl in twitch_clips(run, h, gid, run.t - dt.timedelta(days=7), 3):
            totals[cl["broadcaster_id"]] = totals.get(cl["broadcaster_id"], 0) + int(cl["view_count"])
            names[cl["broadcaster_id"]] = cl["broadcaster_name"]
        for uid in sorted(totals, key=lambda u: -totals[u]):
            if len(ranked) >= n:
                break
            if uid not in ranked:
                ranked.append(uid)
                peaks.setdefault(uid, {"n": names[uid], "d": {}})
    w.update(day=day, ids=ranked, names=[peaks.get(u, {}).get("n", u) for u in ranked])
    return set(ranked)


def clips_twitch(run, cs, seeding, cands, rockets):
    c = run.cfg["clips"]
    h = twitch_auth(run)
    if not h:
        return
    gid = twitch_game_id(run, cs, h)
    if not gid:
        run.error("clips_twitch", "игра не найдена")
        return
    r = run.get("clips_twitch", HELIX + "streams", headers=h, params={"game_id": gid, "first": 100})
    streams = r.json().get("data", []) if r is not None else []
    watch = update_watchlist(run, cs, h, gid, streams)
    tw = cs.setdefault("tw", {})
    pushed_before = run.state["seen"].setdefault("twitch_clip", {})   # общий с основным модулем Twitch: один пуш на клип
    for cl in twitch_clips(run, h, gid, run.t - dt.timedelta(hours=24), c["twitch_pages"]):
        views = int(cl.get("view_count", 0))
        created = parse_iso(cl["created_at"])
        rec = tw.get(cl["id"])
        if rec is None:
            rec = tw[cl["id"]] = {"c": int(created.timestamp()), "p": 0}
        age_h = (run.ts - rec["c"]) / 3600
        avg, speed = speeds(rec, views, run.ts)
        snap(rec, views, run.ts)
        line_args = ("Twitch", cl["broadcaster_name"], views, age_h, speed, cl.get("title", ""), cl.get("duration", 0), cl["url"])
        if rec["p"]:
            if rec["p"] > 0 and age_h < 24 and rec.get("pr", 0) > 0 and speed >= c["rocket_speed_multiplier"] * rec["pr"]:
                run.drop("🚀 Разгоняется: клип уже присылали (одно видео — один пуш)",
                         f"{cl['broadcaster_name']}: {cl.get('title', '')}")
                rec["pr"] = speed
            continue
        why = tw_trigger(c, views, age_h, speed, cl["broadcaster_id"] in watch)
        if not why:
            continue
        if seeding or cl["id"] in pushed_before or run.was_sent("tw:" + cl["id"]):
            rec["p"] = -1          # уже было до запуска модуля / уже пушил основной модуль — молчим
            continue
        cands.append({"rec": rec, "seen": ("twitch_clip", cl["id"]), "key": "tw:" + cl["id"], "new": True,
                      "b": "tw:" + cl["broadcaster_id"],
                      "who": cl["broadcaster_name"], "words": sorted(title_words(cl.get("title", ""))),
                      "platform": "Twitch", "title": cl.get("title", ""), "views": views, "url": cl["url"],
                      "speed": speed, "line": clip_line("🔥", "Клип", *line_args), "why": why})
    for k in [k for k, v in tw.items() if run.ts - v["c"] > 26 * 3600]:
        del tw[k]


# ---------------------------------------------------------------- YouTube
def pacific_day(t):
    return (t - dt.timedelta(hours=8)).strftime("%Y-%m-%d")


def yt_videos(run, ids):
    """videos.list (statistics + contentDetails + liveStreamingDetails): 1 единица квоты на 50 роликов при любом
    наборе part; общий дневной лимит бота. Возвращает id -> (просмотры, секунды, стрим/премьера ли)."""
    key = os.environ.get("YT_API_KEY")
    out = {}
    if not key:
        return out
    q = run.state.setdefault("yt", {}).setdefault("quota", {})
    day = pacific_day(run.t)
    if q.get("day") != day:
        q.clear()
        q.update(day=day, used=0)
    for i in range(0, len(ids), 50):
        if q["used"] + 1 > run.th["youtube_daily_quota_cap"]:
            run.error("clips_youtube", "дневной лимит квоты исчерпан")
            break
        q["used"] += 1
        r = run.get("clips_youtube", "https://www.googleapis.com/youtube/v3/videos",
                    params={"part": "statistics,contentDetails,liveStreamingDetails", "id": ",".join(ids[i:i + 50]),
                            "key": key})
        for it in (r.json().get("items", []) if r is not None else []):
            out[it["id"]] = (int(it.get("statistics", {}).get("viewCount", 0)),
                             iso_dur(it.get("contentDetails", {}).get("duration")),
                             "liveStreamingDetails" in it)
    return out


def is_short(run, vid, seconds):
    """Шортс ли ролик: /shorts/<id> отвечает 200 только шортсам (без ключа и квоты)."""
    try:
        r = run.http.head(f"https://www.youtube.com/shorts/{vid}", allow_redirects=False, timeout=15)
        return r.status_code == 200
    except requests.RequestException:
        return 0 < seconds <= 60


def yt_feeds(run, channels):
    """Ленты каналов через run.yt_feed (watcher.py): повторы при 503, кэш на прогон — 17 из 19 каналов модуля
    уже скачал основной модуль YouTube, их сбой он же и посчитал (youtube_rss); здесь — только новые."""
    cached = set(run._feeds)

    def one(ch):
        entries, err = run.yt_feed(ch)
        return ch, entries, err
    with ThreadPoolExecutor(max_workers=4) as ex:
        res = list(ex.map(one, channels))
    fails = [f"{ch['name']} ({err})" for ch, e, err in res if e is None and ch["id"] not in cached]
    if fails:
        run.error("clips_youtube", f"{len(fails)} каналов не ответили и после повторов: {', '.join(fails[:5])}")
    return res


def yt_baseline(c, cs, cid, entries, now_ts, live=()):
    """Средняя скорость канала (просм./ч за первые сутки) по последним 10 роликам.
    Свои замеры (ролик увидели в возрасте ~сутки) — когда их ≥ 3; до того — из RSS:
    просмотры роликов старше суток × доля первых суток / 24. Записи стримов и премьеры (live) не в счёт."""
    own = cs.setdefault("yt_base", {}).get(cid, [])
    if len(own) >= 3:
        return statistics.mean(own[-10:])
    old = []
    for e in entries:
        if e.get("yt_videoid") in live:
            continue
        if e.get("published_parsed") and e.get("media_statistics"):
            age_d = (now_ts - dt.datetime(*e.published_parsed[:6], tzinfo=UTC).timestamp()) / 86400
            if 1 <= age_d <= 60:
                old.append(int(e["media_statistics"].get("views", 0)))
    old = old[:10]
    return statistics.mean(old) * c["youtube_bootstrap_first_day_share"] / 24 if len(old) >= 3 else None


POE_RE = re.compile(r"path of exile|\bpoe\s?2?\b|пое|wraeclast", re.IGNORECASE)
POE1_RE = re.compile(r"\b3\.\d{2}\b|\bpoe\s*1\b|path of exile(?!\s*(?:2|ii)\b)", re.IGNORECASE)


def youtube_wanted(run, ch, title, summary=""):
    """Ролик в модуль: PoE 1 (явно «Path of Exile» без 2, «3.27», «PoE 1») — нет; у общих каналов — только про PoE."""
    if POE1_RE.search(title) and not run.kw_poe2.search(title):
        return False
    return not ch.get("poe_only") or bool(POE_RE.search(title))   # описание не в счёт: там ссылки и теги


def clips_youtube(run, cs, seeding, cands, rockets):
    c = run.cfg["clips"]
    if run.ts - cs.get("yt_last", 0) < c["youtube_every_minutes"] * 60 - 90:
        return
    cs["yt_last"] = run.ts
    yv = cs.setdefault("yt", {})
    live = run.state.setdefault("yt", {}).setdefault("live", {})   # стримы/премьеры, найденные watcher.py и здесь
    fresh = {}
    for ch, entries, _ in yt_feeds(run, c["youtube_channels"]):
        if not entries:
            continue
        base = yt_baseline(c, cs, ch["id"], entries, run.ts, live)
        promo = run.yt_promo(entries)
        for e in entries:
            vid = e.get("yt_videoid")
            if not vid or not e.get("published_parsed"):
                continue
            pub = dt.datetime(*e.published_parsed[:6], tzinfo=UTC)
            age_h = (run.t - pub).total_seconds() / 3600
            if age_h > 30:
                continue
            if not youtube_wanted(run, ch, e.get("title", ""), e.get("summary", "")):
                continue
            rec = yv.setdefault(vid, {"c": int(pub.timestamp()), "p": 0})
            rec.update(ch=ch["id"], n=ch["name"], t=e.get("title", ""), base=base,
                       k=run.yt_kind(vid, e.get("title", ""), e.get("summary", ""), promo))
            fresh[vid] = rec
    stats = yt_videos(run, list(fresh))
    old_speed = run.state.setdefault("yt", {}).setdefault("videos", {})   # ролики основного модуля (🚀 в N× быстрее нормы)
    for vid, rec in fresh.items():
        if vid not in stats:
            continue
        views, seconds, is_live = stats[vid]
        rec["d"] = seconds
        if is_live or vid in live or (old_speed.get(vid) or {}).get("live"):
            # Идущий стрим / запись стрима / премьера — не клип: ни 🔥, ни замер скорости канала.
            # Строку «Стрим …» в дайджест ставит основной модуль (для каналов из его списка). Клипы ИЗ стрима —
            # отдельные ролики/клипы со своими id, их правила не меняются.
            live.setdefault(vid, run.ts)
            if not rec.get("lv"):
                rec["lv"] = 1
                run.drop("🔥: запись стрима / премьера — не клип", f"{rec['n']}: {rec['t']}")
            continue
        age_h = (run.ts - rec["c"]) / 3600
        avg, speed = speeds(rec, views, run.ts)
        snap(rec, views, run.ts)
        if 22 <= age_h <= 30 and not rec.get("fd"):
            rec["fd"] = 1
            b = cs["yt_base"].setdefault(rec["ch"], [])
            b.append(views / age_h)
            del b[:-10]
        url = f"https://www.youtube.com/watch?v={vid}"
        if rec["p"]:
            if rec["p"] > 0 and rec.get("pr", 0) > 0 and speed >= c["rocket_speed_multiplier"] * rec["pr"]:
                run.drop("🚀 Разгоняется: ролик уже присылали (одно видео — один пуш)", f"{rec['n']}: {rec['t']}")
                rec["pr"] = speed
            continue
        why = yt_trigger(c, views, age_h, avg, rec.get("base"))
        if not why:
            continue
        if seeding or (old_speed.get(vid) or {}).get("a"):
            rec["p"] = -1
            continue
        if not run.yt_gate(vid, rec["ch"], rec["n"], rec["t"], rec.get("k"), url):
            if run.was_sent("yt:" + vid):  # уже присылали / компиляция → дайджест / реакция → 🎙 источник
                rec["p"] = -1
                if vid in old_speed:
                    old_speed[vid]["a"] = 1
            continue                      # не помечен — API не ответил, следующий прогон проверит снова
        rec["sh"] = is_short(run, vid, seconds)
        plat = "YouTube Shorts" if rec["sh"] else "YouTube"
        cands.append({"rec": rec, "seen": None, "key": "yt:" + vid, "new": True, "b": "yt:" + rec["ch"], "who": rec["n"],
                      "words": sorted(title_words(rec["t"])), "platform": plat, "title": rec["t"], "views": views,
                      "url": url, "speed": avg, "why": why,
                      "line": clip_line("🔥", "Клип", plat, rec["n"], views, age_h, avg, rec["t"], seconds, url)})
        if vid in old_speed:
            old_speed[vid]["a"] = 1      # основной модуль не пришлёт второй пуш про тот же ролик
    for k in [k for k, v in yv.items() if run.ts - v["c"] > 32 * 3600]:
        del yv[k]


# ---------------------------------------------------------------- вход
def check_clips(run):
    c = run.cfg.get("clips")
    if not c:
        return
    seeding = "clips" not in run.state and not run.args.no_seed
    cs = run.state.setdefault("clips", {})
    cands, rockets = [], []
    for name, fn in (("clips_twitch", clips_twitch), ("clips_youtube", clips_youtube)):
        try:
            fn(run, cs, seeding, cands, rockets)
        except Exception as e:  # упавшая половина не роняет другую
            run.error(name, f"{type(e).__name__}: {str(e)[:200]}")
    # Недавние пуши (за час) участвуют в группировке, но второй раз не шлются.
    recent = [r for r in cs.get("recent", []) if run.ts - r["ts"] <= EVENT_WINDOW]
    ev_keys = [e for e in cs.get("events", []) if run.ts - e["ts"] <= EVENT_WINDOW]
    pool = [dict(r, new=False) for r in recent]
    attached = []
    for cd in cands:                                   # кандидат про уже объявленное событие — тихо присоединяем
        ws = set(cd["words"])
        if any(cd["b"] in e["b"] or ws & set(e["w"]) for e in ev_keys):
            attached.append(cd)
        else:
            pool.append(cd)
    out = []
    events = pick_events(pool, c["event_min_clips"])
    in_event = {id(i) for _, items in events for i in items}
    for label, items in events:
        out.append(event_text(label, items))
        ev_keys.append({"ts": run.ts, "b": sorted({i["b"] for i in items}),
                        "w": sorted(set.intersection(*(set(i["words"]) for i in items))) if label.startswith("«") else []})
    for cd in cands:
        if id(cd) not in in_event and cd not in attached:
            out.append(cd["line"])
    for cd in cands:
        cd["rec"]["p"], cd["rec"]["pr"] = run.ts, max(cd["speed"], 1)
        run.mark_sent(cd["key"])
        if cd["seen"]:
            run.mark(*cd["seen"])
        recent.append({"ts": run.ts, "b": cd["b"], "who": cd["who"], "words": cd["words"], "platform": cd["platform"],
                       "title": cd["title"][:120], "views": cd["views"], "url": cd["url"]})
    cs["recent"], cs["events"] = recent[-60:], ev_keys[-30:]
    run.urgent.extend(out + rockets)
    print(f"[clips] кандидатов {len(cands)}, событий {len(events)}, 🚀 {len(rockets)}"
          + (" (первый запуск — молча запомнил)" if seeding else ""))


# ---------------------------------------------------------------- симуляция последних суток
def simulate(run, curve=0.5):
    """Оценка: что бот прислал бы за последние 24 ч, если бы проверял каждые 10 (Twitch) / 15 (YouTube) мин.
    Прошлых снапшотов нет, поэтому траектория просмотров восстанавливается моделью v(t) = V·(t/A)^curve
    (быстро вначале, медленнее потом) — это оценка, а не запись."""
    c = run.cfg["clips"]
    cs = {}
    hits = []
    h = twitch_auth(run)
    if h:
        gid = twitch_game_id(run, cs, h)
        r = run.get("clips_twitch", HELIX + "streams", headers=h, params={"game_id": gid, "first": 100})
        watch = update_watchlist(run, cs, h, gid, r.json().get("data", []) if r is not None else [])
        clips = twitch_clips(run, h, gid, run.t - dt.timedelta(hours=24), c["twitch_pages"])
        print(f"[sim] Twitch: клипов за 24 ч {len(clips)}, watchlist {len(watch)}: "
              + ", ".join(cs["tw_watch"]["names"][:12]) + " …")
        for cl in clips:
            V, A = int(cl["view_count"]), max((run.t - parse_iso(cl["created_at"])).total_seconds() / 3600, 0.05)
            prev_v, t = 0.0, 1 / 6
            while t <= min(A, c["twitch_max_age_hours"]):
                v = V * (t / A) ** curve
                speed = (v - prev_v) * 6 if t > 1 / 6 else v / t
                why = tw_trigger(c, v, t, speed, cl["broadcaster_id"] in watch)
                if why:
                    hits.append({"ts": run.ts - (A - t) * 3600, "platform": "Twitch", "who": cl["broadcaster_name"],
                                 "b": "tw:" + cl["broadcaster_id"], "views": v, "age": t, "speed": speed,
                                 "now": V, "title": cl.get("title", ""), "url": cl["url"], "why": why,
                                 "words": sorted(title_words(cl.get("title", ""))), "new": True, "dur": cl.get("duration", 0)})
                    break
                prev_v, t = v, t + 1 / 6
    # YouTube
    vids, meta = [], {}
    for ch, entries, _ in yt_feeds(run, c["youtube_channels"]):
        if not entries:
            continue
        base = yt_baseline(c, cs, ch["id"], entries, run.ts)
        for e in entries:
            if not e.get("published_parsed"):
                continue
            pub = dt.datetime(*e.published_parsed[:6], tzinfo=UTC)
            A = (run.t - pub).total_seconds() / 3600
            if A <= 24 and youtube_wanted(run, ch, e.get("title", ""), e.get("summary", "")):
                vids.append(e["yt_videoid"])
                meta[e["yt_videoid"]] = (ch, e.get("title", ""), A, base)
    stats = yt_videos(run, vids)
    print(f"[sim] YouTube: роликов за 24 ч {len(vids)} на {len(c['youtube_channels'])} каналах")
    for vid in vids:
        if vid not in stats:
            continue
        (V, secs, is_live), (ch, title, A, base) = stats[vid], meta[vid]
        if is_live:
            continue                      # стрим / премьера — не клип (как в clips_youtube)
        t = 0.25
        while t <= A:
            v = V * (t / A) ** curve
            why = yt_trigger(c, v, t, v / t, base)
            if why:
                hits.append({"ts": run.ts - (A - t) * 3600, "platform": "YouTube", "who": ch["name"], "b": "yt:" + ch["id"],
                             "views": v, "age": t, "speed": v / t, "now": V, "title": title, "why": why, "dur": secs,
                             "url": f"https://www.youtube.com/watch?v={vid}", "words": sorted(title_words(title)), "new": True,
                             "base": base})
                break
            t += 0.25
    # Пуши по времени, с группировкой в события внутри часа.
    hits.sort(key=lambda x: x["ts"])
    msgs, i = [], 0
    while i < len(hits):
        win = [x for x in hits[i:] if x["ts"] - hits[i]["ts"] <= EVENT_WINDOW]
        evs = pick_events(win, c["event_min_clips"])
        if evs:
            label, items = evs[0]
            msgs.append(("event", label, items))
            ids = {id(x) for x in items}
            hits = hits[:i] + [x for x in hits[i:] if id(x) not in ids]
            continue
        msgs.append(("clip", None, [hits[i]]))
        i += 1
    return msgs


def main():
    import argparse
    import json
    import types
    here = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, here)
    import watcher
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sim24", action="store_true", help="оценить пуши за последние сутки")
    ap.add_argument("--set", action="append", default=[], help="подменить порог clips: ключ=значение")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    watcher.load_env()
    cfg = json.load(open(os.path.join(here, "config.json"), encoding="utf-8"))
    for kv in args.set:
        k, v = kv.split("=", 1)
        cfg["clips"][k] = float(v) if "." in v else int(v)
    run = watcher.Run(cfg, {}, types.SimpleNamespace(dry_run=True, no_seed=True))
    msgs = simulate(run)
    pushes = [m for m in msgs]
    print(f"\n[sim] пушей за сутки: {len(pushes)} (клипов {sum(m[0] == 'clip' for m in msgs)}, "
          f"событий {sum(m[0] == 'event' for m in msgs)})\n")
    for kind, label, items in msgs:
        t = watcher.rome_time(dt.datetime.fromtimestamp(items[0]["ts"], UTC)).strftime("%d.%m %H:%M")
        if kind == "event":
            print(f"{t} 🔥 Событие: {label} — {len(items)} клипа: " + " | ".join(f"{x['who']} «{x['title'][:40]}»" for x in items))
            continue
        x = items[0]
        print(f"{t} {x['platform']:7s} {x['who'][:18]:18s} {num(x['views']):>8} за {hours(x['age']):>7} "
              f"(+{num(x['speed'])}/ч) → сейчас {num(x['now'])} [{x['why']}] «{x['title'][:60]}» {dur(x['dur'])}")


if __name__ == "__main__":
    main()
