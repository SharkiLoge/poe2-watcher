#!/usr/bin/env python3
"""Узнать chat_id: владелец бота пишет ему /start, скрипт читает getUpdates и вписывает TG_CHAT_ID в .env.
Токен берётся из .env (TG_TOKEN) и нигде не печатается."""
import os
import sys

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
ENV = os.path.join(HERE, ".env")


def read_env():
    out = {}
    if os.path.exists(ENV):
        for line in open(ENV, encoding="utf-8-sig"):
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def set_env(key, value):
    lines = open(ENV, encoding="utf-8-sig").read().splitlines() if os.path.exists(ENV) else []
    for i, line in enumerate(lines):
        if line.split("=", 1)[0].strip() == key:
            lines[i] = f"{key}={value}"
            break
    else:
        lines.append(f"{key}={value}")
    open(ENV, "w", encoding="utf-8").write("\n".join(lines) + "\n")


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    token = read_env().get("TG_TOKEN")
    if not token:
        print("TG_TOKEN в .env пустой")
        sys.exit(1)
    try:
        me = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=20)
        upd = requests.get(f"https://api.telegram.org/bot{token}/getUpdates", timeout=20)
    except requests.RequestException as e:
        print("сеть:", type(e).__name__)
        sys.exit(1)
    if not me.ok:
        print("токен не принят Telegram: HTTP", me.status_code)
        sys.exit(1)
    print("бот:", "@" + me.json()["result"]["username"])
    chats = {}
    for u in upd.json().get("result", []):
        msg = u.get("message") or u.get("my_chat_member") or {}
        chat = msg.get("chat")
        if chat and chat.get("type") == "private":
            chats[chat["id"]] = chat.get("first_name") or chat.get("username") or "?"
    if not chats:
        print("личных сообщений боту нет — напиши боту /start и запусти ещё раз")
        sys.exit(2)
    if len(chats) > 1:
        print("боту писали несколько человек:", ", ".join(chats.values()), "— беру последнего")
    cid, name = list(chats.items())[-1]
    set_env("TG_CHAT_ID", str(cid))
    print(f"chat_id найден (чат с {name}) и записан в .env")


if __name__ == "__main__":
    main()
