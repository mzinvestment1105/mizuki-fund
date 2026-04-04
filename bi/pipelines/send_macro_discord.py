"""
マクロレポートを Discord のマクロチャンネルに送信する。
本文をテキストで送った後、MDファイルを添付する。

使い方:
  python send_macro_discord.py          # 今日のレポートを自動検索
  python send_macro_discord.py --date 2026-04-05  # 日付指定
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv

MARKET_DIR = Path("../../market/daily")
_ENV_PATH = Path(__file__).resolve().parent / ".env"
_CHUNK_SIZE = 1900


def find_report(target_date: str) -> Path:
    candidates = sorted(MARKET_DIR.glob(f"{target_date}_*sonnet*.md"), reverse=True)
    if not candidates:
        raise FileNotFoundError(
            f"{target_date}_*sonnet*.md が {MARKET_DIR} に見つかりません。"
        )
    return candidates[0]


def _split_chunks(text: str) -> list[str]:
    chunks = []
    while text:
        if len(text) <= _CHUNK_SIZE:
            chunks.append(text)
            break
        split = text[:_CHUNK_SIZE].rfind("\n")
        if split == -1:
            split = _CHUNK_SIZE
        chunks.append(text[:split])
        text = text[split:]
    return chunks


def send_to_discord(webhook_url: str, report_path: Path, target_date: str) -> None:
    text = report_path.read_text(encoding="utf-8")
    chunks = _split_chunks(text)
    total = len(chunks)

    for i, chunk in enumerate(chunks):
        r = requests.post(webhook_url, json={"content": chunk})
        r.raise_for_status()
        print(f"  本文 [{i+1}/{total}] 送信完了")

    caption = json.dumps({"content": f"📎 マクロレポート ({target_date})"})
    with open(report_path, "rb") as f:
        r = requests.post(
            webhook_url,
            data={"payload_json": caption},
            files={"file": (report_path.name, f, "text/plain")},
        )
    r.raise_for_status()
    print(f"  添付ファイル 送信完了: {report_path.name}")


def main() -> None:
    load_dotenv(_ENV_PATH)

    parser = argparse.ArgumentParser(description="マクロレポートを Discord に送信")
    parser.add_argument("--date", default=date.today().strftime("%Y-%m-%d"), help="日付 例: 2026-04-05")
    args = parser.parse_args()

    webhook_url = os.environ.get("DISCORD_WEBHOOK_MACRO", "").strip()
    if not webhook_url:
        raise ValueError("DISCORD_WEBHOOK_MACRO が .env に未設定です。Discord でwebhookを作成して .env に追加してください。")

    report_path = find_report(args.date)
    print(f"送信: {report_path.name}  ({report_path.stat().st_size / 1024:.1f} KB)")

    send_to_discord(webhook_url, report_path, args.date)
    print("完了")


if __name__ == "__main__":
    main()
