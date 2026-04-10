"""
動意銘柄レポートを Discord に送信する。
分析ファイル（*_mover_analysis.md）があればそれを、なければ生データ（*_movers_raw.md）を送信。

使い方:
  python send_mover_discord.py          # 今日のレポートを自動検索
  python send_mover_discord.py --date 2026-04-10  # 日付指定
  python send_mover_discord.py --raw    # 生データを強制送信

環境変数:
  DISCORD_WEBHOOK_MOVERS  … 動意銘柄用チャンネルのWebhook URL
  DISCORD_WEBHOOK_MACRO   … フォールバック（MOVERSが未設定の場合）
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


def find_report(target_date: str, prefer_raw: bool = False) -> Path:
    if not prefer_raw:
        # 分析ファイル優先
        analysis = sorted(MARKET_DIR.glob(f"{target_date}_mover_analysis.md"), reverse=True)
        if analysis:
            return analysis[0]
    # 生データフォールバック
    raw = sorted(MARKET_DIR.glob(f"{target_date}_movers_raw.md"), reverse=True)
    if raw:
        return raw[0]
    raise FileNotFoundError(
        f"{target_date}_mover_analysis.md / _movers_raw.md が {MARKET_DIR} に見つかりません。"
    )


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
    is_raw = "_movers_raw" in report_path.name
    label = "動意銘柄 生データ" if is_raw else "動意銘柄レポート"

    text = report_path.read_text(encoding="utf-8")
    chunks = _split_chunks(text)
    total = len(chunks)

    for i, chunk in enumerate(chunks):
        r = requests.post(webhook_url, json={"content": chunk})
        r.raise_for_status()
        print(f"  本文 [{i+1}/{total}] 送信完了")

    caption = json.dumps({"content": f"📎 {label} ({target_date})"})
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

    parser = argparse.ArgumentParser(description="動意銘柄レポートを Discord に送信")
    parser.add_argument("--date", default=date.today().strftime("%Y-%m-%d"), help="日付 例: 2026-04-10")
    parser.add_argument("--raw", action="store_true", help="生データを強制送信（分析ファイルがあっても）")
    args = parser.parse_args()

    webhook_url = (
        os.environ.get("DISCORD_WEBHOOK_MOVERS", "").strip()
        or os.environ.get("DISCORD_WEBHOOK_MACRO", "").strip()
    )
    if not webhook_url:
        raise ValueError("DISCORD_WEBHOOK_MOVERS または DISCORD_WEBHOOK_MACRO が .env に未設定です。")

    report_path = find_report(args.date, prefer_raw=args.raw)
    print(f"送信: {report_path.name}  ({report_path.stat().st_size / 1024:.1f} KB)")

    send_to_discord(webhook_url, report_path, args.date)
    print("完了")


if __name__ == "__main__":
    main()
