"""
Deep Dive レポートを Discord の #個別銘柄report チャンネルに送信する。

使い方:
  python send_report_discord.py --code 7256
"""

from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv

from jq_client_utils import normalize_code_4

OUTPUT_DIR = Path("../../research/stocks")
_ENV_PATH = Path(__file__).resolve().parent / ".env"


def find_latest_report(code: str) -> Path:
    candidates = sorted(OUTPUT_DIR.glob(f"{code}_*_sonnet_deepdive.md"), reverse=True)
    if not candidates:
        raise FileNotFoundError(
            f"{code}_*_sonnet_deepdive.md が {OUTPUT_DIR} に見つかりません。"
        )
    return candidates[0]


def send_to_discord(webhook_url: str, report_path: Path, code: str) -> None:
    today = date.today().strftime("%Y-%m-%d")
    message = f"**{code} Deep Dive レポート** ({today})"

    with open(report_path, "rb") as f:
        response = requests.post(
            webhook_url,
            data={"payload_json": f'{{"content": "{message}"}}'},
            files={"file": (report_path.name, f, "text/plain")},
        )

    response.raise_for_status()
    print(f"送信完了: {report_path.name} -> #個別銘柄report")


def main() -> None:
    load_dotenv(_ENV_PATH)

    parser = argparse.ArgumentParser(description="Deep Dive レポートを Discord に送信")
    parser.add_argument("--code", required=True, help="証券コード（4桁）例: 7256")
    args = parser.parse_args()

    webhook_url = os.environ.get("DISCORD_WEBHOOK_RESEARCH", "").strip()
    if not webhook_url:
        raise ValueError("DISCORD_WEBHOOK_RESEARCH が .env に未設定です。")

    code = normalize_code_4(args.code)
    report_path = find_latest_report(code)
    print(f"送信するファイル: {report_path.name}  ({report_path.stat().st_size / 1024:.1f} KB)")

    send_to_discord(webhook_url, report_path, code)


if __name__ == "__main__":
    main()
