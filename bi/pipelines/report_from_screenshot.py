"""
スクリーンショット画像から銘柄を読み取り、report_mover_reasons と同じパイプラインでレポートを生成する。
"""

from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path

from mover_report_core import (
    _maybe_load_dotenv,
    build_markdown_report,
    parse_as_of_date,
    vision_extract_screenshot,
    vision_result_to_rows,
)

REPORTS_DIR = Path("reports")


def _guess_mime(path: Path) -> str:
    suf = path.suffix.lower()
    if suf in (".jpg", ".jpeg"):
        return "image/jpeg"
    if suf == ".webp":
        return "image/webp"
    return "image/png"


def run_from_image_file(
    image_path: Path,
    *,
    as_of_override: date | None,
    model_vision: str,
    model_chat: str,
) -> tuple[str, Path, date]:
    _maybe_load_dotenv()
    data = image_path.read_bytes()
    mime = _guess_mime(image_path)
    vision = vision_extract_screenshot(data, mime=mime, model=model_vision)
    rows, date_hint = vision_result_to_rows(vision)
    if not rows:
        raise SystemExit("画像から4桁の銘柄コードを読み取れませんでした。")

    default_date = date.today()
    as_of = as_of_override or parse_as_of_date(date_hint, default_date)

    tavily_key = os.environ.get("TAVILY_API_KEY", "").strip()
    if not tavily_key:
        raise SystemExit("TAVILY_API_KEY が未設定です。")
    if not os.environ.get("OPENAI_API_KEY", "").strip():
        raise SystemExit("OPENAI_API_KEY が未設定です。")

    md = build_markdown_report(
        rows,
        as_of=as_of,
        tavily_api_key=tavily_key,
        model=model_chat,
    )
    return md, image_path, as_of


def main() -> None:
    _maybe_load_dotenv()
    p = argparse.ArgumentParser(description="画像→Vision→材料レポート")
    p.add_argument("--image", type=Path, required=True, help="スクリーンショット（png/jpg/webp）")
    p.add_argument("--as-of", type=str, default=None, help="基準日 YYYY-MM-DD（画像より優先）")
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="出力 Markdown（既定 reports/screenshot_<日付>.md）",
    )
    p.add_argument(
        "--vision-model",
        type=str,
        default=os.environ.get("OPENAI_VISION_MODEL", "gpt-4o-mini"),
        help="画像理解用モデル（マルチモーダル対応）",
    )
    p.add_argument(
        "--model",
        type=str,
        default=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        help="本文生成用チャットモデル",
    )
    args = p.parse_args()

    print(
        "\n[report_from_screenshot] これから行うこと:\n"
        "  1) 指定した画像ファイルを読みます。\n"
        "  2) OpenAI に画像を送り、写っている銘柄コード・銘柄名を抽出します（外部サーバーに画像が送られます）。\n"
        "  3) 各銘柄について Tavily で検索し、OpenAI でレポートを書きます。\n"
        "  4) reports フォルダに Markdown を保存します。\n"
        "（API 利用料がかかります。）\n"
    )

    if not args.image.is_file():
        raise SystemExit(f"ファイルがありません: {args.image}")

    as_of_override = date.fromisoformat(args.as_of[:10]) if args.as_of else None
    md, img_path, as_of_used = run_from_image_file(
        args.image,
        as_of_override=as_of_override,
        model_vision=args.vision_model,
        model_chat=args.model,
    )

    out = args.output
    if out is None:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORTS_DIR / f"screenshot_{as_of_used.isoformat()}_{img_path.stem}.md"
    else:
        out.parent.mkdir(parents=True, exist_ok=True)

    out.write_text(md, encoding="utf-8")
    print(f"saved: {out}")


if __name__ == "__main__":
    main()
