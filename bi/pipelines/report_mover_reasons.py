"""
scan_daily_movers の JSON（または銘柄指定）を入力に、Web 検索 + LLM で Markdown レポートを生成する。
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

from jq_client_utils import normalize_code_4
from mover_report_core import _maybe_load_dotenv, build_markdown_report

REPORTS_DIR = Path("reports")


def load_movers_file(path: Path) -> tuple[date, list[dict]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    as_of_s = data.get("as_of")
    if not as_of_s:
        raise ValueError("JSON に as_of がありません")
    as_of = date.fromisoformat(str(as_of_s)[:10])
    movers = data.get("movers") or []
    if not isinstance(movers, list):
        raise ValueError("movers が配列ではありません")
    return as_of, movers


def filter_movers(
    movers: list[dict],
    *,
    only_up: bool,
    only_down: bool,
    codes: set[str] | None,
    max_stocks: int | None,
) -> list[dict]:
    rows = list(movers)
    if codes:
        rows = [r for r in rows if normalize_code_4(r.get("Code", "")) in codes]

    def _is_up(r: dict) -> bool:
        if r.get("direction") == "up":
            return True
        if r.get("direction") == "down":
            return False
        rt = r.get("return")
        if rt is None:
            return True
        return float(rt) >= 0

    def _is_down(r: dict) -> bool:
        if r.get("direction") == "down":
            return True
        if r.get("direction") == "up":
            return False
        rt = r.get("return")
        if rt is None:
            return True
        return float(rt) < 0

    if only_up and not only_down:
        rows = [r for r in rows if _is_up(r)]
    elif only_down and not only_up:
        rows = [r for r in rows if _is_down(r)]

    if max_stocks is not None and max_stocks > 0:
        rows = rows[:max_stocks]
    return rows


def rows_from_code_args(pairs: list[tuple[str, str]]) -> list[dict]:
    """--code 7203 --company-name トヨタ のような指定から行を作る。"""
    out: list[dict] = []
    for code_raw, name in pairs:
        code = normalize_code_4(code_raw)
        if len(code) != 4 or not code.isdigit():
            continue
        out.append({"Code": code, "CompanyName": name})
    return out


def main() -> None:
    _maybe_load_dotenv()

    p = argparse.ArgumentParser(description="値幅銘柄の材料レポート（Tavily + OpenAI）")
    p.add_argument(
        "--input",
        type=Path,
        default=None,
        help="scan_daily_movers が出力した movers_YYYY-MM-DD.json",
    )
    p.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="基準日 YYYY-MM-DD（--input があるとき上書き）",
    )
    p.add_argument("--only-up", action="store_true", help="上昇のみ")
    p.add_argument("--only-down", action="store_true", help="下落のみ")
    p.add_argument("--max-stocks", type=int, default=0, help="処理する銘柄の上限（0=制限なし）")
    p.add_argument(
        "--code",
        action="append",
        default=None,
        help="銘柄コード4桁（複数可）。--input と併用時はその中に限定",
    )
    p.add_argument(
        "--company-name",
        action="append",
        default=None,
        help="--code と同じ順序で銘柄名（省略可）",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="出力 Markdown（既定 reports/movers_AS-OF.md）",
    )
    p.add_argument(
        "--model",
        type=str,
        default=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        help="OpenAI チャットモデル（環境変数 OPENAI_MODEL でも指定可）",
    )
    args = p.parse_args()

    print(
        "\n[report_mover_reasons] これから行うこと:\n"
        "  1) 指定した JSON または銘柄コード一覧を読みます。\n"
        "  2) 銘柄ごとに Tavily で Web 検索し、記事の要約と URL を集めます。\n"
        "  3) OpenAI がその内容だけを根拠に日本語レポート（Markdown）を書きます。\n"
        "  4) reports フォルダ（または --output）に .md を保存します。\n"
        "（OpenAI・Tavily の API 利用料がかかります。銘柄数が多いほど時間とコストが増えます。）\n"
    )

    tavily_key = os.environ.get("TAVILY_API_KEY", "").strip()
    if not tavily_key:
        raise SystemExit("TAVILY_API_KEY が未設定です（.env または環境変数）。")
    if not os.environ.get("OPENAI_API_KEY", "").strip():
        raise SystemExit("OPENAI_API_KEY が未設定です。")

    code_list = list(args.code or [])
    name_list = list(args.company_name or [])

    codes_filter: set[str] | None = None
    if code_list:
        codes_filter = {normalize_code_4(c) for c in code_list if c}

    max_n = args.max_stocks if args.max_stocks and args.max_stocks > 0 else None

    if args.input:
        as_of, movers = load_movers_file(args.input)
        movers = filter_movers(
            movers,
            only_up=args.only_up,
            only_down=args.only_down,
            codes=codes_filter,
            max_stocks=max_n,
        )
    else:
        if not code_list:
            raise SystemExit("--input が無い場合は --code を1つ以上指定してください。")
        pairs: list[tuple[str, str]] = []
        for i, c in enumerate(code_list):
            nm = name_list[i] if i < len(name_list) else ""
            pairs.append((c, nm))
        movers = rows_from_code_args(pairs)
        movers = filter_movers(
            movers,
            only_up=args.only_up,
            only_down=args.only_down,
            codes=codes_filter,
            max_stocks=max_n,
        )
        as_of = date.today()
        if args.as_of:
            as_of = date.fromisoformat(args.as_of[:10])

    if args.as_of and args.input:
        as_of = date.fromisoformat(args.as_of[:10])

    if not movers:
        raise SystemExit("対象銘柄が0件です。フィルタ条件を確認してください。")

    md = build_markdown_report(
        movers,
        as_of=as_of,
        tavily_api_key=tavily_key,
        model=args.model,
    )

    out = args.output
    if out is None:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORTS_DIR / f"movers_{as_of.isoformat()}.md"
    else:
        out.parent.mkdir(parents=True, exist_ok=True)

    out.write_text(md, encoding="utf-8")
    print(f"saved: {out} stocks={len(movers)} as_of={as_of}")


if __name__ == "__main__":
    main()
