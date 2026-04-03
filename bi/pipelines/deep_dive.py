"""
銘柄 Deep Dive レポート生成。

使い方:
  python deep_dive.py --code 7203
  python deep_dive.py --code 7203 --type annual    # 有報（デフォルト）
  python deep_dive.py --code 7203 --type quarterly # 四半期報告書

出力: research/stocks/{code}_{date}_deepdive.md
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from io import BytesIO
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from edinet_client import (
    DOC_TYPE_ANNUAL,
    DOC_TYPE_QUARTERLY,
    FILE_TYPE_PDF,
    FILE_TYPE_XBRL,
    download_document,
    find_latest_filing,
)
from edinet_xbrl_parser import parse_xbrl_zip
from edinet_pdf_extractor import extract_sections_from_bytes
from jq_client_utils import normalize_code_4

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None  # type: ignore[misc, assignment]


OUTPUT_DIR = Path("../../research/stocks")
_ENV_PATH = Path(__file__).resolve().parent / ".env"

_SYSTEM_PROMPT = """あなたは日本株専門のアナリストです。
提供された有価証券報告書のデータ（XBRL財務データ + テキストセクション）をもとに、
スイングトレーダー視点で実用的な Deep Dive レポートを日本語 Markdown で作成してください。

レポート構成:
1. **企業概要サマリー** - 事業の核心を3行以内で
2. **財務ハイライト** - XBRL データからCF・収益性を簡潔に
3. **セグメント分析** - 各セグメントの売上・利益構成、成長ドライバー
4. **事業リスク Top3** - 株価に影響しやすいリスクを優先
5. **MD&A 要点** - 経営陣が強調しているポイント
6. **株主・需給** - 大株主構成、注目点
7. **トレーダー視点メモ** - カタリスト候補・注意点を箇条書き

数値は億円単位で表記。不明な項目は「データなし」と明記してください。"""


def _format_xbrl_for_prompt(xbrl: dict[str, Any]) -> str:
    """XBRL データをプロンプト用テキストに整形。"""
    if "error" in xbrl:
        return f"[XBRL取得エラー: {xbrl['error']}]"

    lines = [f"決算期末: {xbrl.get('period_end', '不明')}"]

    summary = xbrl.get("summary", {})
    cf = xbrl.get("cashflow", {})

    def _fmt(val: float | None, unit: str = "百万円") -> str:
        if val is None:
            return "N/A"
        return f"{val:,.0f} {unit}"

    lines.append("\n【財務サマリー（連結）】")
    lines.append(f"  売上高: {_fmt(summary.get('net_sales'))}")
    lines.append(f"  営業利益: {_fmt(summary.get('operating_profit'))}")
    lines.append(f"  経常利益: {_fmt(summary.get('ordinary_profit'))}")
    lines.append(f"  当期純利益: {_fmt(summary.get('net_income'))}")
    lines.append(f"  総資産: {_fmt(summary.get('total_assets'))}")
    lines.append(f"  純資産: {_fmt(summary.get('equity'))}")
    eq_ratio = summary.get("equity_ratio")
    lines.append(f"  自己資本比率: {eq_ratio:.1f}%" if eq_ratio is not None else "  自己資本比率: N/A")
    lines.append(f"  EPS: {_fmt(summary.get('eps'), '円')}")

    lines.append("\n【キャッシュフロー（連結）】")
    lines.append(f"  営業CF: {_fmt(cf.get('operating_cf'))}")
    lines.append(f"  投資CF: {_fmt(cf.get('investing_cf'))}")
    lines.append(f"  財務CF: {_fmt(cf.get('financing_cf'))}")
    lines.append(f"  期末現預金: {_fmt(cf.get('cash_end'))}")

    return "\n".join(lines)


def _format_sections_for_prompt(sections: dict[str, str]) -> str:
    """PDF セクションをプロンプト用テキストに整形。"""
    if "error" in sections:
        return f"[PDF取得エラー: {sections['error']}]"

    label_map = {
        "business_overview": "事業の概要",
        "business_detail": "事業の状況",
        "risk_factors": "事業等のリスク",
        "mda": "MD&A（経営者による分析）",
        "segment": "セグメント情報",
        "shareholder": "大株主の状況",
    }

    parts = []
    for key, label in label_map.items():
        text = sections.get(key, "")
        if text:
            parts.append(f"【{label}】\n{text}")

    return "\n\n".join(parts) if parts else "[テキストセクション取得なし]"


def generate_report(
    code: str,
    filing_meta: dict[str, Any],
    xbrl_data: dict[str, Any],
    pdf_sections: dict[str, str],
    *,
    model: str = "gpt-4o-mini",
    api_key: str,
) -> str:
    """OpenAI API を呼び出して Deep Dive レポートを生成。"""
    if OpenAI is None:
        raise ImportError("openai が必要です: pip install openai")

    client = OpenAI(api_key=api_key)

    company_name = filing_meta.get("filerName", "不明")
    doc_type = filing_meta.get("docDescription", "有価証券報告書")
    period_end = filing_meta.get("periodEnd", "不明")

    user_content = f"""# Deep Dive 対象
銘柄コード: {code}
企業名: {company_name}
書類種別: {doc_type}
決算期: {period_end}

---
{_format_xbrl_for_prompt(xbrl_data)}

---
{_format_sections_for_prompt(pdf_sections)}
"""

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        temperature=0.3,
        max_tokens=4096,
    )
    return resp.choices[0].message.content or ""


def main() -> None:
    load_dotenv(_ENV_PATH)

    parser = argparse.ArgumentParser(description="銘柄 Deep Dive レポート生成（EDINET 有報ベース）")
    parser.add_argument("--code", required=True, help="証券コード（4桁）例: 7203")
    parser.add_argument(
        "--type",
        choices=["annual", "quarterly"],
        default="annual",
        help="書類種別（annual: 有報, quarterly: 四半期報告書）。デフォルト: annual",
    )
    parser.add_argument("--model", default="gpt-4o-mini", help="OpenAI モデル名。デフォルト: gpt-4o-mini")
    parser.add_argument("--no-llm", action="store_true", help="LLM 呼び出しをスキップ（データ確認用）")
    args = parser.parse_args()

    edinet_key = os.environ.get("EDINET_API_KEY", "").strip()
    if not edinet_key:
        raise ValueError("EDINET_API_KEY が未設定です。.env に追記してください。")

    openai_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not args.no_llm and not openai_key:
        raise ValueError("OPENAI_API_KEY が未設定です。.env に追記してください。")

    code = normalize_code_4(args.code)
    doc_type_codes = [DOC_TYPE_ANNUAL] if args.type == "annual" else [DOC_TYPE_QUARTERLY]
    doc_type_label = "有報" if args.type == "annual" else "四半期報告書"

    # 1) EDINET で最新の書類を検索
    print(f"[1/4] EDINET で {code} の最新{doc_type_label}を検索中...")
    filing = find_latest_filing(code, edinet_key, doc_type_codes=doc_type_codes)
    if filing is None:
        raise RuntimeError(f"{code} の {doc_type_label} が EDINET で見つかりませんでした（直近400日）。")

    doc_id = filing["docID"]
    company_name = filing.get("filerName", "不明")
    period_end = filing.get("periodEnd", "不明")
    print(f"  → {company_name} | 期末: {period_end} | docID: {doc_id}")

    # 2) XBRL ダウンロード・パース
    print("[2/4] XBRL をダウンロード・パース中...")
    xbrl_zip = download_document(doc_id, edinet_key, file_type=FILE_TYPE_XBRL)
    xbrl_data = parse_xbrl_zip(xbrl_zip)
    if "error" not in xbrl_data:
        cf = xbrl_data.get("cashflow", {})
        print(f"  → 営業CF: {cf.get('operating_cf')} | 投資CF: {cf.get('investing_cf')}")
    else:
        print(f"  → XBRL エラー: {xbrl_data['error']}")

    # 3) PDF ダウンロード・セクション抽出（type=2 は直接 PDF バイト列）
    print("[3/4] PDF をダウンロード・セクション抽出中...")
    pdf_bytes = download_document(doc_id, edinet_key, file_type=FILE_TYPE_PDF)
    pdf_sections = extract_sections_from_bytes(pdf_bytes)

    if args.no_llm:
        print("\n--- XBRL データ ---")
        print(json.dumps(xbrl_data, ensure_ascii=False, indent=2, default=str))
        print("\n--- PDF セクション（先頭200文字）---")
        for k, v in pdf_sections.items():
            print(f"[{k}] {v[:200]}")
        return

    # 4) LLM でレポート生成
    print(f"[4/4] {args.model} でレポート生成中...")
    report_md = generate_report(
        code=code,
        filing_meta=filing,
        xbrl_data=xbrl_data,
        pdf_sections=pdf_sections,
        model=args.model,
        api_key=openai_key,
    )

    # 5) 保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today_str = date.today().strftime("%Y-%m-%d")
    out_path = OUTPUT_DIR / f"{code}_{today_str}_deepdive.md"

    header = f"# {company_name}（{code}）Deep Dive\n\n"
    header += f"- **決算期**: {period_end}\n"
    header += f"- **書類種別**: {doc_type_label}\n"
    header += f"- **生成日**: {today_str}\n"
    header += f"- **ソース**: EDINET {doc_id}\n\n---\n\n"

    out_path.write_text(header + report_md, encoding="utf-8")
    print(f"\n✅ 保存完了: {out_path}")


if __name__ == "__main__":
    main()
