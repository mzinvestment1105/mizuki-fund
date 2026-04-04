"""
銘柄 Deep Dive データ収集。

EDINET から有報/四半期報告書を取得し、XBRL財務データ＋PDFテキストセクションを
整形 Markdown として保存する。レポート本文はClaudeが生成する。

使い方:
  python deep_dive.py --code 7256
  python deep_dive.py --code 7256 --type quarterly

出力: research/stocks/{code}_{date}_data.md
"""

from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path

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


OUTPUT_DIR = Path("../../research/stocks")
_ENV_PATH = Path(__file__).resolve().parent / ".env"


def _fmt(val: float | None, unit: str = "百万円") -> str:
    if val is None:
        return "N/A"
    return f"{val:,.0f} {unit}"


def build_data_markdown(
    code: str,
    filing_meta: dict,
    xbrl_data: dict,
    pdf_sections: dict,
) -> str:
    company_name = filing_meta.get("filerName", "不明")
    doc_type = filing_meta.get("docDescription", "有価証券報告書")
    period_end = filing_meta.get("periodEnd", xbrl_data.get("period_end", "不明"))
    today_str = date.today().strftime("%Y-%m-%d")

    lines = [
        f"# {company_name}（{code}）有報データ",
        f"",
        f"- **決算期**: {period_end}",
        f"- **書類種別**: {doc_type}",
        f"- **収集日**: {today_str}",
        f"- **ソース**: EDINET {filing_meta.get('docID', '')}",
        f"",
        f"---",
        f"",
        f"## 財務サマリー（XBRL・連結）",
        f"",
    ]

    if "error" in xbrl_data:
        lines.append(f"> XBRL取得エラー: {xbrl_data['error']}")
    else:
        summary = xbrl_data.get("summary", {})
        cf = xbrl_data.get("cashflow", {})
        lines += [
            f"| 項目 | 値 |",
            f"|------|----|",
            f"| 売上高 | {_fmt(summary.get('net_sales'))} |",
            f"| 営業利益 | {_fmt(summary.get('operating_profit'))} |",
            f"| 経常利益 | {_fmt(summary.get('ordinary_profit'))} |",
            f"| 当期純利益 | {_fmt(summary.get('net_income'))} |",
            f"| 総資産 | {_fmt(summary.get('total_assets'))} |",
            f"| 純資産 | {_fmt(summary.get('equity'))} |",
            f"| 自己資本比率 | {str(round(summary['equity_ratio'], 1)) + '%' if summary.get('equity_ratio') is not None else 'N/A'} |",
            f"| EPS | {_fmt(summary.get('eps'), '円')} |",
            f"",
            f"**キャッシュフロー（連結）**",
            f"",
            f"| 項目 | 値 |",
            f"|------|----|",
            f"| 営業CF | {_fmt(cf.get('operating_cf'))} |",
            f"| 投資CF | {_fmt(cf.get('investing_cf'))} |",
            f"| 財務CF | {_fmt(cf.get('financing_cf'))} |",
            f"| 期末現預金 | {_fmt(cf.get('cash_end'))} |",
        ]

    label_map = {
        "business_overview": "事業の概要",
        "business_detail": "事業の状況",
        "risk_factors": "事業等のリスク",
        "mda": "MD&A（経営者による分析）",
        "segment": "セグメント情報",
        "shareholder": "大株主の状況",
    }

    lines += ["", "---", ""]
    for key, label in label_map.items():
        text = pdf_sections.get(key, "")
        if key == "error":
            continue
        lines.append(f"## {label}")
        lines.append("")
        if text:
            lines.append(text)
        else:
            lines.append("*取得なし*")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    load_dotenv(_ENV_PATH)

    parser = argparse.ArgumentParser(description="銘柄 Deep Dive データ収集（EDINET 有報ベース）")
    parser.add_argument("--code", required=True, help="証券コード（4桁）例: 7256")
    parser.add_argument(
        "--type",
        choices=["annual", "quarterly"],
        default="annual",
        help="書類種別（annual: 有報, quarterly: 四半期報告書）。デフォルト: annual",
    )
    args = parser.parse_args()

    edinet_key = os.environ.get("EDINET_API_KEY", "").strip()
    if not edinet_key:
        raise ValueError("EDINET_API_KEY が未設定です。.env に追記してください。")

    code = normalize_code_4(args.code)
    doc_type_codes = [DOC_TYPE_ANNUAL] if args.type == "annual" else [DOC_TYPE_QUARTERLY]
    doc_type_label = "有報" if args.type == "annual" else "四半期報告書"

    # 1) EDINET で最新の書類を検索
    print(f"[1/3] EDINET で {code} の最新{doc_type_label}を検索中...")
    filing = find_latest_filing(code, edinet_key, doc_type_codes=doc_type_codes)
    if filing is None:
        raise RuntimeError(f"{code} の {doc_type_label} が EDINET で見つかりませんでした（直近400日）。")

    doc_id = filing["docID"]
    company_name = filing.get("filerName", "不明")
    period_end = filing.get("periodEnd", "不明")
    print(f"  → {company_name} | 期末: {period_end} | docID: {doc_id}")

    # 2) XBRL ダウンロード・パース
    print("[2/3] XBRL をダウンロード・パース中...")
    xbrl_zip = download_document(doc_id, edinet_key, file_type=FILE_TYPE_XBRL)
    xbrl_data = parse_xbrl_zip(xbrl_zip)
    if "error" not in xbrl_data:
        cf = xbrl_data.get("cashflow", {})
        print(f"  → 営業CF: {cf.get('operating_cf')} | 投資CF: {cf.get('investing_cf')}")
    else:
        print(f"  → XBRL エラー: {xbrl_data['error']}")

    # 3) PDF ダウンロード・セクション抽出（type=2 は直接 PDF バイト列）
    print("[3/3] PDF をダウンロード・セクション抽出中...")
    pdf_bytes = download_document(doc_id, edinet_key, file_type=FILE_TYPE_PDF)
    pdf_sections = extract_sections_from_bytes(pdf_bytes)

    # 4) Markdown に整形して保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today_str = date.today().strftime("%Y-%m-%d")
    out_path = OUTPUT_DIR / f"{code}_{today_str}_data.md"

    md = build_data_markdown(code, filing, xbrl_data, pdf_sections)
    out_path.write_text(md, encoding="utf-8")
    print(f"\n保存完了: {out_path}")
    print("→ Claudeにこのファイルを渡してDeep Diveレポートを依頼してください。")


if __name__ == "__main__":
    main()
