"""J-Quants × Google CSE → ローカルLLM用 Markdown（LLM API は呼ばない）"""

from pathlib import Path

from rich.console import Console

from .config import config
from .brave_search_client import BraveSearchClient
from .jquants_client import DailyQuote, JQuantsClient, TopMoversBundle
from .local_llm_material import (
    QueryBundle,
    assemble_full_report,
    build_stock_section,
)

console = Console()


def _markdown_diagnostic(bundle: TopMoversBundle) -> str:
    """銘柄0件のとき、ファイル先頭に載せる説明（ユーザー向け）"""
    td = bundle.target_date
    r, m, t = bundle.raw_daily_count, bundle.matched_mover_count, bundle.tse_master_rows
    parts = [
        "**【診断】**",
        f"- J-Quants 日次株価の件数: **{r}**",
        f"- 東証プライム・スタンダード・グロースのマスタ行数: **{t}**",
        f"- マスタと突合し、前日比≠0 まで絞った件数: **{m}**",
        "",
    ]
    if r == 0:
        parts.append(
            f"- **想定される原因**: 指定日 `{td}` は**休場**で、日次株価が提供されていない可能性が高いです（祝日・土日など）。"
        )
        parts.append(
            "- **例**: 2025-03-20 は **春分の日** で東証は休みでした。"
        )
        parts.append(
            "- **対処**: **直近の取引日**を `--date` で指定し直してください（例: `--date 2025-03-19` や `--date 2025-03-21`）。"
        )
    elif m == 0 and t > 0:
        parts.append(
            "- **想定される原因**: 株価データはありますが、東証3市場のマスタと銘柄コードが一致しない、または前日比がすべて計算できていません。"
        )
    elif t == 0:
        parts.append("- **想定される原因**: マスタ取得に失敗しているか、APIプランでマスタが取得できていません。")
    return "\n".join(parts)


def _search_bundles_for_stock(
    cse: GoogleCseClient,
    name: str,
    target_date: str,
    direction_word: str,
    num_results: int,
) -> list[QueryBundle]:
    """銘柄あたり2クエリ（無料枠の目安: 銘柄数×2）"""
    q_reason = f"{name} 株価 {direction_word} 理由 {target_date}"
    q_ir = f"{name} IR ニュース"
    bundles: list[QueryBundle] = []
    hits1, err1 = cse.search(q_reason, num=num_results)
    bundles.append(
        QueryBundle(label="株価変動の理由・日付", query=q_reason, hits=hits1, error=err1)
    )
    hits2, err2 = cse.search(q_ir, num=num_results)
    bundles.append(QueryBundle(label="IR・ニュース", query=q_ir, hits=hits2, error=err2))
    return bundles


def collect_material(
    target_date_str: str,
    gainers: list[DailyQuote],
    losers: list[DailyQuote],
    num_results: int = 8,
    diagnostic_note: str = "",
) -> tuple[str, int]:
    """
    Returns:
        (markdown全文, Google CSE 呼び出し回数)
    """
    cse = BraveSearchClient()
    sections: list[str] = []

    sections.append(f"## 上昇トップ {len(gainers)} {target_date_str}\n")
    for i, q in enumerate(gainers, 1):
        nm = (q.name or "").strip() or q.code
        bundles = _search_bundles_for_stock(
            cse, nm, target_date_str, "上昇", num_results
        )
        sections.append(build_stock_section(i, "上昇", q, bundles))

    sections.append(f"## 下落トップ {len(losers)} {target_date_str}\n")
    for i, q in enumerate(losers, 1):
        nm = (q.name or "").strip() or q.code
        bundles = _search_bundles_for_stock(
            cse, nm, target_date_str, "下落", num_results
        )
        sections.append(build_stock_section(i, "下落", q, bundles))

    n_stocks = len(gainers) + len(losers)
    md = assemble_full_report(
        target_date=target_date_str,
        sections=sections,
        cse_calls=cse.queries_today,
        stock_count=n_stocks,
        diagnostic_note=diagnostic_note,
    )
    return md, cse.queries_today


def run_from_jquants(
    target_date=None,
    top_n: int = 20,
    output_path: Path | None = None,
    num_results: int = 8,
) -> Path:
    """J-Quants から東証トップ movers を取り、Markdown を書き出す。"""
    config.validate_jquants()
    config.validate_google_cse()
    config.REPORT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with JQuantsClient() as jq:
        bundle = jq.get_tse_top_movers(target_date=target_date, top_n=top_n)

    td = bundle.target_date
    console.print(
        f"[cyan]対象日[/cyan] {td}  /  上昇 [bold]{len(bundle.gainers)}[/bold]  下落 [bold]{len(bundle.losers)}[/bold]"
    )
    n_pick = len(bundle.gainers) + len(bundle.losers)
    diag = ""
    if n_pick == 0:
        diag = _markdown_diagnostic(bundle)
        console.print(
            "[yellow]銘柄が0件のため Google 検索はスキップされました。"
            " 休場日を指定していないか確認してください。[/yellow]"
        )

    md, n_calls = collect_material(
        target_date_str=td,
        gainers=bundle.gainers,
        losers=bundle.losers,
        num_results=num_results,
        diagnostic_note=diag,
    )

    if output_path is None:
        output_path = config.REPORT_OUTPUT_DIR / f"local_llm_material_{td.replace('-', '')}.md"
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(md, encoding="utf-8")
    console.print(f"[green]Google CSE 呼び出し: {n_calls} 回[/green]")
    console.print(f"[bold green]出力: {output_path}[/bold green]")
    return output_path
