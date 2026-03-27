#!/usr/bin/env python3
"""
TSE Reporter - 東証株価変動レポートツール

使い方:
  # 機能1: スクリーンショット解析
  python main.py screenshot path/to/image.png

  # 機能2: J-Quants ±10% 銘柄スクリーナー
  python main.py screener

  # 機能2（日付・閾値を指定）
  python main.py screener --date 2024-01-15 --threshold 5 --max-stocks 20

  # プロトタイプ: J-Quants 東証トップ + Google検索のみ → ローカルLLM用 .md
  python main.py material
  python main.py material --date 2024-01-15 --top-n 5
"""

import sys
from datetime import date
from pathlib import Path

import click
from rich.console import Console

from src.config import config

console = Console()


@click.group()
def cli():
    """東証株価変動レポートツール"""
    pass


# --------------------------------------------------------------------------- #
# 機能1: スクリーンショット解析
# --------------------------------------------------------------------------- #

@cli.command()
@click.argument("image_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="出力ファイルパス（省略時は reports/ に自動生成）",
)
@click.option(
    "--format", "fmt",
    type=click.Choice(["markdown", "html"], case_sensitive=False),
    default=None,
    help="出力フォーマット（省略時は .env の REPORT_FORMAT）",
)
def screenshot(image_path: Path, output: Path | None, fmt: str | None):
    """スクリーンショット画像から銘柄・変動理由を解析してレポートを生成する"""
    try:
        config.validate()
        config.validate_google_cse()
        if fmt:
            config.REPORT_FORMAT = fmt

        from src.screenshot_analyzer import ScreenshotAnalyzer
        from src.web_researcher import WebResearcher
        from src.report_generator import ReportGenerator

        # Step 1: 画像解析
        console.print(f"[cyan]スクリーンショット解析中: {image_path}[/cyan]")
        analyzer = ScreenshotAnalyzer()
        stock_info = analyzer.analyze(image_path)

        console.print(f"  証券コード : [bold]{stock_info.code or '不明'}[/bold]")
        console.print(f"  銘柄名     : [bold]{stock_info.name or '不明'}[/bold]")
        console.print(f"  前日比     : [bold]{stock_info.change_rate:+.2f}%[/bold]"
                      if stock_info.change_rate is not None else "  前日比     : 不明")

        if stock_info.change_rate is None:
            console.print("[red]前日比が取得できませんでした。画像を確認してください。[/red]")
            sys.exit(1)

        # Step 2: 変動理由調査
        code = stock_info.code or "UNKNOWN"
        name = stock_info.name or code
        console.print(f"\n[cyan]変動理由をWeb検索中...[/cyan]")
        researcher = WebResearcher()
        research = researcher.research(
            code=code,
            name=name,
            change_rate=stock_info.change_rate,
        )

        # Step 3: レポート生成
        reporter = ReportGenerator()
        report_path = reporter.generate_screenshot_report(
            stock_info=stock_info,
            research=research,
            output_path=output,
        )

        console.print(f"\n[bold green]レポート生成完了: {report_path}[/bold green]")

    except Exception as e:
        console.print(f"[bold red]エラー: {e}[/bold red]")
        sys.exit(1)


# --------------------------------------------------------------------------- #
# 機能2: J-Quants スクリーナー
# --------------------------------------------------------------------------- #

@cli.command()
@click.option(
    "--date", "target_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="対象日（YYYY-MM-DD）。省略時は昨営業日。",
)
@click.option(
    "--threshold", "-t",
    type=float,
    default=None,
    help=f"変動率の閾値（%%）。省略時は {config.PRICE_CHANGE_THRESHOLD}%%。",
)
@click.option(
    "--max-stocks", "-n",
    type=int,
    default=None,
    help="調査する最大銘柄数（コスト制御用）。省略時は全件。",
)
@click.option(
    "--format", "fmt",
    type=click.Choice(["markdown", "html"], case_sensitive=False),
    default=None,
    help="出力フォーマット",
)
def screener(target_date, threshold: float | None, max_stocks: int | None, fmt: str | None):
    """J-Quants APIで前日比±10%以上の銘柄を検出してレポートを生成する"""
    try:
        config.validate()
        config.validate_jquants()
        config.validate_google_cse()
        if fmt:
            config.REPORT_FORMAT = fmt

        from src.stock_screener import StockScreener

        parsed_date: date | None = target_date.date() if target_date else None

        with StockScreener() as screener_inst:
            screener_inst.run(
                target_date=parsed_date,
                threshold=threshold,
                max_stocks=max_stocks,
            )

    except Exception as e:
        console.print(f"[bold red]エラー: {e}[/bold red]")
        sys.exit(1)


# --------------------------------------------------------------------------- #
# プロトタイプ: Google検索まで → ローカルLLM用マテリアル（LLM API 不使用）
# --------------------------------------------------------------------------- #

@cli.command("material")
@click.option(
    "--date", "target_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="株価・マスタの基準日（YYYY-MM-DD）。省略時は昨営業日。",
)
@click.option(
    "--top-n",
    type=int,
    default=20,
    help="上昇・下落それぞれの上位件数（デフォルト20）。テスト時は 2〜5 推奨。",
)
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="出力 Markdown（省略時は reports/local_llm_material_YYYYMMDD.md）",
)
@click.option(
    "--num-results",
    type=int,
    default=8,
    help="各検索クエリあたり取得する件数（最大10）。",
)
def material_cmd(target_date, top_n: int, output: Path | None, num_results: int):
    """東証トップ変動銘柄を J-Quants で選び、Google検索結果をローカルLLM向けに保存する"""
    try:
        parsed_date = target_date.date() if target_date else None
        from src.material_runner import run_from_jquants

        run_from_jquants(
            target_date=parsed_date,
            top_n=top_n,
            output_path=output,
            num_results=num_results,
        )
    except Exception as e:
        console.print(f"[bold red]エラー: {e}[/bold red]")
        sys.exit(1)


if __name__ == "__main__":
    cli()
