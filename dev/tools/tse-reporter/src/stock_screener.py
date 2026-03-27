"""J-Quants API を使った ±10% 銘柄スクリーナー（機能2）"""

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
import time

from .config import config
from .jquants_client import JQuantsClient, DailyQuote
from .web_researcher import WebResearcher, ResearchResult
from .report_generator import ReportGenerator

console = Console()


class StockScreener:
    """J-Quants から ±10% 銘柄を取得し、変動理由をレポートにまとめる"""

    def __init__(self):
        self._jquants = JQuantsClient()
        self._researcher = WebResearcher()
        self._reporter = ReportGenerator()

    def run(
        self,
        target_date: Optional[date] = None,
        threshold: Optional[float] = None,
        max_stocks: Optional[int] = None,
    ) -> str:
        """
        スクリーナーを実行してレポートを生成する。

        Args:
            target_date: 対象日（省略時は直近営業日）
            threshold: 変動率閾値（省略時は config 値 10%）
            max_stocks: 調査する最大銘柄数（コスト制御用）

        Returns:
            生成されたレポートファイルのパス
        """
        if threshold is None:
            threshold = config.PRICE_CHANGE_THRESHOLD

        # ---- 使用モデル表示 ----
        console.print(f"[bold yellow]使用モデル: {config.ANTHROPIC_MODEL}[/bold yellow]")

        # ---- Step 1: J-Quants から大幅変動銘柄を取得 ----
        console.rule("[bold cyan]Step 1: J-Quants データ取得")
        with console.status("[cyan]株価データを取得中...", spinner="dots"):
            if target_date is None:
                target_date = self._jquants._last_business_day()
            movers: list[DailyQuote] = self._jquants.get_large_movers(
                target_date=target_date, threshold=threshold
            )

        target_date_str = target_date.strftime("%Y-%m-%d")

        # 変動率の大きい順にソート
        movers.sort(key=lambda q: abs(q.change_rate or 0), reverse=True)
        if max_stocks:
            movers = movers[:max_stocks]

        if not movers:
            console.print(f"[yellow]{target_date_str} の ±{threshold:.0f}% 以上の銘柄は見つかりませんでした。[/yellow]")
            return ""

        # 銘柄一覧テーブル表示
        table = Table(title=f"{target_date_str}  前日比 ±{threshold:.0f}%以上  {len(movers)}銘柄")
        table.add_column("証券コード", style="cyan")
        table.add_column("終値", justify="right")
        table.add_column("前日比", justify="right")
        for q in movers:
            rate_str = f"{q.change_rate:+.2f}%"
            style = "red" if (q.change_rate or 0) > 0 else "blue"
            table.add_row(q.code, f"{q.close:,.0f}" if q.close else "-", f"[{style}]{rate_str}[/{style}]")
        console.print(table)

        # ---- Step 2: 各銘柄の変動理由を Web 調査 ----
        console.rule("[bold cyan]Step 2: Web検索で変動理由を調査")
        results: list[ResearchResult] = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("調査中...", total=len(movers))
            for q in movers:
                rate_str = f"{q.change_rate:+.1f}%"
                progress.update(task, description=f"[cyan]{q.code}[/cyan] ({rate_str}) を調査中...")
                try:
                    result = self._researcher.research(
                        code=q.code,
                        change_rate=q.change_rate or 0,
                        target_date=target_date_str,
                    )
                    results.append(result)
                    console.log(f"[green]OK[/green] {q.code} ({result.name}) 調査完了")
                except Exception as e:
                    console.log(f"[red]NG {q.code} 調査失敗: {e}[/red]")
                    results.append(ResearchResult(
                        code=q.code,
                        name=q.code,
                        change_rate=q.change_rate or 0,
                        summary="調査中にエラーが発生しました。",
                    ))
                progress.advance(task)
                # レートリミット対策: 銘柄間に待機
                if q != movers[-1]:
                    time.sleep(20)

        # ---- Step 3: レポート生成 ----
        console.rule("[bold cyan]Step 3: Markdownレポート生成")
        report_path = self._reporter.generate_screener_report(
            results=results,
            target_date=target_date_str,
        )
        console.print(f"\n[bold green]レポート生成完了: {report_path}[/bold green]")
        return str(report_path)

    def close(self) -> None:
        self._jquants.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
