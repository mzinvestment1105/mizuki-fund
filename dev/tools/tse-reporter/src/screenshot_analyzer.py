"""スクリーンショット解析モジュール（機能1）

Claude Code CLI 経由で Vision 解析を行う（Anthropic API クレジット不要）。
"""

from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from .claude_cli import run_claude


@dataclass
class StockInfo:
    """スクリーンショットから抽出した株価情報"""
    code: Optional[str] = None          # 証券コード（例: 7203）
    name: Optional[str] = None          # 銘柄名（例: トヨタ自動車）
    change_rate: Optional[float] = None # 前日比（%）
    current_price: Optional[float] = None
    raw_text: str = ""                  # Claude が読み取った生テキスト


class ScreenshotAnalyzer:
    """Claude CLI を使って株価スクリーンショットを解析する"""

    PROMPT = (
        "この株価スクリーンショットから情報を抽出してください。\n"
        "以下の形式で回答してください：\n"
        "証券コード: <コード または null>\n"
        "銘柄名: <名前 または null>\n"
        "現在株価: <価格 または null>\n"
        "前日比: <±XX.X% または null>\n"
        "その他気づいた情報: <自由記述>"
    )

    def analyze(self, image_path: str | Path) -> StockInfo:
        """
        スクリーンショット画像を解析して株価情報を抽出する。

        Args:
            image_path: 画像ファイルパス（PNG / JPG / GIF / WebP）

        Returns:
            StockInfo: 抽出された銘柄情報
        """
        image_path = Path(image_path)
        if not image_path.exists():
            raise FileNotFoundError(f"画像ファイルが見つかりません: {image_path}")

        raw_text = run_claude(self.PROMPT, image_path=image_path)
        return self._parse_response(raw_text)

    def _parse_response(self, text: str) -> StockInfo:
        """Claude の回答テキストをパースして StockInfo に変換する"""
        info = StockInfo(raw_text=text)
        for line in text.splitlines():
            if "証券コード:" in line:
                val = line.split(":", 1)[1].strip()
                if val.lower() != "null":
                    info.code = val.strip()
            elif "銘柄名:" in line:
                val = line.split(":", 1)[1].strip()
                if val.lower() != "null":
                    info.name = val.strip()
            elif "現在株価:" in line:
                val = line.split(":", 1)[1].strip().replace(",", "").replace("円", "")
                try:
                    info.current_price = float(val)
                except ValueError:
                    pass
            elif "前日比:" in line:
                val = line.split(":", 1)[1].strip().replace("%", "").replace("＋", "+")
                try:
                    info.change_rate = float(val)
                except ValueError:
                    pass
        return info
