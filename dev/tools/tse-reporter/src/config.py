"""設定管理モジュール"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def _first_nonempty_env(*names: str) -> str:
    """複数の環境変数名のうち、最初に値があるものを返す"""
    for name in names:
        v = (os.getenv(name) or "").strip()
        if v:
            return v
    return ""


# Windows コンソールを UTF-8 出力に統一
if sys.platform == "win32":
    import io
    if hasattr(sys.stdout, "buffer"):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "buffer"):
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


class Config:
    # Anthropic
    # !! コスト注意 !!
    # claude-opus-4-6  : $5/$25 per 1M tokens  (高精度・高コスト)
    # claude-sonnet-4-6: $3/$15 per 1M tokens
    # claude-haiku-4-5 : $1/$5  per 1M tokens  (推奨)
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
    ANTHROPIC_MODEL: str = "claude-haiku-4-5"

    # J-Quants
    JQUANTS_REFRESH_TOKEN: str = os.getenv("JQUANTS_REFRESH_TOKEN", "")
    JQUANTS_EMAIL: str = os.getenv("JQUANTS_EMAIL", "")
    JQUANTS_PASSWORD: str = os.getenv("JQUANTS_PASSWORD", "")
    JQUANTS_BASE_URL: str = "https://api.jquants.com/v1"

    # Report
    REPORT_OUTPUT_DIR: Path = Path(os.getenv("REPORT_OUTPUT_DIR", "./reports"))
    REPORT_FORMAT: str = os.getenv("REPORT_FORMAT", "markdown")

    # Screening threshold
    PRICE_CHANGE_THRESHOLD: float = 10.0  # ±10%

    # Google Custom Search JSON API（プロトタイプ: 検索材料の取得）
    GOOGLE_API_KEY: str = _first_nonempty_env("GOOGLE_API_KEY", "GOOGLE_CSE_API_KEY")
    GOOGLE_CSE_CX: str = _first_nonempty_env(
        "GOOGLE_CSE_CX",
        "GOOGLE_CSE_ID",
        "GOOGLE_CUSTOM_SEARCH_ENGINE_ID",
        "GOOGLE_SEARCH_ENGINE_ID",
    )

    # 東証プライム・スタンダード・グロース（J-Quants Mkt コード）
    TSE_MOVER_MARKET_CODES: frozenset[str] = frozenset({"0111", "0112", "0113"})

    def validate(self) -> None:
        """必須設定のバリデーション"""
        self.REPORT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def validate_jquants(self) -> None:
        """J-Quants 必須設定のバリデーション（v2: API キーのみ必要）"""
        if not self.JQUANTS_REFRESH_TOKEN:
            raise ValueError("JQUANTS_REFRESH_TOKEN が設定されていません")

    def validate_google_cse(self) -> None:
        """Google Custom Search JSON API 用"""
        if not self.GOOGLE_API_KEY:
            raise ValueError(
                "GOOGLE_API_KEY が設定されていません。.env に API キーを設定するか、"
                "別名 GOOGLE_CSE_API_KEY でも指定できます。"
            )
        if not self.GOOGLE_CSE_CX:
            raise ValueError(
                "検索エンジンID（cx）が設定されていません。.env に次のいずれかを設定してください:\n"
                "  GOOGLE_CSE_CX=xxxxxxxx  または  GOOGLE_CSE_ID=xxxxxxxx\n"
                "（別名: GOOGLE_CUSTOM_SEARCH_ENGINE_ID / GOOGLE_SEARCH_ENGINE_ID）\n"
                "取得手順: https://programmablesearchengine.google.com/ で検索エンジンを作成 → "
                "「設定」→「検索エンジンID」をコピー。JSON API 用に「ウェブ全体を検索」を有効にしてください。"
            )


config = Config()
