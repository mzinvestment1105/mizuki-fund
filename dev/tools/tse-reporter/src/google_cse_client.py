"""Google Custom Search JSON API（検索スニペット取得・プロトタイプ用）"""

import json
from dataclasses import dataclass
from typing import Optional

import httpx

from .config import config

CSE_URL = "https://www.googleapis.com/customsearch/v1"
CSE_API_ENABLE_URL = (
    "https://console.cloud.google.com/apis/library/customsearch.googleapis.com"
)


def _humanize_cse_http_error(resp: httpx.Response) -> str:
    """API からの JSON を短い日本語説明にまとめる（403 の典型例など）"""
    raw = (resp.text or "")[:1200]
    try:
        body = json.loads(resp.text)
        msg = (body.get("error") or {}).get("message") or ""
        status = (body.get("error") or {}).get("status") or ""
        if resp.status_code == 403 and "Custom Search JSON API" in msg:
            return (
                "HTTP 403: この API キーが属する Google Cloud プロジェクトで "
                "「Custom Search API（Custom Search JSON API）」が有効になっていません。\n\n"
                "対処手順:\n"
                f"1. {CSE_API_ENABLE_URL} を開く\n"
                "2. ご利用のプロジェクトを選び「有効にする」\n"
                "3. 「APIとサービス」→「認証情報」で、その**同じプロジェクト**用の API キーを作成／利用する\n"
                "4. .env の GOOGLE_API_KEY をそのキーに差し替える\n\n"
                "（別プロジェクトのキーと検索エンジン cx の組み合わせでも、API 無効だと同様のエラーになります）"
            )
        if resp.status_code == 403 and status == "PERMISSION_DENIED":
            return f"HTTP 403 PERMISSION_DENIED: {msg or raw[:400]}"
        if msg:
            return f"HTTP {resp.status_code}: {msg}"
    except json.JSONDecodeError:
        pass
    return f"HTTP {resp.status_code}: {raw[:500]}"


@dataclass
class SearchHit:
    title: str
    link: str
    snippet: str


class GoogleCseClient:
    """1リクエスト＝1日次クエリとしてカウント（無料枠100/日の目安）"""

    def __init__(self) -> None:
        self.queries_today = 0
        # API 全体が 403（未有効化）のとき、無駄な再試行とレポートの重複を避ける
        self._cached_forbidden_message: Optional[str] = None

    def search(self, query: str, num: int = 8) -> tuple[list[SearchHit], Optional[str]]:
        """
        Custom Search を1回実行する。

        Returns:
            (ヒット一覧, エラーメッセージ) エラー時はヒット空＋メッセージ
        """
        if self._cached_forbidden_message is not None:
            self.queries_today += 1
            return [], (
                "（Custom Search API が利用できないためスキップ — 直前の銘柄と同じ原因です。"
                " Google Cloud で API を有効化してから再実行してください。）"
            )

        num = max(1, min(10, num))
        self.queries_today += 1
        params = {
            "key": config.GOOGLE_API_KEY,
            "cx": config.GOOGLE_CSE_CX,
            "q": query,
            "num": num,
            "lr": "lang_ja",
        }
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(CSE_URL, params=params)
        if resp.status_code != 200:
            err = _humanize_cse_http_error(resp)
            if resp.status_code == 403 and "Custom Search JSON API" in (resp.text or ""):
                self._cached_forbidden_message = err
            return [], err
        data = resp.json()
        items = data.get("items") or []
        hits: list[SearchHit] = []
        for it in items:
            hits.append(
                SearchHit(
                    title=it.get("title") or "",
                    link=it.get("link") or "",
                    snippet=it.get("snippet") or "",
                )
            )
        return hits, None
