"""Brave Search API クライアント（Google CSE の代替）

無料枠: 2,000 クエリ/月
ドキュメント: https://api.search.brave.com/app/documentation/web-search/get-started
"""

from dataclasses import dataclass
from typing import Optional

import httpx

from .config import config

BRAVE_API_URL = "https://api.search.brave.com/res/v1/web/search"


@dataclass
class SearchHit:
    title: str
    link: str
    snippet: str


class BraveSearchClient:
    """Brave Search API を使った Web 検索クライアント"""

    def __init__(self) -> None:
        self.queries_this_month = 0

    def search(self, query: str, num: int = 8) -> tuple[list[SearchHit], Optional[str]]:
        """
        Brave Search API で検索する。

        Returns:
            (ヒット一覧, エラーメッセージ) エラー時はヒット空＋メッセージ
        """
        num = max(1, min(20, num))
        self.queries_this_month += 1

        headers = {
            "X-Subscription-Token": config.BRAVE_API_KEY,
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
        }
        params = {
            "q": query,
            "count": num,
            "search_lang": "ja",
            "country": "JP",
            "safesearch": "off",
        }

        try:
            with httpx.Client(timeout=30.0) as client:
                resp = client.get(BRAVE_API_URL, headers=headers, params=params)
        except httpx.RequestError as e:
            return [], f"接続エラー: {e}"

        if resp.status_code != 200:
            return [], f"HTTP {resp.status_code}: {resp.text[:300]}"

        data = resp.json()
        results = (data.get("web") or {}).get("results") or []

        hits: list[SearchHit] = []
        for item in results:
            hits.append(SearchHit(
                title=item.get("title") or "",
                link=item.get("url") or "",
                snippet=item.get("description") or "",
            ))
        return hits, None
