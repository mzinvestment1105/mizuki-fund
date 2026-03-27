"""J-Quants API v2 クライアント"""

import httpx
from datetime import date, timedelta
from dataclasses import dataclass
from typing import Optional

from .config import config


@dataclass
class DailyQuote:
    """日次株価データ"""
    code: str
    date: str
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[float]
    change_rate: Optional[float]  # 前日比（%）
    name: Optional[str] = None  # マスタ照合時に付与


@dataclass
class TopMoversBundle:
    """東証（プライム・スタンダード・グロース）の上昇・下落トップN"""
    target_date: str
    gainers: list[DailyQuote]
    losers: list[DailyQuote]
    # 診断用（空のときの原因調査）
    raw_daily_count: int = 0
    tse_master_rows: int = 0
    matched_mover_count: int = 0


class JQuantsClient:
    """J-Quants API v2 クライアント（x-api-key ヘッダー認証）"""

    BASE_URL = "https://api.jquants.com/v2"

    def __init__(self):
        self._client = httpx.Client(timeout=30.0)

    def authenticate(self) -> None:
        """v2 は x-api-key ヘッダーで直接認証するため事前処理不要。設定確認のみ実行。"""
        config.validate_jquants()

    def _headers(self) -> dict:
        return {"x-api-key": config.JQUANTS_REFRESH_TOKEN}

    def _fetch_quotes_raw(self, target_date: date) -> list[dict]:
        """指定日の生データを返す（v2 フィールド: Date/Code/O/H/L/C/AdjC 等）"""
        resp = self._client.get(
            f"{self.BASE_URL}/equities/bars/daily",
            headers=self._headers(),
            params={"date": target_date.strftime("%Y-%m-%d")},
        )
        resp.raise_for_status()
        return resp.json().get("data", [])

    def get_daily_quotes(self, target_date: Optional[date] = None) -> list[DailyQuote]:
        """
        指定日の全銘柄日次株価を取得する。
        前日比は当日・前営業日の調整済終値から計算する。
        """
        if target_date is None:
            target_date = self._last_business_day()

        today_rows = self._fetch_quotes_raw(target_date)
        if not today_rows:
            return []

        # 前営業日データを取得して前日比を計算
        prev_date = self._prev_business_day(target_date)
        prev_rows = self._fetch_quotes_raw(prev_date)
        prev_close: dict[str, float] = {}
        for row in prev_rows:
            if row.get("AdjC") is None:
                continue
            rc = row["Code"]
            nc = rc[:-1] if len(rc) == 5 and rc.endswith("0") else rc
            prev_close[nc] = row["AdjC"]

        quotes = []
        for item in today_rows:
            raw_code = item.get("Code", "")
            # J-Quants は4桁コードを末尾に0を付けた5桁で返す（例: 7203 → 72030）
            code = raw_code[:-1] if len(raw_code) == 5 and raw_code.endswith("0") else raw_code
            adj_close = item.get("AdjC")
            prev_c = prev_close.get(code)

            change_rate: Optional[float] = None
            if adj_close is not None and prev_c and prev_c != 0:
                change_rate = round((adj_close / prev_c - 1) * 100, 2)

            quotes.append(
                DailyQuote(
                    code=code,
                    date=item.get("Date", target_date.strftime("%Y-%m-%d")),
                    open=item.get("AdjO"),
                    high=item.get("AdjH"),
                    low=item.get("AdjL"),
                    close=adj_close,
                    volume=item.get("AdjVo"),
                    change_rate=change_rate,
                    name=None,
                )
            )
        return quotes

    def get_large_movers(
        self,
        target_date: Optional[date] = None,
        threshold: float = 10.0,
    ) -> list[DailyQuote]:
        """前日比 ±threshold% 以上の銘柄を返す"""
        quotes = self.get_daily_quotes(target_date)
        return [
            q for q in quotes
            if q.change_rate is not None and abs(q.change_rate) >= threshold
        ]

    def _fetch_master_all(self, target_date: date) -> list[dict]:
        """指定日時点の上場銘柄マスタ（ページネーション対応）"""
        rows: list[dict] = []
        pagination_key: Optional[str] = None
        params_base: dict = {"date": target_date.strftime("%Y-%m-%d")}
        while True:
            params = dict(params_base)
            if pagination_key:
                params["pagination_key"] = pagination_key
            resp = self._client.get(
                f"{self.BASE_URL}/equities/master",
                headers=self._headers(),
                params=params,
            )
            resp.raise_for_status()
            body = resp.json()
            rows.extend(body.get("data") or [])
            pagination_key = body.get("pagination_key")
            if not pagination_key:
                break
        return rows

    def get_tse_top_movers(
        self,
        target_date: Optional[date] = None,
        top_n: int = 20,
    ) -> TopMoversBundle:
        """
        東証プライム・スタンダード・グロースに限定し、
        前日比の上昇トップ top_n・下落トップ top_n を返す。
        """
        if target_date is None:
            target_date = self._last_business_day()

        master = self._fetch_master_all(target_date)
        tse_codes: dict[str, str] = {}
        tse_master_rows = 0
        for row in master:
            raw = (row.get("Code") or "").strip()
            # 5桁末尾0 → 4桁に正規化
            code = raw[:-1] if len(raw) == 5 and raw.endswith("0") else raw
            mkt = row.get("Mkt") or ""
            if mkt not in config.TSE_MOVER_MARKET_CODES:
                continue
            name = row.get("CoName") or ""
            if code:
                tse_master_rows += 1
                tse_codes[code] = name

        quotes = self.get_daily_quotes(target_date)
        raw_daily_count = len(quotes)
        filtered: list[DailyQuote] = []

        def _resolve_name(qc: str) -> Optional[str]:
            return tse_codes.get(qc)

        for q in quotes:
            qc = (q.code or "").strip()
            nm = _resolve_name(qc)
            if nm is None:
                continue
            if q.change_rate is None:
                continue
            if q.change_rate == 0:
                continue
            filtered.append(
                DailyQuote(
                    code=q.code,
                    date=q.date,
                    open=q.open,
                    high=q.high,
                    low=q.low,
                    close=q.close,
                    volume=q.volume,
                    change_rate=q.change_rate,
                    name=nm,
                )
            )

        gainers = [q for q in filtered if q.change_rate > 0]
        losers = [q for q in filtered if q.change_rate < 0]
        gainers.sort(key=lambda x: x.change_rate or 0, reverse=True)
        losers.sort(key=lambda x: x.change_rate or 0)

        return TopMoversBundle(
            target_date=target_date.strftime("%Y-%m-%d"),
            gainers=gainers[:top_n],
            losers=losers[:top_n],
            raw_daily_count=raw_daily_count,
            tse_master_rows=tse_master_rows,
            matched_mover_count=len(filtered),
        )

    @staticmethod
    def _last_business_day() -> date:
        """今日から遡って直近の営業日（土日を除く簡易版）"""
        d = date.today() - timedelta(days=1)
        while d.weekday() >= 5:  # 5=土, 6=日
            d -= timedelta(days=1)
        return d

    @staticmethod
    def _prev_business_day(d: date) -> date:
        """指定日の前営業日（土日を除く簡易版）"""
        prev = d - timedelta(days=1)
        while prev.weekday() >= 5:
            prev -= timedelta(days=1)
        return prev

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
