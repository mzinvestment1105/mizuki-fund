import os
import time
from pathlib import Path

import pandas as pd

import jquantsapi


REQUIRED_PRICE_COLS = ["Date", "Code", "O", "H", "L", "C", "Vo"]
REQUEST_SLEEP_SECONDS = 1.0
MAX_RETRIES_PER_CODE = 6
LIMIT_CODES = int(os.environ.get("JQ_LIMIT_CODES", "0").strip() or "0")


def _normalize_code_4(code: object) -> str:
    """
    J-Quants v2 は銘柄コードが5桁（例: 72030）で返ることがあるため、4桁に丸める。
    """
    s = str(code).strip()
    return s[:4] if len(s) >= 4 else s


def _is_4digit_code(code4: str) -> bool:
    s = str(code4).strip()
    return len(s) == 4 and s.isdigit()


def _fetch_daily_with_backoff(client: jquantsapi.ClientV2, code4: str) -> pd.DataFrame:
    """
    429 (rate limit) を踏んだ場合に待機しながら再試行する。
    """
    backoff = 2.0
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES_PER_CODE + 1):
        try:
            # polite pacing (even on first attempt)
            time.sleep(REQUEST_SLEEP_SECONDS)
            return client.get_eq_bars_daily(code=code4)
        except Exception as e:
            last_err = e
            msg = str(e)
            # jquantsapi は urllib3 の RetryError を返すことがある
            is_rate_limited = (" 429 " in msg) or ("too many 429" in msg.lower())
            if not is_rate_limited:
                raise

            wait = min(120.0, backoff)
            print(f"rate limited (429): {code4} attempt {attempt}/{MAX_RETRIES_PER_CODE} wait {wait:.1f}s")
            time.sleep(wait)
            backoff *= 2.0

    assert last_err is not None
    raise last_err


def main() -> None:
    api_key = os.environ.get("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise ValueError(
            "JQUANTS_API_KEY が未設定です。"
            "ダッシュボードで発行した API Key を環境変数 JQUANTS_API_KEY に設定してください。"
        )

    universe_path = Path("data") / "universe" / "prime_list.csv"
    if not universe_path.exists():
        raise FileNotFoundError(
            f"{universe_path} が見つかりません。先に make_prime_universe.py を実行してください。"
        )

    universe = pd.read_csv(universe_path, dtype={"Code": "string"}, encoding="utf-8-sig")
    codes = (
        universe["Code"]
        .astype("string")
        .dropna()
        .map(lambda x: str(x).strip())
        .loc[lambda s: s != ""]
        .map(_normalize_code_4)
        .map(lambda s: str(s).strip())
        .drop_duplicates()
        .tolist()
    )
    if LIMIT_CODES > 0:
        codes = codes[:LIMIT_CODES]

    client = jquantsapi.ClientV2(api_key=api_key)

    frames: list[pd.DataFrame] = []
    failures: list[tuple[str, str]] = []
    skipped: list[tuple[str, str]] = []

    total = len(codes)
    for i, code4 in enumerate(codes, start=1):
        try:
            if not _is_4digit_code(code4):
                skipped.append((code4, "skip: non-4digit numeric code"))
                continue
            df = _fetch_daily_with_backoff(client, code4)
            if df is None or df.empty:
                raise RuntimeError("empty result")

            # keep required columns if present
            missing = [c for c in REQUIRED_PRICE_COLS if c not in df.columns]
            if missing:
                raise RuntimeError(f"missing columns: {missing}")

            out = df[REQUIRED_PRICE_COLS].copy()
            out["Code"] = out["Code"].map(_normalize_code_4)
            frames.append(out)
        except Exception as e:
            failures.append((code4, f"{type(e).__name__}: {e}"))
        finally:
            # progress
            if i == 1 or i % 50 == 0 or i == total:
                print(
                    f"progress: {i}/{total} (ok={len(frames)} fail={len(failures)} skip={len(skipped)})"
                )

    if frames:
        all_prices = pd.concat(frames, ignore_index=True)
        all_prices["Date"] = pd.to_datetime(all_prices["Date"], errors="coerce")
        all_prices = all_prices.dropna(subset=["Date"])
        all_prices["Code"] = all_prices["Code"].astype("string")
        all_prices = all_prices.sort_values(["Code", "Date"]).reset_index(drop=True)
    else:
        all_prices = pd.DataFrame(columns=REQUIRED_PRICE_COLS)

    output_path = Path("data") / "raw" / "prime_prices.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    all_prices.to_parquet(output_path, index=False)
    print(f"saved: {output_path} ({len(all_prices)} rows)")

    if skipped:
        head_n = min(20, len(skipped))
        print(f"skipped: {len(skipped)} (showing {head_n})")
        for code4, msg in skipped[:head_n]:
            print(f"- {code4}: {msg}")

    if failures:
        # show only the first few to keep output readable
        head_n = min(20, len(failures))
        print(f"failures: {len(failures)} (showing {head_n})")
        for code4, msg in failures[:head_n]:
            print(f"- {code4}: {msg}")


if __name__ == "__main__":
    main()

