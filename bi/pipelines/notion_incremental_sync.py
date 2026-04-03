"""Incremental Notion database sync (single DB only).

Designed to avoid broad reads:
- Queries exactly one Notion database ID
- Uses last_edited_time filter (incremental)
- Does not fetch block children (no page body)
- Optional: restrict exported property columns via NOTION_SYNC_PROPERTIES
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

NOTION_VERSION = "2022-06-28"


def _default_paths() -> tuple[Path, Path]:
    pipelines = Path(__file__).resolve().parent
    out = pipelines.parent / "outputs"
    return (
        out / "notion_sync_state.json",
        out / "notion_db_incremental.parquet",
    )


def _load_dotenv(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def _read_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_state(state_path: Path, state: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _as_iso(dt: datetime) -> str:
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _extract_property_value(prop: dict[str, Any]) -> Any:
    p_type = prop.get("type")
    if p_type == "title":
        return "".join((x.get("plain_text") or "") for x in prop.get("title", []))
    if p_type == "rich_text":
        return "".join((x.get("plain_text") or "") for x in prop.get("rich_text", []))
    if p_type == "number":
        return prop.get("number")
    if p_type == "checkbox":
        return prop.get("checkbox")
    if p_type == "select":
        sel = prop.get("select")
        return sel.get("name") if isinstance(sel, dict) else None
    if p_type == "multi_select":
        return ",".join((x.get("name") or "") for x in prop.get("multi_select", []))
    if p_type == "status":
        st = prop.get("status")
        return st.get("name") if isinstance(st, dict) else None
    if p_type == "date":
        d = prop.get("date")
        return (d or {}).get("start")
    if p_type == "url":
        return prop.get("url")
    if p_type == "email":
        return prop.get("email")
    if p_type == "phone_number":
        return prop.get("phone_number")
    return None


def _query_database(
    token: str,
    database_id: str,
    edited_after_iso: str,
) -> list[dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    payload: dict[str, Any] = {
        "page_size": 100,
        "filter": {
            "timestamp": "last_edited_time",
            "last_edited_time": {"on_or_after": edited_after_iso},
        },
        "sorts": [{"timestamp": "last_edited_time", "direction": "ascending"}],
    }

    results: list[dict[str, Any]] = []
    while True:
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        body = resp.json()
        results.extend(body.get("results", []))
        if not body.get("has_more"):
            break
        payload["start_cursor"] = body.get("next_cursor")
    return results


def _pages_to_df(pages: list[dict[str, Any]], keep_props: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for p in pages:
        row: dict[str, Any] = {
            "id": p.get("id"),
            "created_time": p.get("created_time"),
            "last_edited_time": p.get("last_edited_time"),
            "archived": p.get("archived"),
            "url": p.get("url"),
        }
        props = p.get("properties", {})
        for key, val in props.items():
            if keep_props and key not in keep_props:
                continue
            row[key] = _extract_property_value(val)
        rows.append(row)
    if not rows:
        return pd.DataFrame(
            columns=["id", "created_time", "last_edited_time", "archived", "url"]
        )
    df = pd.DataFrame(rows)
    if "last_edited_time" in df.columns:
        df["last_edited_time"] = pd.to_datetime(
            df["last_edited_time"], utc=True, errors="coerce"
        )
    return df


def _merge_incremental(output_path: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    if output_path.exists():
        old_df = pd.read_parquet(output_path)
        if "last_edited_time" in old_df.columns:
            old_df["last_edited_time"] = pd.to_datetime(
                old_df["last_edited_time"], utc=True, errors="coerce"
            )
        merged = pd.concat([old_df, new_df], ignore_index=True)
        merged = merged.sort_values("last_edited_time").drop_duplicates(
            subset=["id"], keep="last"
        )
        return merged
    return new_df


def main() -> None:
    here = Path(__file__).resolve().parent
    _load_dotenv(here / ".env")

    token = os.getenv("NOTION_API_TOKEN", "").strip()
    database_id = os.getenv("NOTION_DATABASE_ID", "").strip()
    if not token:
        raise RuntimeError("NOTION_API_TOKEN is not set. Set it in bi/pipelines/.env")
    if not database_id:
        raise RuntimeError("NOTION_DATABASE_ID is not set. Set it in bi/pipelines/.env")

    def_state, def_out = _default_paths()
    state_path = Path(
        os.getenv("NOTION_SYNC_STATE_PATH", str(def_state))
    )
    output_path = Path(
        os.getenv("NOTION_SYNC_OUTPUT_PATH", str(def_out))
    )
    if not state_path.is_absolute():
        state_path = (here / state_path).resolve()
    if not output_path.is_absolute():
        output_path = (here / output_path).resolve()
    keep_props_env = os.getenv("NOTION_SYNC_PROPERTIES", "").strip()
    keep_props = [x.strip() for x in keep_props_env.split(",") if x.strip()]

    state = _read_state(state_path)
    last_synced = state.get("last_synced_utc")
    if last_synced:
        edited_after = last_synced
    else:
        initial_days = int(os.getenv("NOTION_INITIAL_DAYS", "30"))
        edited_after = _as_iso(
            datetime.now(timezone.utc) - timedelta(days=initial_days)
        )

    pages = _query_database(
        token=token, database_id=database_id, edited_after_iso=edited_after
    )
    new_df = _pages_to_df(pages, keep_props=keep_props)

    now_iso = _as_iso(datetime.now(timezone.utc))

    if not new_df.empty:
        merged = _merge_incremental(output_path, new_df)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(output_path, index=False)
        max_edited = pd.to_datetime(
            merged["last_edited_time"], utc=True, errors="coerce"
        ).max()
        last_synced_out = (
            _as_iso(max_edited.to_pydatetime())
            if pd.notna(max_edited)
            else now_iso
        )
    elif last_synced:
        last_synced_out = last_synced
    else:
        # 初回で0件のときは「今」を基準にし、以降は差分のみ取得
        last_synced_out = now_iso

    state_out = {
        "database_id": database_id,
        "last_synced_utc": last_synced_out,
        "last_run_utc": now_iso,
        "last_fetch_count": int(len(pages)),
        "output_path": str(output_path),
    }
    _write_state(state_path, state_out)

    print(f"Fetched pages: {len(pages)}")
    print(f"Output: {output_path}")
    print(f"State: {state_path}")


if __name__ == "__main__":
    main()
