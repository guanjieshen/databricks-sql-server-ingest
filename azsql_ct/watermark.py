"""JSON-backed watermark store for change-tracking sync state.

Each table directory contains two files:

* ``watermarks.json`` -- latest state per table (overwritten each sync).
* ``sync_history.jsonl`` -- append-only audit log (one JSON line per sync).

Legacy files that store a bare integer (``{table: version}``) are read
transparently and upgraded on the next write.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

_FILENAME = "watermarks.json"
_HISTORY_FILENAME = "sync_history.jsonl"


def _path(output_dir: str) -> str:
    return os.path.join(output_dir, _FILENAME)


def _history_path(output_dir: str) -> str:
    return os.path.join(output_dir, _HISTORY_FILENAME)


def _normalize_entry(value: Any) -> dict:
    """Accept either a rich dict or a legacy bare int and return a dict."""
    if isinstance(value, dict):
        return value
    return {"version": value}


def _build_entry(
    table_name: str,
    version: int,
    *,
    since_version: Optional[int] = None,
    rows_synced: Optional[int] = None,
    mode: Optional[str] = None,
    files: Optional[List[str]] = None,
    duration_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    return {
        "table": table_name,
        "version": version,
        "since_version": since_version,
        "rows_synced": rows_synced,
        "mode": mode,
        "files": files or [],
        "duration_seconds": round(duration_seconds, 2) if duration_seconds is not None else None,
        "synced_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ── read ─────────────────────────────────────────────────────────────────────


def load_all(output_dir: str) -> Dict[str, dict]:
    """Return the full ``{table: metadata_dict}`` mapping, or ``{}``."""
    p = _path(output_dir)
    if not os.path.isfile(p):
        return {}
    with open(p) as f:
        raw = json.load(f)
    return {k: _normalize_entry(v) for k, v in raw.items()}


def get(output_dir: str, table_name: str) -> Optional[int]:
    """Return the last synced version for *table_name*, or ``None``."""
    entry = load_all(output_dir).get(table_name)
    if entry is None:
        return None
    return entry.get("version")


def get_metadata(output_dir: str, table_name: str) -> Optional[dict]:
    """Return the full metadata dict for *table_name*, or ``None``."""
    return load_all(output_dir).get(table_name)


def load_history(output_dir: str) -> List[dict]:
    """Return all history entries from ``sync_history.jsonl``, oldest first."""
    p = _history_path(output_dir)
    if not os.path.isfile(p):
        return []
    entries: List[dict] = []
    with open(p) as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries


# ── write ────────────────────────────────────────────────────────────────────


def save(
    output_dir: str,
    table_name: str,
    version: int,
    *,
    since_version: Optional[int] = None,
    rows_synced: Optional[int] = None,
    mode: Optional[str] = None,
    files: Optional[List[str]] = None,
    duration_seconds: Optional[float] = None,
) -> None:
    """Persist watermark metadata for *table_name* and append to history."""
    entry = _build_entry(
        table_name, version,
        since_version=since_version,
        rows_synced=rows_synced,
        mode=mode,
        files=files,
        duration_seconds=duration_seconds,
    )

    os.makedirs(output_dir, exist_ok=True)

    data = load_all(output_dir)
    watermark_entry = {k: v for k, v in entry.items() if k != "table"}
    data[table_name] = watermark_entry
    p = _path(output_dir)
    with open(p, "wb") as f:
        f.write(json.dumps(data, indent=2).encode("utf-8"))
        f.write(b"\n")

    hp = _history_path(output_dir)
    existing = b""
    if os.path.isfile(hp):
        with open(hp, "rb") as f:
            existing = f.read()
    with open(hp, "wb") as f:
        f.write(existing)
        f.write(json.dumps(entry).encode("utf-8"))
        f.write(b"\n")
