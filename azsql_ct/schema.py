"""Per-table schema file for tracking column metadata.

Each table's watermark directory contains a ``schema.json`` file that
records the cumulative column set and a schema version hash.

Update semantics are **append-only** for column names: new columns are
appended, removed or renamed source columns are never deleted so
downstream consumers see nulls instead of missing fields.

Column **types** are updated in-place when a source type change is
detected; the previous type is preserved as ``previous_type`` for
auditability.  Each column carries a ``last_seen`` timestamp indicating
the most recent sync that included it.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .watermark import _atomic_write
from .writer import _CT_COLUMNS

logger = logging.getLogger(__name__)

_FILENAME = "schema.json"

MSSQL_TYPE_MAP: Dict[type, str] = {
    int: "int",
    str: "nvarchar",
    float: "float",
    bool: "bit",
    bytes: "varbinary",
}

try:
    import decimal
    MSSQL_TYPE_MAP[decimal.Decimal] = "decimal"
except ImportError:
    pass

try:
    import uuid as _uuid
    MSSQL_TYPE_MAP[_uuid.UUID] = "uniqueidentifier"
except ImportError:
    pass

try:
    from datetime import date as _date, datetime as _datetime
    MSSQL_TYPE_MAP[_datetime] = "datetime2"
    MSSQL_TYPE_MAP[_date] = "date"
except ImportError:
    pass


def _path(wm_dir: str) -> str:
    return os.path.join(wm_dir, _FILENAME)


def load(wm_dir: str) -> Dict[str, Any]:
    """Load ``schema.json`` from a per-table watermark directory.

    Returns ``{}`` if the file is missing.
    """
    p = _path(wm_dir)
    if not os.path.isfile(p):
        return {}
    with open(p) as f:
        return json.load(f)


def save(
    wm_dir: str,
    columns: List[Dict[str, Any]],
    schema_version: int,
) -> None:
    """Write or merge ``schema.json`` with append-only column semantics.

    New column names are appended to the existing list.  Columns that
    disappeared from the source are kept so downstream consumers see
    nulls.  ``schema_version`` and ``updated_at`` are always overwritten.

    Column dicts may contain optional keys beyond ``name`` and ``type``
    (e.g. ``precision``, ``scale``).  For existing columns, extra keys
    from newer syncs are merged in without overwriting existing values.

    If a column's ``type`` changes, the new type replaces the old one
    and ``previous_type`` is recorded.  Stale ``precision``/``scale``
    are cleared and re-applied from the incoming column dict.

    Columns present in the incoming batch receive a ``last_seen``
    timestamp.  Columns no longer in the source retain their previous
    ``last_seen`` value.
    """
    os.makedirs(wm_dir, exist_ok=True)

    existing = load(wm_dir)
    existing_cols: List[Dict[str, Any]] = existing.get("columns", [])
    existing_by_name = {c["name"]: c for c in existing_cols}

    changed = False
    for c in columns:
        if c["name"] in existing_by_name:
            dest = existing_by_name[c["name"]]

            old_type = dest.get("type")
            new_type = c.get("type")
            if old_type and new_type and old_type != new_type:
                logger.warning(
                    "Column %r type changed: %s -> %s",
                    c["name"], old_type, new_type,
                )
                dest["previous_type"] = old_type
                dest["type"] = new_type
                for numeric_key in ("precision", "scale"):
                    dest.pop(numeric_key, None)
                    if numeric_key in c:
                        dest[numeric_key] = c[numeric_key]
                changed = True
            else:
                for numeric_key in ("precision", "scale"):
                    new_val = c.get(numeric_key)
                    if new_val is not None and dest.get(numeric_key) != new_val:
                        dest[numeric_key] = new_val
                        changed = True

            for key, val in c.items():
                if key not in dest:
                    dest[key] = val
                    changed = True
        else:
            existing_cols.append(c)
            changed = True
    merged = existing_cols

    if not changed and existing.get("schema_version") == schema_version:
        return

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    active_names = {c["name"] for c in columns}
    for col in merged:
        if col["name"] in active_names:
            col["last_seen"] = now

    data = {
        "schema_version": schema_version,
        "columns": merged,
        "updated_at": now,
    }
    payload = json.dumps(data, indent=2).encode("utf-8") + b"\n"
    _atomic_write(_path(wm_dir), payload)


def columns_from_description(
    description: Sequence[Tuple[str, ...]],
) -> List[Dict[str, str]]:
    """Convert ``cursor.description`` to a column list for the schema file.

    Maps Python type objects to SQL Server type names via
    :data:`MSSQL_TYPE_MAP`.  Filters out change-tracking metadata columns.
    """
    if not description:
        return []
    cols: List[Dict[str, str]] = []
    for col in description:
        name = col[0]
        if name in _CT_COLUMNS:
            continue
        if len(col) > 1 and col[1] is not None:
            type_obj = col[1]
            type_name = MSSQL_TYPE_MAP.get(type_obj, str(type_obj))
        else:
            type_name = "unknown"
        cols.append({"name": name, "type": type_name})
    return cols
