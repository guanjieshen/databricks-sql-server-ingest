"""Per-table schema file for tracking column metadata.

Each table's watermark directory contains a ``schema.json`` file that
records the cumulative column set and a schema version hash.

Update semantics are **append-only**: new columns are appended, deleted
or renamed source columns are never removed so downstream consumers see
nulls instead of missing fields.  ``schema_version`` is always updated
to the latest value.
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
    columns: List[Dict[str, str]],
    schema_version: int,
) -> None:
    """Write or merge ``schema.json`` with append-only column semantics.

    New column names are appended to the existing list.  Columns that
    disappeared from the source are kept so downstream consumers see
    nulls.  ``schema_version`` and ``updated_at`` are always overwritten.
    """
    os.makedirs(wm_dir, exist_ok=True)

    existing = load(wm_dir)
    existing_cols: List[Dict[str, str]] = existing.get("columns", [])
    existing_names = {c["name"] for c in existing_cols}
    added = [c for c in columns if c["name"] not in existing_names]
    merged = existing_cols + added

    data = {
        "schema_version": schema_version,
        "columns": merged,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
