"""SQL helpers for Azure SQL change-tracking queries.

All database-specific query logic is isolated here so it can be tested
independently from the sync orchestration and I/O layers.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Union


def list_tracked_tables(cursor: Any) -> List[str]:
    """Return fully-qualified names of all change-tracked tables."""
    cursor.execute(
        "SELECT OBJECT_SCHEMA_NAME(t.object_id) + '.' + OBJECT_NAME(t.object_id) "
        "FROM sys.change_tracking_tables t ORDER BY 1"
    )
    return [row[0] for row in cursor.fetchall()]


def build_tracked_lookup(tracked: List[str]) -> Dict[str, str]:
    """Build a case-insensitive lookup dict from a tracked-table list.

    Returns ``{lowercase_name: original_name}`` for O(1) resolution.
    """
    return {t.lower(): t for t in tracked}


def resolve_table(
    name: str, tracked: "Union[List[str], Dict[str, str]]",
) -> Optional[str]:
    """Match a user-supplied name against tracked tables (case-insensitive).

    *tracked* may be a list (legacy, O(n) scan) or a dict from
    :func:`build_tracked_lookup` (O(1) lookup).
    """
    want = name.strip().lower()
    if "." not in want:
        want = "dbo." + want
    if isinstance(tracked, dict):
        return tracked.get(want)
    for t in tracked:
        if t.lower() == want:
            return t
    return None


def current_version(cursor: Any) -> int:
    """Return the current change-tracking version for the database."""
    cursor.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()")
    return cursor.fetchone()[0]


def min_valid_version_for_table(cursor: Any, full_table_name: str) -> int:
    """Return the minimum valid change-tracking version for *full_table_name*."""
    cursor.execute(
        "SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(?))",
        (full_table_name,),
    )
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else 0


def primary_key_columns(cursor: Any, full_table_name: str) -> List[str]:
    """Return the primary-key column names for *full_table_name*."""
    cursor.execute(
        """
        SELECT c.name
        FROM sys.indexes i
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.is_primary_key = 1
          AND i.object_id = OBJECT_ID(?)
        ORDER BY ic.key_ordinal
        """,
        (full_table_name,),
    )
    return [row[0] for row in cursor.fetchall()]


def build_full_query(full_table_name: str) -> str:
    """Return the SQL for a full-table SELECT with CT metadata columns.

    Produces the same column schema as :func:`build_incremental_query` so
    that downstream consumers always see a consistent set of columns
    regardless of sync mode.
    """
    return (
        f"SELECT CHANGE_TRACKING_CURRENT_VERSION() AS SYS_CHANGE_VERSION, "
        f"CAST(NULL AS BIGINT) AS SYS_CHANGE_CREATION_VERSION, "
        f"CAST('L' AS NCHAR(1)) AS SYS_CHANGE_OPERATION, "
        f"t.* "
        f"FROM {full_table_name} AS t"
    )


def table_columns(cursor: Any, full_table_name: str) -> List[str]:
    """Return the column names for *full_table_name* (in table-definition order)."""
    cursor.execute(f"SELECT TOP 0 * FROM {full_table_name}")
    return [col[0] for col in cursor.description]


def _bracket_quote(name: str) -> str:
    """Bracket-quote a SQL Server identifier, escaping embedded ``]``."""
    return f"[{name.replace(']', ']]')}]"


def build_incremental_query(
    full_table_name: str, pk_cols: List[str], all_cols: List[str],
) -> str:
    """Return the SQL for an incremental change-tracking SELECT.

    The query expects a single ``?`` parameter for the *since_version*.
    Column types are cast to match :func:`build_full_query` so that initial
    and incremental Parquet outputs have identical schemas (avoids downstream
    schema merge errors).

    PK columns are selected via ``COALESCE(t.[col], ct.[col])`` so that
    DELETE records (where the source row is gone and ``t.*`` is all NULLs)
    still carry the primary-key values from the change-tracking table.
    """
    pk_lower = {c.lower() for c in pk_cols}
    col_exprs = []
    for c in all_cols:
        q = _bracket_quote(c)
        if c.lower() in pk_lower:
            col_exprs.append(f"COALESCE(t.{q}, ct.{q}) AS {q}")
        else:
            col_exprs.append(f"t.{q}")
    data_cols = ", ".join(col_exprs)
    join_cond = " AND ".join(
        f"t.{_bracket_quote(c)} = ct.{_bracket_quote(c)}" for c in pk_cols
    )
    return (
        f"SELECT ct.SYS_CHANGE_VERSION, "
        f"CAST(ct.SYS_CHANGE_CREATION_VERSION AS BIGINT) AS SYS_CHANGE_CREATION_VERSION, "
        f"CAST(ct.SYS_CHANGE_OPERATION AS NCHAR(1)) AS SYS_CHANGE_OPERATION, "
        f"{data_cols} "
        f"FROM CHANGETABLE(CHANGES {full_table_name}, ?) AS ct "
        f"LEFT JOIN {full_table_name} AS t ON {join_cond}"
    )


def build_change_check_query(table_watermarks: Dict[str, int]) -> str:
    """Build a UNION ALL query that returns table names having changes since their version.

    Each subquery uses EXISTS(SELECT 1 FROM CHANGETABLE(CHANGES table, version))
    so SQL Server stops at the first change record per table.
    Returns empty string if table_watermarks is empty.
    """
    if not table_watermarks:
        return ""
    parts = []
    for full_table_name, since_version in table_watermarks.items():
        parts.append(
            f"SELECT '{full_table_name}' AS table_name "
            f"WHERE EXISTS(SELECT 1 FROM CHANGETABLE(CHANGES {full_table_name}, {since_version}) AS ct)"
        )
    return " UNION ALL ".join(parts)


def fetch_tables_with_changes(cursor: Any, table_watermarks: Dict[str, int]) -> Set[str]:
    """Execute the change-check query and return set of table names that have changes."""
    if not table_watermarks:
        return set()
    sql = build_change_check_query(table_watermarks)
    cursor.execute(sql)
    return {row[0] for row in cursor.fetchall()}


def column_metadata(cursor: Any, full_table_name: str) -> List[Dict[str, Any]]:
    """Return column names and native SQL Server types for *full_table_name*.

    Queries ``INFORMATION_SCHEMA.COLUMNS`` so that downstream consumers
    receive the real SQL Server type name (e.g. ``bigint``, ``varchar``,
    ``money``) instead of the lossy Python type inferred from
    ``cursor.description``.

    Each returned dict contains ``name`` and ``type``; ``precision`` and
    ``scale`` are included when the column has non-NULL numeric metadata.
    """
    schema_name, table_name = full_table_name.split(".", 1)
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
        "ORDER BY ORDINAL_POSITION",
        (schema_name, table_name),
    )
    columns: List[Dict[str, Any]] = []
    for row in cursor.fetchall():
        col: Dict[str, Any] = {"name": row[0], "type": row[1]}
        if row[2] is not None:
            col["precision"] = row[2]
            col["scale"] = row[3]
        columns.append(col)
    return columns


def fetch_all_ct_metadata(cursor: Any) -> Dict[str, Any]:
    """Fetch DB version, tracked table names, and min valid versions in one query.

    Returns ``{"db_version": int, "tables": {full_name: min_version, ...}}``.
    Replaces separate calls to :func:`list_tracked_tables`,
    :func:`current_version`, and :func:`min_valid_versions_batch`.
    """
    cursor.execute(
        "SELECT CHANGE_TRACKING_CURRENT_VERSION() AS db_version, "
        "OBJECT_SCHEMA_NAME(t.object_id) + '.' + OBJECT_NAME(t.object_id) AS table_name, "
        "CHANGE_TRACKING_MIN_VALID_VERSION(t.object_id) AS min_version "
        "FROM sys.change_tracking_tables t "
        "ORDER BY table_name"
    )
    rows = cursor.fetchall()
    if not rows:
        return {"db_version": 0, "tables": {}}
    return {"db_version": rows[0][0], "tables": {r[1]: r[2] for r in rows}}


def fetch_all_primary_keys(cursor: Any) -> Dict[str, List[str]]:
    """Fetch primary-key columns for every table in one query.

    Returns ``{full_name: [col1, col2, ...], ...}``.
    Replaces per-table calls to :func:`primary_key_columns`.
    """
    cursor.execute(
        "SELECT "
        "OBJECT_SCHEMA_NAME(i.object_id) + '.' + OBJECT_NAME(i.object_id) AS table_name, "
        "c.name AS pk_col "
        "FROM sys.indexes i "
        "JOIN sys.index_columns ic "
        "  ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
        "JOIN sys.columns c "
        "  ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
        "WHERE i.is_primary_key = 1 "
        "ORDER BY table_name, ic.key_ordinal"
    )
    pk_map: Dict[str, List[str]] = {}
    for row in cursor.fetchall():
        pk_map.setdefault(row[0], []).append(row[1])
    return pk_map


def min_valid_versions_batch(cursor: Any, table_names: List[str]) -> Dict[str, int]:
    """Return min valid version for each table in one query. Uses 0 for missing/NULL."""
    if not table_names:
        return {}
    placeholders = ", ".join("?" * len(table_names))
    cursor.execute(
        "SELECT OBJECT_SCHEMA_NAME(t.object_id) + '.' + OBJECT_NAME(t.object_id), "
        "CHANGE_TRACKING_MIN_VALID_VERSION(t.object_id) "
        "FROM sys.change_tracking_tables t "
        f"WHERE OBJECT_SCHEMA_NAME(t.object_id) + '.' + OBJECT_NAME(t.object_id) IN ({placeholders})",
        tuple(table_names),
    )
    result = {}
    for row in cursor.fetchall():
        name, min_ver = row[0], row[1]
        result[name] = min_ver if min_ver is not None else 0
    return result
