"""SQL helpers for Azure SQL change-tracking queries.

All database-specific query logic is isolated here so it can be tested
independently from the sync orchestration and I/O layers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    import pyodbc


def list_tracked_tables(cursor: "pyodbc.Cursor") -> List[str]:
    """Return fully-qualified names of all change-tracked tables."""
    cursor.execute(
        "SELECT OBJECT_SCHEMA_NAME(t.object_id) + '.' + OBJECT_NAME(t.object_id) "
        "FROM sys.change_tracking_tables t ORDER BY 1"
    )
    return [row[0] for row in cursor.fetchall()]


def resolve_table(name: str, tracked: List[str]) -> Optional[str]:
    """Match a user-supplied name against the tracked-table list (case-insensitive)."""
    want = name.strip().lower()
    if "." not in want:
        want = "dbo." + want
    for t in tracked:
        if t.lower() == want:
            return t
    return None


def current_version(cursor: "pyodbc.Cursor") -> int:
    """Return the current change-tracking version for the database."""
    cursor.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()")
    return cursor.fetchone()[0]


def min_valid_version_for_table(cursor: "pyodbc.Cursor", full_table_name: str) -> int:
    """Return the minimum valid change-tracking version for *full_table_name*."""
    cursor.execute(
        "SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(?))",
        (full_table_name,),
    )
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else 0


def primary_key_columns(cursor: "pyodbc.Cursor", full_table_name: str) -> List[str]:
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


def build_incremental_query(full_table_name: str, pk_cols: List[str]) -> str:
    """Return the SQL for an incremental change-tracking SELECT.

    The query expects a single ``?`` parameter for the *since_version*.
    """
    join_cond = " AND ".join(f"t.[{c}] = ct.[{c}]" for c in pk_cols)
    return (
        f"SELECT ct.SYS_CHANGE_VERSION, ct.SYS_CHANGE_CREATION_VERSION, "
        f"ct.SYS_CHANGE_OPERATION, t.* "
        f"FROM CHANGETABLE(CHANGES {full_table_name}, ?) AS ct "
        f"LEFT JOIN {full_table_name} AS t ON {join_cond}"
    )
