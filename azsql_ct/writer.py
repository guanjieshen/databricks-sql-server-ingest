"""Output writers for sync results.

Defines the ``OutputWriter`` protocol and the ``ParquetWriter`` implementation.

Both follow a **write-then-rename** strategy: each file is written to a
``.tmp`` suffix first and only renamed to its final name after the full
write completes successfully.  This prevents downstream consumers from
reading partial files and makes cleanup of crash leftovers trivial.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple

logger = logging.getLogger(__name__)

DEFAULT_ROW_GROUP_SIZE = 500_000
_TEMP_SUFFIX = ".tmp"

WriteResult = Tuple[List[str], int]


def _finalize_temp_files(temp_to_final: List[Tuple[str, str]]) -> List[str]:
    """Atomically rename each temp file to its final name.

    Returns the list of final paths.  If any rename fails, already-renamed
    files are not rolled back (at-least-once is acceptable).
    """
    finals: List[str] = []
    for tmp, final in temp_to_final:
        os.replace(tmp, final)
        finals.append(final)
    return finals


def _value_to_partition_date(value: Any) -> str:
    """Return YYYY-MM-DD for partitioning, or '_unknown' for None/invalid."""
    if value is None:
        return "_unknown"
    if isinstance(value, datetime):
        d = value.date() if hasattr(value, "date") else value
        return d.isoformat()
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        try:
            # Try YYYY-MM-DD or datetime string
            if "T" in value or " " in value:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
            return value[:10] if len(value) >= 10 else "_unknown"
        except Exception:
            return "_unknown"
    return "_unknown"


class OutputWriter(Protocol):
    """Protocol that any output backend must satisfy."""

    @property
    def file_type(self) -> str:
        """File extension/type produced by this writer (e.g. ``parquet``)."""
        ...

    def write(
        self,
        rows: Iterable,
        description: Sequence[Tuple[str, ...]],
        dir_path: str,
        prefix: str,
    ) -> WriteResult:
        """Write *rows* (with column metadata in *description*) to *dir_path*.

        Returns ``(file_paths, row_count)``.
        """
        ...  # pragma: no cover


class ParquetWriter:
    """Write query results to Parquet files.

    Splits output into multiple files when *max_rows_per_file* is exceeded.
    Uses *row_group_size* to control the row-group granularity inside each
    Parquet file.

    If *partition_column* is set, files are written under *dir_path* in
    day subdirectories (YYYY-MM-DD) based on that column's value. Rows
    with null/invalid values go under ``_unknown``.

    Requires ``pyarrow`` (pre-installed on Databricks; install separately
    elsewhere with ``pip install pyarrow``).
    """

    def __init__(
        self,
        max_rows_per_file: int = 1_000_000,
        row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
        partition_column: Optional[str] = None,
    ) -> None:
        self.max_rows_per_file = max_rows_per_file
        self.row_group_size = row_group_size
        self.partition_column = partition_column

    @property
    def file_type(self) -> str:
        return "parquet"

    @staticmethod
    def _coerce_column(values: list) -> list:
        """Convert values that PyArrow can't infer (e.g. UUID) to strings."""
        import uuid

        for v in values:
            if v is None:
                continue
            if isinstance(v, uuid.UUID):
                return [str(x) if x is not None else None for x in values]
            break
        return values

    @staticmethod
    def _write_file(
        path: str,
        col_names: List[str],
        batch: List[tuple],
        row_group_size: int,
    ) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        columns = {
            name: ParquetWriter._coerce_column([row[i] for row in batch])
            for i, name in enumerate(col_names)
        }
        table = pa.table(columns)

        # When every value in a column is None, PyArrow infers type `null`.
        # Cast these to int64 so the Parquet schema stays consistent across
        # full (all-NULL metadata cols) and incremental writes.
        null_cols = [
            f for f in table.schema if pa.types.is_null(f.type)
        ]
        if null_cols:
            table = table.cast(
                pa.schema(
                    f.with_type(pa.int64()) if pa.types.is_null(f.type) else f
                    for f in table.schema
                )
            )

        pq.write_table(table, path, row_group_size=row_group_size)

    def write(
        self,
        rows: Iterable,
        description: Sequence[Tuple[str, ...]],
        dir_path: str,
        prefix: str,
    ) -> WriteResult:
        col_names = [col[0] for col in description]
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        pending: List[Tuple[str, str]] = []
        row_count = 0

        partition_idx: Optional[int] = None
        if self.partition_column and self.partition_column in col_names:
            partition_idx = col_names.index(self.partition_column)
        if partition_idx is not None:
            return self._write_partitioned(
                rows, col_names, dir_path, prefix, ts, pending, partition_idx,
            )

        part = 1
        batch: List[tuple] = []
        for row in rows:
            batch.append(tuple(row))
            row_count += 1

            if len(batch) >= self.max_rows_per_file:
                final = os.path.join(dir_path, f"{prefix}_{ts}_part{part}.parquet")
                tmp = final + _TEMP_SUFFIX
                self._write_file(tmp, col_names, batch, self.row_group_size)
                pending.append((tmp, final))
                batch = []
                part += 1

        if batch:
            final = os.path.join(dir_path, f"{prefix}_{ts}_part{part}.parquet")
            tmp = final + _TEMP_SUFFIX
            self._write_file(tmp, col_names, batch, self.row_group_size)
            pending.append((tmp, final))

        files = _finalize_temp_files(pending)
        logger.debug("Wrote %d file(s) (%d rows) to %s", len(files), row_count, dir_path)
        return files, row_count

    def _write_partitioned(
        self,
        rows: Iterable,
        col_names: List[str],
        dir_path: str,
        prefix: str,
        ts: str,
        pending: List[Tuple[str, str]],
        partition_idx: int,
    ) -> WriteResult:
        """Buffer rows by partition date, then write one or more files per day."""
        by_day: Dict[str, List[tuple]] = {}
        row_count = 0

        for row in rows:
            t = tuple(row)
            row_count += 1
            key = _value_to_partition_date(t[partition_idx] if partition_idx < len(t) else None)
            by_day.setdefault(key, []).append(t)

        for part_date, batch in by_day.items():
            part_dir = os.path.join(dir_path, part_date)
            os.makedirs(part_dir, exist_ok=True)
            offset = 0
            part = 1
            while offset < len(batch):
                chunk = batch[offset : offset + self.max_rows_per_file]
                offset += len(chunk)
                final = os.path.join(part_dir, f"{prefix}_{ts}_part{part}.parquet")
                tmp = final + _TEMP_SUFFIX
                self._write_file(tmp, col_names, chunk, self.row_group_size)
                pending.append((tmp, final))
                part += 1

        files = _finalize_temp_files(pending)
        logger.debug(
            "Wrote %d file(s) (%d rows) to %s (partitioned by day)",
            len(files), row_count, dir_path,
        )
        return files, row_count
