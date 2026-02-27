"""Output writers for sync results.

Defines the ``OutputWriter`` protocol with ``ParquetWriter`` (default) and
``CsvWriter`` implementations.

Both writers follow a **write-then-rename** strategy: each file is written
to a ``.tmp`` suffix first and only renamed to its final name after the
full write completes successfully.  This prevents downstream consumers from
reading partial files and makes cleanup of crash leftovers trivial.
"""

from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timezone
from typing import Iterable, List, Protocol, Sequence, Tuple

logger = logging.getLogger(__name__)

DEFAULT_MAX_BYTES = 50 * 1024 * 1024  # ~50 MB
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


class OutputWriter(Protocol):
    """Protocol that any output backend must satisfy."""

    @property
    def file_type(self) -> str:
        """File extension/type produced by this writer (e.g. ``parquet``, ``csv``)."""
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

    Requires ``pyarrow`` (pre-installed on Databricks; install separately
    elsewhere with ``pip install pyarrow``).
    """

    def __init__(
        self,
        max_rows_per_file: int = 1_000_000,
        row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
    ) -> None:
        self.max_rows_per_file = max_rows_per_file
        self.row_group_size = row_group_size

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

        if batch or row_count == 0:
            final = os.path.join(dir_path, f"{prefix}_{ts}_part{part}.parquet")
            tmp = final + _TEMP_SUFFIX
            self._write_file(tmp, col_names, batch, self.row_group_size)
            pending.append((tmp, final))

        files = _finalize_temp_files(pending)
        logger.debug("Wrote %d file(s) (%d rows) to %s", len(files), row_count, dir_path)
        return files, row_count


class CsvWriter:
    """Write query results to one or more CSV files, each <= *max_bytes*."""

    def __init__(self, max_bytes: int = DEFAULT_MAX_BYTES) -> None:
        self.max_bytes = max_bytes

    @property
    def file_type(self) -> str:
        return "csv"

    @staticmethod
    def _open_part(
        dir_path: str, prefix: str, ts: str, part: int, col_names: List[str],
    ) -> Tuple[io.TextIOWrapper, str, str, int]:
        """Open a new CSV temp file, write the header.

        Returns ``(handle, tmp_path, final_path, header_bytes)``.
        """
        final = os.path.join(dir_path, f"{prefix}_{ts}_part{part}.csv")
        tmp = final + _TEMP_SUFFIX
        fh = open(tmp, "w", newline="")
        header_buf = io.StringIO()
        csv.writer(header_buf).writerow(col_names)
        header_line = header_buf.getvalue()
        fh.write(header_line)
        return fh, tmp, final, len(header_line.encode("utf-8"))

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
        part = 1

        f, tmp, final, bytes_written = self._open_part(dir_path, prefix, ts, part, col_names)
        pending.append((tmp, final))

        for row in rows:
            buf = io.StringIO()
            csv.writer(buf).writerow(list(row))
            line = buf.getvalue()
            line_bytes = len(line.encode("utf-8"))

            if bytes_written + line_bytes > self.max_bytes and bytes_written > len(col_names):
                f.close()
                part += 1
                f, tmp, final, bytes_written = self._open_part(dir_path, prefix, ts, part, col_names)
                pending.append((tmp, final))

            f.write(line)
            bytes_written += line_bytes
            row_count += 1

        f.close()

        files = _finalize_temp_files(pending)
        logger.debug("Wrote %d file(s) (%d rows) to %s", len(files), row_count, dir_path)
        return files, row_count
