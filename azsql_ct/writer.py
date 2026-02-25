"""Output writers for sync results.

Defines the ``OutputWriter`` protocol and a ``CsvWriter`` implementation that
splits output into files of roughly ``max_bytes`` each.
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

WriteResult = Tuple[List[str], int]


class OutputWriter(Protocol):
    """Protocol that any output backend must satisfy."""

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


class CsvWriter:
    """Write query results to one or more CSV files, each <= *max_bytes*."""

    def __init__(self, max_bytes: int = DEFAULT_MAX_BYTES) -> None:
        self.max_bytes = max_bytes

    @staticmethod
    def _open_part(
        dir_path: str, prefix: str, ts: str, part: int, col_names: List[str],
    ) -> Tuple[io.TextIOWrapper, str, int]:
        """Open a new CSV part file, write the header, and return (handle, path, header_bytes)."""
        path = os.path.join(dir_path, f"{prefix}_{ts}_part{part}.csv")
        fh = open(path, "w", newline="")
        header_buf = io.StringIO()
        csv.writer(header_buf).writerow(col_names)
        header_line = header_buf.getvalue()
        fh.write(header_line)
        return fh, path, len(header_line.encode("utf-8"))

    def write(
        self,
        rows: Iterable,
        description: Sequence[Tuple[str, ...]],
        dir_path: str,
        prefix: str,
    ) -> WriteResult:
        col_names = [col[0] for col in description]
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        files: List[str] = []
        row_count = 0
        part = 1

        f, path, bytes_written = self._open_part(dir_path, prefix, ts, part, col_names)
        files.append(path)

        for row in rows:
            buf = io.StringIO()
            csv.writer(buf).writerow(list(row))
            line = buf.getvalue()
            line_bytes = len(line.encode("utf-8"))

            if bytes_written + line_bytes > self.max_bytes and bytes_written > len(col_names):
                f.close()
                part += 1
                f, path, bytes_written = self._open_part(dir_path, prefix, ts, part, col_names)
                files.append(path)

            f.write(line)
            bytes_written += line_bytes
            row_count += 1

        f.close()

        logger.debug("Wrote %d file(s) (%d rows) to %s", len(files), row_count, dir_path)
        return files, row_count
