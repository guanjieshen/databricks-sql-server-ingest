"""Core sync engine: fetch change-tracking deltas and write to output files.

Supports config-driven extraction across multiple databases/tables,
three sync modes per table, and pluggable output writers.

Modes:
    ``full``               Always reload the entire table.
    ``incremental``        Strict incremental via change tracking; raises if
                           no watermark exists yet.
    ``full_incremental``   Full load when no watermark exists, incremental
                           on subsequent runs.

Data goes to ``output_dir/{database}/{schema_table}/``.
Watermarks go to ``watermark_dir/{database}/{schema_table}/watermarks.json``,
mirroring the data directory structure but in a separate root.
"""

from __future__ import annotations

import errno
import fcntl
import glob
import logging
import os
import time
from datetime import date
from typing import Any, Generator, List, Optional, Tuple

from . import queries, watermark
from ._constants import DEFAULT_BATCH_SIZE, VALID_MODES
from .writer import OutputWriter, ParquetWriter

logger = logging.getLogger(__name__)

_DEFAULT_WRITER: OutputWriter = ParquetWriter()
_LOCK_FILENAME = ".sync.lock"


class _TableLock:
    """Per-table advisory file lock to prevent concurrent syncs.

    Uses ``fcntl.flock`` (POSIX) with ``LOCK_EX | LOCK_NB`` so a second
    process attempting to sync the same table will fail fast rather than
    block indefinitely.
    """

    def __init__(self, lock_dir: str) -> None:
        os.makedirs(lock_dir, exist_ok=True)
        self._path = os.path.join(lock_dir, _LOCK_FILENAME)
        self._fd: Optional[int] = None

    def __enter__(self) -> "_TableLock":
        self._fd = os.open(self._path, os.O_CREAT | os.O_RDWR)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(self._fd)
            self._fd = None
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                raise RuntimeError(
                    f"Another sync is already running for this table "
                    f"(lock file: {self._path})"
                ) from exc
            raise
        return self

    def __exit__(self, *_: Any) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None


def _iter_cursor(
    cursor: Any, batch_size: int
) -> Generator[Tuple, None, None]:
    """Yield rows from *cursor* in batches of *batch_size* without
    materialising the entire result set in memory."""
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        yield from batch


def _sub_dir(root: str, database: str, full_table_name: str) -> str:
    """Return ``root/database/schema/table/`` and ensure it exists."""
    schema, table = full_table_name.split(".", 1)
    d = os.path.join(root, database, schema, table)
    os.makedirs(d, exist_ok=True)
    return d


def _clear_data_files(data_dir: str, *, keep: Optional[set] = None) -> int:
    """Remove writer-produced files from *data_dir* (and subdirs), skipping *keep*.

    Supports partitioned layout where files live under day subdirectories.
    Returns the number of files deleted.
    """
    keep = keep or set()
    removed = 0
    for dirpath, _dirnames, filenames in os.walk(data_dir, topdown=False):
        for name in filenames:
            path = os.path.join(dirpath, name)
            if path not in keep:
                os.remove(path)
                removed += 1
        if dirpath != data_dir:
            try:
                os.rmdir(dirpath)
            except OSError:
                pass
    if removed:
        logger.info("Cleared %d existing file(s) from %s", removed, data_dir)
    return removed


_TEMP_SUFFIX = ".tmp"


def _clean_temp_files(data_dir: str) -> int:
    """Remove leftover temp files from a previous crashed write.

    Walks subdirectories so partition subdirs are cleaned too.
    Returns the number of files cleaned up.
    """
    removed = 0
    for dirpath, _dirnames, filenames in os.walk(data_dir):
        for name in filenames:
            if name.endswith(_TEMP_SUFFIX):
                path = os.path.join(dirpath, name)
                os.remove(path)
                removed += 1
    if removed:
        logger.info(
            "Cleaned up %d orphaned temp file(s) from %s", removed, data_dir,
        )
    return removed


def sync_table(
    conn: Any,
    table_name: str,
    database: str,
    output_dir: str = "./data",
    watermark_dir: str = "./watermarks",
    mode: str = "full_incremental",
    writer: Optional[OutputWriter] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    snapshot_isolation: bool = False,
) -> dict:
    """Sync one change-tracked table.

    Args:
        conn:               Open DB-API connection to the target *database*.
        table_name:         Fully-qualified table name (e.g. ``dbo.table_1``).
        database:           Database name (used for directory layout).
        output_dir:         Root directory for data files.
        watermark_dir:      Root directory for watermark files (mirrors data layout).
        mode:               ``"full_incremental"`` (default), ``"full"``, or
                            ``"incremental"``.
        writer:             An ``OutputWriter`` instance; defaults to ``ParquetWriter()``.
        batch_size:         Number of rows fetched from the database at a time
                            (default ``10_000``).  Controls peak memory usage.
        snapshot_isolation: If ``True``, execute the data query under
                            ``SNAPSHOT`` isolation for point-in-time
                            consistent reads.  Requires the database to have
                            ``ALLOW_SNAPSHOT_ISOLATION ON``.

    Returns:
        Summary dict.
    """
    if mode not in VALID_MODES:
        raise ValueError(
            f"Invalid mode {mode!r}; must be one of {sorted(VALID_MODES)}"
        )

    if writer is None:
        writer = _DEFAULT_WRITER

    cur = conn.cursor()
    tracked = queries.list_tracked_tables(cur)
    full_name = queries.resolve_table(table_name, tracked)
    if full_name is None:
        raise ValueError(
            f"Table '{table_name}' is not change-tracked in {database}. "
            f"Tracked tables: {tracked}"
        )

    data_dir = _sub_dir(output_dir, database, full_name)
    wm_dir = _sub_dir(watermark_dir, database, full_name)

    with _TableLock(wm_dir):
        return _sync_table_locked(
            cur, full_name, database, data_dir, wm_dir,
            mode=mode, writer=writer, batch_size=batch_size,
            snapshot_isolation=snapshot_isolation,
        )


def _sync_table_locked(
    cur: Any,
    full_name: str,
    database: str,
    data_dir: str,
    wm_dir: str,
    *,
    mode: str,
    writer: OutputWriter,
    batch_size: int,
    snapshot_isolation: bool = False,
) -> dict:
    """Inner sync logic, called while holding the per-table lock."""
    cur_ver = queries.current_version(cur)
    min_ver = queries.min_valid_version_for_table(cur, full_name)
    since = watermark.get(wm_dir, full_name)

    is_initial = since is None

    if mode == "incremental" and is_initial:
        raise RuntimeError(
            f"No watermark exists for {database}.{full_name} and mode is "
            f"'incremental'. Use mode='full_incremental' for initial load "
            f"followed by incremental syncs, or mode='full' for a full reload."
        )

    if mode in ("incremental", "full_incremental") and not is_initial and since < min_ver:
        logger.warning(
            "Watermark (%d) for %s.%s is older than min valid version (%d); "
            "falling back to full sync.",
            since, database, full_name, min_ver,
        )
        mode = "full"

    t0 = time.monotonic()

    pk_cols = queries.primary_key_columns(cur, full_name)

    do_full = mode == "full" or (mode == "full_incremental" and is_initial)

    if snapshot_isolation:
        cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")

    if do_full:
        logger.info("Full sync for %s.%s (version %d)", database, full_name, cur_ver)
        sql = queries.build_full_query(full_name)
        cur.execute(sql)
        actual_mode = "full"
    else:
        if not pk_cols:
            raise RuntimeError(
                f"Could not determine primary key for {full_name}. "
                "A primary key is required for incremental change tracking JOINs."
            )
        sql = queries.build_incremental_query(full_name, pk_cols)
        logger.info(
            "Incremental sync for %s.%s (since version %d)",
            database, full_name, since,
        )
        cur.execute(sql, (since,))
        actual_mode = "incremental"

    day_dir = os.path.join(data_dir, date.today().isoformat())
    os.makedirs(day_dir, exist_ok=True)

    _clean_temp_files(day_dir)

    description = cur.description
    safe_name = full_name.replace(".", "_")
    row_iter = _iter_cursor(cur, batch_size)
    output_files, row_count = writer.write(row_iter, description, day_dir, safe_name)

    if do_full:
        _clear_data_files(day_dir, keep=set(output_files))

    elapsed = time.monotonic() - t0
    since_ver = since if actual_mode == "incremental" else None

    watermark.save(
        wm_dir,
        full_name,
        cur_ver,
        since_version=since_ver,
        rows_synced=row_count,
        mode=actual_mode,
        files=output_files,
        duration_seconds=elapsed,
    )

    logger.info(
        "%s.%s: %d rows written, watermark -> %d (%.1fs)",
        database, full_name, row_count, cur_ver, elapsed,
    )
    return {
        "database": database,
        "table": full_name,
        "mode": actual_mode,
        "since_version": since_ver,
        "current_version": cur_ver,
        "rows_written": row_count,
        "files": output_files,
        "duration_seconds": round(elapsed, 2),
        "primary_key": pk_cols,
    }
