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
import logging
import os
import time
from datetime import date
from typing import Any, Dict, Generator, List, Optional, Tuple

from . import queries, schema, watermark
from ._constants import DEFAULT_BATCH_SIZE, DEFAULT_SCD_TYPE, VALID_MODES
from .schema import columns_from_description
from .writer import OutputWriter, ParquetWriter, _compute_schema_version, _make_uoid

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
    batch_num = 0
    while True:
        batch_num += 1
        logger.debug("fetchmany batch %d (size=%d)", batch_num, batch_size)
        batch = cursor.fetchmany(batch_size)
        logger.debug("fetchmany batch %d returned %d rows", batch_num, len(batch))
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
    scd_type: int = DEFAULT_SCD_TYPE,
    uc_catalog: Optional[str] = None,
    soft_delete: bool = False,
    tracked_lookup: Optional[Dict[str, str]] = None,
    db_version: Optional[int] = None,
    min_ver: Optional[int] = None,
    pk_cols: Optional[List[str]] = None,
) -> dict:
    """Sync one change-tracked table.

    Args:
        conn:               Open DB-API connection to the target *database*.
        table_name:         Fully-qualified table name (e.g. ``dbo.table_1``).
        database:           Database name (used for directory layout).
        output_dir:         Root directory for data files.
        watermark_dir:      Root directory for watermark files (mirrors data layout).
        mode:               ``"full_incremental"`` (default) or
                            ``"incremental"``.
        writer:             An ``OutputWriter`` instance; defaults to ``ParquetWriter()``.
        batch_size:         Number of rows fetched from the database at a time
                            (default ``10_000``).  Controls peak memory usage.
        snapshot_isolation: If ``True``, execute the data query under
                            ``SNAPSHOT`` isolation for point-in-time
                            consistent reads.  Requires the database to have
                            ``ALLOW_SNAPSHOT_ISOLATION ON``.
        scd_type:           SCD type for this table (``1`` = overwrite,
                            ``2`` = historical tracking).  Passed through to
                            the result dict and output manifest.
        uc_catalog:         Optional Unity Catalog name; passed through to
                            the writer as ``table_metadata["catalog"]``.
        soft_delete:        If ``True``, deletes are tracked via an
                            ``_is_deleted`` flag rather than physical removal.
                            Passed through to the result dict and output manifest.
        tracked_lookup:     Pre-fetched ``{lowercase: original}`` tracked-table
                            lookup from :func:`queries.fetch_all_ct_metadata`.
                            When provided, skips the per-table
                            ``list_tracked_tables`` query.
        db_version:         Pre-fetched CT version from
                            :func:`queries.fetch_all_ct_metadata`.
                            Skips the per-table ``current_version`` query.
        min_ver:            Pre-fetched minimum valid version for this table.
                            Skips the per-table ``min_valid_version_for_table``
                            query.
        pk_cols:            Pre-fetched primary-key column list from
                            :func:`queries.fetch_all_primary_keys`.
                            Skips the per-table ``primary_key_columns`` query.

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
    if tracked_lookup is None:
        tracked = queries.list_tracked_tables(cur)
        tracked_lookup = queries.build_tracked_lookup(tracked)
    full_name = queries.resolve_table(table_name, tracked_lookup)
    if full_name is None:
        raise ValueError(
            f"Table '{table_name}' is not change-tracked in {database}. "
            f"Tracked tables: {list(tracked_lookup.values())}"
        )

    data_dir = _sub_dir(output_dir, database, full_name)
    wm_dir = _sub_dir(watermark_dir, database, full_name)

    with _TableLock(wm_dir):
        return _sync_table_locked(
            conn, cur, full_name, database, data_dir, wm_dir,
            mode=mode, writer=writer, batch_size=batch_size,
            snapshot_isolation=snapshot_isolation,
            scd_type=scd_type,
            uc_catalog=uc_catalog,
            soft_delete=soft_delete,
            db_version=db_version,
            min_ver=min_ver,
            pk_cols=pk_cols,
        )


def _sync_table_locked(
    conn: Any,
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
    scd_type: int = DEFAULT_SCD_TYPE,
    uc_catalog: Optional[str] = None,
    soft_delete: bool = False,
    db_version: Optional[int] = None,
    min_ver: Optional[int] = None,
    pk_cols: Optional[List[str]] = None,
) -> dict:
    """Inner sync logic, called while holding the per-table lock."""
    cur_ver = db_version if db_version is not None else queries.current_version(cur)
    min_ver_val = min_ver if min_ver is not None else queries.min_valid_version_for_table(cur, full_name)
    since = watermark.get(wm_dir, full_name)

    is_initial = since is None

    if mode == "incremental" and is_initial:
        raise RuntimeError(
            f"No watermark exists for {database}.{full_name} and mode is "
            f"'incremental'. Use mode='full_incremental' for initial load "
            f"followed by incremental syncs."
        )

    if mode in ("incremental", "full_incremental") and not is_initial and since < min_ver_val:
        logger.warning(
            "Watermark (%d) for %s.%s is older than min valid version (%d); "
            "falling back to full sync.",
            since, database, full_name, min_ver_val,
        )
        mode = "full"  # internal-only; not a user-facing mode

    t0 = time.monotonic()

    if pk_cols is None:
        pk_cols = queries.primary_key_columns(cur, full_name)

    do_full = mode == "full" or (mode == "full_incremental" and is_initial)

    logger.debug("[%s.%s] Creating fresh data cursor", database, full_name)
    data_cur = conn.cursor()
    logger.debug("[%s.%s] Data cursor created", database, full_name)

    if snapshot_isolation:
        data_cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")

    if do_full:
        logger.info("Full sync for %s.%s (version %d)", database, full_name, cur_ver)
        sql = queries.build_full_query(full_name)
        logger.debug("[%s.%s] Executing full query", database, full_name)
        data_cur.execute(sql)
        logger.debug("[%s.%s] Full query executed", database, full_name)
        actual_mode = "full"
    else:
        if not pk_cols:
            raise RuntimeError(
                f"Could not determine primary key for {full_name}. "
                "A primary key is required for incremental change tracking JOINs."
            )
        all_cols = queries.table_columns(cur, full_name)
        sql = queries.build_incremental_query(full_name, pk_cols, all_cols)
        logger.info(
            "Incremental sync for %s.%s (since version %d)",
            database, full_name, since,
        )
        logger.debug("[%s.%s] Executing incremental query", database, full_name)
        data_cur.execute(sql, (since,))
        logger.debug("[%s.%s] Incremental query executed, description=%s",
                      database, full_name, data_cur.description is not None)
        actual_mode = "incremental"

    day_dir = os.path.join(data_dir, date.today().isoformat())
    os.makedirs(day_dir, exist_ok=True)

    _clean_temp_files(day_dir)

    description = data_cur.description
    safe_name = full_name.replace(".", "_")
    row_iter = _iter_cursor(data_cur, batch_size)

    schema_name, table_only = full_name.split(".", 1)
    schema_ver = _compute_schema_version(description) if description else 0
    extraction_ts = int(time.time() * 1000)

    table_metadata = {
        "database": database,
        "schema": schema_name,
        "table": table_only,
        "catalog": uc_catalog or database,
        "uoid": _make_uoid(database, schema_name, table_only),
        "extraction_timestamp": extraction_ts,
        "schema_version": schema_ver,
    }

    logger.debug("[%s.%s] Starting writer.write", database, full_name)
    output_files, row_count = writer.write(
        row_iter, description, day_dir, safe_name,
        table_metadata=table_metadata,
    )
    logger.debug("[%s.%s] writer.write completed: %d rows, %d files",
                  database, full_name, row_count, len(output_files))

    if do_full:
        _clear_data_files(data_dir, keep=set(output_files))

    elapsed = time.monotonic() - t0
    since_ver = since if actual_mode == "incremental" else None

    logger.debug("[%s.%s] Saving watermark", database, full_name)
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
    logger.debug("[%s.%s] Watermark saved", database, full_name)

    result = {
        "database": database,
        "table": full_name,
        "mode": actual_mode,
        "scd_type": scd_type,
        "soft_delete": soft_delete,
        "since_version": since_ver,
        "current_version": cur_ver,
        "rows_written": row_count,
        "files": output_files,
        "duration_seconds": round(elapsed, 2),
        "primary_key": pk_cols,
        "columns": [],
        "schema_version": schema_ver,
        "schema_changed": False,
    }

    try:
        logger.debug("[%s.%s] Creating metadata cursor", database, full_name)
        try:
            meta_cur = conn.cursor()
            columns = queries.column_metadata(meta_cur, full_name)
        except Exception:
            logger.debug(
                "INFORMATION_SCHEMA lookup failed for %s.%s; "
                "falling back to cursor.description",
                database, full_name,
            )
            columns = columns_from_description(description)

        existing_schema = schema.load(wm_dir)
        schema_changed = False
        if existing_schema:
            existing_by_name = {
                c["name"]: c for c in existing_schema.get("columns", [])
            }
            incoming_names = {c["name"] for c in columns}
            existing_names = set(existing_by_name)

            added = incoming_names - existing_names
            removed = existing_names - incoming_names
            type_changed = [
                (c["name"], existing_by_name[c["name"]]["type"], c["type"])
                for c in columns
                if c["name"] in existing_by_name
                and c.get("type") != existing_by_name[c["name"]].get("type")
            ]

            if added or removed or type_changed:
                schema_changed = True
                logger.warning(
                    "Schema change detected for %s.%s (version %d -> %d)",
                    database, full_name,
                    existing_schema.get("schema_version", 0), schema_ver,
                )
                if added:
                    logger.warning("  Columns added: %s", sorted(added))
                if removed:
                    logger.warning(
                        "  Columns removed (retained as null): %s", sorted(removed),
                    )
                for col_name, old_type, new_type in type_changed:
                    logger.warning(
                        "  Column %r type changed: %s -> %s",
                        col_name, old_type, new_type,
                    )
            elif schema_ver != existing_schema.get("schema_version"):
                logger.info(
                    "Schema version changed for %s.%s (%d -> %d) "
                    "but columns unchanged",
                    database, full_name,
                    existing_schema.get("schema_version", 0), schema_ver,
                )

        schema.save(wm_dir, columns, schema_ver)
        result["columns"] = columns
        result["schema_changed"] = schema_changed
    except Exception as exc:
        logger.warning(
            "Post-sync metadata update failed for %s.%s: %s "
            "(data was written successfully)",
            database, full_name, exc,
        )
        result["columns"] = columns_from_description(description) if description else []

    logger.info(
        "%s.%s: %d rows written, watermark -> %d (%.1fs)",
        database, full_name, row_count, cur_ver, elapsed,
    )
    return result
