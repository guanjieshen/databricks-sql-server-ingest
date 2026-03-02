"""High-level SDK facade for Azure SQL change-tracking sync.

``ChangeTracker`` is the primary user-facing entry point::

    from azsql_ct import ChangeTracker

    ct = ChangeTracker("myserver.database.windows.net", "sqladmin", "secret")

    # List format -- all tables synced with the default mode (full_incremental):
    ct.tables = {"database_1": {"dbo": ["table_1", "table_2"]}}
    ct.sync()

    # Dict format -- per-table mode:
    ct.tables = {
        "database_1": {
            "dbo": {
                "table_1": "full_incremental",
                "table_2": "incremental",
            }
        }
    }
    ct.sync()

    # Dict format with SCD type per table:
    ct.tables = {
        "database_1": {
            "dbo": {
                "table_1": {"mode": "full_incremental", "scd_type": 1},
                "table_2": {"mode": "full_incremental", "scd_type": 2},
            }
        }
    }
    ct.sync()

    # Parallel sync (4 tables at a time):
    ct = ChangeTracker("server", "user", "pw", max_workers=4)
    ct.tables = {"db1": {"dbo": [f"table_{i}" for i in range(1, 101)]}}
    ct.sync()

Modes:
    ``incremental``        Strict incremental via change tracking; raises if
                           no watermark exists yet.
    ``full_incremental``   Full load when no watermark exists, incremental
                           on subsequent runs.  This is the default.
"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from queue import Empty, Queue
from types import TracebackType
from typing import Any, Dict, List, Optional, Set, Tuple, Type, Union

from . import queries
from ._constants import (
    DEFAULT_BATCH_SIZE, DEFAULT_OUTPUT_FORMAT, DEFAULT_PARQUET_COMPRESSION,
    DEFAULT_ROW_GROUP_SIZE,
)
from .config import (
    expand_env, resolve_value, _load_config_file, _normalize_table_map,
    _extract_uc_metadata, _flat_config_to_table_map,
    TableMap, FlatEntry,
    _flatten_table_map, _validate_table_map,
)
from .connection import AzureSQLConnection, load_dotenv
from .incremental_output import write as incremental_output_write
from .output_manifest import load as manifest_load, merge_add as manifest_merge_add, save as manifest_save
from .sync import sync_table
from . import watermark
from .writer import OutputWriter, ParquetWriter, UnifiedParquetWriter

logger = logging.getLogger(__name__)


def set_databricks_task_values(results: List[dict]) -> int:
    """Set Databricks task values from sync results for downstream workflow tasks.

    When running in a Databricks job, sets:

    - ``total_rows_changed`` (int): Sum of ``rows_written`` across all non-error
      results. Reference via ``{{tasks.<task_name>.values.total_rows_changed}}``.
    - ``schema_changes_detected`` (bool): ``True`` if any synced table had a
      schema change (columns added, removed, or type changed). Reference via
      ``{{tasks.<task_name>.values.schema_changes_detected}}`` for notifications
      or conditional branching.

    When run locally (no ``dbutils``), this is a no-op.

    Returns:
        Total rows written across all non-error results.
    """
    total_rows = sum(
        r.get("rows_written", 0)
        for r in results
        if r.get("status") != "error"
    )
    has_schema_changes = any(
        r.get("schema_changed")
        for r in results
        if r.get("status") != "error"
    )
    try:
        import sys
        main = sys.modules.get("__main__")
        dbutils = getattr(main, "dbutils", None) if main else None
        if dbutils is not None:
            dbutils.jobs.taskValues.set(key="total_rows_changed", value=total_rows)
            dbutils.jobs.taskValues.set(
                key="schema_changes_detected", value=has_schema_changes
            )
    except Exception:
        pass
    return total_rows


class _ConnectionPool:
    """Thread-safe pool of :class:`AzureSQLConnection` objects keyed by database.

    Workers :meth:`acquire` a connection before syncing a table and
    :meth:`release` it back when done.  Connections are created lazily on
    first acquire and reused on subsequent ones, so the pool never holds
    more connections than the number of concurrent workers.

    On sync failure the caller should **not** release the connection (it may
    be in a bad state); a fresh one will be created on the next acquire.
    """

    def __init__(self, server: str, user: str, password: str) -> None:
        self._server = server
        self._user = user
        self._password = password
        self._queues: Dict[str, Queue] = {}
        self._lock = threading.Lock()

    def _queue_for(self, database: str) -> Queue:
        with self._lock:
            if database not in self._queues:
                self._queues[database] = Queue()
            return self._queues[database]

    def acquire(self, database: str) -> Tuple[AzureSQLConnection, Any]:
        """Return an ``(AzureSQLConnection, conn)`` pair, reusing an idle one
        if available or creating a new one otherwise.

        Reused connections are health-checked with ``SELECT 1``; stale
        connections are discarded and a fresh one is created.
        """
        q = self._queue_for(database)
        try:
            az = q.get_nowait()
            conn = az.connect()
            try:
                conn.cursor().execute("SELECT 1")
                return az, conn
            except Exception:
                logger.debug("Stale connection for %s, reconnecting", database)
                az.close()
        except Empty:
            pass
        az = AzureSQLConnection(
            server=self._server, user=self._user,
            password=self._password, database=database,
        )
        return az, az.connect()

    def release(self, database: str, az: AzureSQLConnection) -> None:
        """Return a healthy connection to the pool for reuse."""
        self._queue_for(database).put(az)

    def close_all(self) -> None:
        """Close every connection in the pool."""
        for q in self._queues.values():
            while True:
                try:
                    az = q.get_nowait()
                    az.close()
                except Empty:
                    break


def _load_watermarks_for_tables(
    watermark_dir: str, database: str, tables: List[str],
) -> Dict[str, int]:
    """Load watermark version for each table. Returns {full_table_name: version} for tables with watermarks."""
    result: Dict[str, int] = {}
    for full_table_name in tables:
        schema, table = full_table_name.split(".", 1)
        wm_dir = os.path.join(watermark_dir, database, schema, table)
        since = watermark.get(wm_dir, full_table_name)
        if since is not None:
            result[full_table_name] = since
    return result


def _fetch_db_metadata(conn: Any) -> Dict[str, Any]:
    """Fetch all CT metadata and primary keys for a database in two queries.

    Returns a dict with ``db_version``, ``tables`` (name -> min_version),
    ``tracked_lookup`` (lowercase -> original), and ``pk_map`` (name -> [cols]).
    """
    cur = conn.cursor()
    ct_meta = queries.fetch_all_ct_metadata(cur)
    pk_map = queries.fetch_all_primary_keys(cur)
    tracked_lookup = queries.build_tracked_lookup(list(ct_meta["tables"].keys()))
    return {
        "db_version": ct_meta["db_version"],
        "tables": ct_meta["tables"],
        "tracked_lookup": tracked_lookup,
        "pk_map": pk_map,
    }


def _compute_tables_to_skip(
    database: str,
    conn: Any,
    flat_entries: List[FlatEntry],
    watermark_dir: str,
    mode_override: Optional[str],
    db_metadata: Optional[Dict[str, Any]] = None,
) -> Set[str]:
    """Return set of full_table_name (config names) to skip (incremental tables with no changes).

    When *db_metadata* is provided (from :func:`_fetch_db_metadata`), reuses
    the pre-fetched tracked-table list and min valid versions instead of
    querying them again.
    """
    try:
        if db_metadata is not None:
            tracked_lookup = db_metadata["tracked_lookup"]
            ct_tables = db_metadata["tables"]
        else:
            cur = conn.cursor()
            ct_meta = queries.fetch_all_ct_metadata(cur)
            tracked_lookup = queries.build_tracked_lookup(list(ct_meta["tables"].keys()))
            ct_tables = ct_meta["tables"]

        resolved_to_config: Dict[str, str] = {}
        for _db, full_table_name, table_mode, _scd, _soft in flat_entries:
            if _db != database:
                continue
            mode = mode_override or table_mode or "full_incremental"
            if mode not in ("incremental", "full_incremental"):
                continue
            resolved = queries.resolve_table(full_table_name, tracked_lookup)
            if resolved is None:
                continue
            resolved_to_config[resolved] = full_table_name

        if not resolved_to_config:
            return set()

        watermarks = _load_watermarks_for_tables(
            watermark_dir, database, list(resolved_to_config.keys()),
        )
        resolved_with_wm = [r for r in resolved_to_config if r in watermarks]
        if not resolved_with_wm:
            return set()

        table_watermarks: Dict[str, int] = {}
        for resolved in resolved_with_wm:
            since = watermarks[resolved]
            min_ver = ct_tables.get(resolved, 0)
            if since >= min_ver:
                table_watermarks[resolved] = since

        if not table_watermarks:
            return set()

        cur = conn.cursor()
        changed = queries.fetch_tables_with_changes(cur, table_watermarks)
        to_skip_resolved = {t for t in table_watermarks if t not in changed}
        return {resolved_to_config[r] for r in to_skip_resolved}
    except Exception as exc:
        logger.warning(
            "Change check failed for %s, syncing all tables: %s", database, exc,
        )
        return set()


def _skipped_result(database: str, table: str) -> dict:
    """Result dict for a table skipped due to no changes."""
    return {
        "database": database,
        "table": table,
        "status": "skipped",
        "reason": "no changes",
    }


class ChangeTracker:
    """User-friendly facade for Azure SQL change-tracking sync.

    Args:
        server:        Azure SQL server FQDN.
        user:          SQL login username.
        password:      SQL login password.
        output_dir:    Root directory for data files (default ``./data``).
        watermark_dir: Root directory for watermark files (default ``./watermarks``).
        writer:        An :class:`~azsql_ct.writer.OutputWriter` instance
                       (default ``ParquetWriter()``).
        max_workers:   Number of tables to sync in parallel (default ``1`` =
                       sequential).  Each worker opens its own database
                       connection, so this is safe with both backends.
        batch_size:    Number of rows fetched from the database at a time
                       (default ``10_000``).  Controls peak memory usage for
                       large tables.
        output_manifest: Optional path to a YAML file to record where synced
                         files are written; updated only when new tables are
                         added (user-filled UC fields preserved).
    """

    def __init__(
        self,
        server: str,
        user: str,
        password: str,
        *,
        output_dir: str = "./data",
        watermark_dir: str = "./watermarks",
        writer: Optional[OutputWriter] = None,
        max_workers: int = 1,
        batch_size: int = DEFAULT_BATCH_SIZE,
        snapshot_isolation: bool = False,
        output_manifest: Optional[str] = None,
        output_format: str = DEFAULT_OUTPUT_FORMAT,
        parquet_compression: str = DEFAULT_PARQUET_COMPRESSION,
        parquet_compression_level: Optional[int] = None,
        row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
    ) -> None:
        self.server = server
        self.user = user
        self._password = password
        self.output_dir = output_dir
        self.watermark_dir = watermark_dir
        self.output_format = output_format
        if writer is not None:
            self.writer: OutputWriter = writer
        elif output_format == "unified":
            self.writer = UnifiedParquetWriter(
                compression=parquet_compression,
                compression_level=parquet_compression_level,
                row_group_size=row_group_size,
            )
        else:
            self.writer = ParquetWriter(
                compression=parquet_compression,
                compression_level=parquet_compression_level,
                row_group_size=row_group_size,
            )
        self.max_workers = max(1, max_workers)
        self.batch_size = batch_size
        self.snapshot_isolation = snapshot_isolation
        self.output_manifest = output_manifest
        self.ingest_pipeline: Optional[str] = None
        self._uc_metadata: Dict[str, Dict[str, Any]] = {}

        self._table_map: TableMap = {}
        self._flat_tables: List[FlatEntry] = []

    # -- factory ------------------------------------------------------------

    @classmethod
    def from_config(
        cls,
        config: Union[str, Path, dict],
        *,
        max_workers: Optional[int] = None,
        output_dir: Optional[str] = None,
        watermark_dir: Optional[str] = None,
        output_manifest: Optional[str] = None,
        batch_size: Optional[int] = None,
        snapshot_isolation: Optional[bool] = None,
    ) -> "ChangeTracker":
        """Create a fully configured ``ChangeTracker`` from a config file or dict.

        Accepts either a file path (YAML/JSON) or an already-parsed dict.
        Loads ``.env`` automatically and expands ``${VAR}`` references in
        string values.

        Supports three config layouts:

        **Structured (recommended YAML):**

        .. code-block:: yaml

            connection:
              server: myserver.database.windows.net
              sql_login: sqladmin
              password: ${ADMIN_PASSWORD}
            storage:
              ingest_pipeline: ./ingest_pipeline
            max_workers: 8
            databases:
              db1:
                uc_catalog: my_catalog    # informational, not used by sync
                schemas:
                  dbo:
                    uc_schema: my_schema  # informational, not used by sync
                    tables:
                      orders: full_incremental

        **Legacy nested (still supported):**

        .. code-block:: yaml

            databases:
              db1:
                dbo:
                  orders: full_incremental

        **Flat (JSON-style, for backward compatibility):**

        .. code-block:: json

            {
              "server": "myserver.database.windows.net",
              "user": "sqladmin",
              "password": "secret",
              "tables": [
                {"database": "db1", "table": "dbo.orders", "mode": "full_incremental"}
              ]
            }
        """
        load_dotenv()

        if isinstance(config, (str, Path)):
            config = _load_config_file(config)

        is_flat = "tables" in config and isinstance(config.get("tables"), list)
        base: Optional[str] = None
        cfg_parquet_compression: Optional[str] = None
        cfg_parquet_compression_level: Optional[int] = None
        cfg_row_group_size: Optional[int] = None

        if is_flat:
            server = resolve_value(config.get("server", ""))
            user = resolve_value(config.get("user", ""))
            password = resolve_value(config.get("password", ""))
            table_map = _flat_config_to_table_map(config["tables"])
            cfg_output = config.get("output_dir")
            cfg_watermark = config.get("watermark_dir")
            cfg_manifest = config.get("output_manifest")
            cfg_workers = config.get("max_workers") or config.get("parallelism")
            cfg_snapshot = config.get("snapshot_isolation")
            cfg_output_format = None
        else:
            conn_cfg = config.get("connection", {})
            server = resolve_value(conn_cfg.get("server", ""))
            user = resolve_value(
                conn_cfg.get("sql_login", conn_cfg.get("user", ""))
            )
            password = resolve_value(conn_cfg.get("password", ""))
            raw_databases = config.get("databases", {})
            uc_metadata = _extract_uc_metadata(raw_databases)
            table_map = _normalize_table_map(raw_databases)
            storage = config.get("storage", {})
            base = storage.get("ingest_pipeline")
            if base:
                cfg_output = storage.get("data_dir") or os.path.join(base, "data")
                cfg_watermark = storage.get("watermark_dir") or os.path.join(base, "watermarks")
                cfg_manifest = storage.get("output_manifest") or os.path.join(base, "output.yaml")
            else:
                cfg_output = storage.get("data_dir")
                cfg_watermark = storage.get("watermark_dir")
                cfg_manifest = storage.get("output_manifest")
            cfg_workers = config.get("max_workers") or config.get("parallelism")
            cfg_snapshot = config.get("snapshot_isolation")
            cfg_output_format = storage.get("output_format")
            cfg_parquet_compression = storage.get("parquet_compression")
            cfg_parquet_compression_level = storage.get("parquet_compression_level")
            cfg_row_group_size = storage.get("row_group_size")

        ct = cls(
            server=server,
            user=user,
            password=password,
            output_dir=output_dir or cfg_output or "./data",
            watermark_dir=watermark_dir or cfg_watermark or "./watermarks",
            max_workers=max_workers if max_workers is not None else (cfg_workers or 1),
            batch_size=batch_size if batch_size is not None else DEFAULT_BATCH_SIZE,
            snapshot_isolation=(
                snapshot_isolation if snapshot_isolation is not None
                else bool(cfg_snapshot)
            ),
            output_manifest=output_manifest or cfg_manifest,
            output_format=cfg_output_format or DEFAULT_OUTPUT_FORMAT,
            parquet_compression=cfg_parquet_compression or DEFAULT_PARQUET_COMPRESSION,
            parquet_compression_level=cfg_parquet_compression_level,
            row_group_size=cfg_row_group_size if cfg_row_group_size is not None else DEFAULT_ROW_GROUP_SIZE,
        )
        if not is_flat and base:
            ct.ingest_pipeline = base
        if not is_flat:
            ct._uc_metadata = uc_metadata
        if table_map:
            ct.tables = table_map
        return ct

    # -- table configuration ------------------------------------------------

    @property
    def tables(self) -> TableMap:
        """The current ``{database: {schema: tables}}`` mapping.

        *tables* can be a list of names (no per-table mode) or a dict
        mapping each name to ``"full_incremental"`` or ``"incremental"``.
        """
        return self._table_map

    @tables.setter
    def tables(self, value: TableMap) -> None:
        validated = _validate_table_map(value)
        self._table_map = validated
        self._flat_tables = _flatten_table_map(validated)
        logger.info(
            "Configured %d table(s) across %d database(s)",
            len(self._flat_tables),
            len(self._table_map),
        )

    # -- connectivity -------------------------------------------------------

    def test_connectivity(self, database: str = "master") -> bool:
        """Open a throwaway connection to *database* and return ``True`` on success."""
        az = AzureSQLConnection(
            server=self.server, user=self.user,
            password=self._password, database=database,
        )
        try:
            return az.test_connectivity()
        finally:
            az.close()

    # -- sync operations ----------------------------------------------------

    def sync(self) -> List[dict]:
        """Sync every table using its per-table mode.

        Tables configured with the dict format use their specified mode.
        Tables configured with the list format default to
        ``"full_incremental"``.

        Returns a list of per-table result dicts.
        """
        return self._run_sync(mode_override=None)

    def incremental_load(self) -> List[dict]:
        """Incremental-sync every table (ignores per-table modes).

        Returns a list of per-table result dicts.
        """
        return self._run_sync(mode_override="incremental")

    def _run_sync(self, mode_override: Optional[str] = None) -> List[dict]:
        if not self._flat_tables:
            raise RuntimeError(
                "No tables configured. Set .tables before calling sync."
            )

        if self.max_workers > 1:
            results = self._run_sync_parallel(mode_override)
        else:
            results = self._run_sync_sequential(mode_override)

        if self.output_manifest:
            self._update_output_manifest(results)
        return results

    def _update_output_manifest(self, results: List[dict]) -> None:
        """Update output manifest with sync results (add new tables only)."""
        if not self.output_manifest:
            return
        try:
            manifest = manifest_load(self.output_manifest)
            file_type = getattr(self.writer, "file_type", "parquet")
            manifest_merge_add(manifest, results, self.output_dir, file_type, uc_metadata=self._uc_metadata)
            if self.ingest_pipeline is not None:
                manifest["ingest_pipeline"] = self.ingest_pipeline
            manifest_save(self.output_manifest, manifest)
            logger.debug("Updated output manifest %s", self.output_manifest)

            if self.ingest_pipeline is not None:
                inc_path = os.path.join(self.ingest_pipeline, "incremental_output.yaml")
                try:
                    incremental_output_write(
                        results,
                        self.output_dir,
                        getattr(self.writer, "file_type", "parquet"),
                        inc_path,
                        uc_metadata=self._uc_metadata,
                        ingest_pipeline=self.ingest_pipeline,
                    )
                except Exception as inc_exc:
                    logger.warning("Failed to write incremental output %s: %s", inc_path, inc_exc)
        except Exception as exc:
            logger.warning("Failed to update output manifest %s: %s", self.output_manifest, exc)

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _error_result(
        database: str, table: str, exc: Exception, mode: Optional[str] = None,
    ) -> dict:
        result: dict = {"database": database, "table": table, "status": "error", "error": str(exc)}
        if mode is not None:
            result["mode"] = mode
        return result

    # -- sequential (max_workers=1) -----------------------------------------

    def _run_sync_sequential(self, mode_override: Optional[str]) -> List[dict]:
        conns: Dict[str, Any] = {}
        skip_cache: Dict[str, Set[str]] = {}
        metadata_cache: Dict[str, Dict[str, Any]] = {}
        results: List[dict] = []

        try:
            for database, full_table_name, table_mode, scd_type, soft_delete in self._flat_tables:
                if database not in conns:
                    try:
                        az = AzureSQLConnection(
                            server=self.server, user=self.user,
                            password=self._password, database=database,
                        )
                        conns[database] = az.connect()
                    except Exception as exc:
                        logger.error(
                            "Failed to connect to %s: %s", database, exc,
                        )
                        results.append(self._error_result(database, full_table_name, exc))
                        continue
                    try:
                        metadata_cache[database] = _fetch_db_metadata(conns[database])
                    except Exception as exc:
                        logger.warning(
                            "Batch metadata fetch failed for %s, "
                            "falling back to per-table queries: %s",
                            database, exc,
                        )
                        metadata_cache[database] = {}
                    db_entries = [
                        e for e in self._flat_tables
                        if e[0] == database
                    ]
                    db_meta = metadata_cache.get(database) or None
                    skip_cache[database] = _compute_tables_to_skip(
                        database,
                        conns[database],
                        db_entries,
                        self.watermark_dir,
                        mode_override,
                        db_metadata=db_meta,
                    )

                mode = mode_override or table_mode or "full_incremental"
                if full_table_name in skip_cache.get(database, set()):
                    results.append(_skipped_result(database, full_table_name))
                    continue

                uc_catalog = self._uc_metadata.get(database, {}).get("uc_catalog")
                db_meta = metadata_cache.get(database, {})
                tracked_lookup = db_meta.get("tracked_lookup")
                resolved_name = (
                    queries.resolve_table(full_table_name, tracked_lookup)
                    if tracked_lookup else None
                )
                try:
                    results.append(
                        sync_table(
                            conns[database],
                            full_table_name,
                            database=database,
                            output_dir=self.output_dir,
                            watermark_dir=self.watermark_dir,
                            mode=mode,
                            writer=self.writer,
                            batch_size=self.batch_size,
                            snapshot_isolation=self.snapshot_isolation,
                            scd_type=scd_type,
                            uc_catalog=uc_catalog,
                            soft_delete=soft_delete,
                            tracked_lookup=tracked_lookup,
                            db_version=db_meta.get("db_version"),
                            min_ver=(
                                db_meta["tables"].get(resolved_name, 0)
                                if db_meta.get("tables") and resolved_name
                                else None
                            ),
                            pk_cols=(
                                db_meta["pk_map"].get(resolved_name)
                                if db_meta.get("pk_map") and resolved_name
                                else None
                            ),
                        )
                    )
                except Exception as exc:
                    logger.exception(
                        "Failed to sync %s.%s", database, full_table_name,
                    )
                    results.append(self._error_result(database, full_table_name, exc, mode))
        finally:
            for conn in conns.values():
                conn.close()

        return results

    # -- parallel (max_workers>1) -------------------------------------------

    def _sync_one_table(
        self,
        pool: _ConnectionPool,
        database: str,
        full_table_name: str,
        mode: str,
        scd_type: int,
        soft_delete: bool = False,
        db_metadata: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """Sync a single table using a connection from *pool*. Thread-safe."""
        try:
            az, conn = pool.acquire(database)
        except Exception as exc:
            logger.error("Failed to connect to %s: %s", database, exc)
            return self._error_result(database, full_table_name, exc)

        uc_catalog = self._uc_metadata.get(database, {}).get("uc_catalog")
        db_meta = db_metadata or {}
        tracked_lookup = db_meta.get("tracked_lookup")
        resolved_name = (
            queries.resolve_table(full_table_name, tracked_lookup)
            if tracked_lookup else None
        )
        try:
            result = sync_table(
                conn,
                full_table_name,
                database=database,
                output_dir=self.output_dir,
                watermark_dir=self.watermark_dir,
                mode=mode,
                writer=self.writer,
                batch_size=self.batch_size,
                snapshot_isolation=self.snapshot_isolation,
                scd_type=scd_type,
                uc_catalog=uc_catalog,
                soft_delete=soft_delete,
                tracked_lookup=tracked_lookup,
                db_version=db_meta.get("db_version"),
                min_ver=(
                    db_meta["tables"].get(resolved_name, 0)
                    if db_meta.get("tables") and resolved_name
                    else None
                ),
                pk_cols=(
                    db_meta["pk_map"].get(resolved_name)
                    if db_meta.get("pk_map") and resolved_name
                    else None
                ),
            )
            pool.release(database, az)
            return result
        except Exception as exc:
            logger.exception("Failed to sync %s.%s", database, full_table_name)
            az.close()
            return self._error_result(database, full_table_name, exc, mode)

    def _run_sync_parallel(self, mode_override: Optional[str]) -> List[dict]:
        logger.info("Parallel sync with max_workers=%d", self.max_workers)

        work: List[Tuple[int, str, str, str, int, bool]] = []
        for idx, (database, full_table_name, table_mode, scd_type, soft_delete) in enumerate(
            self._flat_tables
        ):
            mode = mode_override or table_mode or "full_incremental"
            work.append((idx, database, full_table_name, mode, scd_type, soft_delete))

        results: List[Optional[dict]] = [None] * len(work)
        conn_pool = _ConnectionPool(self.server, self.user, self._password)

        skip_set: Set[Tuple[str, str]] = set()
        db_entries: Dict[str, List[FlatEntry]] = {}
        for database, full_table_name, table_mode, scd_type, soft_delete in self._flat_tables:
            db_entries.setdefault(database, []).append(
                (database, full_table_name, table_mode, scd_type, soft_delete),
            )

        metadata_cache: Dict[str, Dict[str, Any]] = {}
        metadata_lock = threading.Lock()

        def _check_db(database: str, entries: List[FlatEntry]) -> Set[str]:
            try:
                az, conn = conn_pool.acquire(database)
                try:
                    db_meta = _fetch_db_metadata(conn)
                except Exception as exc:
                    logger.warning(
                        "Batch metadata fetch failed for %s, "
                        "falling back to per-table queries: %s",
                        database, exc,
                    )
                    db_meta = {}
                with metadata_lock:
                    metadata_cache[database] = db_meta
                to_skip = _compute_tables_to_skip(
                    database, conn, entries,
                    self.watermark_dir, mode_override,
                    db_metadata=db_meta or None,
                )
                conn_pool.release(database, az)
                return to_skip
            except Exception as exc:
                logger.warning(
                    "Change check failed for %s, syncing all tables: %s",
                    database, exc,
                )
                return set()

        with ThreadPoolExecutor(max_workers=self.max_workers) as check_executor:
            check_futures = {
                check_executor.submit(_check_db, db, entries): db
                for db, entries in db_entries.items()
            }
            for future in as_completed(check_futures):
                db = check_futures[future]
                for tbl in future.result():
                    skip_set.add((db, tbl))

        for idx, db, tbl, mode, scd, sd in work:
            if (db, tbl) in skip_set:
                results[idx] = _skipped_result(db, tbl)

        work_to_run = [(idx, db, tbl, mode, scd, sd) for idx, db, tbl, mode, scd, sd in work if (db, tbl) not in skip_set]

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_idx = {
                    executor.submit(
                        self._sync_one_table, conn_pool, db, tbl, mode, scd, sd,
                        db_metadata=metadata_cache.get(db),
                    ): idx
                    for idx, db, tbl, mode, scd, sd in work_to_run
                }
                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    results[idx] = future.result()
        finally:
            conn_pool.close_all()

        return results  # type: ignore[return-value]

    # -- context manager / lifecycle ----------------------------------------

    def close(self) -> None:
        """No-op for API symmetry; connections are opened and closed per load call."""

    def __enter__(self) -> "ChangeTracker":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        n = len(self._flat_tables)
        return (
            f"ChangeTracker(server={self.server!r}, user={self.user!r}, "
            f"tables={n}, max_workers={self.max_workers})"
        )
