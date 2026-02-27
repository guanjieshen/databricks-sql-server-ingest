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
                "table_2": "full",
                "table_3": "incremental",
            }
        }
    }
    ct.sync()

    # Parallel sync (4 tables at a time):
    ct = ChangeTracker("server", "user", "pw", max_workers=4)
    ct.tables = {"db1": {"dbo": [f"table_{i}" for i in range(1, 101)]}}
    ct.full_load()

Modes:
    ``full``               Always reload the entire table.
    ``incremental``        Strict incremental via change tracking; raises if
                           no watermark exists yet.
    ``full_incremental``   Full load when no watermark exists, incremental
                           on subsequent runs.  This is the default.
"""

from __future__ import annotations

import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from types import TracebackType
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from ._constants import DEFAULT_BATCH_SIZE, VALID_MODES
from .connection import AzureSQLConnection, load_dotenv
from .output_manifest import load as manifest_load, merge_add as manifest_merge_add, save as manifest_save
from .sync import sync_table
from .writer import OutputWriter, ParquetWriter

logger = logging.getLogger(__name__)


def expand_env(value: str) -> str:
    """Expand ``${VAR}`` references in *value* with environment variables."""

    def _repl(m):
        name = m.group(1)
        if name not in os.environ:
            raise KeyError(
                f"Environment variable {name!r} is not set "
                f"(referenced in config as ${{{name}}})"
            )
        return os.environ[name]

    return re.sub(r"\$\{(\w+)}", _repl, str(value))


def _load_config_file(path: Union[str, Path]) -> dict:
    """Load a YAML or JSON config file, chosen by extension."""
    p = Path(path)
    text = p.read_text()
    if p.suffix in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError:
            raise ImportError(
                "PyYAML is required for YAML config files. "
                "Install it with: pip install azsql_ct[yaml]"
            )
        return yaml.safe_load(text)
    return json.loads(text)


def _flat_config_to_table_map(tables: List[dict]) -> Dict[str, Dict[str, Dict[str, str]]]:
    """Convert a flat ``[{"database": ..., "table": ..., "mode": ...}]`` list
    into the nested ``{db: {schema: {table: mode}}}`` format."""
    result: Dict[str, Dict[str, Dict[str, str]]] = {}
    for entry in tables:
        db = entry["database"]
        full_name = entry["table"]
        mode = entry.get("mode", "full_incremental")
        if "." in full_name:
            schema, table = full_name.split(".", 1)
        else:
            schema, table = "dbo", full_name
        result.setdefault(db, {}).setdefault(schema, {})[table] = mode
    return result


TableSpec = Union[List[str], Dict[str, str]]
TableMap = Dict[str, Dict[str, TableSpec]]
FlatEntry = Tuple[str, str, Optional[str]]


def _flatten_table_map(table_map: TableMap) -> List[FlatEntry]:
    """Convert a table map to ``[(db, "schema.table", mode_or_none), ...]``.

    *mode_or_none* is ``None`` for list-format entries (no per-table mode).
    """
    entries: List[FlatEntry] = []
    for database, schemas in table_map.items():
        for schema, tables in schemas.items():
            if isinstance(tables, dict):
                for table, mode in tables.items():
                    entries.append((database, f"{schema}.{table}", mode))
            else:
                for table in tables:
                    entries.append((database, f"{schema}.{table}", None))
    return entries


def _validate_table_map(value: object) -> TableMap:
    """Raise ``TypeError`` / ``ValueError`` if *value* is not a valid table map."""
    if not isinstance(value, dict):
        raise TypeError(f"tables must be a dict, got {type(value).__name__}")
    for db, schemas in value.items():
        if not isinstance(db, str):
            raise TypeError(f"database key must be str, got {type(db).__name__}")
        if not isinstance(schemas, dict):
            raise TypeError(
                f"schemas for database '{db}' must be a dict, "
                f"got {type(schemas).__name__}"
            )
        for schema, tables in schemas.items():
            if not isinstance(schema, str):
                raise TypeError(
                    f"schema key must be str, got {type(schema).__name__}"
                )
            if isinstance(tables, dict):
                if not tables:
                    raise ValueError(
                        f"table dict for '{db}'.'{schema}' must not be empty"
                    )
                for tbl, mode in tables.items():
                    if not isinstance(tbl, str):
                        raise TypeError(
                            f"table name must be str, got {type(tbl).__name__}"
                        )
                    if mode not in VALID_MODES:
                        raise ValueError(
                            f"mode for '{db}'.'{schema}'.'{tbl}' must be one "
                            f"of {sorted(VALID_MODES)}, got {mode!r}"
                        )
            elif isinstance(tables, list):
                if not tables:
                    raise ValueError(
                        f"table list for '{db}'.'{schema}' must not be empty"
                    )
            else:
                raise TypeError(
                    f"tables for '{db}'.'{schema}' must be a list or dict, "
                    f"got {type(tables).__name__}"
                )
    if not value:
        raise ValueError("tables dict must not be empty")
    return value  # type: ignore[return-value]


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
    ) -> None:
        self.server = server
        self.user = user
        self._password = password
        self.output_dir = output_dir
        self.watermark_dir = watermark_dir
        self.writer: OutputWriter = writer or ParquetWriter()
        self.max_workers = max(1, max_workers)
        self.batch_size = batch_size
        self.snapshot_isolation = snapshot_isolation
        self.output_manifest = output_manifest
        self.ingest_pipeline: Optional[str] = None

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

        Supports two config layouts:

        **Nested (YAML-style):**

        .. code-block:: yaml

            connection:
              server: myserver.database.windows.net
              sql_login: sqladmin
              password: ${ADMIN_PASSWORD}
            storage:          # optional
              ingest_pipeline: ./ingest_pipeline   # or set data_dir, watermark_dir, output_manifest explicitly
              data_dir: ./data
              watermark_dir: ./watermarks
            max_workers: 8    # optional
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
                {"database": "db1", "table": "dbo.orders", "mode": "full"}
              ]
            }
        """
        load_dotenv()

        if isinstance(config, (str, Path)):
            config = _load_config_file(config)

        is_flat = "tables" in config and isinstance(config.get("tables"), list)
        base: Optional[str] = None

        if is_flat:
            server = expand_env(config.get("server", ""))
            user = expand_env(config.get("user", ""))
            password = expand_env(config.get("password", ""))
            table_map = _flat_config_to_table_map(config["tables"])
            cfg_output = config.get("output_dir")
            cfg_watermark = config.get("watermark_dir")
            cfg_manifest = config.get("output_manifest")
            cfg_workers = config.get("max_workers") or config.get("parallelism")
            cfg_snapshot = config.get("snapshot_isolation")
        else:
            conn_cfg = config.get("connection", {})
            server = expand_env(conn_cfg.get("server", ""))
            user = expand_env(
                conn_cfg.get("sql_login", conn_cfg.get("user", ""))
            )
            password = expand_env(conn_cfg.get("password", ""))
            table_map = config.get("databases", {})
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
        )
        if not is_flat and base:
            ct.ingest_pipeline = base
        if table_map:
            ct.tables = table_map
        return ct

    # -- table configuration ------------------------------------------------

    @property
    def tables(self) -> TableMap:
        """The current ``{database: {schema: tables}}`` mapping.

        *tables* can be a list of names (no per-table mode) or a dict
        mapping each name to ``"full"``, ``"incremental"``, or
        ``"full_incremental"``.
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

    def full_load(self) -> List[dict]:
        """Full-sync every table (ignores per-table modes).

        Returns a list of per-table result dicts.
        """
        return self._run_sync(mode_override="full")

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
            manifest_merge_add(manifest, results, self.output_dir, file_type)
            if self.ingest_pipeline is not None:
                manifest["ingest_pipeline"] = self.ingest_pipeline
            manifest_save(self.output_manifest, manifest)
            logger.debug("Updated output manifest %s", self.output_manifest)
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
        results: List[dict] = []

        try:
            for database, full_table_name, table_mode in self._flat_tables:
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

                mode = mode_override or table_mode or "full_incremental"
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
                        )
                    )
                except Exception as exc:
                    logger.error(
                        "Failed to sync %s.%s: %s", database, full_table_name, exc,
                    )
                    results.append(self._error_result(database, full_table_name, exc, mode))
        finally:
            for conn in conns.values():
                conn.close()

        return results

    # -- parallel (max_workers>1) -------------------------------------------

    def _sync_one_table(
        self, database: str, full_table_name: str, mode: str,
    ) -> dict:
        """Sync a single table with its own connection. Thread-safe."""
        try:
            az = AzureSQLConnection(
                server=self.server, user=self.user,
                password=self._password, database=database,
            )
            conn = az.connect()
        except Exception as exc:
            logger.error("Failed to connect to %s: %s", database, exc)
            return self._error_result(database, full_table_name, exc)

        try:
            return sync_table(
                conn,
                full_table_name,
                database=database,
                output_dir=self.output_dir,
                watermark_dir=self.watermark_dir,
                mode=mode,
                writer=self.writer,
                batch_size=self.batch_size,
                snapshot_isolation=self.snapshot_isolation,
            )
        except Exception as exc:
            logger.error("Failed to sync %s.%s: %s", database, full_table_name, exc)
            return self._error_result(database, full_table_name, exc, mode)
        finally:
            az.close()

    def _run_sync_parallel(self, mode_override: Optional[str]) -> List[dict]:
        logger.info("Parallel sync with max_workers=%d", self.max_workers)

        # Build (database, table, mode) work items preserving input order
        work: List[Tuple[int, str, str, str]] = []
        for idx, (database, full_table_name, table_mode) in enumerate(
            self._flat_tables
        ):
            mode = mode_override or table_mode or "full_incremental"
            work.append((idx, database, full_table_name, mode))

        results: List[Optional[dict]] = [None] * len(work)

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            future_to_idx = {
                pool.submit(self._sync_one_table, db, tbl, mode): idx
                for idx, db, tbl, mode in work
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                results[idx] = future.result()

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
