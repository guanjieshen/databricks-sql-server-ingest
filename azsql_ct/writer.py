"""Output writers for sync results.

Defines the ``OutputWriter`` protocol and two implementations:

* ``ParquetWriter`` -- writes native per-table Parquet (original behaviour).
* ``UnifiedParquetWriter`` -- writes a Lakeflow-Connect-style bronze
  envelope schema (JSON ``data`` column, ``table_id`` struct, ``cursor``
  struct, ``operation``, ``schemaVersion``).

Both follow a **write-then-rename** strategy: each file is written to a
``.tmp`` suffix first and only renamed to its final name after the full
write completes successfully.  This prevents downstream consumers from
reading partial files and makes cleanup of crash leftovers trivial.

``ParquetWriter`` streams row groups incrementally via
``pyarrow.parquet.ParquetWriter`` so that peak memory is proportional to
*row_group_size* (default 500 K rows) rather than *max_rows_per_file*.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple

logger = logging.getLogger(__name__)

DEFAULT_ROW_GROUP_SIZE = 500_000
_PARTITION_BUFFER_CAP = 10_000
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
        **kwargs: Any,
    ) -> WriteResult:
        """Write *rows* (with column metadata in *description*) to *dir_path*.

        Returns ``(file_paths, row_count)``.

        Implementations may accept extra keyword arguments (e.g.
        ``table_metadata``) and should ignore any they do not recognise.
        """
        ...  # pragma: no cover


class ParquetWriter:
    """Write query results to Parquet files via streaming row groups.

    Rows are buffered in chunks of *row_group_size* and written incrementally
    using ``pyarrow.parquet.ParquetWriter``.  Peak memory is proportional to
    *row_group_size* rather than *max_rows_per_file*.

    Splits output into multiple files when *max_rows_per_file* is exceeded.

    If *partition_column* is set, files are written under *dir_path* in
    day subdirectories (YYYY-MM-DD) based on that column's value.  Rows
    with null/invalid values go under ``_unknown``.  Per-partition buffers
    are capped at ``min(row_group_size, 10_000)`` to bound memory when many
    partitions are open simultaneously.

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
    def _rows_to_batch(
        rows: List[tuple],
        col_names: List[str],
        schema: Any = None,
    ) -> Tuple[Any, Any]:
        """Convert a buffer of row tuples into a ``pa.RecordBatch``.

        On the first call (*schema* is ``None``), the schema is inferred from
        the data.  All-null columns are cast to ``int64`` so the Parquet
        schema stays consistent across full and incremental writes.

        Returns ``(record_batch, schema)`` so the caller can reuse the schema
        for subsequent batches.
        """
        import pyarrow as pa

        columns = {
            name: ParquetWriter._coerce_column([row[i] for row in rows])
            for i, name in enumerate(col_names)
        }

        if schema is None:
            table = pa.table(columns)
            has_nulls = any(pa.types.is_null(f.type) for f in table.schema)
            schema = pa.schema(
                f.with_type(pa.int64()) if pa.types.is_null(f.type) else f
                for f in table.schema
            )
            if has_nulls:
                table = table.cast(schema)
            return table.to_batches()[0], schema

        return pa.RecordBatch.from_pydict(columns, schema=schema), schema

    def write(
        self,
        rows: Iterable,
        description: Sequence[Tuple[str, ...]],
        dir_path: str,
        prefix: str,
        **kwargs: Any,
    ) -> WriteResult:
        import pyarrow.parquet as pq

        col_names = [col[0] for col in description]
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        pending: List[Tuple[str, str]] = []
        row_count = 0

        partition_idx: Optional[int] = None
        if self.partition_column and self.partition_column in col_names:
            partition_idx = col_names.index(self.partition_column)
        if partition_idx is not None:
            return self._write_partitioned(
                rows, col_names, dir_path, prefix, ts, partition_idx,
            )

        schema: Any = None
        pq_writer: Optional[pq.ParquetWriter] = None
        rows_in_file = 0
        part = 1
        buf: List[tuple] = []

        def _flush() -> None:
            nonlocal schema, pq_writer
            if not buf:
                return
            batch, schema = self._rows_to_batch(buf, col_names, schema)
            if pq_writer is None:
                final = os.path.join(dir_path, f"{prefix}_{ts}_part{part}.parquet")
                tmp = final + _TEMP_SUFFIX
                pq_writer = pq.ParquetWriter(tmp, schema)
                pending.append((tmp, final))
            pq_writer.write_batch(batch)
            buf.clear()

        def _close_file() -> None:
            nonlocal pq_writer, rows_in_file, part
            if pq_writer is not None:
                pq_writer.close()
                pq_writer = None
            rows_in_file = 0
            part += 1

        for row in rows:
            buf.append(tuple(row))
            row_count += 1
            rows_in_file += 1

            if len(buf) >= self.row_group_size:
                _flush()

            if rows_in_file >= self.max_rows_per_file:
                _flush()
                _close_file()

        _flush()
        if pq_writer is not None:
            pq_writer.close()

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
        partition_idx: int,
    ) -> WriteResult:
        """Stream rows into per-partition Parquet writers.

        Each partition maintains a small buffer that is flushed as a row group
        when it reaches ``min(row_group_size, _PARTITION_BUFFER_CAP)``.  This
        bounds memory even when many partitions are open simultaneously.
        """
        import pyarrow.parquet as pq

        flush_size = min(self.row_group_size, _PARTITION_BUFFER_CAP)
        pending: List[Tuple[str, str]] = []
        row_count = 0

        parts: Dict[str, dict] = {}

        def _ensure(key: str) -> dict:
            if key not in parts:
                parts[key] = {
                    "writer": None,
                    "buf": [],
                    "rows_in_file": 0,
                    "part_num": 1,
                    "schema": None,
                }
            return parts[key]

        def _flush_part(key: str, ps: dict) -> None:
            if not ps["buf"]:
                return
            batch, ps["schema"] = self._rows_to_batch(
                ps["buf"], col_names, ps["schema"],
            )
            if ps["writer"] is None:
                part_dir = os.path.join(dir_path, key)
                os.makedirs(part_dir, exist_ok=True)
                final = os.path.join(
                    part_dir, f"{prefix}_{ts}_part{ps['part_num']}.parquet",
                )
                tmp = final + _TEMP_SUFFIX
                ps["writer"] = pq.ParquetWriter(tmp, ps["schema"])
                pending.append((tmp, final))
            ps["writer"].write_batch(batch)
            ps["buf"].clear()

        def _close_part_file(ps: dict) -> None:
            if ps["writer"] is not None:
                ps["writer"].close()
                ps["writer"] = None
            ps["rows_in_file"] = 0
            ps["part_num"] += 1

        for row in rows:
            t = tuple(row)
            row_count += 1
            key = _value_to_partition_date(
                t[partition_idx] if partition_idx < len(t) else None,
            )
            ps = _ensure(key)
            ps["buf"].append(t)
            ps["rows_in_file"] += 1

            if len(ps["buf"]) >= flush_size:
                _flush_part(key, ps)

            if ps["rows_in_file"] >= self.max_rows_per_file:
                _flush_part(key, ps)
                _close_part_file(ps)

        for key, ps in parts.items():
            _flush_part(key, ps)
            if ps["writer"] is not None:
                ps["writer"].close()

        files = _finalize_temp_files(pending)
        logger.debug(
            "Wrote %d file(s) (%d rows) to %s (partitioned by day)",
            len(files), row_count, dir_path,
        )
        return files, row_count


# ---------------------------------------------------------------------------
# Unified (bronze envelope) writer
# ---------------------------------------------------------------------------

_CT_COLUMNS = frozenset({
    "SYS_CHANGE_VERSION",
    "SYS_CHANGE_CREATION_VERSION",
    "SYS_CHANGE_OPERATION",
})

OP_MAP: Dict[str, str] = {
    "I": "INSERT",
    "U": "UPDATE",
    "D": "DELETE",
    "L": "LOAD",
}


def _make_uoid(database: str, schema: str, table: str) -> str:
    """Deterministic UUID5 from the (database, schema, table) triple."""
    key = f"{database}.{schema}.{table}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, key))


def _compute_schema_version(description: Sequence[Tuple[str, ...]]) -> int:
    """Stable int64 hash from column names and type info in *description*.

    Only data columns are considered (CT metadata columns are excluded).
    """
    parts = []
    for col in description:
        name = col[0]
        if name in _CT_COLUMNS:
            continue
        type_code = col[1] if len(col) > 1 else ""
        parts.append(f"{name}:{type_code}")
    sig = "|".join(parts)
    digest = hashlib.sha256(sig.encode()).hexdigest()
    return int(digest[:15], 16)


def _bronze_schema() -> Any:
    """Return the PyArrow schema for the unified bronze envelope."""
    import pyarrow as pa

    return pa.schema([
        ("data", pa.string()),
        ("table_id", pa.struct([
            ("catalog", pa.string()),
            ("schema", pa.string()),
            ("name", pa.string()),
            ("uoid", pa.string()),
        ])),
        ("cursor", pa.struct([
            ("lsn", pa.string()),
            ("seqNum", pa.string()),
            ("sequence", pa.string()),
            ("timestamp", pa.int64()),
        ])),
        ("extractionTimestamp", pa.int64()),
        ("operation", pa.string()),
        ("schemaVersion", pa.int64()),
    ])


class UnifiedParquetWriter:
    """Write query results as a Lakeflow-Connect-style bronze envelope.

    Every row is transformed into the unified bronze schema:

    * ``data`` -- JSON string of all non-CT data columns.
    * ``table_id`` -- struct identifying the source table.
    * ``cursor`` -- struct with change-tracking position info.
    * ``extractionTimestamp`` -- epoch milliseconds of the sync.
    * ``operation`` -- INSERT / UPDATE / DELETE / LOAD.
    * ``schemaVersion`` -- deterministic hash of the source column set.

    Requires ``table_metadata`` kwarg to be passed to :meth:`write`.

    Streaming and file-splitting behaviour mirrors :class:`ParquetWriter`.
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

    def write(
        self,
        rows: Iterable,
        description: Sequence[Tuple[str, ...]],
        dir_path: str,
        prefix: str,
        **kwargs: Any,
    ) -> WriteResult:
        import pyarrow as pa
        import pyarrow.parquet as pq

        meta: Dict[str, Any] = kwargs.get("table_metadata", {})
        database = meta.get("database", "")
        schema_name = meta.get("schema", "")
        table_name = meta.get("table", "")
        catalog = meta.get("catalog") or database
        uoid = meta.get("uoid") or _make_uoid(database, schema_name, table_name)
        extraction_ts = meta.get("extraction_timestamp") or int(time.time() * 1000)
        schema_version = meta.get("schema_version") or _compute_schema_version(description)

        col_names = [col[0] for col in description]
        ct_map: Dict[str, int] = {}
        data_indices: List[int] = []
        data_col_names: List[str] = []
        for i, name in enumerate(col_names):
            if name in _CT_COLUMNS:
                ct_map[name] = i
            else:
                data_indices.append(i)
                data_col_names.append(name)

        table_id_val = {
            "catalog": catalog,
            "schema": schema_name,
            "name": table_name,
            "uoid": uoid,
        }

        arrow_schema = _bronze_schema()
        ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        pending: List[Tuple[str, str]] = []
        row_count = 0
        pq_writer: Optional[pq.ParquetWriter] = None
        rows_in_file = 0
        part = 1

        buf_data: List[str] = []
        buf_table_id: List[dict] = []
        buf_cursor: List[dict] = []
        buf_extraction_ts: List[int] = []
        buf_operation: List[str] = []
        buf_schema_version: List[int] = []

        def _flush() -> None:
            nonlocal pq_writer
            if not buf_data:
                return
            batch = pa.RecordBatch.from_pydict(
                {
                    "data": buf_data,
                    "table_id": buf_table_id,
                    "cursor": buf_cursor,
                    "extractionTimestamp": buf_extraction_ts,
                    "operation": buf_operation,
                    "schemaVersion": buf_schema_version,
                },
                schema=arrow_schema,
            )
            if pq_writer is None:
                final = os.path.join(dir_path, f"{prefix}_{ts_str}_part{part}.parquet")
                tmp = final + _TEMP_SUFFIX
                pq_writer = pq.ParquetWriter(tmp, arrow_schema)
                pending.append((tmp, final))
            pq_writer.write_batch(batch)
            buf_data.clear()
            buf_table_id.clear()
            buf_cursor.clear()
            buf_extraction_ts.clear()
            buf_operation.clear()
            buf_schema_version.clear()

        def _close_file() -> None:
            nonlocal pq_writer, rows_in_file, part
            if pq_writer is not None:
                pq_writer.close()
                pq_writer = None
            rows_in_file = 0
            part += 1

        for row in rows:
            t = tuple(row)
            row_count += 1
            rows_in_file += 1

            data_dict = {}
            for j, idx in enumerate(data_indices):
                val = t[idx] if idx < len(t) else None
                data_dict[data_col_names[j]] = val
            buf_data.append(json.dumps(data_dict, default=str))

            buf_table_id.append(table_id_val)

            change_ver = t[ct_map["SYS_CHANGE_VERSION"]] if "SYS_CHANGE_VERSION" in ct_map else None
            raw_op = t[ct_map["SYS_CHANGE_OPERATION"]] if "SYS_CHANGE_OPERATION" in ct_map else None
            buf_cursor.append({
                "lsn": None,
                "seqNum": str(change_ver) if change_ver is not None else None,
                "sequence": str(change_ver) if change_ver is not None else None,
                "timestamp": None,
            })
            buf_extraction_ts.append(extraction_ts)
            buf_operation.append(OP_MAP.get(str(raw_op).strip() if raw_op else "", str(raw_op) if raw_op else "UNKNOWN"))
            buf_schema_version.append(schema_version)

            if len(buf_data) >= self.row_group_size:
                _flush()

            if rows_in_file >= self.max_rows_per_file:
                _flush()
                _close_file()

        _flush()
        if pq_writer is not None:
            pq_writer.close()

        files = _finalize_temp_files(pending)
        logger.debug(
            "Wrote %d unified file(s) (%d rows) to %s",
            len(files), row_count, dir_path,
        )
        return files, row_count
