"""Parquet-based sync log for observability and auditing.

Writes one Parquet file per sync run to a shared directory.  Multiple
pipeline instances can safely write concurrently — each run produces a
uniquely-named file with no reads or locks required.

File layout::

    <sync_log_dir>/
    └── sync_date=2026-03-03/
        ├── pipeline_1_a1b2c3d4-....parquet
        └── pipeline_2_e5f6g7h8-....parquet
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

SYNC_LOG_SCHEMA = pa.schema([
    # Run identification
    pa.field("run_id", pa.string(), nullable=False),
    pa.field("pipeline_id", pa.string(), nullable=False),
    pa.field("run_started_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("run_completed_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("run_duration_seconds", pa.float64(), nullable=False),
    pa.field("run_status", pa.string(), nullable=False),
    pa.field("pipeline_config", pa.string(), nullable=True),
    # Source
    pa.field("server", pa.string(), nullable=False),
    pa.field("database", pa.string(), nullable=False),
    pa.field("schema_name", pa.string(), nullable=False),
    pa.field("table_name", pa.string(), nullable=False),
    pa.field("table_fqn", pa.string(), nullable=False),
    # Target
    pa.field("uc_catalog", pa.string(), nullable=True),
    pa.field("ingest_pipeline", pa.string(), nullable=True),
    # Sync config
    pa.field("configured_mode", pa.string(), nullable=True),
    pa.field("actual_mode", pa.string(), nullable=True),
    pa.field("scd_type", pa.int32(), nullable=True),
    pa.field("soft_delete", pa.bool_(), nullable=True),
    # Change tracking
    pa.field("ct_since_version", pa.int64(), nullable=True),
    pa.field("ct_current_version", pa.int64(), nullable=True),
    # Data metrics
    pa.field("rows_written", pa.int64(), nullable=True),
    pa.field("files_written", pa.int32(), nullable=True),
    pa.field("table_duration_seconds", pa.float64(), nullable=True),
    # Schema tracking
    pa.field("schema_version", pa.int64(), nullable=True),
    pa.field("schema_changed", pa.bool_(), nullable=True),
    pa.field("column_count", pa.int32(), nullable=True),
    # Status / errors
    pa.field("table_status", pa.string(), nullable=False),
    pa.field("skip_reason", pa.string(), nullable=True),
    pa.field("error_message", pa.string(), nullable=True),
    pa.field("error_type", pa.string(), nullable=True),
    # Partition key
    pa.field("sync_date", pa.date32(), nullable=False),
])


def _derive_run_status(results: List[dict]) -> str:
    statuses = {_table_status(r) for r in results}
    if statuses == {"error"}:
        return "failure"
    if "error" in statuses:
        return "partial_failure"
    return "success"


def _table_status(result: dict) -> str:
    if result.get("status") == "error":
        return "error"
    if result.get("status") == "skipped":
        return "skipped"
    return "success"


def _split_table_name(full_name: str):
    """Split 'schema.table' into (schema, table).  Falls back gracefully."""
    if "." in full_name:
        schema, table = full_name.split(".", 1)
        return schema, table
    return "dbo", full_name


def build_log_rows(
    results: List[dict],
    *,
    run_id: str,
    pipeline_id: str,
    run_started_at: datetime,
    run_completed_at: datetime,
    server: str,
    pipeline_config: Optional[str] = None,
    uc_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
    ingest_pipeline: Optional[str] = None,
    flat_tables: Optional[list] = None,
) -> List[dict]:
    """Convert sync result dicts into log row dicts matching ``SYNC_LOG_SCHEMA``."""

    run_duration = (run_completed_at - run_started_at).total_seconds()
    run_status = _derive_run_status(results)
    sync_dt = run_started_at.date()
    uc_metadata = uc_metadata or {}

    configured_modes: Dict[str, str] = {}
    if flat_tables:
        for entry in flat_tables:
            db, full_table, mode, _scd, _sd, *_rest = entry
            configured_modes[f"{db}.{full_table}"] = mode or "full_incremental"

    rows: List[dict] = []
    for r in results:
        database = r.get("database", "")
        full_table = r.get("table", "")
        schema_name, table_name = _split_table_name(full_table)
        table_fqn = f"{database}.{schema_name}.{table_name}"
        status = _table_status(r)

        uc_catalog = uc_metadata.get(database, {}).get("uc_catalog")
        cfg_mode = configured_modes.get(f"{database}.{full_table}")

        files = r.get("files")
        columns = r.get("columns")

        row = {
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "run_started_at": run_started_at,
            "run_completed_at": run_completed_at,
            "run_duration_seconds": round(run_duration, 2),
            "run_status": run_status,
            "pipeline_config": pipeline_config,
            "server": server,
            "database": database,
            "schema_name": schema_name,
            "table_name": table_name,
            "table_fqn": table_fqn,
            "uc_catalog": uc_catalog,
            "ingest_pipeline": ingest_pipeline,
            "configured_mode": cfg_mode,
            "actual_mode": r.get("mode"),
            "scd_type": r.get("scd_type"),
            "soft_delete": r.get("soft_delete"),
            "ct_since_version": r.get("since_version"),
            "ct_current_version": r.get("current_version"),
            "rows_written": r.get("rows_written"),
            "files_written": len(files) if files else None,
            "table_duration_seconds": r.get("duration_seconds"),
            "schema_version": r.get("schema_version"),
            "schema_changed": r.get("schema_changed"),
            "column_count": len(columns) if columns else None,
            "table_status": status,
            "skip_reason": r.get("reason"),
            "error_message": r.get("error"),
            "error_type": r.get("error_type"),
            "sync_date": sync_dt,
        }
        rows.append(row)

    return rows


def write_sync_log(
    sync_log_dir: str,
    results: List[dict],
    *,
    run_id: str,
    pipeline_id: str,
    run_started_at: datetime,
    run_completed_at: datetime,
    server: str,
    pipeline_config: Optional[str] = None,
    uc_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
    ingest_pipeline: Optional[str] = None,
    flat_tables: Optional[list] = None,
) -> str:
    """Build and write the sync log Parquet file.  Returns the output path."""

    rows = build_log_rows(
        results,
        run_id=run_id,
        pipeline_id=pipeline_id,
        run_started_at=run_started_at,
        run_completed_at=run_completed_at,
        server=server,
        pipeline_config=pipeline_config,
        uc_metadata=uc_metadata,
        ingest_pipeline=ingest_pipeline,
        flat_tables=flat_tables,
    )

    arrays: Dict[str, list] = {field.name: [] for field in SYNC_LOG_SCHEMA}
    for row in rows:
        for col in arrays:
            arrays[col].append(row.get(col))

    table = pa.table(arrays, schema=SYNC_LOG_SCHEMA)

    partition_dir = os.path.join(
        sync_log_dir,
        f"sync_date={run_started_at.date().isoformat()}",
    )
    os.makedirs(partition_dir, exist_ok=True)

    filename = f"{pipeline_id}_{run_id}.parquet"
    final_path = os.path.join(partition_dir, filename)

    fd, tmp_path = tempfile.mkstemp(
        dir=partition_dir, prefix=f".tmp_{pipeline_id}_", suffix=".parquet",
    )
    os.close(fd)

    try:
        pq.write_table(table, tmp_path)
        os.replace(tmp_path, final_path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    logger.info("Sync log written: %s (%d row(s))", final_path, len(rows))
    return final_path
