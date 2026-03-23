"""Tests for azsql_ct.sync_log — row building, Parquet writing, and naming."""

from __future__ import annotations

import os
import sys
import types
from datetime import datetime, timezone

import pytest

if "mssql_python" not in sys.modules:
    sys.modules["mssql_python"] = types.ModuleType("mssql_python")

from azsql_ct.sync_log import (
    _derive_run_status,
    _split_table_name,
    _table_status,
    build_log_rows,
    write_sync_log,
    SYNC_LOG_SCHEMA,
)

_NOW = datetime(2026, 3, 3, 12, 0, 0, tzinfo=timezone.utc)
_LATER = datetime(2026, 3, 3, 12, 5, 30, tzinfo=timezone.utc)
_RUN_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _success_result() -> dict:
    return {
        "database": "db1",
        "table": "dbo.orders",
        "mode": "incremental",
        "scd_type": 1,
        "soft_delete": False,
        "since_version": 10,
        "current_version": 15,
        "rows_written": 42,
        "files": ["/data/db1/dbo/orders/2026-03-03/orders_001.parquet"],
        "duration_seconds": 3.21,
        "primary_key": ["order_id"],
        "columns": [
            {"name": "order_id", "type": "int"},
            {"name": "amount", "type": "decimal"},
        ],
        "schema_version": 123456,
        "schema_changed": False,
    }


def _error_result() -> dict:
    return {
        "database": "db1",
        "table": "dbo.bad_table",
        "status": "error",
        "error": "Connection reset",
        "error_type": "ConnectionError",
        "mode": "full",
    }


def _skipped_result() -> dict:
    return {
        "database": "db1",
        "table": "dbo.static_table",
        "status": "skipped",
        "reason": "no changes",
    }


def _common_kwargs():
    return dict(
        run_id=_RUN_ID,
        pipeline_id="pipeline_1",
        run_started_at=_NOW,
        run_completed_at=_LATER,
        server="myserver.database.windows.net",
    )


class TestBuildLogRows:
    def test_success_result(self):
        rows = build_log_rows([_success_result()], **_common_kwargs())
        assert len(rows) == 1
        r = rows[0]
        assert r["run_id"] == _RUN_ID
        assert r["pipeline_id"] == "pipeline_1"
        assert r["server"] == "myserver.database.windows.net"
        assert r["database"] == "db1"
        assert r["schema_name"] == "dbo"
        assert r["table_name"] == "orders"
        assert r["table_fqn"] == "db1.dbo.orders"
        assert r["actual_mode"] == "incremental"
        assert r["rows_written"] == 42
        assert r["files_written"] == 1
        assert r["ct_since_version"] == 10
        assert r["ct_current_version"] == 15
        assert r["schema_version"] == 123456
        assert r["schema_changed"] is False
        assert r["column_count"] == 2
        assert r["table_status"] == "success"
        assert r["run_status"] == "success"
        assert r["run_duration_seconds"] == 330.0
        assert r["sync_date"] == _NOW.date()

    def test_error_result(self):
        rows = build_log_rows([_error_result()], **_common_kwargs())
        assert len(rows) == 1
        r = rows[0]
        assert r["table_status"] == "error"
        assert r["error_message"] == "Connection reset"
        assert r["error_type"] == "ConnectionError"
        assert r["run_status"] == "failure"
        assert r["rows_written"] is None

    def test_skipped_result(self):
        rows = build_log_rows([_skipped_result()], **_common_kwargs())
        assert len(rows) == 1
        r = rows[0]
        assert r["table_status"] == "skipped"
        assert r["skip_reason"] == "no changes"
        assert r["run_status"] == "success"

    def test_mixed_results_partial_failure(self):
        rows = build_log_rows(
            [_success_result(), _error_result(), _skipped_result()],
            **_common_kwargs(),
        )
        assert len(rows) == 3
        assert all(r["run_status"] == "partial_failure" for r in rows)

    def test_all_errors_failure(self):
        rows = build_log_rows(
            [_error_result(), _error_result()],
            **_common_kwargs(),
        )
        assert all(r["run_status"] == "failure" for r in rows)

    def test_uc_metadata_populated(self):
        uc = {"db1": {"uc_catalog": "my_catalog", "schemas": {"dbo": "my_schema"}}}
        rows = build_log_rows(
            [_success_result()], **_common_kwargs(), uc_metadata=uc,
        )
        assert rows[0]["uc_catalog"] == "my_catalog"

    def test_configured_mode_from_flat_tables(self):
        flat = [("db1", "dbo.orders", "full_incremental", 1, False, None)]
        rows = build_log_rows(
            [_success_result()], **_common_kwargs(), flat_tables=flat,
        )
        assert rows[0]["configured_mode"] == "full_incremental"

    def test_table_name_without_dot(self):
        result = _success_result()
        result["table"] = "orders"
        rows = build_log_rows([result], **_common_kwargs())
        assert rows[0]["schema_name"] == "dbo"
        assert rows[0]["table_name"] == "orders"


class TestWriteSyncLog:
    def test_write_and_read_back(self, tmp_path):
        import pyarrow.parquet as pq

        path = write_sync_log(
            str(tmp_path),
            [_success_result()],
            **_common_kwargs(),
        )
        assert os.path.isfile(path)
        table = pq.ParquetFile(path).read()
        assert table.num_rows == 1
        assert table.schema.equals(SYNC_LOG_SCHEMA)

    def test_hive_partition_directory(self, tmp_path):
        path = write_sync_log(
            str(tmp_path),
            [_success_result()],
            **_common_kwargs(),
        )
        partition_dir = os.path.basename(os.path.dirname(path))
        assert partition_dir == f"sync_date={_NOW.date().isoformat()}"

    def test_file_naming(self, tmp_path):
        path = write_sync_log(
            str(tmp_path),
            [_success_result()],
            **_common_kwargs(),
        )
        filename = os.path.basename(path)
        assert filename == f"pipeline_1_{_RUN_ID}.parquet"

    def test_no_temp_files_left(self, tmp_path):
        write_sync_log(str(tmp_path), [_success_result()], **_common_kwargs())
        for dirpath, _, filenames in os.walk(str(tmp_path)):
            for f in filenames:
                assert not f.startswith(".tmp_"), f"Temp file left behind: {f}"

    def test_concurrent_writes(self, tmp_path):
        """Two pipelines writing to the same directory should not collide."""
        kw1 = {**_common_kwargs(), "run_id": "run-1111", "pipeline_id": "pipe_a"}
        kw2 = {**_common_kwargs(), "run_id": "run-2222", "pipeline_id": "pipe_b"}

        p1 = write_sync_log(str(tmp_path), [_success_result()], **kw1)
        p2 = write_sync_log(str(tmp_path), [_success_result()], **kw2)

        assert os.path.isfile(p1)
        assert os.path.isfile(p2)
        assert p1 != p2
        assert "pipe_a_run-1111" in os.path.basename(p1)
        assert "pipe_b_run-2222" in os.path.basename(p2)

    def test_multiple_results(self, tmp_path):
        import pyarrow.parquet as pq

        results = [_success_result(), _error_result(), _skipped_result()]
        path = write_sync_log(str(tmp_path), results, **_common_kwargs())
        table = pq.ParquetFile(path).read()
        assert table.num_rows == 3


class TestDeriveRunStatus:
    """Tests for _derive_run_status -- aggregate run status from table results."""

    def test_all_success(self):
        assert _derive_run_status([_success_result()]) == "success"

    def test_all_errors(self):
        assert _derive_run_status([_error_result(), _error_result()]) == "failure"

    def test_mixed(self):
        assert _derive_run_status([_success_result(), _error_result()]) == "partial_failure"

    def test_skipped_only(self):
        assert _derive_run_status([_skipped_result()]) == "success"


class TestTableStatus:
    """Tests for _table_status -- per-table status string."""

    def test_error(self):
        assert _table_status({"status": "error"}) == "error"

    def test_skipped(self):
        assert _table_status({"status": "skipped"}) == "skipped"

    def test_success_implicit(self):
        assert _table_status({"rows_written": 10}) == "success"


class TestSplitTableName:
    """Tests for _split_table_name -- schema.table splitting."""

    def test_schema_dot_table(self):
        assert _split_table_name("dbo.orders") == ("dbo", "orders")

    def test_no_dot_defaults_to_dbo(self):
        assert _split_table_name("orders") == ("dbo", "orders")

    def test_multiple_dots_splits_on_first(self):
        assert _split_table_name("dbo.my.table") == ("dbo", "my.table")
