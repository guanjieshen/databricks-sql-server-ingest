"""Tests for incremental_output module."""

import re

import pytest

from azsql_ct.incremental_output import write
from azsql_ct.output_manifest import load


class TestWrite:
    def test_includes_only_tables_with_rows_written_gt_zero(self, tmp_path):
        results = [
            {"database": "db1", "table": "dbo.orders", "rows_written": 10},
            {"database": "db1", "table": "dbo.empty_table", "rows_written": 0},
            {"database": "db1", "table": "dbo.skipped", "status": "skipped", "reason": "no changes"},
        ]
        path = tmp_path / "incremental_output.yaml"
        write(results, "/data", "parquet", str(path))
        manifest = load(str(path))
        assert "orders" in manifest["databases"]["db1"]["dbo"]
        assert "empty_table" not in manifest["databases"]["db1"]["dbo"]
        assert "skipped" not in manifest["databases"]["db1"]["dbo"]

    def test_excludes_error_results(self, tmp_path):
        results = [
            {"database": "db1", "table": "dbo.orders", "rows_written": 10},
            {"database": "db1", "table": "dbo.failed", "status": "error", "error": "fail"},
        ]
        path = tmp_path / "incremental_output.yaml"
        write(results, "/data", "parquet", str(path))
        manifest = load(str(path))
        assert "orders" in manifest["databases"]["db1"]["dbo"]
        assert "failed" not in manifest["databases"]["db1"]["dbo"]

    def test_excludes_skipped_results(self, tmp_path):
        results = [
            {"database": "db1", "table": "dbo.skipped", "status": "skipped", "reason": "no changes"},
        ]
        path = tmp_path / "incremental_output.yaml"
        write(results, "/data", "parquet", str(path))
        manifest = load(str(path))
        assert manifest["databases"] == {}

    def test_generated_at_timestamp_present(self, tmp_path):
        results = [
            {"database": "db1", "table": "dbo.orders", "rows_written": 5},
        ]
        path = tmp_path / "incremental_output.yaml"
        write(results, "/data", "parquet", str(path))
        manifest = load(str(path))
        assert "generated_at" in manifest
        # ISO 8601 format (e.g. 2025-02-28T14:30:00.123456+00:00)
        assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", manifest["generated_at"])

    def test_empty_results_produces_empty_databases(self, tmp_path):
        results = [
            {"database": "db1", "table": "dbo.empty", "rows_written": 0},
        ]
        path = tmp_path / "incremental_output.yaml"
        write(results, "/data", "parquet", str(path))
        manifest = load(str(path))
        assert manifest["databases"] == {}
        assert "generated_at" in manifest

    def test_ingest_pipeline_set_when_provided(self, tmp_path):
        results = [
            {"database": "db1", "table": "dbo.orders", "rows_written": 3},
        ]
        path = tmp_path / "incremental_output.yaml"
        write(
            results, "/data", "parquet", str(path),
            ingest_pipeline="/pipeline/base",
        )
        manifest = load(str(path))
        assert manifest.get("ingest_pipeline") == "/pipeline/base"

    def test_uc_metadata_propagated(self, tmp_path):
        results = [
            {"database": "db1", "table": "dbo.orders", "rows_written": 5},
        ]
        uc_metadata = {
            "db1": {
                "uc_catalog": "gshen_catalog",
                "schemas": {"dbo": "my_schema"},
            },
        }
        path = tmp_path / "incremental_output.yaml"
        write(
            results, "/data", "parquet", str(path),
            uc_metadata=uc_metadata,
        )
        manifest = load(str(path))
        assert manifest["databases"]["db1"]["uc_catalog_name"] == "gshen_catalog"
        assert manifest["databases"]["db1"]["dbo"]["uc_schema_name"] == "my_schema"

    def test_writes_to_specified_path(self, tmp_path):
        results = [
            {"database": "db1", "table": "dbo.orders", "rows_written": 1},
        ]
        path = tmp_path / "subdir" / "incremental_output.yaml"
        write(results, "/data", "parquet", str(path))
        assert path.exists()
        manifest = load(str(path))
        assert manifest["databases"]["db1"]["dbo"]["orders"]["file_path"] == "/data/db1/dbo/orders"
