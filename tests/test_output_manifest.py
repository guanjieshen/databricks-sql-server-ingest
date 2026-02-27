"""Tests for output_manifest module."""

import pytest

from azsql_ct.output_manifest import load, merge_add, save


class TestLoad:
    def test_missing_file_returns_empty_databases(self, tmp_path):
        path = tmp_path / "nonexistent.yaml"
        assert not path.exists()
        data = load(str(path))
        assert data == {"databases": {}}

    def test_empty_file_returns_empty_databases(self, tmp_path):
        path = tmp_path / "empty.yaml"
        path.write_text("")
        data = load(str(path))
        assert data == {"databases": {}}

    def test_loads_existing_manifest(self, tmp_path):
        path = tmp_path / "manifest.yaml"
        path.write_text("databases:\n  db1:\n    uc_catalog_name: null\n    dbo:\n      uc_schema_name: null\n      t1:\n        uc_table_name: null\n        file_path: /d/db1/dbo/t1\n        file_type: parquet\n")
        data = load(str(path))
        assert "db1" in data["databases"]
        assert data["databases"]["db1"]["dbo"]["t1"]["file_path"] == "/d/db1/dbo/t1"


class TestMergeAdd:
    def test_adds_new_table(self, tmp_path):
        manifest = {"databases": {}}
        results = [
            {"database": "db1", "table": "dbo.orders", "rows_written": 10},
        ]
        merge_add(manifest, results, "/data", "parquet")
        assert manifest["databases"]["db1"]["uc_catalog_name"] is None
        assert manifest["databases"]["db1"]["dbo"]["uc_schema_name"] is None
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert tbl["uc_table_name"] is None
        assert tbl["file_path"] == "/data/db1/dbo/orders"
        assert tbl["file_type"] == "parquet"

    def test_skips_error_results(self):
        manifest = {"databases": {}}
        results = [
            {"database": "db1", "table": "dbo.orders", "status": "error", "error": "fail"},
        ]
        merge_add(manifest, results, "/data", "parquet")
        assert manifest["databases"] == {}

    def test_does_not_overwrite_existing_entry(self):
        manifest = {
            "databases": {
                "db1": {
                    "uc_catalog_name": "my_catalog",
                    "dbo": {
                        "uc_schema_name": "my_schema",
                        "orders": {
                            "uc_table_name": "my_table",
                            "file_path": "/old/path",
                            "file_type": "parquet",
                        },
                    },
                },
            },
        }
        results = [
            {"database": "db1", "table": "dbo.orders", "rows_written": 5},
        ]
        merge_add(manifest, results, "/new/data", "csv")
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert tbl["uc_table_name"] == "my_table"
        assert tbl["file_path"] == "/old/path"
        assert tbl["file_type"] == "parquet"

    def test_adds_second_table_to_existing_schema(self):
        manifest = {"databases": {}}
        merge_add(
            manifest,
            [{"database": "db1", "table": "dbo.orders"}],
            "/data",
            "parquet",
        )
        merge_add(
            manifest,
            [{"database": "db1", "table": "dbo.customers"}],
            "/data",
            "parquet",
        )
        assert "orders" in manifest["databases"]["db1"]["dbo"]
        assert "customers" in manifest["databases"]["db1"]["dbo"]
        assert manifest["databases"]["db1"]["dbo"]["customers"]["file_path"] == "/data/db1/dbo/customers"

    def test_adds_primary_key_when_in_result(self):
        manifest = {"databases": {}}
        merge_add(
            manifest,
            [
                {
                    "database": "db1",
                    "table": "dbo.orders",
                    "rows_written": 10,
                    "primary_key": ["id"],
                },
            ],
            "/data",
            "parquet",
        )
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert tbl["primary_key"] == ["id"]
        assert tbl["file_path"] == "/data/db1/dbo/orders"


class TestSave:
    def test_writes_yaml(self, tmp_path):
        path = tmp_path / "out.yaml"
        manifest = {
            "databases": {
                "db1": {
                    "uc_catalog_name": None,
                    "dbo": {
                        "uc_schema_name": None,
                        "t1": {"uc_table_name": None, "file_path": "/d/db1/dbo/t1", "file_type": "parquet"},
                    },
                },
            },
        }
        save(str(path), manifest)
        assert path.exists()
        data = load(str(path))
        assert data["databases"]["db1"]["dbo"]["t1"]["file_type"] == "parquet"
