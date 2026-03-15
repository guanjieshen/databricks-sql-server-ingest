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
        assert tbl["uc_table_name"] == "orders"
        assert tbl["file_path"] == "/data/db1/dbo/orders"
        assert tbl["file_type"] == "parquet"

    def test_adds_new_table_with_uc_metadata(self):
        manifest = {"databases": {}}
        results = [
            {"database": "db1", "table": "dbo.orders", "rows_written": 10},
        ]
        uc_metadata = {
            "db1": {
                "uc_catalog": "gshen_catalog",
                "schemas": {"dbo": "my_schema"},
            },
        }
        merge_add(manifest, results, "/data", "parquet", uc_metadata=uc_metadata)
        assert manifest["databases"]["db1"]["uc_catalog_name"] == "gshen_catalog"
        assert manifest["databases"]["db1"]["dbo"]["uc_schema_name"] == "my_schema"
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert tbl["file_path"] == "/data/db1/dbo/orders"

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

    def test_adds_scd_type_when_in_result(self):
        manifest = {"databases": {}}
        merge_add(
            manifest,
            [
                {
                    "database": "db1",
                    "table": "dbo.orders",
                    "rows_written": 10,
                    "scd_type": 2,
                },
            ],
            "/data",
            "parquet",
        )
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert tbl["scd_type"] == 2

    def test_scd_type_omitted_when_not_in_result(self):
        manifest = {"databases": {}}
        merge_add(
            manifest,
            [{"database": "db1", "table": "dbo.orders", "rows_written": 10}],
            "/data",
            "parquet",
        )
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert "scd_type" not in tbl

    @pytest.mark.parametrize("result_extra,expect_key,expect_value", [
        ({"soft_delete": True}, True, True),
        ({"soft_delete": False}, False, None),
        ({}, False, None),
    ], ids=["true_stored", "false_omitted", "absent_omitted"])
    def test_soft_delete_in_manifest(self, result_extra, expect_key, expect_value):
        manifest = {"databases": {}}
        result = {"database": "db1", "table": "dbo.orders", "rows_written": 10, **result_extra}
        merge_add(manifest, [result], "/data", "parquet")
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        if expect_key:
            assert tbl["soft_delete"] is expect_value
        else:
            assert "soft_delete" not in tbl

    @pytest.mark.parametrize("result_dict", [
        {"database": "db1", "table": "dbo.empty_table", "rows_written": 0, "files": []},
        {"database": "db1", "table": "dbo.empty_table", "files": []},
        {"database": "db1", "table": "dbo.empty_table", "rows_written": 0},
    ], ids=["zero_rows_with_files", "empty_files_no_rows", "zero_rows_no_files_key"])
    def test_skips_table_with_no_output(self, result_dict):
        manifest = {"databases": {}}
        merge_add(manifest, [result_dict], "/data", "parquet")
        assert manifest["databases"] == {}

    def test_only_empty_table_excluded_from_mixed_results(self):
        manifest = {"databases": {}}
        results = [
            {"database": "db1", "table": "dbo.orders", "rows_written": 10, "files": ["/data/db1/dbo/orders/f1.parquet"]},
            {"database": "db1", "table": "dbo.empty_table", "rows_written": 0, "files": []},
        ]
        merge_add(manifest, results, "/data", "parquet")
        assert "orders" in manifest["databases"]["db1"]["dbo"]
        assert "empty_table" not in manifest["databases"]["db1"]["dbo"]

    def test_soft_delete_true_with_scd_type_2(self):
        """Both soft_delete and scd_type are stored when present."""
        manifest = {"databases": {}}
        merge_add(
            manifest,
            [{"database": "db1", "table": "dbo.orders", "rows_written": 5,
              "scd_type": 2, "soft_delete": True}],
            "/data",
            "parquet",
        )
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert tbl["scd_type"] == 2
        assert tbl["soft_delete"] is True

    def test_soft_delete_column_stored_when_non_default(self):
        manifest = {"databases": {}}
        merge_add(
            manifest,
            [{"database": "db1", "table": "dbo.orders", "rows_written": 5,
              "soft_delete": True, "soft_delete_column": "is_removed"}],
            "/data",
            "parquet",
        )
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert tbl["soft_delete_column"] == "is_removed"

    def test_soft_delete_column_omitted_when_default(self):
        manifest = {"databases": {}}
        merge_add(
            manifest,
            [{"database": "db1", "table": "dbo.orders", "rows_written": 5,
              "soft_delete": True, "soft_delete_column": "_is_deleted"}],
            "/data",
            "parquet",
        )
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert "soft_delete_column" not in tbl

    def test_soft_delete_column_omitted_when_soft_delete_false(self):
        manifest = {"databases": {}}
        merge_add(
            manifest,
            [{"database": "db1", "table": "dbo.orders", "rows_written": 5,
              "soft_delete": False, "soft_delete_column": "is_removed"}],
            "/data",
            "parquet",
        )
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert "soft_delete_column" not in tbl

    def test_manifest_preserves_soft_delete_on_second_merge(self):
        """Adding a second table does not clobber an existing table's soft_delete flag."""
        manifest = {"databases": {}}
        merge_add(
            manifest,
            [{"database": "db1", "table": "dbo.orders", "rows_written": 5, "soft_delete": True}],
            "/data",
            "parquet",
        )
        merge_add(
            manifest,
            [{"database": "db1", "table": "dbo.customers", "rows_written": 3}],
            "/data",
            "parquet",
        )
        assert manifest["databases"]["db1"]["dbo"]["orders"]["soft_delete"] is True
        assert "customers" in manifest["databases"]["db1"]["dbo"]


class TestSave:
    def test_writes_yaml(self, tmp_path):
        path = tmp_path / "out.yaml"
        manifest = {
            "databases": {
                "db1": {
                    "uc_catalog_name": None,
                    "dbo": {
                        "uc_schema_name": None,
                        "t1": {"file_path": "/d/db1/dbo/t1", "file_type": "parquet"},
                    },
                },
            },
        }
        save(str(path), manifest)
        assert path.exists()
        data = load(str(path))
        assert data["databases"]["db1"]["dbo"]["t1"]["file_type"] == "parquet"

    def test_primary_key_list_is_indented_under_key(self, tmp_path):
        """List values (e.g. primary_key) are indented under their key for consistent YAML formatting."""
        path = tmp_path / "out.yaml"
        manifest = {
            "databases": {
                "db1": {
                    "uc_catalog_name": None,
                    "dbo": {
                        "uc_schema_name": None,
                        "t1": {
                            "file_path": "/d/db1/dbo/t1",
                            "file_type": "parquet",
                            "primary_key": ["id"],
                        },
                    },
                },
            },
        }
        save(str(path), manifest)
        raw = path.read_text()
        assert "primary_key:\n- " not in raw
        assert "primary_key:" in raw and "  - " in raw
        data = load(str(path))
        assert data["databases"]["db1"]["dbo"]["t1"]["primary_key"] == ["id"]
