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

    def test_skips_table_with_zero_rows_written(self):
        manifest = {"databases": {}}
        results = [
            {"database": "db1", "table": "dbo.empty_table", "rows_written": 0, "files": []},
        ]
        merge_add(manifest, results, "/data", "parquet")
        assert manifest["databases"] == {}

    def test_skips_table_with_empty_files_list(self):
        manifest = {"databases": {}}
        results = [
            {"database": "db1", "table": "dbo.empty_table", "files": []},
        ]
        merge_add(manifest, results, "/data", "parquet")
        assert manifest["databases"] == {}

    def test_skips_table_with_rows_written_zero_no_files_key(self):
        manifest = {"databases": {}}
        results = [
            {"database": "db1", "table": "dbo.empty_table", "rows_written": 0},
        ]
        merge_add(manifest, results, "/data", "parquet")
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


class TestMergeAddColumnEvolution:
    def test_adds_columns_on_first_sync(self):
        manifest = {"databases": {}}
        results = [{
            "database": "db1", "table": "dbo.orders", "rows_written": 10,
            "columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}],
            "schema_version": 111,
        }]
        merge_add(manifest, results, "/data", "parquet")
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert tbl["columns"] == [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}]
        assert tbl["schema_version"] == 111

    def test_adds_schema_version_on_first_sync(self):
        manifest = {"databases": {}}
        results = [{"database": "db1", "table": "dbo.orders", "rows_written": 10, "schema_version": 42}]
        merge_add(manifest, results, "/data", "parquet")
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert tbl["schema_version"] == 42

    def test_columns_not_added_when_absent(self):
        manifest = {"databases": {}}
        results = [{"database": "db1", "table": "dbo.orders", "rows_written": 10}]
        merge_add(manifest, results, "/data", "parquet")
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert "columns" not in tbl
        assert "schema_version" not in tbl

    def test_appends_new_columns_on_subsequent_sync(self):
        manifest = {"databases": {
            "db1": {"uc_catalog_name": None, "dbo": {"uc_schema_name": None, "orders": {
                "file_path": "/data/db1/dbo/orders", "file_type": "parquet", "uc_table_name": "orders",
                "columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}],
                "schema_version": 111,
            }}},
        }}
        results = [{
            "database": "db1", "table": "dbo.orders", "rows_written": 5,
            "columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}, {"name": "email", "type": "nvarchar"}],
            "schema_version": 222,
        }]
        merge_add(manifest, results, "/data", "parquet")
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert len(tbl["columns"]) == 3
        assert tbl["columns"][2] == {"name": "email", "type": "nvarchar"}
        assert tbl["schema_version"] == 222

    def test_preserves_deleted_columns(self):
        manifest = {"databases": {
            "db1": {"uc_catalog_name": None, "dbo": {"uc_schema_name": None, "orders": {
                "file_path": "/data/db1/dbo/orders", "file_type": "parquet", "uc_table_name": "orders",
                "columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}, {"name": "status", "type": "nvarchar"}],
                "schema_version": 111,
            }}},
        }}
        results = [{
            "database": "db1", "table": "dbo.orders", "rows_written": 5,
            "columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}],
            "schema_version": 222,
        }]
        merge_add(manifest, results, "/data", "parquet")
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        col_names = [c["name"] for c in tbl["columns"]]
        assert "status" in col_names
        assert len(tbl["columns"]) == 3
        assert tbl["schema_version"] == 222

    def test_renamed_column_treated_as_new(self):
        manifest = {"databases": {
            "db1": {"uc_catalog_name": None, "dbo": {"uc_schema_name": None, "orders": {
                "file_path": "/data/db1/dbo/orders", "file_type": "parquet", "uc_table_name": "orders",
                "columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}],
                "schema_version": 111,
            }}},
        }}
        results = [{
            "database": "db1", "table": "dbo.orders", "rows_written": 5,
            "columns": [{"name": "id", "type": "int"}, {"name": "full_name", "type": "nvarchar"}],
            "schema_version": 333,
        }]
        merge_add(manifest, results, "/data", "parquet")
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        col_names = [c["name"] for c in tbl["columns"]]
        assert col_names == ["id", "name", "full_name"]
        assert tbl["schema_version"] == 333

    def test_schema_version_updated_on_column_change(self):
        manifest = {"databases": {
            "db1": {"uc_catalog_name": None, "dbo": {"uc_schema_name": None, "orders": {
                "file_path": "/data/db1/dbo/orders", "file_type": "parquet", "uc_table_name": "orders",
                "columns": [{"name": "id", "type": "int"}],
                "schema_version": 100,
            }}},
        }}
        results = [{
            "database": "db1", "table": "dbo.orders", "rows_written": 5,
            "columns": [{"name": "id", "type": "int"}],
            "schema_version": 200,
        }]
        merge_add(manifest, results, "/data", "parquet")
        assert manifest["databases"]["db1"]["dbo"]["orders"]["schema_version"] == 200

    def test_other_fields_not_overwritten_during_column_merge(self):
        manifest = {"databases": {
            "db1": {"uc_catalog_name": None, "dbo": {"uc_schema_name": None, "orders": {
                "file_path": "/old/path", "file_type": "parquet", "uc_table_name": "orders",
                "columns": [{"name": "id", "type": "int"}],
                "schema_version": 100,
            }}},
        }}
        results = [{
            "database": "db1", "table": "dbo.orders", "rows_written": 5,
            "columns": [{"name": "id", "type": "int"}, {"name": "email", "type": "nvarchar"}],
            "schema_version": 200,
        }]
        merge_add(manifest, results, "/new/data", "csv")
        tbl = manifest["databases"]["db1"]["dbo"]["orders"]
        assert tbl["file_path"] == "/old/path"
        assert tbl["file_type"] == "parquet"
        assert len(tbl["columns"]) == 2


class TestSaveColumns:
    def test_columns_list_round_trips(self, tmp_path):
        path = tmp_path / "out.yaml"
        manifest = {
            "databases": {
                "db1": {"uc_catalog_name": None, "dbo": {"uc_schema_name": None, "t1": {
                    "file_path": "/d", "file_type": "parquet",
                    "columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}],
                }}},
            },
        }
        save(str(path), manifest)
        loaded = load(str(path))
        assert loaded["databases"]["db1"]["dbo"]["t1"]["columns"] == [
            {"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"},
        ]

    def test_schema_version_round_trips(self, tmp_path):
        path = tmp_path / "out.yaml"
        manifest = {
            "databases": {
                "db1": {"uc_catalog_name": None, "dbo": {"uc_schema_name": None, "t1": {
                    "file_path": "/d", "file_type": "parquet",
                    "schema_version": 123456789,
                }}},
            },
        }
        save(str(path), manifest)
        loaded = load(str(path))
        assert loaded["databases"]["db1"]["dbo"]["t1"]["schema_version"] == 123456789
