"""Tests for azsql_ct.schema -- per-table schema file."""

from __future__ import annotations

import json
import sys
import types

if "mssql_python" not in sys.modules:
    sys.modules["mssql_python"] = types.ModuleType("mssql_python")

from azsql_ct.schema import columns_from_description, load, save


class TestSchemaLoad:
    def test_load_returns_empty_for_missing(self, tmp_path):
        assert load(str(tmp_path)) == {}

    def test_load_returns_saved_data(self, tmp_path):
        cols = [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}]
        save(str(tmp_path), cols, 111)
        data = load(str(tmp_path))
        assert data["columns"] == cols
        assert data["schema_version"] == 111


class TestSchemaSave:
    def test_save_creates_schema_file(self, tmp_path):
        d = tmp_path / "wm"
        d.mkdir()
        save(str(d), [{"name": "id", "type": "int"}], 42)
        assert (d / "schema.json").exists()

    def test_schema_version_stored(self, tmp_path):
        save(str(tmp_path), [{"name": "id", "type": "int"}], 999)
        data = load(str(tmp_path))
        assert data["schema_version"] == 999

    def test_updated_at_set(self, tmp_path):
        save(str(tmp_path), [{"name": "id", "type": "int"}], 1)
        data = load(str(tmp_path))
        assert "updated_at" in data
        assert "T" in data["updated_at"]

    def test_append_only_merge_adds_new_columns(self, tmp_path):
        save(str(tmp_path), [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}], 100)
        save(str(tmp_path), [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}, {"name": "email", "type": "nvarchar"}], 200)
        data = load(str(tmp_path))
        col_names = [c["name"] for c in data["columns"]]
        assert col_names == ["id", "name", "email"]
        assert data["schema_version"] == 200

    def test_append_only_merge_preserves_deleted(self, tmp_path):
        save(str(tmp_path), [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}, {"name": "status", "type": "nvarchar"}], 100)
        save(str(tmp_path), [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}], 200)
        data = load(str(tmp_path))
        col_names = [c["name"] for c in data["columns"]]
        assert "status" in col_names
        assert len(data["columns"]) == 3
        assert data["schema_version"] == 200

    def test_renamed_column_treated_as_new(self, tmp_path):
        save(str(tmp_path), [{"name": "id", "type": "int"}, {"name": "name", "type": "nvarchar"}], 100)
        save(str(tmp_path), [{"name": "id", "type": "int"}, {"name": "full_name", "type": "nvarchar"}], 300)
        data = load(str(tmp_path))
        col_names = [c["name"] for c in data["columns"]]
        assert col_names == ["id", "name", "full_name"]
        assert data["schema_version"] == 300


class TestColumnsFromDescription:
    def test_maps_python_types_to_sql_server_names(self):
        desc = [("id", int), ("name", str), ("active", bool)]
        cols = columns_from_description(desc)
        type_map = {c["name"]: c["type"] for c in cols}
        assert type_map["id"] == "int"
        assert type_map["name"] == "nvarchar"
        assert type_map["active"] == "bit"

    def test_excludes_ct_columns(self):
        desc = [
            ("SYS_CHANGE_VERSION", int),
            ("SYS_CHANGE_CREATION_VERSION", int),
            ("SYS_CHANGE_OPERATION", str),
            ("id", int),
            ("name", str),
        ]
        cols = columns_from_description(desc)
        col_names = [c["name"] for c in cols]
        assert "SYS_CHANGE_VERSION" not in col_names
        assert "SYS_CHANGE_CREATION_VERSION" not in col_names
        assert "SYS_CHANGE_OPERATION" not in col_names
        assert col_names == ["id", "name"]
