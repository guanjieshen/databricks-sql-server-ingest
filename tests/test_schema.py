"""Tests for azsql_ct.schema -- per-table schema file."""

from __future__ import annotations

import json
import sys
import types

import pytest

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


    def test_type_change_preserves_original_type(self, tmp_path):
        """Append-only merge matches by name; the original type is kept."""
        save(str(tmp_path), [{"name": "id", "type": "int"}], 100)
        save(str(tmp_path), [{"name": "id", "type": "bigint"}], 200)
        data = load(str(tmp_path))
        assert len(data["columns"]) == 1
        assert data["columns"][0]["type"] == "int"
        assert data["schema_version"] == 200

    def test_column_name_case_sensitivity(self, tmp_path):
        """Names differing only in case are treated as distinct columns."""
        save(str(tmp_path), [{"name": "Name", "type": "nvarchar"}], 100)
        save(str(tmp_path), [{"name": "name", "type": "nvarchar"}], 200)
        data = load(str(tmp_path))
        col_names = [c["name"] for c in data["columns"]]
        assert col_names == ["Name", "name"]

    def test_many_sequential_evolutions(self, tmp_path):
        """Cumulative schema after many add/drop cycles is correct."""
        save(str(tmp_path), [{"name": "a", "type": "int"}], 1)
        save(str(tmp_path), [{"name": "a", "type": "int"}, {"name": "b", "type": "int"}], 2)
        save(str(tmp_path), [{"name": "b", "type": "int"}, {"name": "c", "type": "int"}], 3)
        save(str(tmp_path), [{"name": "c", "type": "int"}, {"name": "d", "type": "int"}], 4)
        save(str(tmp_path), [{"name": "a", "type": "int"}, {"name": "d", "type": "int"}, {"name": "e", "type": "int"}], 5)
        data = load(str(tmp_path))
        col_names = [c["name"] for c in data["columns"]]
        assert col_names == ["a", "b", "c", "d", "e"]
        assert data["schema_version"] == 5

    def test_corrupted_schema_json_raises(self, tmp_path):
        """Invalid JSON in schema.json surfaces a JSONDecodeError."""
        (tmp_path / "schema.json").write_text("{bad json!!!")
        with pytest.raises(json.JSONDecodeError):
            load(str(tmp_path))


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

    def test_empty_description_returns_empty(self):
        assert columns_from_description([]) == []

    def test_description_tuple_with_no_type(self):
        """Single-element tuples (name only) map type to 'unknown'."""
        cols = columns_from_description([("col1",)])
        assert cols == [{"name": "col1", "type": "unknown"}]

    def test_unmapped_python_type_uses_str_repr(self):
        cols = columns_from_description([("data", memoryview)])
        assert cols[0]["name"] == "data"
        assert cols[0]["type"] == str(memoryview)
