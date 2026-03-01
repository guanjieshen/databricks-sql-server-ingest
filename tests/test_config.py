"""Tests for azsql_ct.config -- table map parsing, validation, normalization."""

from __future__ import annotations

import pytest

from azsql_ct.config import (
    _extract_uc_metadata,
    _flatten_table_map,
    _normalize_table_map,
    _validate_table_map,
)


class TestFlattenTableMap:
    def test_list_format(self):
        m = {"db1": {"dbo": ["t1", "t2"]}}
        assert _flatten_table_map(m) == [
            ("db1", "dbo.t1", None, 1, False),
            ("db1", "dbo.t2", None, 1, False),
        ]

    def test_dict_format(self):
        m = {"db1": {"dbo": {"t1": "full_incremental", "t2": "incremental"}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", "full_incremental", 1, False) in flat
        assert ("db1", "dbo.t2", "incremental", 1, False) in flat

    def test_dict_format_with_scd_type(self):
        m = {"db1": {"dbo": {
            "t1": {"mode": "full_incremental", "scd_type": 2},
            "t2": "full_incremental",
        }}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", "full_incremental", 2, False) in flat
        assert ("db1", "dbo.t2", "full_incremental", 1, False) in flat

    def test_dict_format_scd_type_defaults_to_1(self):
        m = {"db1": {"dbo": {"t1": {"mode": "full_incremental"}}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", "full_incremental", 1, False) in flat

    def test_dict_format_with_soft_delete(self):
        m = {"db1": {"dbo": {
            "t1": {"mode": "full_incremental", "scd_type": 1, "soft_delete": True},
        }}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", "full_incremental", 1, True) in flat

    @pytest.mark.parametrize("table_map", [
        {"db1": {"dbo": {"t1": {"mode": "full_incremental"}}}},
        {"db1": {"dbo": {"t1": "full_incremental"}}},
    ], ids=["dict_config", "string_config"])
    def test_soft_delete_defaults_to_false(self, table_map):
        flat = _flatten_table_map(table_map)
        assert flat[0][4] is False

    def test_multiple_dbs(self):
        m = {"db1": {"dbo": ["a"]}, "db2": {"sales": {"b": "full_incremental"}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.a", None, 1, False) in flat
        assert ("db2", "sales.b", "full_incremental", 1, False) in flat

    def test_multiple_schemas(self):
        m = {"db1": {"dbo": ["t1"], "staging": {"t2": "full_incremental"}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", None, 1, False) in flat
        assert ("db1", "staging.t2", "full_incremental", 1, False) in flat

    def test_empty_map(self):
        assert _flatten_table_map({}) == []


class TestValidateTableMap:
    def test_valid_list(self):
        m = {"db1": {"dbo": ["t1"]}}
        assert _validate_table_map(m) == m

    def test_valid_dict(self):
        m = {"db1": {"dbo": {"t1": "full_incremental", "t2": "incremental"}}}
        assert _validate_table_map(m) == m

    def test_rejects_non_dict(self):
        with pytest.raises(TypeError, match="tables must be a dict"):
            _validate_table_map([("db1", "dbo.t1")])

    def test_rejects_non_dict_schemas(self):
        with pytest.raises(TypeError, match="schemas for database"):
            _validate_table_map({"db1": ["dbo.t1"]})

    def test_rejects_non_list_or_dict_tables(self):
        with pytest.raises(TypeError, match="must be a list or dict"):
            _validate_table_map({"db1": {"dbo": "t1"}})

    def test_rejects_empty_table_list(self):
        with pytest.raises(ValueError, match="must not be empty"):
            _validate_table_map({"db1": {"dbo": []}})

    def test_rejects_empty_table_dict(self):
        with pytest.raises(ValueError, match="must not be empty"):
            _validate_table_map({"db1": {"dbo": {}}})

    def test_rejects_empty_outer_dict(self):
        with pytest.raises(ValueError, match="must not be empty"):
            _validate_table_map({})

    def test_rejects_non_str_database_key(self):
        with pytest.raises(TypeError, match="database key must be str"):
            _validate_table_map({123: {"dbo": ["t1"]}})

    def test_rejects_non_str_schema_key(self):
        with pytest.raises(TypeError, match="schema key must be str"):
            _validate_table_map({"db1": {42: ["t1"]}})

    def test_rejects_invalid_mode_in_dict(self):
        with pytest.raises(ValueError, match="must be one of"):
            _validate_table_map({"db1": {"dbo": {"t1": "snapshot"}}})

    def test_accepts_full_incremental_mode(self):
        m = {"db1": {"dbo": {"t1": "full_incremental"}}}
        assert _validate_table_map(m) == m

    def test_accepts_dict_table_config_with_scd_type(self):
        m = {"db1": {"dbo": {"t1": {"mode": "full_incremental", "scd_type": 2}}}}
        assert _validate_table_map(m) == m

    def test_accepts_dict_table_config_without_scd_type(self):
        m = {"db1": {"dbo": {"t1": {"mode": "full_incremental"}}}}
        assert _validate_table_map(m) == m

    def test_rejects_dict_table_config_invalid_mode(self):
        with pytest.raises(ValueError, match="must be one of"):
            _validate_table_map({"db1": {"dbo": {"t1": {"mode": "snapshot"}}}})

    def test_rejects_dict_table_config_invalid_scd_type(self):
        with pytest.raises(ValueError, match="scd_type"):
            _validate_table_map({"db1": {"dbo": {"t1": {"mode": "full_incremental", "scd_type": 3}}}})

    def test_accepts_dict_table_config_with_soft_delete(self):
        m = {"db1": {"dbo": {"t1": {"mode": "full_incremental", "soft_delete": True}}}}
        assert _validate_table_map(m) == m

    def test_rejects_dict_table_config_invalid_soft_delete(self):
        with pytest.raises(ValueError, match="soft_delete"):
            _validate_table_map({"db1": {"dbo": {"t1": {"mode": "full_incremental", "soft_delete": "yes"}}}})

    def test_rejects_non_str_non_dict_table_config(self):
        with pytest.raises(TypeError, match="table config for"):
            _validate_table_map({"db1": {"dbo": {"t1": 42}}})

    def test_rejects_non_str_table_name_in_dict(self):
        with pytest.raises(TypeError, match="table name must be str"):
            _validate_table_map({"db1": {"dbo": {99: "full_incremental"}}})


class TestNormalizeTableMap:
    """Tests for _normalize_table_map -- structured format support."""

    def test_passthrough_legacy_format(self):
        legacy = {"db1": {"dbo": {"t1": "full_incremental", "t2": "incremental"}}}
        assert _normalize_table_map(legacy) == legacy

    def test_passthrough_legacy_list_format(self):
        legacy = {"db1": {"dbo": ["t1", "t2"]}}
        assert _normalize_table_map(legacy) == legacy

    def test_structured_format_with_tables_key(self):
        raw = {
            "db1": {
                "uc_catalog": "my_catalog",
                "schemas": {
                    "dbo": {
                        "uc_schema": "my_schema",
                        "tables": {"t1": "full_incremental", "t2": "incremental"},
                    }
                },
            }
        }
        expected = {"db1": {"dbo": {"t1": "full_incremental", "t2": "incremental"}}}
        assert _normalize_table_map(raw) == expected

    def test_structured_format_strips_uc_schema_without_tables_key(self):
        raw = {
            "db1": {
                "schemas": {
                    "dbo": {
                        "uc_schema": "my_schema",
                        "t1": "full_incremental",
                    }
                },
            }
        }
        expected = {"db1": {"dbo": {"t1": "full_incremental"}}}
        assert _normalize_table_map(raw) == expected

    def test_multiple_schemas(self):
        raw = {
            "db1": {
                "uc_catalog": "cat",
                "schemas": {
                    "dbo": {"tables": {"t1": "full_incremental"}},
                    "staging": {"tables": {"t2": "incremental"}},
                },
            }
        }
        result = _normalize_table_map(raw)
        assert result == {
            "db1": {
                "dbo": {"t1": "full_incremental"},
                "staging": {"t2": "incremental"},
            }
        }

    def test_mixed_databases(self):
        """One database uses structured format, another uses legacy."""
        raw = {
            "db1": {
                "uc_catalog": "cat",
                "schemas": {"dbo": {"tables": {"t1": "full_incremental"}}},
            },
            "db2": {"dbo": {"t2": "incremental"}},
        }
        result = _normalize_table_map(raw)
        assert result == {
            "db1": {"dbo": {"t1": "full_incremental"}},
            "db2": {"dbo": {"t2": "incremental"}},
        }

    def test_empty_map(self):
        assert _normalize_table_map({}) == {}

    def test_rejects_non_dict_schemas_section(self):
        with pytest.raises(TypeError, match="'schemas' for database"):
            _normalize_table_map({"db1": {"schemas": "bad"}})

    def test_rejects_non_dict_schema_value(self):
        with pytest.raises(TypeError, match="schema 'dbo' in database"):
            _normalize_table_map({"db1": {"schemas": {"dbo": "bad"}}})


class TestExtractUcMetadata:
    """Tests for _extract_uc_metadata -- UC field extraction."""

    def test_extracts_catalog_and_schema(self):
        raw = {
            "db1": {
                "uc_catalog": "my_catalog",
                "schemas": {
                    "dbo": {"uc_schema": "my_schema", "tables": {"t1": "full_incremental"}},
                },
            },
        }
        result = _extract_uc_metadata(raw)
        assert result == {
            "db1": {"uc_catalog": "my_catalog", "schemas": {"dbo": "my_schema"}},
        }

    def test_empty_for_legacy_format(self):
        raw = {"db1": {"dbo": {"t1": "full_incremental"}}}
        assert _extract_uc_metadata(raw) == {}

    def test_empty_dict(self):
        assert _extract_uc_metadata({}) == {}

    def test_catalog_only_no_schemas(self):
        raw = {"db1": {"uc_catalog": "cat", "dbo": {"t1": "full_incremental"}}}
        result = _extract_uc_metadata(raw)
        assert result == {"db1": {"uc_catalog": "cat", "schemas": {}}}

    def test_multiple_databases(self):
        raw = {
            "db1": {
                "uc_catalog": "cat1",
                "schemas": {"dbo": {"uc_schema": "s1", "tables": {"t": "full_incremental"}}},
            },
            "db2": {"dbo": {"t": "full_incremental"}},
        }
        result = _extract_uc_metadata(raw)
        assert "db1" in result
        assert "db2" not in result
        assert result["db1"]["uc_catalog"] == "cat1"
        assert result["db1"]["schemas"]["dbo"] == "s1"
