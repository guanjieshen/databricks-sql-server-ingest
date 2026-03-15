"""Tests for azsql_ct.config -- table map parsing, validation, normalization."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from azsql_ct.config import (
    _extract_uc_metadata,
    _flat_config_to_table_map,
    _flatten_table_map,
    _load_config_file,
    _normalize_table_map,
    _validate_table_map,
    expand_env,
    resolve_secrets,
    resolve_value,
)


class TestFlattenTableMap:
    def test_list_format(self):
        m = {"db1": {"dbo": ["t1", "t2"]}}
        assert _flatten_table_map(m) == [
            ("db1", "dbo.t1", None, 1, False, None),
            ("db1", "dbo.t2", None, 1, False, None),
        ]

    def test_dict_format(self):
        m = {"db1": {"dbo": {"t1": "full_incremental", "t2": "incremental"}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", "full_incremental", 1, False, None) in flat
        assert ("db1", "dbo.t2", "incremental", 1, False, None) in flat

    def test_dict_format_with_scd_type(self):
        m = {"db1": {"dbo": {
            "t1": {"mode": "full_incremental", "scd_type": 2},
            "t2": "full_incremental",
        }}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", "full_incremental", 2, False, None) in flat
        assert ("db1", "dbo.t2", "full_incremental", 1, False, None) in flat

    def test_dict_format_scd_type_defaults_to_1(self):
        m = {"db1": {"dbo": {"t1": {"mode": "full_incremental"}}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", "full_incremental", 1, False, None) in flat

    def test_dict_format_with_soft_delete(self):
        m = {"db1": {"dbo": {
            "t1": {"mode": "full_incremental", "scd_type": 1, "soft_delete": True},
        }}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", "full_incremental", 1, True, None) in flat

    @pytest.mark.parametrize("table_map", [
        {"db1": {"dbo": {"t1": {"mode": "full_incremental"}}}},
        {"db1": {"dbo": {"t1": "full_incremental"}}},
    ], ids=["dict_config", "string_config"])
    def test_soft_delete_defaults_to_false(self, table_map):
        flat = _flatten_table_map(table_map)
        assert flat[0][4] is False

    def test_dict_format_with_soft_delete_column(self):
        m = {"db1": {"dbo": {
            "t1": {"mode": "full_incremental", "soft_delete": True, "soft_delete_column": "is_removed"},
        }}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", "full_incremental", 1, True, "is_removed") in flat

    def test_soft_delete_column_defaults_to_none(self):
        m = {"db1": {"dbo": {"t1": {"mode": "full_incremental", "soft_delete": True}}}}
        flat = _flatten_table_map(m)
        assert flat[0][5] is None

    def test_multiple_dbs(self):
        m = {"db1": {"dbo": ["a"]}, "db2": {"sales": {"b": "full_incremental"}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.a", None, 1, False, None) in flat
        assert ("db2", "sales.b", "full_incremental", 1, False, None) in flat

    def test_multiple_schemas(self):
        m = {"db1": {"dbo": ["t1"], "staging": {"t2": "full_incremental"}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", None, 1, False, None) in flat
        assert ("db1", "staging.t2", "full_incremental", 1, False, None) in flat

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

    def test_accepts_dict_table_config_with_soft_delete_column(self):
        m = {"db1": {"dbo": {"t1": {"mode": "full_incremental", "soft_delete": True, "soft_delete_column": "is_removed"}}}}
        assert _validate_table_map(m) == m

    def test_rejects_dict_table_config_invalid_soft_delete_column(self):
        with pytest.raises(ValueError, match="soft_delete_column"):
            _validate_table_map({"db1": {"dbo": {"t1": {"mode": "full_incremental", "soft_delete": True, "soft_delete_column": 123}}}})

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


class TestResolveSecrets:
    """Tests for resolve_secrets -- Databricks {{secrets/scope/key}} resolution."""

    def test_happy_path(self):
        mock_main = MagicMock()
        mock_main.dbutils.secrets.get.return_value = "s3cret"
        with patch.dict(sys.modules, {"__main__": mock_main}):
            assert resolve_secrets("{{secrets/my-scope/my-key}}") == "s3cret"
        mock_main.dbutils.secrets.get.assert_called_once_with(
            scope="my-scope", key="my-key",
        )

    def test_multiple_references(self):
        mock_main = MagicMock()
        mock_main.dbutils.secrets.get.side_effect = lambda scope, key: f"{scope}:{key}"
        with patch.dict(sys.modules, {"__main__": mock_main}):
            result = resolve_secrets(
                "user={{secrets/s/user}}&pass={{secrets/s/pass}}"
            )
        assert result == "user=s:user&pass=s:pass"

    def test_no_dbutils_raises(self):
        mock_main = MagicMock(spec=[])  # no dbutils attribute
        with patch.dict(sys.modules, {"__main__": mock_main}):
            with pytest.raises(RuntimeError, match="Databricks runtime"):
                resolve_secrets("{{secrets/scope/key}}")

    def test_no_pattern_passthrough(self):
        assert resolve_secrets("plain_password") == "plain_password"

    def test_no_pattern_does_not_require_dbutils(self):
        mock_main = MagicMock(spec=[])  # no dbutils
        with patch.dict(sys.modules, {"__main__": mock_main}):
            assert resolve_secrets("no-secrets-here") == "no-secrets-here"

    def test_env_var_syntax_ignored(self):
        assert resolve_secrets("${ADMIN_PASSWORD}") == "${ADMIN_PASSWORD}"


class TestResolveValue:
    """Tests for resolve_value -- chained secret + env-var resolution."""

    def test_resolves_secret(self):
        mock_main = MagicMock()
        mock_main.dbutils.secrets.get.return_value = "secret_val"
        with patch.dict(sys.modules, {"__main__": mock_main}):
            assert resolve_value("{{secrets/s/k}}") == "secret_val"

    def test_resolves_env_var(self):
        with patch.dict("os.environ", {"MY_VAR": "from_env"}):
            assert resolve_value("${MY_VAR}") == "from_env"

    def test_plain_passthrough(self):
        assert resolve_value("literal_password") == "literal_password"


class TestExpandEnv:
    """Tests for expand_env -- ${VAR} expansion from os.environ."""

    def test_single_var(self):
        with patch.dict("os.environ", {"DB_HOST": "myhost"}):
            assert expand_env("${DB_HOST}") == "myhost"

    def test_multiple_vars(self):
        with patch.dict("os.environ", {"USER": "admin", "PORT": "5432"}):
            assert expand_env("${USER}:${PORT}") == "admin:5432"

    def test_missing_var_raises_key_error(self):
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(KeyError, match="MISSING_VAR"):
                expand_env("${MISSING_VAR}")

    def test_no_pattern_passthrough(self):
        assert expand_env("plain_string") == "plain_string"

    def test_non_string_coerced(self):
        assert expand_env(42) == "42"


class TestLoadConfigFile:
    """Tests for _load_config_file -- YAML and JSON loading by extension."""

    def test_loads_yaml(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text("server: myhost\nport: 5432\n")
        result = _load_config_file(p)
        assert result == {"server": "myhost", "port": 5432}

    def test_loads_yml(self, tmp_path):
        p = tmp_path / "cfg.yml"
        p.write_text("key: value\n")
        assert _load_config_file(p) == {"key": "value"}

    def test_loads_json(self, tmp_path):
        p = tmp_path / "cfg.json"
        p.write_text('{"server": "myhost", "port": 5432}')
        result = _load_config_file(p)
        assert result == {"server": "myhost", "port": 5432}

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _load_config_file(tmp_path / "nonexistent.yaml")

    def test_invalid_json_raises(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text("not valid json{{{")
        with pytest.raises(Exception):
            _load_config_file(p)


class TestFlatConfigToTableMap:
    """Tests for _flat_config_to_table_map -- flat list to nested dict."""

    def test_schema_dot_table(self):
        tables = [{"database": "db1", "table": "sales.orders", "mode": "incremental"}]
        result = _flat_config_to_table_map(tables)
        assert result == {"db1": {"sales": {"orders": "incremental"}}}

    def test_no_dot_defaults_to_dbo(self):
        tables = [{"database": "db1", "table": "orders"}]
        result = _flat_config_to_table_map(tables)
        assert result == {"db1": {"dbo": {"orders": "full_incremental"}}}

    def test_mode_defaults_to_full_incremental(self):
        tables = [{"database": "db1", "table": "dbo.t1"}]
        result = _flat_config_to_table_map(tables)
        assert result["db1"]["dbo"]["t1"] == "full_incremental"

    def test_multiple_databases(self):
        tables = [
            {"database": "db1", "table": "dbo.t1", "mode": "full_incremental"},
            {"database": "db2", "table": "sales.t2", "mode": "incremental"},
        ]
        result = _flat_config_to_table_map(tables)
        assert "db1" in result and "db2" in result
        assert result["db1"]["dbo"]["t1"] == "full_incremental"
        assert result["db2"]["sales"]["t2"] == "incremental"
