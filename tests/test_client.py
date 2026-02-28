"""Tests for azsql_ct.client -- ChangeTracker facade."""

from __future__ import annotations

from typing import List
from unittest.mock import MagicMock, patch

import pytest

from azsql_ct.client import (
    ChangeTracker,
    _extract_uc_metadata,
    _flatten_table_map,
    _normalize_table_map,
    _validate_table_map,
)


# -- helpers / validation ---------------------------------------------------


class TestFlattenTableMap:
    def test_list_format(self):
        m = {"db1": {"dbo": ["t1", "t2"]}}
        assert _flatten_table_map(m) == [
            ("db1", "dbo.t1", None, 1),
            ("db1", "dbo.t2", None, 1),
        ]

    def test_dict_format(self):
        m = {"db1": {"dbo": {"t1": "full", "t2": "incremental"}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", "full", 1) in flat
        assert ("db1", "dbo.t2", "incremental", 1) in flat

    def test_dict_format_with_scd_type(self):
        m = {"db1": {"dbo": {
            "t1": {"mode": "full_incremental", "scd_type": 2},
            "t2": "full",
        }}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", "full_incremental", 2) in flat
        assert ("db1", "dbo.t2", "full", 1) in flat

    def test_dict_format_scd_type_defaults_to_1(self):
        m = {"db1": {"dbo": {"t1": {"mode": "full"}}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", "full", 1) in flat

    def test_multiple_dbs(self):
        m = {"db1": {"dbo": ["a"]}, "db2": {"sales": {"b": "full"}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.a", None, 1) in flat
        assert ("db2", "sales.b", "full", 1) in flat

    def test_multiple_schemas(self):
        m = {"db1": {"dbo": ["t1"], "staging": {"t2": "full"}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", None, 1) in flat
        assert ("db1", "staging.t2", "full", 1) in flat

    def test_empty_map(self):
        assert _flatten_table_map({}) == []


class TestValidateTableMap:
    def test_valid_list(self):
        m = {"db1": {"dbo": ["t1"]}}
        assert _validate_table_map(m) == m

    def test_valid_dict(self):
        m = {"db1": {"dbo": {"t1": "full", "t2": "incremental"}}}
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
        m = {"db1": {"dbo": {"t1": {"mode": "full"}}}}
        assert _validate_table_map(m) == m

    def test_rejects_dict_table_config_invalid_mode(self):
        with pytest.raises(ValueError, match="must be one of"):
            _validate_table_map({"db1": {"dbo": {"t1": {"mode": "snapshot"}}}})

    def test_rejects_dict_table_config_invalid_scd_type(self):
        with pytest.raises(ValueError, match="scd_type"):
            _validate_table_map({"db1": {"dbo": {"t1": {"mode": "full", "scd_type": 3}}}})

    def test_rejects_non_str_non_dict_table_config(self):
        with pytest.raises(TypeError, match="table config for"):
            _validate_table_map({"db1": {"dbo": {"t1": 42}}})

    def test_rejects_non_str_table_name_in_dict(self):
        with pytest.raises(TypeError, match="table name must be str"):
            _validate_table_map({"db1": {"dbo": {99: "full"}}})


class TestNormalizeTableMap:
    """Tests for _normalize_table_map -- structured format support."""

    def test_passthrough_legacy_format(self):
        legacy = {"db1": {"dbo": {"t1": "full", "t2": "incremental"}}}
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
                        "tables": {"t1": "full", "t2": "incremental"},
                    }
                },
            }
        }
        expected = {"db1": {"dbo": {"t1": "full", "t2": "incremental"}}}
        assert _normalize_table_map(raw) == expected

    def test_structured_format_strips_uc_schema_without_tables_key(self):
        raw = {
            "db1": {
                "schemas": {
                    "dbo": {
                        "uc_schema": "my_schema",
                        "t1": "full",
                    }
                },
            }
        }
        expected = {"db1": {"dbo": {"t1": "full"}}}
        assert _normalize_table_map(raw) == expected

    def test_multiple_schemas(self):
        raw = {
            "db1": {
                "uc_catalog": "cat",
                "schemas": {
                    "dbo": {"tables": {"t1": "full"}},
                    "staging": {"tables": {"t2": "incremental"}},
                },
            }
        }
        result = _normalize_table_map(raw)
        assert result == {
            "db1": {
                "dbo": {"t1": "full"},
                "staging": {"t2": "incremental"},
            }
        }

    def test_mixed_databases(self):
        """One database uses structured format, another uses legacy."""
        raw = {
            "db1": {
                "uc_catalog": "cat",
                "schemas": {"dbo": {"tables": {"t1": "full"}}},
            },
            "db2": {"dbo": {"t2": "incremental"}},
        }
        result = _normalize_table_map(raw)
        assert result == {
            "db1": {"dbo": {"t1": "full"}},
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
                    "dbo": {"uc_schema": "my_schema", "tables": {"t1": "full"}},
                },
            },
        }
        result = _extract_uc_metadata(raw)
        assert result == {
            "db1": {"uc_catalog": "my_catalog", "schemas": {"dbo": "my_schema"}},
        }

    def test_empty_for_legacy_format(self):
        raw = {"db1": {"dbo": {"t1": "full"}}}
        assert _extract_uc_metadata(raw) == {}

    def test_empty_dict(self):
        assert _extract_uc_metadata({}) == {}

    def test_catalog_only_no_schemas(self):
        raw = {"db1": {"uc_catalog": "cat", "dbo": {"t1": "full"}}}
        result = _extract_uc_metadata(raw)
        assert result == {"db1": {"uc_catalog": "cat", "schemas": {}}}

    def test_multiple_databases(self):
        raw = {
            "db1": {
                "uc_catalog": "cat1",
                "schemas": {"dbo": {"uc_schema": "s1", "tables": {"t": "full"}}},
            },
            "db2": {"dbo": {"t": "full"}},
        }
        result = _extract_uc_metadata(raw)
        assert "db1" in result
        assert "db2" not in result
        assert result["db1"]["uc_catalog"] == "cat1"
        assert result["db1"]["schemas"]["dbo"] == "s1"


# -- ChangeTracker ----------------------------------------------------------


class TestChangeTrackerInit:
    def test_stores_params(self):
        ct = ChangeTracker("srv", "usr", "pw")
        assert ct.server == "srv"
        assert ct.user == "usr"
        assert ct.output_dir == "./data"
        assert ct.watermark_dir == "./watermarks"

    def test_custom_dirs(self):
        ct = ChangeTracker("s", "u", "p", output_dir="/out", watermark_dir="/wm")
        assert ct.output_dir == "/out"
        assert ct.watermark_dir == "/wm"

    def test_tables_initially_empty(self):
        ct = ChangeTracker("s", "u", "p")
        assert ct.tables == {}

    def test_repr_no_password(self):
        ct = ChangeTracker("srv", "usr", "secret")
        r = repr(ct)
        assert "srv" in r
        assert "usr" in r
        assert "secret" not in r


class TestTablesProperty:
    def test_setter_stores_list_format(self):
        ct = ChangeTracker("s", "u", "p")
        ct.tables = {"db1": {"dbo": ["t1", "t2"]}}
        assert ct.tables == {"db1": {"dbo": ["t1", "t2"]}}
        assert len(ct._flat_tables) == 2

    def test_setter_stores_dict_format(self):
        ct = ChangeTracker("s", "u", "p")
        ct.tables = {"db1": {"dbo": {"t1": "full", "t2": "incremental"}}}
        assert len(ct._flat_tables) == 2

    def test_setter_rejects_bad_input(self):
        ct = ChangeTracker("s", "u", "p")
        with pytest.raises(TypeError):
            ct.tables = "not a dict"

    def test_can_reassign(self):
        ct = ChangeTracker("s", "u", "p")
        ct.tables = {"db1": {"dbo": ["a"]}}
        ct.tables = {"db2": {"dbo": ["b", "c"]}}
        assert len(ct._flat_tables) == 2
        assert ct._flat_tables[0][0] == "db2"


class TestTestConnectivity:
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_returns_true_on_success(self, MockAz):
        instance = MockAz.return_value
        instance.test_connectivity.return_value = True
        ct = ChangeTracker("srv", "usr", "pw")
        assert ct.test_connectivity() is True
        MockAz.assert_called_once_with(
            server="srv", user="usr", password="pw", database="master",
        )
        instance.close.assert_called_once()

    @patch("azsql_ct.client.AzureSQLConnection")
    def test_returns_false_on_failure(self, MockAz):
        instance = MockAz.return_value
        instance.test_connectivity.return_value = False
        ct = ChangeTracker("srv", "usr", "pw")
        assert ct.test_connectivity() is False

    @patch("azsql_ct.client.AzureSQLConnection")
    def test_custom_database(self, MockAz):
        instance = MockAz.return_value
        instance.test_connectivity.return_value = True
        ct = ChangeTracker("srv", "usr", "pw")
        ct.test_connectivity(database="mydb")
        MockAz.assert_called_once_with(
            server="srv", user="usr", password="pw", database="mydb",
        )


class TestSync:
    def test_raises_when_no_tables(self):
        ct = ChangeTracker("s", "u", "p")
        with pytest.raises(RuntimeError, match="No tables configured"):
            ct.sync()

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_uses_per_table_modes(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": {"t1": "full", "t2": "incremental"}}}
        ct.sync()

        assert mock_sync.call_count == 2
        modes = {
            call.kwargs["mode"]
            for call in mock_sync.call_args_list
        }
        assert modes == {"full", "incremental"}

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_list_format_defaults_to_full_incremental(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": ["t1"]}}
        ct.sync()

        assert mock_sync.call_args.kwargs["mode"] == "full_incremental"
        assert mock_sync.call_args.kwargs["scd_type"] == 1

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_passes_scd_type_to_sync_table(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": {
            "t1": {"mode": "full_incremental", "scd_type": 2},
            "t2": "full",
        }}}
        ct.sync()

        assert mock_sync.call_count == 2
        scd_types = {
            call.args[1]: call.kwargs["scd_type"]
            for call in mock_sync.call_args_list
        }
        assert scd_types["dbo.t1"] == 2
        assert scd_types["dbo.t2"] == 1

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_continues_after_table_error(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.side_effect = [RuntimeError("boom"), {"table": "dbo.t2", "ok": True}]

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": {"t1": "full", "t2": "full"}}}
        results = ct.sync()

        assert len(results) == 2
        assert results[0]["status"] == "error"
        assert "boom" in results[0]["error"]
        assert results[1]["ok"] is True

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_connections_closed_after_errors(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.side_effect = RuntimeError("boom")

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": {"t1": "full"}}}
        results = ct.sync()

        assert results[0]["status"] == "error"
        mock_conn.close.assert_called_once()


class TestFullLoad:
    def test_raises_when_no_tables(self):
        ct = ChangeTracker("s", "u", "p")
        with pytest.raises(RuntimeError, match="No tables configured"):
            ct.full_load()

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_overrides_all_modes_to_full(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": {"t1": "incremental", "t2": "incremental"}}}
        ct.full_load()

        assert mock_sync.call_count == 2
        for call in mock_sync.call_args_list:
            assert call.kwargs["mode"] == "full"

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_one_connection_per_database(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": ["t1", "t2", "t3"]}}
        ct.full_load()

        MockAz.assert_called_once()

    @patch("azsql_ct.client.sync_table", side_effect=RuntimeError("boom"))
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_continues_after_error(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": ["t1"]}}
        results = ct.full_load()

        assert len(results) == 1
        assert results[0]["status"] == "error"
        mock_conn.close.assert_called_once()

    @patch("azsql_ct.client.AzureSQLConnection")
    def test_connection_failure_records_error_and_continues(self, MockAz):
        MockAz.return_value.connect.side_effect = Exception("connection refused")

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": ["t1", "t2"]}}
        results = ct.full_load()

        assert len(results) == 2
        assert all(r["status"] == "error" for r in results)
        assert "connection refused" in results[0]["error"]


class TestIncrementalLoad:
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_overrides_all_modes_to_incremental(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": {"t1": "full", "t2": "full"}}}
        ct.incremental_load()

        assert mock_sync.call_count == 2
        for call in mock_sync.call_args_list:
            assert call.kwargs["mode"] == "incremental"

    def test_raises_when_no_tables(self):
        ct = ChangeTracker("s", "u", "p")
        with pytest.raises(RuntimeError, match="No tables configured"):
            ct.incremental_load()


class TestRunSyncIntegration:
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_passes_output_and_watermark_dirs(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("s", "u", "p", output_dir="/mydata", watermark_dir="/mywm")
        ct.tables = {"db1": {"dbo": ["t1"]}}
        ct.full_load()

        _, kwargs = mock_sync.call_args
        assert kwargs["output_dir"] == "/mydata"
        assert kwargs["watermark_dir"] == "/mywm"

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_passes_custom_writer(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        custom_writer = MagicMock()
        ct = ChangeTracker("s", "u", "p", writer=custom_writer)
        ct.tables = {"db1": {"dbo": ["t1"]}}
        ct.full_load()

        _, kwargs = mock_sync.call_args
        assert kwargs["writer"] is custom_writer

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_multiple_databases_get_separate_connections(self, MockAz, mock_sync):
        conns = {}

        def make_az(**kw):
            m = MagicMock()
            c = MagicMock()
            conns[kw["database"]] = c
            m.connect.return_value = c
            return m

        MockAz.side_effect = make_az
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {
            "db1": {"dbo": ["t1"]},
            "db2": {"dbo": ["t2"]},
        }
        ct.full_load()

        assert MockAz.call_count == 2
        for c in conns.values():
            c.close.assert_called_once()

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_passes_correct_table_and_database(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("s", "u", "p")
        ct.tables = {"mydb": {"sales": ["orders"]}}
        ct.full_load()

        _, kwargs = mock_sync.call_args
        assert kwargs["database"] == "mydb"
        assert mock_sync.call_args[0][1] == "sales.orders"


class TestParallelSync:
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_syncs_all_tables(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw", max_workers=2)
        ct.tables = {"db1": {"dbo": ["t1", "t2", "t3"]}}
        results = ct.full_load()

        assert len(results) == 3
        assert mock_sync.call_count == 3

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_preserves_result_order(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn

        def fake_sync(conn, table_name, **kw):
            return {"table": table_name}

        mock_sync.side_effect = fake_sync

        ct = ChangeTracker("srv", "usr", "pw", max_workers=4)
        ct.tables = {"db1": {"dbo": ["a", "b", "c", "d"]}}
        results = ct.full_load()

        assert [r["table"] for r in results] == [
            "dbo.a", "dbo.b", "dbo.c", "dbo.d",
        ]

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_connections_bounded_by_workers(self, MockAz, mock_sync):
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw", max_workers=3)
        ct.tables = {"db1": {"dbo": ["t1", "t2", "t3"]}}
        ct.full_load()

        assert MockAz.call_count <= 3

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_continues_after_error(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn

        def fake_sync(conn, table_name, **kw):
            if "t2" in table_name:
                raise RuntimeError("t2 failed")
            return {"table": table_name, "ok": True}

        mock_sync.side_effect = fake_sync

        ct = ChangeTracker("srv", "usr", "pw", max_workers=2)
        ct.tables = {"db1": {"dbo": ["t1", "t2", "t3"]}}
        results = ct.full_load()

        assert len(results) == 3
        assert results[0]["ok"] is True
        assert results[1]["status"] == "error"
        assert "t2 failed" in results[1]["error"]
        assert results[2]["ok"] is True

    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_connection_failure(self, MockAz):
        MockAz.return_value.connect.side_effect = Exception("refused")

        ct = ChangeTracker("srv", "usr", "pw", max_workers=2)
        ct.tables = {"db1": {"dbo": ["t1"]}}
        results = ct.full_load()

        assert len(results) == 1
        assert results[0]["status"] == "error"
        assert "refused" in results[0]["error"]

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_respects_per_table_modes(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn

        def fake_sync(conn, table_name, **kw):
            return {"table": table_name, "mode": kw["mode"]}

        mock_sync.side_effect = fake_sync

        ct = ChangeTracker("srv", "usr", "pw", max_workers=2)
        ct.tables = {"db1": {"dbo": {"t1": "full", "t2": "incremental"}}}
        results = ct.sync()

        modes = {r["table"]: r["mode"] for r in results}
        assert modes["dbo.t1"] == "full"
        assert modes["dbo.t2"] == "incremental"

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_connections_closed(self, MockAz, mock_sync):
        instances = []

        def make_az(**kw):
            m = MagicMock()
            m.connect.return_value = MagicMock()
            instances.append(m)
            return m

        MockAz.side_effect = make_az
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw", max_workers=2)
        ct.tables = {"db1": {"dbo": ["t1", "t2"]}}
        ct.full_load()

        for inst in instances:
            inst.close.assert_called_once()

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_reuses_connections_for_same_database(self, MockAz, mock_sync):
        import time

        instances: List[MagicMock] = []

        def make_az(**kw):
            m = MagicMock()
            m.connect.return_value = MagicMock()
            instances.append(m)
            return m

        MockAz.side_effect = make_az

        def slow_sync(conn, table_name, **kw):
            time.sleep(0.01)
            return {"table": table_name, "ok": True}

        mock_sync.side_effect = slow_sync

        ct = ChangeTracker("srv", "usr", "pw", max_workers=2)
        ct.tables = {"db1": {"dbo": [f"t{i}" for i in range(6)]}}
        results = ct.full_load()

        assert len(results) == 6
        assert all(r["ok"] for r in results)
        assert MockAz.call_count <= 2

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_failed_sync_does_not_return_connection_to_pool(self, MockAz, mock_sync):
        """When a sync fails, the connection is discarded (not released back
        to the pool).  Verifying indirectly: all errors are reported and
        remaining tables still succeed."""
        instances: List[MagicMock] = []

        def make_az(**kw):
            m = MagicMock()
            m.connect.return_value = MagicMock()
            instances.append(m)
            return m

        MockAz.side_effect = make_az

        def fail_t2(conn, table_name, **kw):
            if "t2" in table_name:
                raise RuntimeError("transient error")
            return {"table": table_name, "ok": True}

        mock_sync.side_effect = fail_t2

        ct = ChangeTracker("srv", "usr", "pw", max_workers=2)
        ct.tables = {"db1": {"dbo": ["t1", "t2", "t3"]}}
        results = ct.full_load()

        assert results[0]["ok"] is True
        assert results[1]["status"] == "error"
        assert results[2]["ok"] is True
        for inst in instances:
            inst.close.assert_called_once()


class TestConnectionPool:
    """Unit tests for the _ConnectionPool class."""

    def test_acquire_creates_new_connection(self):
        from azsql_ct.client import _ConnectionPool

        mock_az = MagicMock()
        mock_az.connect.return_value = MagicMock()
        with patch("azsql_ct.client.AzureSQLConnection", return_value=mock_az):
            pool = _ConnectionPool("srv", "usr", "pw")
            az, conn = pool.acquire("db1")
            assert az is mock_az
            mock_az.connect.assert_called_once()

    def test_release_and_reacquire(self):
        from azsql_ct.client import _ConnectionPool

        instances = []

        def make_az(**kw):
            m = MagicMock()
            m.connect.return_value = MagicMock()
            instances.append(m)
            return m

        with patch("azsql_ct.client.AzureSQLConnection", side_effect=make_az):
            pool = _ConnectionPool("srv", "usr", "pw")
            az1, _ = pool.acquire("db1")
            pool.release("db1", az1)
            az2, _ = pool.acquire("db1")
            assert az2 is az1
            assert len(instances) == 1

    def test_separate_pools_per_database(self):
        from azsql_ct.client import _ConnectionPool

        instances = []

        def make_az(**kw):
            m = MagicMock()
            m.connect.return_value = MagicMock()
            instances.append(m)
            return m

        with patch("azsql_ct.client.AzureSQLConnection", side_effect=make_az):
            pool = _ConnectionPool("srv", "usr", "pw")
            az1, _ = pool.acquire("db1")
            az2, _ = pool.acquire("db2")
            assert az1 is not az2
            assert len(instances) == 2

    def test_close_all_closes_pooled_connections(self):
        from azsql_ct.client import _ConnectionPool

        mock_az = MagicMock()
        mock_az.connect.return_value = MagicMock()
        with patch("azsql_ct.client.AzureSQLConnection", return_value=mock_az):
            pool = _ConnectionPool("srv", "usr", "pw")
            az, _ = pool.acquire("db1")
            pool.release("db1", az)
            pool.close_all()
            mock_az.close.assert_called_once()


class TestMaxWorkersConfig:
    def test_default_is_1(self):
        ct = ChangeTracker("s", "u", "p")
        assert ct.max_workers == 1

    def test_custom_value(self):
        ct = ChangeTracker("s", "u", "p", max_workers=8)
        assert ct.max_workers == 8

    def test_clamps_to_1(self):
        ct = ChangeTracker("s", "u", "p", max_workers=0)
        assert ct.max_workers == 1
        ct2 = ChangeTracker("s", "u", "p", max_workers=-5)
        assert ct2.max_workers == 1

    def test_repr_includes_max_workers(self):
        ct = ChangeTracker("srv", "usr", "pw", max_workers=4)
        assert "max_workers=4" in repr(ct)


class TestBatchSizeConfig:
    def test_default_batch_size(self):
        ct = ChangeTracker("s", "u", "p")
        assert ct.batch_size == 10_000

    def test_custom_batch_size(self):
        ct = ChangeTracker("s", "u", "p", batch_size=500)
        assert ct.batch_size == 500

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_passes_batch_size_to_sync_table(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw", batch_size=2000)
        ct.tables = {"db1": {"dbo": ["t1"]}}
        ct.full_load()

        _, kwargs = mock_sync.call_args
        assert kwargs["batch_size"] == 2000

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_passes_batch_size(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw", max_workers=2, batch_size=3000)
        ct.tables = {"db1": {"dbo": ["t1"]}}
        ct.full_load()

        _, kwargs = mock_sync.call_args
        assert kwargs["batch_size"] == 3000


class TestContextManager:
    def test_enter_returns_self(self):
        ct = ChangeTracker("s", "u", "p")
        with ct as obj:
            assert obj is ct

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_usable_inside_with(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        with ChangeTracker("s", "u", "p") as ct:
            ct.tables = {"db": {"dbo": ["t"]}}
            ct.full_load()

        mock_sync.assert_called_once()


class TestOutputManifest:
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_sync_updates_output_manifest(self, MockAz, mock_sync, tmp_path):
        from azsql_ct.output_manifest import load

        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {
            "database": "db1",
            "table": "dbo.orders",
            "mode": "full",
            "rows_written": 10,
        }

        manifest_path = tmp_path / "output.yaml"
        ct = ChangeTracker(
            "srv", "usr", "pw",
            output_dir=str(tmp_path / "data"),
            output_manifest=str(manifest_path),
        )
        ct.tables = {"db1": {"dbo": ["orders"]}}
        ct.sync()

        assert manifest_path.exists()
        manifest = load(str(manifest_path))
        assert manifest["databases"]["db1"]["dbo"]["orders"]["file_path"] == str(tmp_path / "data" / "db1" / "dbo" / "orders")
        assert manifest["databases"]["db1"]["dbo"]["orders"]["file_type"] == "parquet"

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_manifest_includes_ingest_pipeline_location_when_configured(self, MockAz, mock_sync, tmp_path):
        from azsql_ct.output_manifest import load

        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {
            "database": "db1",
            "table": "dbo.orders",
            "mode": "full",
            "rows_written": 10,
        }

        base = str(tmp_path / "ingest_pipeline")
        ct = ChangeTracker.from_config({
            "connection": {"server": "srv", "sql_login": "u", "password": "p"},
            "storage": {"ingest_pipeline": base},
            "databases": {"db1": {"dbo": ["orders"]}},
        })
        ct.sync()

        manifest_path = tmp_path / "ingest_pipeline" / "output.yaml"
        assert manifest_path.exists()
        manifest = load(str(manifest_path))
        assert manifest.get("ingest_pipeline") == base
        assert "databases" in manifest

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_uc_metadata_propagated_to_manifest(self, MockAz, mock_sync, tmp_path):
        from azsql_ct.output_manifest import load

        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {
            "database": "db1",
            "table": "dbo.orders",
            "mode": "full",
            "rows_written": 10,
        }

        base = str(tmp_path / "pipeline")
        ct = ChangeTracker.from_config({
            "connection": {"server": "srv", "sql_login": "u", "password": "p"},
            "storage": {"ingest_pipeline": base},
            "databases": {
                "db1": {
                    "uc_catalog": "gshen_catalog",
                    "schemas": {
                        "dbo": {
                            "uc_schema": "my_schema",
                            "tables": {"orders": "full"},
                        },
                    },
                },
            },
        })
        ct.sync()

        manifest_path = tmp_path / "pipeline" / "output.yaml"
        assert manifest_path.exists()
        manifest = load(str(manifest_path))
        assert manifest["databases"]["db1"]["uc_catalog_name"] == "gshen_catalog"
        assert manifest["databases"]["db1"]["dbo"]["uc_schema_name"] == "my_schema"
        assert manifest["databases"]["db1"]["dbo"]["orders"]["uc_table_name"] == "orders"


class TestOutputFormatConfig:
    def test_output_format_defaults_to_per_table(self):
        ct = ChangeTracker("s", "u", "p")
        assert ct.output_format == "per_table"

    def test_output_format_stored(self):
        ct = ChangeTracker("s", "u", "p", output_format="unified")
        assert ct.output_format == "unified"

    def test_output_format_from_config_storage_section(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "storage": {"output_format": "unified"},
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.output_format == "unified"

    def test_output_format_from_config_defaults_per_table(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.output_format == "per_table"

    def test_unified_creates_unified_writer(self):
        from azsql_ct.writer import UnifiedParquetWriter
        ct = ChangeTracker("s", "u", "p", output_format="unified")
        assert isinstance(ct.writer, UnifiedParquetWriter)

    def test_per_table_creates_parquet_writer(self):
        from azsql_ct.writer import ParquetWriter
        ct = ChangeTracker("s", "u", "p", output_format="per_table")
        assert isinstance(ct.writer, ParquetWriter)


class TestSyncPassesUcCatalog:
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_sync_passes_uc_catalog_to_sync_table(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "databases": {
                "db1": {
                    "uc_catalog": "my_catalog",
                    "schemas": {"dbo": {"tables": {"t1": "full"}}},
                },
            },
        })
        ct.sync()

        _, kwargs = mock_sync.call_args
        assert kwargs["uc_catalog"] == "my_catalog"

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_sync_passes_uc_catalog(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "databases": {
                "db1": {
                    "uc_catalog": "my_catalog",
                    "schemas": {"dbo": {"tables": {"t1": "full"}}},
                },
            },
        })
        ct.max_workers = 2
        ct.sync()

        _, kwargs = mock_sync.call_args
        assert kwargs["uc_catalog"] == "my_catalog"
