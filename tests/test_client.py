"""Tests for azsql_ct.client -- ChangeTracker facade."""

from __future__ import annotations

from typing import List
from unittest.mock import MagicMock, patch

import pytest

from azsql_ct.client import ChangeTracker, _flatten_table_map, _validate_table_map


# -- helpers / validation ---------------------------------------------------


class TestFlattenTableMap:
    def test_list_format(self):
        m = {"db1": {"dbo": ["t1", "t2"]}}
        assert _flatten_table_map(m) == [
            ("db1", "dbo.t1", None),
            ("db1", "dbo.t2", None),
        ]

    def test_dict_format(self):
        m = {"db1": {"dbo": {"t1": "full", "t2": "incremental"}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", "full") in flat
        assert ("db1", "dbo.t2", "incremental") in flat

    def test_multiple_dbs(self):
        m = {"db1": {"dbo": ["a"]}, "db2": {"sales": {"b": "full"}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.a", None) in flat
        assert ("db2", "sales.b", "full") in flat

    def test_multiple_schemas(self):
        m = {"db1": {"dbo": ["t1"], "staging": {"t2": "full"}}}
        flat = _flatten_table_map(m)
        assert ("db1", "dbo.t1", None) in flat
        assert ("db1", "staging.t2", "full") in flat

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

    def test_rejects_non_str_table_name_in_dict(self):
        with pytest.raises(TypeError, match="table name must be str"):
            _validate_table_map({"db1": {"dbo": {99: "full"}}})


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
    def test_parallel_each_table_gets_own_connection(self, MockAz, mock_sync):
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw", max_workers=3)
        ct.tables = {"db1": {"dbo": ["t1", "t2", "t3"]}}
        ct.full_load()

        assert MockAz.call_count == 3

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
