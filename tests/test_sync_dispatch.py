"""Tests for sync dispatch -- sequential sync, mode overrides, skip logic."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from azsql_ct.client import ChangeTracker


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
        ct.tables = {"db1": {"dbo": {"t1": "full_incremental", "t2": "incremental"}}}
        ct.sync()

        assert mock_sync.call_count == 2
        modes = {
            call.kwargs["mode"]
            for call in mock_sync.call_args_list
        }
        assert modes == {"full_incremental", "incremental"}

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
            "t2": "full_incremental",
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
    def test_passes_soft_delete_to_sync_table(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": {
            "t1": {"mode": "full_incremental", "soft_delete": True},
            "t2": "full_incremental",
        }}}
        ct.sync()

        assert mock_sync.call_count == 2
        soft_deletes = {
            call.args[1]: call.kwargs["soft_delete"]
            for call in mock_sync.call_args_list
        }
        assert soft_deletes["dbo.t1"] is True
        assert soft_deletes["dbo.t2"] is False

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_passes_soft_delete_column_to_sync_table(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": {
            "t1": {"mode": "full_incremental", "soft_delete": True, "soft_delete_column": "is_removed"},
            "t2": {"mode": "full_incremental", "soft_delete": True},
        }}}
        ct.sync()

        assert mock_sync.call_count == 2
        sd_cols = {
            call.args[1]: call.kwargs["soft_delete_column"]
            for call in mock_sync.call_args_list
        }
        assert sd_cols["dbo.t1"] == "is_removed"
        assert sd_cols["dbo.t2"] == "_is_deleted"

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_pipeline_level_soft_delete_column_used_as_fallback(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw", soft_delete_column="deleted_flag")
        ct.tables = {"db1": {"dbo": {
            "t1": {"mode": "full_incremental", "soft_delete": True},
        }}}
        ct.sync()

        _, kwargs = mock_sync.call_args
        assert kwargs["soft_delete_column"] == "deleted_flag"

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_continues_after_table_error(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.side_effect = [RuntimeError("boom"), {"table": "dbo.t2", "ok": True}]

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": {"t1": "full_incremental", "t2": "full_incremental"}}}
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
        ct.tables = {"db1": {"dbo": {"t1": "full_incremental"}}}
        results = ct.sync()

        assert results[0]["status"] == "error"
        mock_conn.close.assert_called_once()


class TestIncrementalLoad:
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_overrides_all_modes_to_incremental(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": {"t1": "full_incremental", "t2": "full_incremental"}}}
        ct.incremental_load()

        assert mock_sync.call_count == 2
        for call in mock_sync.call_args_list:
            assert call.kwargs["mode"] == "incremental"

    def test_raises_when_no_tables(self):
        ct = ChangeTracker("s", "u", "p")
        with pytest.raises(RuntimeError, match="No tables configured"):
            ct.incremental_load()


class TestSkipUnchangedTables:
    """Tests for batch change-check optimization: skip incremental tables with no changes."""

    @patch("azsql_ct.client._compute_tables_to_skip")
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_skips_all_incremental_when_no_changes(self, MockAz, mock_sync, mock_compute):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_compute.return_value = {"dbo.t1", "dbo.t2"}

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": {"t1": "incremental", "t2": "incremental"}}}
        results = ct.sync()

        assert mock_sync.call_count == 0
        assert len(results) == 2
        assert all(r["status"] == "skipped" for r in results)
        assert all(r["reason"] == "no changes" for r in results)

    @patch("azsql_ct.client._compute_tables_to_skip")
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_skip_and_sync_mix(self, MockAz, mock_sync, mock_compute):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}
        mock_compute.return_value = {"dbo.t2"}

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": {
            "t1": "full_incremental",
            "t2": "full_incremental",
            "t3": "full_incremental",
        }}}
        results = ct.sync()

        assert mock_sync.call_count == 2
        assert len(results) == 3
        tables_synced = {c.args[1] for c in mock_sync.call_args_list}
        assert tables_synced == {"dbo.t1", "dbo.t3"}
        skipped = [r for r in results if r.get("status") == "skipped"]
        assert len(skipped) == 1
        assert skipped[0]["table"] == "dbo.t2"

    @patch("azsql_ct.client._compute_tables_to_skip")
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_skip_preserves_result_order(self, MockAz, mock_sync, mock_compute):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn

        def fake_sync(conn, table_name, **kw):
            return {"table": table_name, "ok": True}

        mock_sync.side_effect = fake_sync
        mock_compute.return_value = {"dbo.b"}

        ct = ChangeTracker("srv", "usr", "pw", max_workers=2)
        ct.tables = {"db1": {"dbo": ["a", "b", "c"]}}
        results = ct.sync()

        assert [r["table"] for r in results] == ["dbo.a", "dbo.b", "dbo.c"]
        assert results[0]["ok"] is True
        assert results[1]["status"] == "skipped"
        assert results[2]["ok"] is True

    @patch("azsql_ct.client._compute_tables_to_skip")
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_change_check_failure_falls_back_to_sync_all(self, MockAz, mock_sync, mock_compute):
        """_compute_tables_to_skip handles errors internally by returning an
        empty set, so all tables are synced when the change check fails."""
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}
        mock_compute.return_value = set()

        ct = ChangeTracker("srv", "usr", "pw")
        ct.tables = {"db1": {"dbo": {"t1": "incremental", "t2": "incremental"}}}
        results = ct.sync()

        assert mock_sync.call_count == 2
        assert len(results) == 2
        assert all(r.get("ok") for r in results)


class TestRunSyncIntegration:
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_passes_output_and_watermark_dirs(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("s", "u", "p", output_dir="/mydata", watermark_dir="/mywm")
        ct.tables = {"db1": {"dbo": ["t1"]}}
        ct.sync()

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
        ct.sync()

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
        ct.sync()

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
        ct.sync()

        _, kwargs = mock_sync.call_args
        assert kwargs["database"] == "mydb"
        assert mock_sync.call_args[0][1] == "sales.orders"
