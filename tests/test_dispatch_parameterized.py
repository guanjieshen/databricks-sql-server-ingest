"""Parameterized dispatch tests covering behaviors shared by sequential
and parallel sync paths.  Each test is run with max_workers=1 (sequential)
and max_workers=2 (parallel) to ensure both paths behave identically."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from azsql_ct.client import ChangeTracker


@pytest.fixture(params=[1, 2], ids=["sequential", "parallel"])
def workers(request):
    return request.param


class TestDispatchCommon:
    """Behaviors that must hold regardless of sequential vs parallel execution."""

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_respects_per_table_modes(self, MockAz, mock_sync, workers):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn

        def fake_sync(conn, table_name, **kw):
            return {"table": table_name, "mode": kw["mode"]}

        mock_sync.side_effect = fake_sync

        ct = ChangeTracker("srv", "usr", "pw", max_workers=workers)
        ct.tables = {"db1": {"dbo": {"t1": "full_incremental", "t2": "incremental"}}}
        results = ct.sync()

        modes = {r["table"]: r["mode"] for r in results}
        assert modes["dbo.t1"] == "full_incremental"
        assert modes["dbo.t2"] == "incremental"

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_continues_after_table_error(self, MockAz, mock_sync, workers):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn

        def fake_sync(conn, table_name, **kw):
            if "t2" in table_name:
                raise RuntimeError("t2 failed")
            return {"table": table_name, "ok": True}

        mock_sync.side_effect = fake_sync

        ct = ChangeTracker("srv", "usr", "pw", max_workers=workers)
        ct.tables = {"db1": {"dbo": ["t1", "t2", "t3"]}}
        results = ct.sync()

        assert len(results) == 3
        assert results[0]["ok"] is True
        assert results[1]["status"] == "error"
        assert "t2 failed" in results[1]["error"]
        assert results[2]["ok"] is True

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_passes_batch_size(self, MockAz, mock_sync, workers):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw", max_workers=workers, batch_size=2000)
        ct.tables = {"db1": {"dbo": ["t1"]}}
        ct.sync()

        _, kwargs = mock_sync.call_args
        assert kwargs["batch_size"] == 2000

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_syncs_all_tables(self, MockAz, mock_sync, workers):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw", max_workers=workers)
        ct.tables = {"db1": {"dbo": ["t1", "t2", "t3"]}}
        results = ct.sync()

        assert len(results) == 3
        assert mock_sync.call_count == 3

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_preserves_result_order(self, MockAz, mock_sync, workers):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn

        def fake_sync(conn, table_name, **kw):
            return {"table": table_name}

        mock_sync.side_effect = fake_sync

        ct = ChangeTracker("srv", "usr", "pw", max_workers=workers)
        ct.tables = {"db1": {"dbo": ["a", "b", "c", "d"]}}
        results = ct.sync()

        assert [r["table"] for r in results] == [
            "dbo.a", "dbo.b", "dbo.c", "dbo.d",
        ]
