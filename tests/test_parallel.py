"""Tests for parallel sync and connection pooling."""

from __future__ import annotations

from typing import List
from unittest.mock import MagicMock, patch

from azsql_ct.client import ChangeTracker


class TestParallelSync:
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_connections_bounded_by_workers(self, MockAz, mock_sync):
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker("srv", "usr", "pw", max_workers=3)
        ct.tables = {"db1": {"dbo": ["t1", "t2", "t3"]}}
        ct.sync()

        assert MockAz.call_count >= 1, "At least one connection should be created"
        assert MockAz.call_count <= 3
        assert mock_sync.call_count == 3, "All tables should have been synced"

    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_connection_failure(self, MockAz):
        MockAz.return_value.connect.side_effect = Exception("refused")

        ct = ChangeTracker("srv", "usr", "pw", max_workers=2)
        ct.tables = {"db1": {"dbo": ["t1"]}}
        results = ct.sync()

        assert len(results) == 1
        assert results[0]["status"] == "error"
        assert "refused" in results[0]["error"]

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
        ct.sync()

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
        results = ct.sync()

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
        results = ct.sync()

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

    def test_acquire_discards_stale_connection_and_creates_new(self):
        from azsql_ct.client import _ConnectionPool

        stale_conn = MagicMock()
        stale_conn.cursor.return_value.execute.side_effect = Exception("connection dead")
        stale_az = MagicMock()
        stale_az.connect.return_value = stale_conn

        fresh_az = MagicMock()
        fresh_az.connect.return_value = MagicMock()

        with patch("azsql_ct.client.AzureSQLConnection", return_value=fresh_az):
            pool = _ConnectionPool("srv", "usr", "pw")
            pool.release("db1", stale_az)
            az, conn = pool.acquire("db1")
            assert az is fresh_az
            stale_az.close.assert_called_once()

    def test_acquire_reuses_healthy_connection(self):
        from azsql_ct.client import _ConnectionPool

        healthy_conn = MagicMock()
        healthy_az = MagicMock()
        healthy_az.connect.return_value = healthy_conn

        with patch("azsql_ct.client.AzureSQLConnection") as MockAz:
            pool = _ConnectionPool("srv", "usr", "pw")
            pool.release("db1", healthy_az)
            az, conn = pool.acquire("db1")
            assert az is healthy_az
            assert conn is healthy_conn
            MockAz.assert_not_called()

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
