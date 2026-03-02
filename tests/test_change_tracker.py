"""Tests for ChangeTracker -- init, properties, connectivity, context manager."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from azsql_ct.client import ChangeTracker


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
        ct.tables = {"db1": {"dbo": {"t1": "full_incremental", "t2": "incremental"}}}
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
        ct.sync()

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
        ct.sync()

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
            ct.sync()

        mock_sync.assert_called_once()
