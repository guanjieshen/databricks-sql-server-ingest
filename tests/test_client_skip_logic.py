"""Tests for client helper functions: _load_watermarks_for_tables,
_fetch_db_metadata, and _compute_tables_to_skip."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from azsql_ct import watermark
from azsql_ct.client import (
    _compute_tables_to_skip,
    _fetch_db_metadata,
    _load_watermarks_for_tables,
)


class TestLoadWatermarksForTables:
    def test_returns_versions_for_tables_with_watermarks(self, tmp_path):
        wm_dir = tmp_path / "db1" / "dbo" / "T1"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T1", 42)

        result = _load_watermarks_for_tables(str(tmp_path), "db1", ["dbo.T1"])
        assert result == {"dbo.T1": 42}

    def test_skips_tables_without_watermarks(self, tmp_path):
        result = _load_watermarks_for_tables(str(tmp_path), "db1", ["dbo.Missing"])
        assert result == {}

    def test_mixed_tables_with_and_without_watermarks(self, tmp_path):
        wm_dir = tmp_path / "db1" / "dbo" / "T1"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T1", 10)

        result = _load_watermarks_for_tables(
            str(tmp_path), "db1", ["dbo.T1", "dbo.T2"],
        )
        assert result == {"dbo.T1": 10}

    def test_empty_table_list(self, tmp_path):
        assert _load_watermarks_for_tables(str(tmp_path), "db1", []) == {}

    def test_handles_non_dbo_schema(self, tmp_path):
        wm_dir = tmp_path / "db1" / "sales" / "Orders"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "sales.Orders", 99)

        result = _load_watermarks_for_tables(str(tmp_path), "db1", ["sales.Orders"])
        assert result == {"sales.Orders": 99}


class TestFetchDbMetadata:
    def test_returns_all_metadata_keys(self, fake_cursor):
        cur = fake_cursor(results=[
            [(50, "dbo.T1", 5), (50, "dbo.T2", 10)],
            [("dbo.T1", "id"), ("dbo.T2", "pk")],
        ])
        conn = MagicMock()
        conn.cursor.return_value = cur

        result = _fetch_db_metadata(conn)

        assert result["db_version"] == 50
        assert result["tables"] == {"dbo.T1": 5, "dbo.T2": 10}
        assert result["tracked_lookup"]["dbo.t1"] == "dbo.T1"
        assert result["tracked_lookup"]["dbo.t2"] == "dbo.T2"
        assert result["pk_map"] == {"dbo.T1": ["id"], "dbo.T2": ["pk"]}

    def test_empty_database(self, fake_cursor):
        cur = fake_cursor(results=[[], []])
        conn = MagicMock()
        conn.cursor.return_value = cur

        result = _fetch_db_metadata(conn)

        assert result["db_version"] == 0
        assert result["tables"] == {}
        assert result["tracked_lookup"] == {}
        assert result["pk_map"] == {}


class TestComputeTablesToSkip:
    """Direct unit tests for _compute_tables_to_skip covering all branches."""

    def _flat_entry(self, db, table, mode="full_incremental", scd=1, soft=False, sd_col=None):
        return (db, table, mode, scd, soft, sd_col)

    def test_no_watermarks_returns_empty(self, tmp_path):
        db_meta = {
            "tracked_lookup": {"dbo.t1": "dbo.T1"},
            "tables": {"dbo.T1": 0},
        }
        entries = [self._flat_entry("db1", "dbo.T1")]
        conn = MagicMock()

        result = _compute_tables_to_skip(
            "db1", conn, entries, str(tmp_path), None, db_metadata=db_meta,
        )
        assert result == set()

    def test_skips_table_with_no_changes(self, tmp_path):
        wm_dir = tmp_path / "db1" / "dbo" / "T1"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T1", 50)

        db_meta = {
            "tracked_lookup": {"dbo.t1": "dbo.T1"},
            "tables": {"dbo.T1": 10},
        }
        entries = [self._flat_entry("db1", "dbo.T1")]

        cur = MagicMock()
        cur.fetchall.return_value = []  # no changes
        conn = MagicMock()
        conn.cursor.return_value = cur

        result = _compute_tables_to_skip(
            "db1", conn, entries, str(tmp_path), None, db_metadata=db_meta,
        )
        assert result == {"dbo.T1"}

    def test_does_not_skip_table_with_changes(self, tmp_path):
        wm_dir = tmp_path / "db1" / "dbo" / "T1"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T1", 50)

        db_meta = {
            "tracked_lookup": {"dbo.t1": "dbo.T1"},
            "tables": {"dbo.T1": 10},
        }
        entries = [self._flat_entry("db1", "dbo.T1")]

        cur = MagicMock()
        cur.fetchall.return_value = [("dbo.T1",)]  # has changes
        conn = MagicMock()
        conn.cursor.return_value = cur

        result = _compute_tables_to_skip(
            "db1", conn, entries, str(tmp_path), None, db_metadata=db_meta,
        )
        assert result == set()

    def test_mixed_changed_and_unchanged(self, tmp_path):
        for tbl in ("T1", "T2"):
            wm_dir = tmp_path / "db1" / "dbo" / tbl
            wm_dir.mkdir(parents=True)
            watermark.save(str(wm_dir), f"dbo.{tbl}", 50)

        db_meta = {
            "tracked_lookup": {"dbo.t1": "dbo.T1", "dbo.t2": "dbo.T2"},
            "tables": {"dbo.T1": 10, "dbo.T2": 10},
        }
        entries = [
            self._flat_entry("db1", "dbo.T1"),
            self._flat_entry("db1", "dbo.T2"),
        ]

        cur = MagicMock()
        cur.fetchall.return_value = [("dbo.T1",)]  # only T1 changed
        conn = MagicMock()
        conn.cursor.return_value = cur

        result = _compute_tables_to_skip(
            "db1", conn, entries, str(tmp_path), None, db_metadata=db_meta,
        )
        assert result == {"dbo.T2"}

    def test_stale_watermark_below_min_ver_not_skipped(self, tmp_path):
        wm_dir = tmp_path / "db1" / "dbo" / "T1"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T1", 5)

        db_meta = {
            "tracked_lookup": {"dbo.t1": "dbo.T1"},
            "tables": {"dbo.T1": 50},  # min_ver > watermark
        }
        entries = [self._flat_entry("db1", "dbo.T1")]
        conn = MagicMock()

        result = _compute_tables_to_skip(
            "db1", conn, entries, str(tmp_path), None, db_metadata=db_meta,
        )
        assert result == set()

    def test_query_failure_returns_empty_set(self, tmp_path):
        """Internal exception is caught and empty set returned."""
        wm_dir = tmp_path / "db1" / "dbo" / "T1"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T1", 50)

        db_meta = {
            "tracked_lookup": {"dbo.t1": "dbo.T1"},
            "tables": {"dbo.T1": 10},
        }
        entries = [self._flat_entry("db1", "dbo.T1")]

        conn = MagicMock()
        conn.cursor.return_value.fetchall.side_effect = RuntimeError("query failed")

        result = _compute_tables_to_skip(
            "db1", conn, entries, str(tmp_path), None, db_metadata=db_meta,
        )
        assert result == set()

    def test_fetches_metadata_when_not_provided(self, tmp_path):
        """Without db_metadata, queries CT metadata from the connection."""
        wm_dir = tmp_path / "db1" / "dbo" / "T1"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T1", 50)

        entries = [self._flat_entry("db1", "dbo.T1")]

        cur = MagicMock()
        call_count = {"n": 0}

        def fake_fetchall():
            idx = call_count["n"]
            call_count["n"] += 1
            if idx == 0:
                return [(100, "dbo.T1", 10)]  # fetch_all_ct_metadata
            if idx == 1:
                return []  # fetch_tables_with_changes (no changes)
            return []

        cur.fetchall = MagicMock(side_effect=fake_fetchall)
        conn = MagicMock()
        conn.cursor.return_value = cur

        result = _compute_tables_to_skip(
            "db1", conn, entries, str(tmp_path), None, db_metadata=None,
        )
        assert result == {"dbo.T1"}

    def test_entries_from_different_database_ignored(self, tmp_path):
        wm_dir = tmp_path / "db1" / "dbo" / "T1"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T1", 50)

        db_meta = {
            "tracked_lookup": {"dbo.t1": "dbo.T1"},
            "tables": {"dbo.T1": 10},
        }
        entries = [
            self._flat_entry("db2", "dbo.T1"),  # different database
        ]
        conn = MagicMock()

        result = _compute_tables_to_skip(
            "db1", conn, entries, str(tmp_path), None, db_metadata=db_meta,
        )
        assert result == set()

    def test_full_mode_tables_excluded_from_skip_check(self, tmp_path):
        """Tables with mode 'full' are never candidates for skipping."""
        wm_dir = tmp_path / "db1" / "dbo" / "T1"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T1", 50)

        db_meta = {
            "tracked_lookup": {"dbo.t1": "dbo.T1"},
            "tables": {"dbo.T1": 10},
        }
        entries = [self._flat_entry("db1", "dbo.T1", mode="full")]
        conn = MagicMock()

        result = _compute_tables_to_skip(
            "db1", conn, entries, str(tmp_path), None, db_metadata=db_meta,
        )
        assert result == set()

    def test_mode_override_applied(self, tmp_path):
        """mode_override overrides per-table modes."""
        wm_dir = tmp_path / "db1" / "dbo" / "T1"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T1", 50)

        db_meta = {
            "tracked_lookup": {"dbo.t1": "dbo.T1"},
            "tables": {"dbo.T1": 10},
        }
        entries = [self._flat_entry("db1", "dbo.T1", mode="full")]

        cur = MagicMock()
        cur.fetchall.return_value = []
        conn = MagicMock()
        conn.cursor.return_value = cur

        result = _compute_tables_to_skip(
            "db1", conn, entries, str(tmp_path),
            mode_override="incremental", db_metadata=db_meta,
        )
        assert result == {"dbo.T1"}
