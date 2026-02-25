"""Tests for azsql_ct.watermark -- pure file I/O, no mocking needed."""

from __future__ import annotations

import json

from azsql_ct import watermark


class TestLoadAll:
    def test_returns_empty_when_no_file(self, tmp_path):
        assert watermark.load_all(str(tmp_path)) == {}

    def test_returns_rich_mapping(self, tmp_path):
        data = {
            "dbo.table_1": {
                "version": 100, "since_version": None, "rows_synced": 50,
                "mode": "full", "files": ["a.csv"], "duration_seconds": 1.5,
                "synced_at": "2025-01-01T00:00:00Z",
            },
        }
        (tmp_path / "watermarks.json").write_text(json.dumps(data))
        result = watermark.load_all(str(tmp_path))
        assert result["dbo.table_1"]["version"] == 100
        assert result["dbo.table_1"]["rows_synced"] == 50
        assert result["dbo.table_1"]["files"] == ["a.csv"]

    def test_normalizes_legacy_bare_int(self, tmp_path):
        data = {"dbo.table_1": 100, "dbo.table_2": 200}
        (tmp_path / "watermarks.json").write_text(json.dumps(data))
        result = watermark.load_all(str(tmp_path))
        assert result["dbo.table_1"] == {"version": 100}
        assert result["dbo.table_2"] == {"version": 200}


class TestGet:
    def test_returns_none_for_missing_table(self, tmp_path):
        assert watermark.get(str(tmp_path), "dbo.no_such") is None

    def test_returns_version_from_rich_entry(self, tmp_path):
        data = {"dbo.t": {"version": 42, "rows_synced": 10, "mode": "full",
                          "synced_at": "2025-01-01T00:00:00Z"}}
        (tmp_path / "watermarks.json").write_text(json.dumps(data))
        assert watermark.get(str(tmp_path), "dbo.t") == 42

    def test_returns_version_from_legacy_int(self, tmp_path):
        (tmp_path / "watermarks.json").write_text(json.dumps({"dbo.t": 42}))
        assert watermark.get(str(tmp_path), "dbo.t") == 42


class TestGetMetadata:
    def test_returns_none_for_missing(self, tmp_path):
        assert watermark.get_metadata(str(tmp_path), "dbo.x") is None

    def test_returns_full_dict(self, tmp_path):
        data = {"dbo.t": {
            "version": 10, "since_version": 5, "rows_synced": 3,
            "mode": "incremental", "files": ["/data/f.csv"],
            "duration_seconds": 0.75, "synced_at": "2025-06-01T12:00:00Z",
        }}
        (tmp_path / "watermarks.json").write_text(json.dumps(data))
        meta = watermark.get_metadata(str(tmp_path), "dbo.t")
        assert meta["version"] == 10
        assert meta["since_version"] == 5
        assert meta["rows_synced"] == 3
        assert meta["mode"] == "incremental"
        assert meta["files"] == ["/data/f.csv"]
        assert meta["duration_seconds"] == 0.75
        assert meta["synced_at"] == "2025-06-01T12:00:00Z"


class TestSet:
    def test_creates_file_with_all_fields(self, tmp_path):
        d = str(tmp_path / "sub")
        watermark.save(
            d, "dbo.x", 10,
            since_version=None, rows_synced=3, mode="full",
            files=["/data/x.csv"], duration_seconds=2.567,
        )
        meta = watermark.get_metadata(d, "dbo.x")
        assert meta["version"] == 10
        assert meta["since_version"] is None
        assert meta["rows_synced"] == 3
        assert meta["mode"] == "full"
        assert meta["files"] == ["/data/x.csv"]
        assert meta["duration_seconds"] == 2.57
        assert "synced_at" in meta

    def test_incremental_records_since_version(self, tmp_path):
        d = str(tmp_path)
        watermark.save(d, "dbo.t", 100, since_version=50, rows_synced=10,
                      mode="incremental", duration_seconds=0.3)
        meta = watermark.get_metadata(d, "dbo.t")
        assert meta["since_version"] == 50
        assert meta["mode"] == "incremental"

    def test_updates_existing(self, tmp_path):
        d = str(tmp_path)
        watermark.save(d, "dbo.a", 1, rows_synced=10, mode="full",
                      duration_seconds=1.0)
        watermark.save(d, "dbo.b", 2, rows_synced=20, mode="full",
                      duration_seconds=2.0)
        watermark.save(d, "dbo.a", 99, since_version=1, rows_synced=5,
                      mode="incremental", duration_seconds=0.5)
        all_data = watermark.load_all(d)
        assert all_data["dbo.a"]["version"] == 99
        assert all_data["dbo.a"]["rows_synced"] == 5
        assert all_data["dbo.a"]["since_version"] == 1
        assert all_data["dbo.b"]["version"] == 2

    def test_file_ends_with_newline(self, tmp_path):
        d = str(tmp_path)
        watermark.save(d, "dbo.t", 5)
        text = (tmp_path / "watermarks.json").read_text()
        assert text.endswith("\n")

    def test_records_synced_at_timestamp(self, tmp_path):
        d = str(tmp_path)
        watermark.save(d, "dbo.t", 5, rows_synced=0, mode="full",
                      duration_seconds=0.1)
        meta = watermark.get_metadata(d, "dbo.t")
        assert meta["synced_at"].endswith("Z")
        assert "T" in meta["synced_at"]

    def test_defaults_to_none_for_optional_fields(self, tmp_path):
        d = str(tmp_path)
        watermark.save(d, "dbo.t", 5)
        meta = watermark.get_metadata(d, "dbo.t")
        assert meta["version"] == 5
        assert meta["since_version"] is None
        assert meta["rows_synced"] is None
        assert meta["mode"] is None
        assert meta["files"] == []
        assert meta["duration_seconds"] is None

    def test_duration_rounds_to_two_decimals(self, tmp_path):
        d = str(tmp_path)
        watermark.save(d, "dbo.t", 5, duration_seconds=1.23456789)
        meta = watermark.get_metadata(d, "dbo.t")
        assert meta["duration_seconds"] == 1.23

    def test_upgrades_legacy_on_write(self, tmp_path):
        d = str(tmp_path)
        (tmp_path / "watermarks.json").write_text(json.dumps({"dbo.old": 50}))
        watermark.save(d, "dbo.new", 100, rows_synced=10, mode="full",
                      duration_seconds=1.0)
        all_data = watermark.load_all(d)
        assert all_data["dbo.old"] == {"version": 50}
        assert all_data["dbo.new"]["version"] == 100
        assert all_data["dbo.new"]["rows_synced"] == 10


class TestLoadHistory:
    def test_returns_empty_when_no_file(self, tmp_path):
        assert watermark.load_history(str(tmp_path)) == []

    def test_returns_entries_oldest_first(self, tmp_path):
        d = str(tmp_path)
        watermark.save(d, "dbo.t", 10, rows_synced=100, mode="full",
                      duration_seconds=1.0)
        watermark.save(d, "dbo.t", 20, since_version=10, rows_synced=5,
                      mode="incremental", duration_seconds=0.3)

        history = watermark.load_history(d)
        assert len(history) == 2
        assert history[0]["version"] == 10
        assert history[0]["mode"] == "full"
        assert history[1]["version"] == 20
        assert history[1]["mode"] == "incremental"
        assert history[1]["since_version"] == 10

    def test_each_entry_has_table_name(self, tmp_path):
        d = str(tmp_path)
        watermark.save(d, "dbo.t", 10, rows_synced=1, mode="full")
        history = watermark.load_history(d)
        assert history[0]["table"] == "dbo.t"

    def test_history_survives_watermark_overwrite(self, tmp_path):
        d = str(tmp_path)
        watermark.save(d, "dbo.t", 1, rows_synced=10, mode="full")
        watermark.save(d, "dbo.t", 2, rows_synced=20, mode="full")
        watermark.save(d, "dbo.t", 3, rows_synced=30, mode="full")

        assert watermark.get(d, "dbo.t") == 3
        history = watermark.load_history(d)
        assert len(history) == 3
        assert [h["version"] for h in history] == [1, 2, 3]
        assert [h["rows_synced"] for h in history] == [10, 20, 30]

    def test_history_includes_all_fields(self, tmp_path):
        d = str(tmp_path)
        watermark.save(d, "dbo.t", 50, since_version=40, rows_synced=5,
                      mode="incremental", files=["/data/f.csv"],
                      duration_seconds=1.23)
        entry = watermark.load_history(d)[0]
        assert entry["table"] == "dbo.t"
        assert entry["version"] == 50
        assert entry["since_version"] == 40
        assert entry["rows_synced"] == 5
        assert entry["mode"] == "incremental"
        assert entry["files"] == ["/data/f.csv"]
        assert entry["duration_seconds"] == 1.23
        assert entry["synced_at"].endswith("Z")

    def test_skips_blank_lines(self, tmp_path):
        d = str(tmp_path)
        hp = tmp_path / "sync_history.jsonl"
        hp.write_text(
            '{"table":"dbo.t","version":1}\n'
            '\n'
            '{"table":"dbo.t","version":2}\n'
        )
        history = watermark.load_history(d)
        assert len(history) == 2
