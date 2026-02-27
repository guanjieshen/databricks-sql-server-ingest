"""Tests for azsql_ct.sync -- orchestration with mocked DB and writer."""

from __future__ import annotations

import json
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from azsql_ct import watermark
from azsql_ct.sync import sync_table


class StubWriter:
    """Captures write calls instead of hitting the filesystem."""

    def __init__(self):
        self.calls: list = []

    def write(
        self, rows, description: list, dir_path: str, prefix: str
    ):
        materialised = list(rows)
        self.calls.append(
            {"rows": materialised, "description": description, "dir": dir_path, "prefix": prefix}
        )
        return [f"{dir_path}/{prefix}.csv"], len(materialised)


def _make_cursor(tracked, cur_ver, min_ver, pk_cols, rows, desc):
    """Build a MagicMock cursor that responds to the queries sync_table makes."""
    cursor = MagicMock()
    call_count = {"n": 0}

    results_sequence = [
        [tuple([t]) for t in tracked],  # list_tracked_tables
        [(cur_ver,)],                    # current_version
        [(min_ver,)],                    # min_valid_version
    ]

    descs_sequence = [None, None, None]

    if pk_cols is not None:
        results_sequence.append([tuple([c]) for c in pk_cols])
        descs_sequence.append(None)

    results_sequence.append(rows)
    descs_sequence.append(desc)

    def side_effect_execute(sql, params=None):
        pass

    cursor.execute = MagicMock(side_effect=side_effect_execute)

    def side_effect_fetchall():
        idx = call_count["n"]
        call_count["n"] += 1
        if idx < len(results_sequence):
            return results_sequence[idx]
        return []

    cursor.fetchall = MagicMock(side_effect=side_effect_fetchall)

    def side_effect_fetchone():
        idx = call_count["n"]
        call_count["n"] += 1
        if idx < len(results_sequence) and results_sequence[idx]:
            return results_sequence[idx][0]
        return None

    cursor.fetchone = MagicMock(side_effect=side_effect_fetchone)

    _fetchmany_exhausted = {"done": False}

    def side_effect_fetchmany(size=1):
        if _fetchmany_exhausted["done"]:
            return []
        _fetchmany_exhausted["done"] = True
        return rows

    cursor.fetchmany = MagicMock(side_effect=side_effect_fetchmany)

    cursor.description = desc

    return cursor


class TestSyncTable:
    def test_full_sync_writes_rows(self, tmp_path):
        desc = [("SYS_CHANGE_VERSION",), ("SYS_CHANGE_CREATION_VERSION",), ("SYS_CHANGE_OPERATION",), ("id",), ("name",)]
        rows = [(100, None, "L", 1, "Alice"), (100, None, "L", 2, "Bob")]
        cursor = _make_cursor(
            tracked=["dbo.Foo"],
            cur_ver=100,
            min_ver=1,
            pk_cols=None,
            rows=rows,
            desc=desc,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        writer = StubWriter()
        result = sync_table(
            conn,
            "dbo.Foo",
            database="db1",
            output_dir=str(tmp_path / "data"),
            watermark_dir=str(tmp_path / "wm"),
            mode="full",
            writer=writer,
        )

        assert result["mode"] == "full"
        assert result["rows_written"] == 2
        assert result["current_version"] == 100
        assert len(writer.calls) == 1

    def test_raises_for_untracked_table(self, tmp_path):
        cursor = _make_cursor(
            tracked=["dbo.Other"],
            cur_ver=1, min_ver=0, pk_cols=None, rows=[], desc=None,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with pytest.raises(ValueError, match="not change-tracked"):
            sync_table(
                conn, "dbo.Missing", database="db1",
                output_dir=str(tmp_path / "data"),
                watermark_dir=str(tmp_path / "wm"),
            )

    def test_watermark_updated_after_sync(self, tmp_path):
        desc = [("SYS_CHANGE_VERSION",), ("SYS_CHANGE_CREATION_VERSION",), ("SYS_CHANGE_OPERATION",), ("id",)]
        cursor = _make_cursor(
            tracked=["dbo.T"],
            cur_ver=55,
            min_ver=0,
            pk_cols=None,
            rows=[(55, None, "L", 1)],
            desc=desc,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        sync_table(
            conn, "dbo.T", database="db1",
            output_dir=str(tmp_path / "data"),
            watermark_dir=str(tmp_path / "wm"),
            mode="full",
            writer=StubWriter(),
        )

        wm_dir = tmp_path / "wm" / "db1" / "dbo" / "T"
        assert watermark.get(str(wm_dir), "dbo.T") == 55

    def test_stale_watermark_falls_back_to_full(self, tmp_path):
        """When watermark is below min_valid_version, auto-fallback to full sync."""
        wm_dir = tmp_path / "wm" / "db1" / "dbo" / "T"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T", 5)

        desc = [("SYS_CHANGE_VERSION",), ("SYS_CHANGE_CREATION_VERSION",), ("SYS_CHANGE_OPERATION",), ("id",)]
        rows = [(100, None, "L", 1)]
        cursor = _make_cursor(
            tracked=["dbo.T"],
            cur_ver=100,
            min_ver=50,
            pk_cols=None,
            rows=rows,
            desc=desc,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        writer = StubWriter()
        result = sync_table(
            conn, "dbo.T", database="db1",
            output_dir=str(tmp_path / "data"),
            watermark_dir=str(tmp_path / "wm"),
            mode="incremental",
            writer=writer,
        )

        assert result["mode"] == "full"
        assert result["current_version"] == 100

    def test_incremental_sync_uses_changetable(self, tmp_path):
        wm_dir = tmp_path / "wm" / "db1" / "dbo" / "T"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T", 20)

        desc = [("SYS_CHANGE_VERSION",), ("id",), ("name",)]
        rows = [(25, 1, "Alice")]
        cursor = _make_cursor(
            tracked=["dbo.T"],
            cur_ver=30,
            min_ver=10,
            pk_cols=["id"],
            rows=rows,
            desc=desc,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        writer = StubWriter()
        result = sync_table(
            conn, "dbo.T", database="db1",
            output_dir=str(tmp_path / "data"),
            watermark_dir=str(tmp_path / "wm"),
            mode="incremental",
            writer=writer,
        )

        assert result["mode"] == "incremental"
        assert result["since_version"] == 20
        assert result["rows_written"] == 1

    def test_incremental_no_pk_raises(self, tmp_path):
        wm_dir = tmp_path / "wm" / "db1" / "dbo" / "T"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T", 20)

        cursor = _make_cursor(
            tracked=["dbo.T"],
            cur_ver=30,
            min_ver=10,
            pk_cols=[],
            rows=[],
            desc=None,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with pytest.raises(RuntimeError, match="primary key"):
            sync_table(
                conn, "dbo.T", database="db1",
                output_dir=str(tmp_path / "data"),
                watermark_dir=str(tmp_path / "wm"),
                mode="incremental",
            )

    def test_incremental_raises_without_watermark(self, tmp_path):
        """Strict incremental mode must error when no watermark exists."""
        cursor = _make_cursor(
            tracked=["dbo.T"],
            cur_ver=50,
            min_ver=1,
            pk_cols=None,
            rows=[],
            desc=None,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with pytest.raises(RuntimeError, match="No watermark exists"):
            sync_table(
                conn, "dbo.T", database="db1",
                output_dir=str(tmp_path / "data"),
                watermark_dir=str(tmp_path / "wm"),
                mode="incremental",
            )

    def test_full_incremental_does_full_when_no_watermark(self, tmp_path):
        """full_incremental should do a full load on first run."""
        desc = [("SYS_CHANGE_VERSION",), ("SYS_CHANGE_CREATION_VERSION",), ("SYS_CHANGE_OPERATION",), ("id",), ("name",)]
        rows = [(50, None, "L", 1, "Alice")]
        cursor = _make_cursor(
            tracked=["dbo.T"],
            cur_ver=50,
            min_ver=1,
            pk_cols=None,
            rows=rows,
            desc=desc,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        writer = StubWriter()
        result = sync_table(
            conn, "dbo.T", database="db1",
            output_dir=str(tmp_path / "data"),
            watermark_dir=str(tmp_path / "wm"),
            mode="full_incremental",
            writer=writer,
        )

        assert result["mode"] == "full"
        assert result["rows_written"] == 1

    def test_full_incremental_does_incremental_with_watermark(self, tmp_path):
        """full_incremental should use CT once a watermark exists."""
        wm_dir = tmp_path / "wm" / "db1" / "dbo" / "T"
        wm_dir.mkdir(parents=True)
        watermark.save(str(wm_dir), "dbo.T", 20)

        desc = [("SYS_CHANGE_VERSION",), ("id",)]
        rows = [(25, 1)]
        cursor = _make_cursor(
            tracked=["dbo.T"],
            cur_ver=30,
            min_ver=10,
            pk_cols=["id"],
            rows=rows,
            desc=desc,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        writer = StubWriter()
        result = sync_table(
            conn, "dbo.T", database="db1",
            output_dir=str(tmp_path / "data"),
            watermark_dir=str(tmp_path / "wm"),
            mode="full_incremental",
            writer=writer,
        )

        assert result["mode"] == "incremental"
        assert result["since_version"] == 20

    def test_invalid_mode_raises(self, tmp_path):
        conn = MagicMock()
        with pytest.raises(ValueError, match="Invalid mode"):
            sync_table(
                conn, "dbo.T", database="db1",
                output_dir=str(tmp_path / "data"),
                watermark_dir=str(tmp_path / "wm"),
                mode="snapshot",
            )


class TestFromConfig:
    """Tests for ChangeTracker.from_config() factory method."""

    def test_loads_flat_json_config(self, tmp_path):
        from azsql_ct.client import ChangeTracker

        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({
            "server": "srv.database.windows.net",
            "user": "admin",
            "password": "secret",
            "tables": [
                {"database": "db1", "table": "dbo.Foo", "mode": "full"},
                {"database": "db1", "table": "dbo.Bar", "mode": "incremental"},
            ],
        }))

        ct = ChangeTracker.from_config(str(cfg))
        assert ct.server == "srv.database.windows.net"
        assert ct.user == "admin"
        assert len(ct._flat_tables) == 2

    def test_loads_dict_config(self):
        from azsql_ct.client import ChangeTracker

        ct = ChangeTracker.from_config({
            "server": "srv",
            "user": "u",
            "password": "p",
            "tables": [
                {"database": "db1", "table": "dbo.T", "mode": "full"},
            ],
        })
        assert ct.server == "srv"
        assert len(ct._flat_tables) == 1

    def test_loads_nested_dict_config(self):
        from azsql_ct.client import ChangeTracker

        ct = ChangeTracker.from_config({
            "connection": {
                "server": "myserver",
                "sql_login": "admin",
                "password": "pw",
            },
            "databases": {
                "db1": {"dbo": {"t1": "full", "t2": "incremental"}},
            },
        })
        assert ct.server == "myserver"
        assert ct.user == "admin"
        assert len(ct._flat_tables) == 2

    def test_cli_overrides_take_precedence(self):
        from azsql_ct.client import ChangeTracker

        ct = ChangeTracker.from_config(
            {
                "connection": {
                    "server": "srv", "sql_login": "u", "password": "p",
                },
                "storage": {"data_dir": "/default"},
                "max_workers": 2,
                "databases": {"db1": {"dbo": ["t"]}},
            },
            max_workers=8,
            output_dir="/override",
        )
        assert ct.max_workers == 8
        assert ct.output_dir == "/override"

    def test_parallelism_config_key(self):
        from azsql_ct.client import ChangeTracker

        ct = ChangeTracker.from_config({
            "connection": {"server": "srv", "sql_login": "u", "password": "p"},
            "parallelism": 4,
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.max_workers == 4

    def test_output_manifest_from_config(self):
        from azsql_ct.client import ChangeTracker

        ct = ChangeTracker.from_config({
            "connection": {"server": "srv", "sql_login": "u", "password": "p"},
            "storage": {"data_dir": "/d", "watermark_dir": "/w", "output_manifest": "/out.yaml"},
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.output_manifest == "/out.yaml"

    def test_env_var_expansion(self, monkeypatch):
        from azsql_ct.client import ChangeTracker

        monkeypatch.setenv("TEST_SERVER", "expanded.database.windows.net")
        monkeypatch.setenv("TEST_PW", "secret123")

        ct = ChangeTracker.from_config({
            "server": "${TEST_SERVER}",
            "user": "admin",
            "password": "${TEST_PW}",
            "tables": [],
        })
        assert ct.server == "expanded.database.windows.net"

    def test_storage_section_applied(self):
        from azsql_ct.client import ChangeTracker

        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "storage": {"data_dir": "/mydata", "watermark_dir": "/mywm"},
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.output_dir == "/mydata"
        assert ct.watermark_dir == "/mywm"

    def test_ingest_pipeline_derives_paths(self):
        from azsql_ct.client import ChangeTracker

        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "storage": {"ingest_pipeline": "/some/base"},
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.output_dir == "/some/base/data"
        assert ct.watermark_dir == "/some/base/watermarks"
        assert ct.output_manifest == "/some/base/output.yaml"

    def test_ingest_pipeline_explicit_data_dir_overrides(self):
        from azsql_ct.client import ChangeTracker

        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "storage": {
                "ingest_pipeline": "/some/base",
                "data_dir": "/custom/data",
            },
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.output_dir == "/custom/data"
        assert ct.watermark_dir == "/some/base/watermarks"
        assert ct.output_manifest == "/some/base/output.yaml"
