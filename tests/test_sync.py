"""Tests for azsql_ct.sync -- orchestration with mocked DB and writer."""

from __future__ import annotations

import json
import os
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
        self, rows, description: list, dir_path: str, prefix: str, **kwargs
    ):
        materialised = list(rows)
        self.calls.append(
            {"rows": materialised, "description": description, "dir": dir_path, "prefix": prefix, "kwargs": kwargs}
        )
        return [f"{dir_path}/{prefix}.csv"], len(materialised)


def _make_cursor(tracked, cur_ver, min_ver, pk_cols, rows, desc, table_cols=None):
    """Build a MagicMock cursor that responds to the queries sync_table makes.

    *table_cols* is a list of column names returned by ``table_columns()``
    (the ``SELECT TOP 0`` call added for incremental syncs).  When provided
    and *pk_cols* is non-empty, an extra execute step is inserted so the
    mock sets ``cursor.description`` appropriately for that call.
    """
    cursor = MagicMock()
    call_count = {"n": 0}
    exec_count = {"n": 0}

    results_sequence = [
        [tuple([t]) for t in tracked],  # list_tracked_tables
        [(cur_ver,)],                    # current_version
        [(min_ver,)],                    # min_valid_version
    ]

    descs_sequence = [None, None, None]

    if pk_cols is not None:
        results_sequence.append([tuple([c]) for c in pk_cols])
        descs_sequence.append(None)

    # table_columns (SELECT TOP 0) — sets description but no fetch
    needs_table_cols = pk_cols is not None and len(pk_cols) > 0 and table_cols is not None
    table_cols_desc = [(c,) for c in table_cols] if table_cols else None

    results_sequence.append(rows)
    descs_sequence.append(desc)

    def side_effect_execute(sql, params=None):
        idx = exec_count["n"]
        exec_count["n"] += 1
        # The table_columns call happens right after primary_key_columns
        # and before the main data query. It reads cursor.description.
        if needs_table_cols:
            tc_exec_idx = 4  # 0=tracked, 1=cur_ver, 2=min_ver, 3=pk, 4=table_cols
            if idx == tc_exec_idx:
                cursor.description = table_cols_desc
                return
            elif idx == tc_exec_idx + 1:
                cursor.description = desc
                return
        cursor.description = desc

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
        assert result["scd_type"] == 1
        assert len(writer.calls) == 1

    def test_scd_type_passed_through_to_result(self, tmp_path):
        desc = [("SYS_CHANGE_VERSION",), ("SYS_CHANGE_CREATION_VERSION",), ("SYS_CHANGE_OPERATION",), ("id",)]
        rows = [(100, None, "L", 1)]
        cursor = _make_cursor(
            tracked=["dbo.Foo"],
            cur_ver=100, min_ver=1, pk_cols=None, rows=rows, desc=desc,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        result = sync_table(
            conn, "dbo.Foo", database="db1",
            output_dir=str(tmp_path / "data"),
            watermark_dir=str(tmp_path / "wm"),
            mode="full", writer=StubWriter(), scd_type=2,
        )
        assert result["scd_type"] == 2

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
            table_cols=["id", "name"],
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
            table_cols=[],
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
            table_cols=["id"],
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

    def test_soft_delete_passed_through_to_result(self, tmp_path):
        desc = [("SYS_CHANGE_VERSION",), ("SYS_CHANGE_CREATION_VERSION",), ("SYS_CHANGE_OPERATION",), ("id",)]
        rows = [(100, None, "L", 1)]
        cursor = _make_cursor(
            tracked=["dbo.Foo"],
            cur_ver=100, min_ver=1, pk_cols=None, rows=rows, desc=desc,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        result = sync_table(
            conn, "dbo.Foo", database="db1",
            output_dir=str(tmp_path / "data"),
            watermark_dir=str(tmp_path / "wm"),
            mode="full", writer=StubWriter(), soft_delete=True,
        )
        assert result["soft_delete"] is True

    def test_soft_delete_defaults_to_false(self, tmp_path):
        desc = [("SYS_CHANGE_VERSION",), ("SYS_CHANGE_CREATION_VERSION",), ("SYS_CHANGE_OPERATION",), ("id",)]
        rows = [(100, None, "L", 1)]
        cursor = _make_cursor(
            tracked=["dbo.Foo"],
            cur_ver=100, min_ver=1, pk_cols=None, rows=rows, desc=desc,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        result = sync_table(
            conn, "dbo.Foo", database="db1",
            output_dir=str(tmp_path / "data"),
            watermark_dir=str(tmp_path / "wm"),
            mode="full", writer=StubWriter(),
        )
        assert result["soft_delete"] is False

    def test_soft_delete_with_scd_type_2(self, tmp_path):
        """Both soft_delete and scd_type=2 coexist in the result dict."""
        desc = [("SYS_CHANGE_VERSION",), ("SYS_CHANGE_CREATION_VERSION",), ("SYS_CHANGE_OPERATION",), ("id",)]
        rows = [(100, None, "L", 1)]
        cursor = _make_cursor(
            tracked=["dbo.Foo"],
            cur_ver=100, min_ver=1, pk_cols=None, rows=rows, desc=desc,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        result = sync_table(
            conn, "dbo.Foo", database="db1",
            output_dir=str(tmp_path / "data"),
            watermark_dir=str(tmp_path / "wm"),
            mode="full", writer=StubWriter(),
            scd_type=2, soft_delete=True,
        )
        assert result["soft_delete"] is True
        assert result["scd_type"] == 2

    def test_invalid_mode_raises(self, tmp_path):
        conn = MagicMock()
        with pytest.raises(ValueError, match="Invalid mode"):
            sync_table(
                conn, "dbo.T", database="db1",
                output_dir=str(tmp_path / "data"),
                watermark_dir=str(tmp_path / "wm"),
                mode="snapshot",
            )


class TestSubDir:
    """Edge cases for the _sub_dir path builder."""

    def test_spaces_in_database_name(self, tmp_path):
        from azsql_ct.sync import _sub_dir
        d = _sub_dir(str(tmp_path), "My Database", "dbo.T")
        assert d == str(tmp_path / "My Database" / "dbo" / "T")
        assert os.path.isdir(d)

    def test_unicode_database_name(self, tmp_path):
        from azsql_ct.sync import _sub_dir
        d = _sub_dir(str(tmp_path), "データベース", "dbo.T")
        assert "データベース" in d
        assert os.path.isdir(d)

    def test_hyphenated_names(self, tmp_path):
        from azsql_ct.sync import _sub_dir
        d = _sub_dir(str(tmp_path), "my-db", "my-schema.my-table")
        assert d == str(tmp_path / "my-db" / "my-schema" / "my-table")
        assert os.path.isdir(d)

    def test_dot_in_table_name_splits_on_first_dot(self, tmp_path):
        """'dbo.my.table' splits to schema='dbo', table='my.table'."""
        from azsql_ct.sync import _sub_dir
        d = _sub_dir(str(tmp_path), "db1", "dbo.my.table")
        assert d == str(tmp_path / "db1" / "dbo" / "my.table")
        assert os.path.isdir(d)


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

    def test_loads_structured_format_with_uc_metadata(self):
        from azsql_ct.client import ChangeTracker

        ct = ChangeTracker.from_config({
            "connection": {
                "server": "myserver",
                "sql_login": "admin",
                "password": "pw",
            },
            "databases": {
                "db1": {
                    "uc_catalog": "my_catalog",
                    "schemas": {
                        "dbo": {
                            "uc_schema": "my_schema",
                            "tables": {
                                "t1": "full",
                                "t2": "incremental",
                            },
                        },
                    },
                },
            },
        })
        assert ct.server == "myserver"
        assert len(ct._flat_tables) == 2
        tables = {t[1] for t in ct._flat_tables}
        assert tables == {"dbo.t1", "dbo.t2"}
        assert ct._uc_metadata == {
            "db1": {"uc_catalog": "my_catalog", "schemas": {"dbo": "my_schema"}},
        }

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

    def test_structured_config_with_scd_type(self):
        from azsql_ct.client import ChangeTracker

        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "databases": {
                "db1": {
                    "schemas": {
                        "dbo": {
                            "tables": {
                                "t1": {"mode": "full_incremental", "scd_type": 2},
                                "t2": "full",
                            },
                        },
                    },
                },
            },
        })
        assert len(ct._flat_tables) == 2
        by_table = {t[1]: t for t in ct._flat_tables}
        assert by_table["dbo.t1"][2] == "full_incremental"
        assert by_table["dbo.t1"][3] == 2
        assert by_table["dbo.t2"][2] == "full"
        assert by_table["dbo.t2"][3] == 1

    def test_structured_config_with_soft_delete(self):
        from azsql_ct.client import ChangeTracker

        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "databases": {
                "db1": {
                    "schemas": {
                        "dbo": {
                            "tables": {
                                "t1": {"mode": "full_incremental", "scd_type": 1, "soft_delete": True},
                                "t2": "full",
                            },
                        },
                    },
                },
            },
        })
        assert len(ct._flat_tables) == 2
        by_table = {t[1]: t for t in ct._flat_tables}
        assert by_table["dbo.t1"][4] is True
        assert by_table["dbo.t2"][4] is False


class TestSyncTableUnifiedWriter:
    """Tests that sync_table passes table_metadata to writer and enriches the result dict."""

    def _sync(self, tmp_path, **extra_kwargs):
        desc = [("SYS_CHANGE_VERSION",), ("SYS_CHANGE_CREATION_VERSION",), ("SYS_CHANGE_OPERATION",), ("id",), ("name",)]
        rows = [(100, None, "L", 1, "Alice")]
        cursor = _make_cursor(tracked=["dbo.Foo"], cur_ver=100, min_ver=1, pk_cols=None, rows=rows, desc=desc)
        conn = MagicMock()
        conn.cursor.return_value = cursor
        writer = StubWriter()
        kwargs = dict(
            database="db1",
            output_dir=str(tmp_path / "data"),
            watermark_dir=str(tmp_path / "wm"),
            mode="full",
            writer=writer,
        )
        kwargs.update(extra_kwargs)
        result = sync_table(conn, "dbo.Foo", **kwargs)
        return result, writer

    def test_table_metadata_passed_to_writer(self, tmp_path):
        _result, writer = self._sync(tmp_path)
        assert len(writer.calls) == 1
        meta = writer.calls[0]["kwargs"]["table_metadata"]
        assert isinstance(meta, dict)

    def test_table_metadata_has_required_fields(self, tmp_path):
        _result, writer = self._sync(tmp_path)
        meta = writer.calls[0]["kwargs"]["table_metadata"]
        required = {"database", "schema", "table", "catalog", "uoid", "extraction_timestamp", "schema_version"}
        assert required.issubset(meta.keys())

    def test_result_dict_includes_schema_version(self, tmp_path):
        result, _writer = self._sync(tmp_path)
        assert "schema_version" in result
        assert isinstance(result["schema_version"], int)

    def test_result_dict_includes_columns(self, tmp_path):
        result, _writer = self._sync(tmp_path)
        assert "columns" in result
        assert isinstance(result["columns"], list)
        col_names = [c["name"] for c in result["columns"]]
        for ct_col in ("SYS_CHANGE_VERSION", "SYS_CHANGE_CREATION_VERSION", "SYS_CHANGE_OPERATION"):
            assert ct_col not in col_names

    def test_uc_catalog_passed_through(self, tmp_path):
        _result, writer = self._sync(tmp_path, uc_catalog="my_cat")
        meta = writer.calls[0]["kwargs"]["table_metadata"]
        assert meta["catalog"] == "my_cat"

    def test_schema_file_written_after_sync(self, tmp_path):
        self._sync(tmp_path)
        schema_path = tmp_path / "wm" / "db1" / "dbo" / "Foo" / "schema.json"
        assert schema_path.exists()

    def test_schema_file_contains_correct_columns(self, tmp_path):
        import json
        self._sync(tmp_path)
        schema_path = tmp_path / "wm" / "db1" / "dbo" / "Foo" / "schema.json"
        data = json.loads(schema_path.read_text())
        col_names = [c["name"] for c in data["columns"]]
        assert "id" in col_names
        assert "name" in col_names
        assert "SYS_CHANGE_VERSION" not in col_names
        assert isinstance(data["schema_version"], int)
