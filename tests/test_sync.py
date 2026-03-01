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


_CT_COLS = frozenset({
    "SYS_CHANGE_VERSION", "SYS_CHANGE_CREATION_VERSION", "SYS_CHANGE_OPERATION",
})


def _make_cursor(tracked, cur_ver, min_ver, pk_cols, rows, desc, table_cols=None,
                 column_metadata_rows=None):
    """Build a MagicMock cursor that responds to the queries sync_table makes.

    *table_cols* is a list of column names returned by ``table_columns()``
    (the ``SELECT TOP 0`` call added for incremental syncs).  When provided
    and *pk_cols* is non-empty, an extra execute step is inserted so the
    mock sets ``cursor.description`` appropriately for that call.

    *column_metadata_rows* overrides the ``INFORMATION_SCHEMA.COLUMNS``
    result used by ``queries.column_metadata()``.  When ``None`` (default),
    a synthetic result is derived from *desc* so existing tests keep passing.
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

    # column_metadata (INFORMATION_SCHEMA.COLUMNS) — called after the write
    if column_metadata_rows is not None:
        _cm_rows = column_metadata_rows
    elif desc:
        _cm_rows = [
            (col[0], "nvarchar", None, None)
            for col in desc if col[0] not in _CT_COLS
        ]
    else:
        _cm_rows = []
    results_sequence.append(_cm_rows)
    descs_sequence.append(None)

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
            mode="full_incremental",
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
            mode="full_incremental", writer=StubWriter(), scd_type=2,
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
            mode="full_incremental",
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

    @pytest.mark.parametrize("extra_kwargs,expected_soft_delete,expected_scd_type", [
        ({"soft_delete": True}, True, 1),
        ({}, False, 1),
        ({"scd_type": 2, "soft_delete": True}, True, 2),
    ], ids=["soft_delete_true", "soft_delete_default_false", "soft_delete_with_scd_type_2"])
    def test_soft_delete_and_scd_type_in_result(self, tmp_path, extra_kwargs, expected_soft_delete, expected_scd_type):
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
            mode="full_incremental", writer=StubWriter(), **extra_kwargs,
        )
        assert result["soft_delete"] is expected_soft_delete
        assert result["scd_type"] == expected_scd_type

    def test_invalid_mode_raises(self, tmp_path):
        conn = MagicMock()
        with pytest.raises(ValueError, match="Invalid mode"):
            sync_table(
                conn, "dbo.T", database="db1",
                output_dir=str(tmp_path / "data"),
                watermark_dir=str(tmp_path / "wm"),
                mode="snapshot",
            )


class TestClearDataFilesOnFullSync:
    """Full sync should clean old day directories, not just today's."""

    def test_full_sync_removes_old_day_files(self, tmp_path):
        old_day = tmp_path / "data" / "db1" / "dbo" / "Foo" / "2025-01-01"
        old_day.mkdir(parents=True)
        stale_file = old_day / "dbo_Foo_old.parquet"
        stale_file.write_text("stale")

        desc = [("SYS_CHANGE_VERSION",), ("SYS_CHANGE_CREATION_VERSION",), ("SYS_CHANGE_OPERATION",), ("id",)]
        rows = [(100, None, "L", 1)]
        cursor = _make_cursor(
            tracked=["dbo.Foo"], cur_ver=100, min_ver=1, pk_cols=None,
            rows=rows, desc=desc,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        writer = StubWriter()
        sync_table(
            conn, "dbo.Foo", database="db1",
            output_dir=str(tmp_path / "data"),
            watermark_dir=str(tmp_path / "wm"),
            mode="full_incremental", writer=writer,
        )

        assert not stale_file.exists()


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


class TestTableLock:
    """Tests for _TableLock -- per-table advisory file lock."""

    def test_successful_acquisition(self, tmp_path):
        from azsql_ct.sync import _TableLock, _LOCK_FILENAME

        lock_dir = str(tmp_path / "locks")
        with _TableLock(lock_dir) as lock:
            assert lock is not None
            assert os.path.isfile(os.path.join(lock_dir, _LOCK_FILENAME))

    def test_contention_raises_runtime_error(self, tmp_path):
        from azsql_ct.sync import _TableLock

        lock_dir = str(tmp_path / "locks")
        with _TableLock(lock_dir):
            with pytest.raises(RuntimeError, match="Another sync is already running"):
                with _TableLock(lock_dir):
                    pass

    def test_release_allows_reacquisition(self, tmp_path):
        from azsql_ct.sync import _TableLock

        lock_dir = str(tmp_path / "locks")
        with _TableLock(lock_dir):
            pass
        with _TableLock(lock_dir):
            pass


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
            mode="full_incremental",
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


class TestColumnMetadataIntegration:
    """Tests that sync_table uses native SQL Server type metadata from INFORMATION_SCHEMA."""

    def _sync(self, tmp_path, column_metadata_rows=None, **extra_kwargs):
        desc = [
            ("SYS_CHANGE_VERSION",), ("SYS_CHANGE_CREATION_VERSION",),
            ("SYS_CHANGE_OPERATION",), ("id",), ("name",),
        ]
        rows = [(100, None, "L", 1, "Alice")]
        cursor = _make_cursor(
            tracked=["dbo.Foo"], cur_ver=100, min_ver=1, pk_cols=None,
            rows=rows, desc=desc, column_metadata_rows=column_metadata_rows,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor
        writer = StubWriter()
        kwargs = dict(
            database="db1",
            output_dir=str(tmp_path / "data"),
            watermark_dir=str(tmp_path / "wm"),
            mode="full_incremental",
            writer=writer,
        )
        kwargs.update(extra_kwargs)
        result = sync_table(conn, "dbo.Foo", **kwargs)
        return result, writer

    def test_schema_uses_native_types(self, tmp_path):
        """column_metadata result is written to schema.json with real SQL Server types."""
        cm_rows = [
            ("id", "bigint", 19, 0),
            ("name", "varchar", None, None),
        ]
        result, _ = self._sync(tmp_path, column_metadata_rows=cm_rows)
        data = json.loads(
            (tmp_path / "wm" / "db1" / "dbo" / "Foo" / "schema.json").read_text()
        )
        type_map = {c["name"]: c["type"] for c in data["columns"]}
        assert type_map["id"] == "bigint"
        assert type_map["name"] == "varchar"

    def test_precision_scale_preserved(self, tmp_path):
        """Precision and scale from INFORMATION_SCHEMA flow into schema.json."""
        cm_rows = [
            ("amount", "decimal", 19, 4),
        ]
        self._sync(tmp_path, column_metadata_rows=cm_rows)
        data = json.loads(
            (tmp_path / "wm" / "db1" / "dbo" / "Foo" / "schema.json").read_text()
        )
        col = data["columns"][0]
        assert col["type"] == "decimal"
        assert col["precision"] == 19
        assert col["scale"] == 4

    def test_falls_back_to_description_on_failure(self, tmp_path):
        """When column_metadata raises, sync uses columns_from_description."""
        desc = [
            ("SYS_CHANGE_VERSION",), ("SYS_CHANGE_CREATION_VERSION",),
            ("SYS_CHANGE_OPERATION",), ("id",), ("name",),
        ]
        rows = [(100, None, "L", 1, "Alice")]
        cursor = _make_cursor(
            tracked=["dbo.Foo"], cur_ver=100, min_ver=1, pk_cols=None,
            rows=rows, desc=desc,
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch("azsql_ct.sync.queries.column_metadata", side_effect=RuntimeError("no access")):
            result = sync_table(
                conn, "dbo.Foo", database="db1",
                output_dir=str(tmp_path / "data"),
                watermark_dir=str(tmp_path / "wm"),
                mode="full_incremental", writer=StubWriter(),
            )

        assert "columns" in result
        col_names = [c["name"] for c in result["columns"]]
        assert "id" in col_names
        assert "name" in col_names

    def test_result_dict_columns_from_native_metadata(self, tmp_path):
        """The result dict 'columns' key uses native types when available."""
        cm_rows = [
            ("id", "int", 10, 0),
            ("name", "nvarchar", None, None),
        ]
        result, _ = self._sync(tmp_path, column_metadata_rows=cm_rows)
        type_map = {c["name"]: c["type"] for c in result["columns"]}
        assert type_map["id"] == "int"
        assert type_map["name"] == "nvarchar"
