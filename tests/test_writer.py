"""Tests for azsql_ct.writer -- ParquetWriter with real file I/O."""

from __future__ import annotations

import os
from datetime import date, datetime

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from azsql_ct.writer import (
    ParquetWriter,
    UnifiedParquetWriter,
    _compute_schema_version,
    _json_dumps,
    _make_uoid,
    OP_MAP,
)


def _desc(*names: str):
    """Build a minimal ``cursor.description``-style list from column names."""
    return [(n,) for n in names]


class TestParquetWriter:
    def test_writes_single_file(self, tmp_path):
        writer = ParquetWriter()
        rows = [(1, "a"), (2, "b")]
        desc = _desc("id", "val")
        files, row_count = writer.write(rows, desc, str(tmp_path), "test")

        assert len(files) == 1
        assert row_count == 2
        assert files[0].endswith(".parquet")
        table = pq.read_table(files[0])
        assert table.num_rows == 2
        assert table.column_names == ["id", "val"]

    def test_content_matches_rows(self, tmp_path):
        writer = ParquetWriter()
        rows = [(10, "hello"), (20, "world")]
        desc = _desc("num", "word")
        files, row_count = writer.write(rows, desc, str(tmp_path), "content")

        assert row_count == 2
        table = pq.read_table(files[0])
        assert table.column("num").to_pylist() == [10, 20]
        assert table.column("word").to_pylist() == ["hello", "world"]

    def test_splits_when_exceeding_max_rows(self, tmp_path):
        writer = ParquetWriter(max_rows_per_file=5)
        rows = [(i, f"v{i}") for i in range(12)]
        desc = _desc("id", "val")
        files, row_count = writer.write(rows, desc, str(tmp_path), "split")

        assert len(files) == 3
        assert row_count == 12
        total = 0
        for path in files:
            assert os.path.isfile(path)
            total += pq.read_table(path).num_rows
        assert total == 12

    def test_empty_rows_produce_no_file(self, tmp_path):
        """Zero rows: no Parquet file is written (avoids all-null schema)."""
        writer = ParquetWriter()
        files, row_count = writer.write([], _desc("a", "b"), str(tmp_path), "empty")

        assert len(files) == 0
        assert row_count == 0
        assert list(tmp_path.iterdir()) == []

    def test_all_null_column_written_as_int64(self, tmp_path):
        """Columns where every value is None get type int64, not null."""
        writer = ParquetWriter()
        rows = [(1, None), (2, None)]
        files, _ = writer.write(rows, _desc("id", "nullable_col"), str(tmp_path), "nulltest")

        table = pq.read_table(files[0])
        assert table.schema.field("nullable_col").type == pa.int64()

    def test_file_naming_includes_prefix(self, tmp_path):
        writer = ParquetWriter()
        files, _ = writer.write([(1,)], _desc("x"), str(tmp_path), "myprefix")
        assert "myprefix" in os.path.basename(files[0])

    def test_accepts_generator_input(self, tmp_path):
        writer = ParquetWriter()
        desc = _desc("id", "val")

        def row_gen():
            for i in range(5):
                yield (i, f"v{i}")

        files, row_count = writer.write(row_gen(), desc, str(tmp_path), "gen")

        assert row_count == 5
        assert len(files) == 1
        table = pq.read_table(files[0])
        assert table.num_rows == 5

    def test_partitioned_by_day_writes_under_date_subdirs(self, tmp_path):
        """With partition_column set, files are written under dir_path/YYYY-MM-DD/."""
        from datetime import timezone

        writer = ParquetWriter(partition_column="dt")
        # Rows with different dates -> different partition dirs
        rows = [
            (1, datetime(2025, 2, 25, 10, 0, 0, tzinfo=timezone.utc)),
            (2, datetime(2025, 2, 25, 11, 0, 0, tzinfo=timezone.utc)),
            (3, date(2025, 2, 26)),
        ]
        desc = _desc("id", "dt")
        files, row_count = writer.write(rows, desc, str(tmp_path), "part")

        assert row_count == 3
        assert len(files) == 2  # one file per day (small batches)
        dirs = {os.path.basename(os.path.dirname(f)) for f in files}
        assert "2025-02-25" in dirs
        assert "2025-02-26" in dirs
        for f in files:
            assert f.startswith(str(tmp_path))
            assert os.path.isfile(f)
        total = sum(pq.read_table(p).num_rows for p in files)
        assert total == 3

    def test_streaming_row_groups(self, tmp_path):
        """row_group_size controls how many row groups are written per file."""
        writer = ParquetWriter(max_rows_per_file=100, row_group_size=3)
        rows = [(i, f"v{i}") for i in range(7)]
        desc = _desc("id", "val")
        files, row_count = writer.write(rows, desc, str(tmp_path), "rg")

        assert row_count == 7
        assert len(files) == 1
        pf = pq.ParquetFile(files[0])
        assert pf.metadata.num_row_groups == 3  # ceil(7 / 3)
        assert pf.metadata.num_rows == 7

    def test_streaming_splits_files_at_max_rows(self, tmp_path):
        """Files are split at max_rows_per_file even with small row_group_size."""
        writer = ParquetWriter(max_rows_per_file=5, row_group_size=2)
        rows = [(i,) for i in range(11)]
        desc = _desc("id")
        files, row_count = writer.write(rows, desc, str(tmp_path), "split")

        assert row_count == 11
        assert len(files) == 3  # 5, 5, 1
        totals = [pq.read_table(f).num_rows for f in files]
        assert totals == [5, 5, 1]

    def test_streaming_partitioned_row_groups(self, tmp_path):
        """Partitioned writes produce per-partition files with correct rows."""
        from datetime import timezone

        writer = ParquetWriter(partition_column="dt", max_rows_per_file=100)
        rows = [
            (i, datetime(2025, 1, 1, i, tzinfo=timezone.utc)) for i in range(4)
        ] + [
            (i, datetime(2025, 1, 2, i, tzinfo=timezone.utc)) for i in range(3)
        ]
        desc = _desc("id", "dt")
        files, row_count = writer.write(rows, desc, str(tmp_path), "part")

        assert row_count == 7
        assert len(files) == 2
        totals = sorted(pq.read_table(f).num_rows for f in files)
        assert totals == [3, 4]


def _sample_metadata():
    return {
        "database": "db1",
        "schema": "dbo",
        "table": "orders",
        "catalog": "my_catalog",
        "uoid": "test-uoid-1234",
        "extraction_timestamp": 1700000000000,
        "schema_version": 999,
    }


class TestUnifiedParquetWriter:
    def test_writes_bronze_schema(self, tmp_path):
        writer = UnifiedParquetWriter()
        desc = _desc("SYS_CHANGE_VERSION", "SYS_CHANGE_CREATION_VERSION", "SYS_CHANGE_OPERATION", "id", "name")
        rows = [(100, None, "L", 1, "Alice")]
        files, _ = writer.write(rows, desc, str(tmp_path), "bronze", table_metadata=_sample_metadata())

        table = pq.read_table(files[0])
        assert table.column_names == [
            "data", "table_id", "cursor", "extractionTimestamp", "operation", "schemaVersion",
        ]

    def test_data_column_is_valid_json(self, tmp_path):
        writer = UnifiedParquetWriter()
        desc = _desc("SYS_CHANGE_VERSION", "SYS_CHANGE_CREATION_VERSION", "SYS_CHANGE_OPERATION", "id", "name")
        rows = [(100, None, "I", 1, "Alice")]
        files, _ = writer.write(rows, desc, str(tmp_path), "json", table_metadata=_sample_metadata())

        table = pq.read_table(files[0])
        import json
        data = json.loads(table.column("data").to_pylist()[0])
        assert "id" in data
        assert "name" in data
        assert "SYS_CHANGE_VERSION" not in data
        assert "SYS_CHANGE_CREATION_VERSION" not in data
        assert "SYS_CHANGE_OPERATION" not in data

    def test_operation_mapping(self, tmp_path):
        writer = UnifiedParquetWriter()
        desc = _desc("SYS_CHANGE_VERSION", "SYS_CHANGE_CREATION_VERSION", "SYS_CHANGE_OPERATION", "id")
        rows = [
            (1, None, "L", 10),
            (2, None, "I", 20),
            (3, None, "U", 30),
            (4, None, "D", 40),
        ]
        files, _ = writer.write(rows, desc, str(tmp_path), "ops", table_metadata=_sample_metadata())

        table = pq.read_table(files[0])
        ops = table.column("operation").to_pylist()
        assert ops == ["LOAD", "INSERT", "UPDATE", "DELETE"]

    def test_table_id_struct_fields(self, tmp_path):
        writer = UnifiedParquetWriter()
        desc = _desc("SYS_CHANGE_VERSION", "SYS_CHANGE_CREATION_VERSION", "SYS_CHANGE_OPERATION", "id")
        rows = [(100, None, "I", 1)]
        files, _ = writer.write(rows, desc, str(tmp_path), "tid", table_metadata=_sample_metadata())

        table = pq.read_table(files[0])
        tid = table.column("table_id").to_pylist()[0]
        assert tid["catalog"] == "my_catalog"
        assert tid["schema"] == "dbo"
        assert tid["name"] == "orders"
        assert tid["uoid"] == "test-uoid-1234"

    def test_cursor_struct_seqnum(self, tmp_path):
        writer = UnifiedParquetWriter()
        desc = _desc("SYS_CHANGE_VERSION", "SYS_CHANGE_CREATION_VERSION", "SYS_CHANGE_OPERATION", "id")
        rows = [(100, None, "I", 1)]
        files, _ = writer.write(rows, desc, str(tmp_path), "cur", table_metadata=_sample_metadata())

        table = pq.read_table(files[0])
        cursor = table.column("cursor").to_pylist()[0]
        assert cursor["seqNum"] == "100"

    def test_schema_version_from_metadata(self, tmp_path):
        writer = UnifiedParquetWriter()
        desc = _desc("SYS_CHANGE_VERSION", "SYS_CHANGE_CREATION_VERSION", "SYS_CHANGE_OPERATION", "id")
        rows = [(1, None, "I", 10)]
        files, _ = writer.write(rows, desc, str(tmp_path), "sv", table_metadata=_sample_metadata())

        table = pq.read_table(files[0])
        assert table.column("schemaVersion").to_pylist() == [999]

    def test_extraction_timestamp_from_metadata(self, tmp_path):
        writer = UnifiedParquetWriter()
        desc = _desc("SYS_CHANGE_VERSION", "SYS_CHANGE_CREATION_VERSION", "SYS_CHANGE_OPERATION", "id")
        rows = [(1, None, "I", 10)]
        files, _ = writer.write(rows, desc, str(tmp_path), "ts", table_metadata=_sample_metadata())

        table = pq.read_table(files[0])
        assert table.column("extractionTimestamp").to_pylist() == [1700000000000]

    def test_splits_files_at_max_rows(self, tmp_path):
        writer = UnifiedParquetWriter(max_rows_per_file=2)
        desc = _desc("SYS_CHANGE_VERSION", "SYS_CHANGE_CREATION_VERSION", "SYS_CHANGE_OPERATION", "id")
        rows = [(i, None, "I", i) for i in range(3)]
        files, row_count = writer.write(rows, desc, str(tmp_path), "split", table_metadata=_sample_metadata())

        assert row_count == 3
        assert len(files) == 2
        total = sum(pq.read_table(f).num_rows for f in files)
        assert total == 3

    def test_empty_rows_no_file(self, tmp_path):
        writer = UnifiedParquetWriter()
        desc = _desc("SYS_CHANGE_VERSION", "SYS_CHANGE_CREATION_VERSION", "SYS_CHANGE_OPERATION", "id")
        files, row_count = writer.write([], desc, str(tmp_path), "empty", table_metadata=_sample_metadata())

        assert len(files) == 0
        assert row_count == 0



class TestHelpers:
    def test_make_uoid_deterministic(self):
        a = _make_uoid("mydb", "dbo", "users")
        b = _make_uoid("mydb", "dbo", "users")
        assert a == b

    def test_make_uoid_different_inputs(self):
        a = _make_uoid("db1", "dbo", "orders")
        b = _make_uoid("db2", "dbo", "orders")
        assert a != b

    def test_compute_schema_version_stable(self):
        desc = _desc("col_a", "col_b")
        a = _compute_schema_version(desc)
        b = _compute_schema_version(desc)
        assert a == b
        assert isinstance(a, int)

    def test_compute_schema_version_differs(self):
        desc_a = _desc("col_a", "col_b")
        desc_b = _desc("col_x", "col_y")
        assert _compute_schema_version(desc_a) != _compute_schema_version(desc_b)

    def test_op_map_coverage(self):
        assert OP_MAP["I"] == "INSERT"
        assert OP_MAP["U"] == "UPDATE"
        assert OP_MAP["D"] == "DELETE"
        assert OP_MAP["L"] == "LOAD"
        assert len(OP_MAP) == 4

    def test_compute_schema_version_column_order_matters(self):
        desc_ab = _desc("col_a", "col_b")
        desc_ba = _desc("col_b", "col_a")
        assert _compute_schema_version(desc_ab) != _compute_schema_version(desc_ba)

    def test_compute_schema_version_all_ct_columns_only(self):
        desc = _desc("SYS_CHANGE_VERSION", "SYS_CHANGE_CREATION_VERSION", "SYS_CHANGE_OPERATION")
        version = _compute_schema_version(desc)
        assert isinstance(version, int)

    def test_compute_schema_version_delimiter_no_collision(self):
        """Column name containing the pipe delimiter must not collide."""
        desc_pipe = _desc("a|b")
        desc_two = _desc("a", "b")
        assert _compute_schema_version(desc_pipe) != _compute_schema_version(desc_two)

    def test_make_uoid_dotted_name_no_collision(self):
        """Different (db, schema, table) triples must not collide even when
        the dot-joined key looks the same."""
        a = _make_uoid("a.b", "c", "d")
        b = _make_uoid("a", "b.c", "d")
        assert a != b


class TestParquetCompression:
    """Verify ZSTD compression is applied to Parquet output."""

    def test_per_table_writer_uses_zstd_by_default(self, tmp_path):
        writer = ParquetWriter()
        desc = _desc("id", "name")
        rows = [(1, "a"), (2, "b")]
        files, _ = writer.write(rows, desc, str(tmp_path), "test")
        assert len(files) == 1
        meta = pq.read_metadata(files[0])
        # Each row group has a column chunk with compression
        rg0 = meta.row_group(0)
        assert rg0.column(0).compression == "ZSTD"

    def test_unified_writer_uses_zstd_by_default(self, tmp_path):
        writer = UnifiedParquetWriter()
        desc = _desc("SYS_CHANGE_VERSION", "SYS_CHANGE_CREATION_VERSION", "SYS_CHANGE_OPERATION", "id")
        rows = [(1, None, "I", 1)]
        files, _ = writer.write(rows, desc, str(tmp_path), "test", table_metadata=_sample_metadata())
        assert len(files) == 1
        meta = pq.read_metadata(files[0])
        rg0 = meta.row_group(0)
        assert rg0.column(0).compression == "ZSTD"

    def test_compression_none_disables_compression(self, tmp_path):
        writer = ParquetWriter(compression="none")
        desc = _desc("id")
        files, _ = writer.write([(1,)], desc, str(tmp_path), "test")
        meta = pq.read_metadata(files[0])
        assert meta.row_group(0).column(0).compression == "UNCOMPRESSED"

    def test_invalid_compression_raises(self):
        with pytest.raises(ValueError, match="Invalid compression"):
            ParquetWriter(compression="invalid")


class TestJsonDumps:
    """_json_dumps uses orjson when available, else stdlib json; output must be valid."""

    def test_produces_valid_json(self):
        import json
        data = {"a": 1, "b": None, "c": "str"}
        out = _json_dumps(data)
        assert json.loads(out) == data

    def test_handles_default_str(self):
        import json
        from datetime import datetime
        data = {"dt": datetime(2025, 1, 15, 12, 0, 0)}
        out = _json_dumps(data)
        parsed = json.loads(out)
        assert "dt" in parsed
