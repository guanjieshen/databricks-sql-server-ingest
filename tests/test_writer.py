"""Tests for azsql_ct.writer -- ParquetWriter with real file I/O."""

from __future__ import annotations

import os
from datetime import date, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from azsql_ct.writer import ParquetWriter


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
