"""Tests for azsql_ct.writer -- ParquetWriter and CsvWriter with real file I/O."""

from __future__ import annotations

import csv
import os

import pyarrow.parquet as pq

from azsql_ct.writer import CsvWriter, ParquetWriter


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

    def test_empty_rows_produce_empty_file(self, tmp_path):
        writer = ParquetWriter()
        files, row_count = writer.write([], _desc("a", "b"), str(tmp_path), "empty")

        assert len(files) == 1
        assert row_count == 0
        table = pq.read_table(files[0])
        assert table.num_rows == 0
        assert table.column_names == ["a", "b"]

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


class TestCsvWriter:
    def test_writes_single_file(self, tmp_path):
        writer = CsvWriter()
        rows = [(1, "a"), (2, "b")]
        desc = _desc("id", "val")
        files, row_count = writer.write(rows, desc, str(tmp_path), "test")

        assert len(files) == 1
        assert row_count == 2
        with open(files[0]) as f:
            reader = csv.reader(f)
            header = next(reader)
            assert header == ["id", "val"]
            data = list(reader)
            assert len(data) == 2

    def test_csv_content_matches_rows(self, tmp_path):
        writer = CsvWriter()
        rows = [(10, "hello"), (20, "world")]
        desc = _desc("num", "word")
        files, row_count = writer.write(rows, desc, str(tmp_path), "content")

        assert row_count == 2
        with open(files[0]) as f:
            reader = csv.DictReader(f)
            records = list(reader)
        assert records[0]["num"] == "10"
        assert records[1]["word"] == "world"

    def test_splits_when_exceeding_max_bytes(self, tmp_path):
        writer = CsvWriter(max_bytes=100)
        rows = [(i, "x" * 50) for i in range(20)]
        desc = _desc("id", "payload")
        files, row_count = writer.write(rows, desc, str(tmp_path), "split")

        assert len(files) > 1
        assert row_count == 20
        for path in files:
            assert os.path.isfile(path)
            with open(path) as f:
                reader = csv.reader(f)
                header = next(reader)
                assert header == ["id", "payload"]

    def test_empty_rows_produce_header_only(self, tmp_path):
        writer = CsvWriter()
        files, row_count = writer.write([], _desc("a", "b"), str(tmp_path), "empty")

        assert len(files) == 1
        assert row_count == 0
        with open(files[0]) as f:
            lines = f.readlines()
        assert len(lines) == 1
        assert "a" in lines[0]

    def test_file_naming_includes_prefix(self, tmp_path):
        writer = CsvWriter()
        files, _ = writer.write([(1,)], _desc("x"), str(tmp_path), "myprefix")
        assert "myprefix" in os.path.basename(files[0])

    def test_accepts_generator_input(self, tmp_path):
        writer = CsvWriter()
        desc = _desc("id", "val")

        def row_gen():
            for i in range(5):
                yield (i, f"v{i}")

        files, row_count = writer.write(row_gen(), desc, str(tmp_path), "gen")

        assert row_count == 5
        assert len(files) == 1
        with open(files[0]) as f:
            reader = csv.reader(f)
            next(reader)
            assert len(list(reader)) == 5
