"""Tests for scripts/parse_output.py — YAML manifest parsing."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from scripts.parse_output import parse_yaml_to_table_dict


class TestParseYamlToTableDict:
    def _write_yaml(self, tmp_path: Path, data) -> Path:
        p = tmp_path / "output.yaml"
        p.write_text(yaml.dump(data, default_flow_style=False))
        return p

    def test_happy_path(self, tmp_path: Path):
        data = {
            "databases": {
                "db1": {
                    "uc_catalog_name": "cat1",
                    "dbo": {
                        "uc_schema_name": "sch1",
                        "orders": {
                            "file_path": "/data/orders.parquet",
                            "file_type": "parquet",
                            "primary_key": ["order_id"],
                        },
                        "items": {
                            "file_path": "/data/items.parquet",
                            "file_type": "parquet",
                        },
                    },
                }
            }
        }
        result = parse_yaml_to_table_dict(self._write_yaml(tmp_path, data))
        assert len(result) == 2
        orders = next(r for r in result if r["table_name"] == "orders")
        assert orders["database_name"] == "db1"
        assert orders["schema_name"] == "dbo"
        assert orders["file_path"] == "/data/orders.parquet"
        assert orders["file_type"] == "parquet"
        assert orders["uc_catalog_name"] == "cat1"
        assert orders["uc_schema_name"] == "sch1"
        assert orders["primary_key"] == ["order_id"]

        items = next(r for r in result if r["table_name"] == "items")
        assert items["primary_key"] is None

    def test_multiple_databases_and_schemas(self, tmp_path: Path):
        data = {
            "databases": {
                "db1": {
                    "dbo": {
                        "t1": {"file_path": "/a", "file_type": "parquet"},
                    },
                    "staging": {
                        "t2": {"file_path": "/b", "file_type": "parquet"},
                    },
                },
                "db2": {
                    "sales": {
                        "t3": {"file_path": "/c", "file_type": "parquet"},
                    },
                },
            }
        }
        result = parse_yaml_to_table_dict(self._write_yaml(tmp_path, data))
        assert len(result) == 3
        dbs = {r["database_name"] for r in result}
        assert dbs == {"db1", "db2"}
        schemas = {r["schema_name"] for r in result}
        assert schemas == {"dbo", "staging", "sales"}

    def test_empty_yaml_returns_empty(self, tmp_path: Path):
        p = tmp_path / "output.yaml"
        p.write_text("")
        assert parse_yaml_to_table_dict(p) == []

    def test_no_databases_key_returns_empty(self, tmp_path: Path):
        result = parse_yaml_to_table_dict(
            self._write_yaml(tmp_path, {"other": "stuff"})
        )
        assert result == []

    def test_non_dict_database_info_skipped(self, tmp_path: Path):
        data = {"databases": {"db1": "not_a_dict"}}
        assert parse_yaml_to_table_dict(self._write_yaml(tmp_path, data)) == []

    def test_non_dict_table_info_skipped(self, tmp_path: Path):
        data = {
            "databases": {
                "db1": {
                    "dbo": {
                        "good": {"file_path": "/x", "file_type": "parquet"},
                        "bad": "not_a_dict",
                    }
                }
            }
        }
        result = parse_yaml_to_table_dict(self._write_yaml(tmp_path, data))
        assert len(result) == 1
        assert result[0]["table_name"] == "good"
