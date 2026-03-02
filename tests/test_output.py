"""Tests for output manifest, output format config, and parquet compression config."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from azsql_ct.client import ChangeTracker


class TestOutputManifest:
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_sync_updates_output_manifest(self, MockAz, mock_sync, tmp_path):
        from azsql_ct.output_manifest import load

        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {
            "database": "db1",
            "table": "dbo.orders",
            "mode": "full",
            "rows_written": 10,
        }

        manifest_path = tmp_path / "output.yaml"
        ct = ChangeTracker(
            "srv", "usr", "pw",
            output_dir=str(tmp_path / "data"),
            output_manifest=str(manifest_path),
        )
        ct.tables = {"db1": {"dbo": ["orders"]}}
        ct.sync()

        assert manifest_path.exists()
        manifest = load(str(manifest_path))
        assert manifest["databases"]["db1"]["dbo"]["orders"]["file_path"] == str(tmp_path / "data" / "db1" / "dbo" / "orders")
        assert manifest["databases"]["db1"]["dbo"]["orders"]["file_type"] == "parquet"
        assert "columns" not in manifest["databases"]["db1"]["dbo"]["orders"]
        assert "schema_version" not in manifest["databases"]["db1"]["dbo"]["orders"]

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_manifest_includes_ingest_pipeline_location_when_configured(self, MockAz, mock_sync, tmp_path):
        from azsql_ct.output_manifest import load

        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {
            "database": "db1",
            "table": "dbo.orders",
            "mode": "full",
            "rows_written": 10,
        }

        base = str(tmp_path / "ingest_pipeline")
        ct = ChangeTracker.from_config({
            "connection": {"server": "srv", "sql_login": "u", "password": "p"},
            "storage": {"ingest_pipeline": base},
            "databases": {"db1": {"dbo": ["orders"]}},
        })
        ct.sync()

        manifest_path = tmp_path / "ingest_pipeline" / "output.yaml"
        assert manifest_path.exists()
        manifest = load(str(manifest_path))
        assert manifest.get("ingest_pipeline") == base
        assert "databases" in manifest

        inc_path = tmp_path / "ingest_pipeline" / "incremental_output.yaml"
        assert inc_path.exists()
        inc_manifest = load(str(inc_path))
        assert "generated_at" in inc_manifest
        assert "orders" in inc_manifest["databases"]["db1"]["dbo"]

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_uc_metadata_propagated_to_manifest(self, MockAz, mock_sync, tmp_path):
        from azsql_ct.output_manifest import load

        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {
            "database": "db1",
            "table": "dbo.orders",
            "mode": "full",
            "rows_written": 10,
        }

        base = str(tmp_path / "pipeline")
        ct = ChangeTracker.from_config({
            "connection": {"server": "srv", "sql_login": "u", "password": "p"},
            "storage": {"ingest_pipeline": base},
            "databases": {
                "db1": {
                    "uc_catalog": "gshen_catalog",
                    "schemas": {
                        "dbo": {
                            "uc_schema": "my_schema",
                            "tables": {"orders": "full_incremental"},
                        },
                    },
                },
            },
        })
        ct.sync()

        manifest_path = tmp_path / "pipeline" / "output.yaml"
        assert manifest_path.exists()
        manifest = load(str(manifest_path))
        assert manifest["databases"]["db1"]["uc_catalog_name"] == "gshen_catalog"
        assert manifest["databases"]["db1"]["dbo"]["uc_schema_name"] == "my_schema"
        assert manifest["databases"]["db1"]["dbo"]["orders"]["uc_table_name"] == "orders"

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_incremental_output_excludes_tables_with_zero_rows(self, MockAz, mock_sync, tmp_path):
        from azsql_ct.output_manifest import load

        def mock_sync_side_effect(conn, table_name, **kwargs):
            if "empty" in table_name:
                return {"database": "db1", "table": "dbo.empty", "rows_written": 0}
            return {"database": "db1", "table": "dbo.orders", "rows_written": 5}

        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.side_effect = mock_sync_side_effect

        base = str(tmp_path / "pipeline")
        ct = ChangeTracker.from_config({
            "connection": {"server": "srv", "sql_login": "u", "password": "p"},
            "storage": {"ingest_pipeline": base},
            "databases": {"db1": {"dbo": ["orders", "empty"]}},
        })
        ct.sync()

        inc_path = tmp_path / "pipeline" / "incremental_output.yaml"
        assert inc_path.exists()
        inc_manifest = load(str(inc_path))
        assert "orders" in inc_manifest["databases"]["db1"]["dbo"]
        assert "empty" not in inc_manifest["databases"]["db1"]["dbo"]

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_incremental_output_not_written_without_ingest_pipeline(self, MockAz, mock_sync, tmp_path):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {
            "database": "db1",
            "table": "dbo.orders",
            "mode": "full",
            "rows_written": 10,
        }

        manifest_path = tmp_path / "output.yaml"
        ct = ChangeTracker(
            "srv", "usr", "pw",
            output_dir=str(tmp_path / "data"),
            output_manifest=str(manifest_path),
        )
        ct.tables = {"db1": {"dbo": ["orders"]}}
        ct.sync()

        assert manifest_path.exists()
        inc_path = tmp_path / "incremental_output.yaml"
        assert not inc_path.exists()


class TestOutputFormatConfig:
    def test_output_format_defaults_to_per_table(self):
        ct = ChangeTracker("s", "u", "p")
        assert ct.output_format == "per_table"

    def test_output_format_stored(self):
        ct = ChangeTracker("s", "u", "p", output_format="unified")
        assert ct.output_format == "unified"

    def test_output_format_from_config_storage_section(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "storage": {"output_format": "unified"},
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.output_format == "unified"

    def test_output_format_from_config_defaults_per_table(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.output_format == "per_table"

    def test_unified_creates_unified_writer(self):
        from azsql_ct.writer import UnifiedParquetWriter
        ct = ChangeTracker("s", "u", "p", output_format="unified")
        assert isinstance(ct.writer, UnifiedParquetWriter)

    def test_per_table_creates_parquet_writer(self):
        from azsql_ct.writer import ParquetWriter
        ct = ChangeTracker("s", "u", "p", output_format="per_table")
        assert isinstance(ct.writer, ParquetWriter)


class TestParquetCompressionConfig:
    def test_parquet_compression_from_config(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "storage": {"parquet_compression": "snappy"},
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.writer.compression == "snappy"

    def test_parquet_compression_level_from_config(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "storage": {"parquet_compression": "zstd", "parquet_compression_level": 5},
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.writer.compression == "zstd"
        assert ct.writer.compression_level == 5

    def test_row_group_size_from_config(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "storage": {"row_group_size": 100000},
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.writer.row_group_size == 100000
