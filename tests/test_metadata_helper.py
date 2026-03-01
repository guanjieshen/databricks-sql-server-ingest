"""Tests for metadata_helper.parse_output_yaml."""

from __future__ import annotations

import pytest

from ingestion_pipeline_examples.metadata_helper import parse_output_yaml

# Minimal output manifest with one table
OUTPUT_YAML_CONTENT = """databases:
  db1:
    uc_catalog_name: my_catalog
    dbo:
      uc_schema_name: my_schema
      t1:
        uc_table_name: t1
        file_path: /data/db1/dbo/t1
        file_type: parquet
        scd_type: 1
        primary_key:
          - id
"""

# Incremental manifest with one table
INCREMENTAL_YAML_CONTENT = """generated_at: '2026-03-01T06:48:53+00:00'
databases:
  db1:
    uc_catalog_name: my_catalog
    dbo:
      uc_schema_name: my_schema
      t1:
        uc_table_name: t1
        file_path: /data/db1/dbo/t1
        file_type: parquet
        scd_type: 1
        primary_key:
          - id
"""

# Empty databases
EMPTY_DATABASES_YAML = """databases: {}
"""


class TestParseOutputYaml:
    def test_parse_output_yaml_uses_output_yaml_by_default(self, tmp_path):
        """Default behavior: loads output.yaml, returns table configs."""
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
storage:
  ingest_pipeline: {tmp_path}
databases: {{}}
""")
        (tmp_path / "output.yaml").write_text(OUTPUT_YAML_CONTENT)

        configs, data_path = parse_output_yaml(str(config_path))

        assert len(configs) == 1
        assert configs[0]["uc_catalog"] == "my_catalog"
        assert configs[0]["uc_schema"] == "my_schema"
        assert configs[0]["uc_table"] == "t1"
        assert configs[0]["database"] == "db1"
        assert configs[0]["schema"] == "dbo"
        assert configs[0]["table"] == "t1"
        assert configs[0]["primary_key"] == ["id"]
        assert configs[0]["scd_type"] == 1
        assert data_path == str(tmp_path / "data")

    def test_parse_output_yaml_with_incremental_when_exists_and_has_tables(
        self, tmp_path
    ):
        """manifest_file=incremental_output.yaml, file exists with tables -> use incremental."""
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
storage:
  ingest_pipeline: {tmp_path}
databases: {{}}
""")
        (tmp_path / "output.yaml").write_text(OUTPUT_YAML_CONTENT)
        (tmp_path / "incremental_output.yaml").write_text(INCREMENTAL_YAML_CONTENT)

        configs, data_path = parse_output_yaml(
            str(config_path), manifest_file="incremental_output.yaml"
        )

        assert len(configs) == 1
        assert configs[0]["uc_table"] == "t1"
        assert data_path == str(tmp_path / "data")

    def test_parse_output_yaml_fallback_when_incremental_missing(self, tmp_path):
        """manifest_file=incremental, file does not exist -> fall back to output.yaml."""
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
storage:
  ingest_pipeline: {tmp_path}
databases: {{}}
""")
        (tmp_path / "output.yaml").write_text(OUTPUT_YAML_CONTENT)
        # incremental_output.yaml does NOT exist

        configs, data_path = parse_output_yaml(
            str(config_path), manifest_file="incremental_output.yaml"
        )

        assert len(configs) == 1
        assert configs[0]["uc_table"] == "t1"
        assert data_path == str(tmp_path / "data")

    def test_parse_output_yaml_fallback_when_incremental_empty(self, tmp_path):
        """manifest_file=incremental, file exists but 0 tables -> fall back to output.yaml."""
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
storage:
  ingest_pipeline: {tmp_path}
databases: {{}}
""")
        (tmp_path / "output.yaml").write_text(OUTPUT_YAML_CONTENT)
        (tmp_path / "incremental_output.yaml").write_text(EMPTY_DATABASES_YAML)

        configs, data_path = parse_output_yaml(
            str(config_path), manifest_file="incremental_output.yaml"
        )

        assert len(configs) == 1
        assert configs[0]["uc_table"] == "t1"
        assert data_path == str(tmp_path / "data")

    def test_parse_output_yaml_raises_when_output_yaml_missing(self, tmp_path):
        """output.yaml missing (default manifest) -> FileNotFoundError."""
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
storage:
  ingest_pipeline: {tmp_path}
databases: {{}}
""")
        # output.yaml does NOT exist

        with pytest.raises(FileNotFoundError) as exc_info:
            parse_output_yaml(str(config_path))

        assert "output.yaml" in str(exc_info.value)

    def test_parse_output_yaml_raises_when_ingest_pipeline_missing(self, tmp_path):
        """Config without storage.ingest_pipeline -> ValueError."""
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text("""
storage: {}
databases: {}
""")

        with pytest.raises(ValueError) as exc_info:
            parse_output_yaml(str(config_path))

        assert "ingest_pipeline" in str(exc_info.value)
