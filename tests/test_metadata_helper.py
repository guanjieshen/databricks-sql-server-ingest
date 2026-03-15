"""Tests for metadata_helper.parse_output_yaml and _parse_manifest_to_configs."""

from __future__ import annotations

import json

import pytest

from lakeflow_pipeline.metadata_helper import parse_output_yaml, _parse_manifest_to_configs

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

        configs, data_path, external_access = parse_output_yaml(str(config_path))

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
        assert external_access is False

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

        configs, data_path, external_access = parse_output_yaml(
            str(config_path), manifest_file="incremental_output.yaml"
        )

        assert len(configs) == 1
        assert configs[0]["uc_table"] == "t1"
        assert data_path == str(tmp_path / "data")
        assert external_access is False

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

        configs, data_path, external_access = parse_output_yaml(
            str(config_path), manifest_file="incremental_output.yaml"
        )

        assert len(configs) == 1
        assert configs[0]["uc_table"] == "t1"
        assert data_path == str(tmp_path / "data")
        assert external_access is False

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

        configs, data_path, external_access = parse_output_yaml(
            str(config_path), manifest_file="incremental_output.yaml"
        )

        assert len(configs) == 1
        assert configs[0]["uc_table"] == "t1"
        assert data_path == str(tmp_path / "data")
        assert external_access is False

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

    def test_external_access_true(self, tmp_path):
        """external_access: true in pipeline config is returned."""
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
storage:
  ingest_pipeline: {tmp_path}
external_access: true
databases: {{}}
""")
        (tmp_path / "output.yaml").write_text(OUTPUT_YAML_CONTENT)

        _configs, _data_path, external_access = parse_output_yaml(str(config_path))

        assert external_access is True

    def test_external_access_explicit_false(self, tmp_path):
        """external_access: false explicitly set."""
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
storage:
  ingest_pipeline: {tmp_path}
external_access: false
databases: {{}}
""")
        (tmp_path / "output.yaml").write_text(OUTPUT_YAML_CONTENT)

        _configs, _data_path, external_access = parse_output_yaml(str(config_path))

        assert external_access is False

    def test_external_access_defaults_to_false(self, tmp_path):
        """Missing external_access key defaults to False."""
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
storage:
  ingest_pipeline: {tmp_path}
databases: {{}}
""")
        (tmp_path / "output.yaml").write_text(OUTPUT_YAML_CONTENT)

        _configs, _data_path, external_access = parse_output_yaml(str(config_path))

        assert external_access is False

    def test_external_access_true_via_incremental_path(self, tmp_path):
        """external_access propagated through the incremental early-return path."""
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
storage:
  ingest_pipeline: {tmp_path}
external_access: true
databases: {{}}
""")
        (tmp_path / "output.yaml").write_text(OUTPUT_YAML_CONTENT)
        (tmp_path / "incremental_output.yaml").write_text(INCREMENTAL_YAML_CONTENT)

        _configs, _data_path, external_access = parse_output_yaml(
            str(config_path), manifest_file="incremental_output.yaml"
        )

        assert external_access is True

    def test_external_access_true_incremental_fallback_missing(self, tmp_path):
        """external_access propagated when incremental is missing and falls back."""
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
storage:
  ingest_pipeline: {tmp_path}
external_access: true
databases: {{}}
""")
        (tmp_path / "output.yaml").write_text(OUTPUT_YAML_CONTENT)

        _configs, _data_path, external_access = parse_output_yaml(
            str(config_path), manifest_file="incremental_output.yaml"
        )

        assert external_access is True

    def test_external_access_true_incremental_fallback_empty(self, tmp_path):
        """external_access propagated when incremental has no tables and falls back."""
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
storage:
  ingest_pipeline: {tmp_path}
external_access: true
databases: {{}}
""")
        (tmp_path / "output.yaml").write_text(OUTPUT_YAML_CONTENT)
        (tmp_path / "incremental_output.yaml").write_text(EMPTY_DATABASES_YAML)

        _configs, _data_path, external_access = parse_output_yaml(
            str(config_path), manifest_file="incremental_output.yaml"
        )

        assert external_access is True


def _make_manifest(db="db1", schema="dbo", table="Orders"):
    """Minimal manifest dict for one table."""
    return {
        "databases": {
            db: {
                "uc_catalog_name": "my_catalog",
                schema: {
                    "uc_schema_name": "my_schema",
                    table: {
                        "uc_table_name": table.lower(),
                        "primary_key": ["id"],
                    },
                },
            },
        },
    }


class TestManifestSchemaEvolution:
    """Verify schema.json columns flow correctly through _parse_manifest_to_configs."""

    def test_evolved_schema_columns_flow_to_config(self, tmp_path):
        wm = tmp_path / "db1" / "dbo" / "Orders"
        wm.mkdir(parents=True)
        (wm / "schema.json").write_text(json.dumps({
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "deleted_col", "type": "nvarchar"},
                {"name": "new_col", "type": "bigint"},
            ],
            "schema_version": 200,
        }))
        configs = _parse_manifest_to_configs(_make_manifest(), str(tmp_path))
        assert len(configs) == 1
        col_names = [c["name"] for c in configs[0]["columns"]]
        assert col_names == ["id", "deleted_col", "new_col"]

    def test_missing_schema_json_returns_empty_columns(self, tmp_path):
        configs = _parse_manifest_to_configs(_make_manifest(), str(tmp_path))
        assert len(configs) == 1
        assert configs[0]["columns"] == []

    def test_type_changed_column_uses_latest_type(self, tmp_path):
        wm = tmp_path / "db1" / "dbo" / "Orders"
        wm.mkdir(parents=True)
        (wm / "schema.json").write_text(json.dumps({
            "columns": [
                {"name": "id", "type": "bigint", "previous_type": "int"},
            ],
            "schema_version": 300,
        }))
        configs = _parse_manifest_to_configs(_make_manifest(), str(tmp_path))
        col = configs[0]["columns"][0]
        assert col["type"] == "bigint"
        assert col["previous_type"] == "int"
