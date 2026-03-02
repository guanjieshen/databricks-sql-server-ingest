import json
import logging
import os
import uuid

import yaml

logger = logging.getLogger(__name__)


def _make_uoid(database: str, schema: str, table: str) -> str:
    """Deterministic UUID5 matching azsql_ct.writer._make_uoid.

    Uses null-byte separator so dotted identifiers like ("a.b", "c", "d")
    and ("a", "b.c", "d") never collide. Must match writer._make_uoid.
    """
    key = f"{database}\x00{schema}\x00{table}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, key))


def _parse_manifest_to_configs(
    output_config: dict,
    watermarks_path: str,
) -> list:
    """Parse manifest YAML structure into flat table config list.

    Args:
        output_config: Loaded YAML dict with 'databases' key.
        watermarks_path: Path to watermarks dir for schema.json lookup.

    Returns:
        List of table config dicts.
    """
    result = []
    databases = output_config.get("databases") or {}
    if not isinstance(databases, dict):
        raise ValueError("Manifest 'databases' must be a mapping/dict")

    for db_name, db_config in databases.items():
        if not isinstance(db_config, dict):
            continue

        uc_catalog = db_config.get("uc_catalog_name")

        for schema_name, schema_config in db_config.items():
            if not isinstance(schema_config, dict):
                continue
            if schema_name == "uc_catalog_name":
                continue

            uc_schema = schema_config.get("uc_schema_name")

            for table_name, table_config in schema_config.items():
                if not isinstance(table_config, dict):
                    continue
                if table_name == "uc_schema_name":
                    continue

                uc_table = table_config.get("uc_table_name")
                if not (uc_catalog and uc_schema and uc_table):
                    continue

                # Load per-table schema.json for from_json parsing
                schema_json_path = os.path.join(
                    watermarks_path, db_name, schema_name, table_name, "schema.json"
                )
                columns = []
                if os.path.exists(schema_json_path):
                    with open(schema_json_path) as sf:
                        columns = json.load(sf).get("columns", [])

                result.append({
                    "uc_location": f"{uc_catalog}.{uc_schema}.{uc_table}",
                    "uc_catalog": uc_catalog,
                    "uc_schema": uc_schema,
                    "uc_table": uc_table,
                    "database": db_name,
                    "schema": schema_name,
                    "table": table_name,
                    "uoid": _make_uoid(db_name, schema_name, table_name),
                    "primary_key": table_config.get("primary_key") or [],
                    "scd_type": table_config.get("scd_type", 1),
                    "soft_delete": bool(table_config.get("soft_delete", False)),
                    "file_path": table_config.get("file_path"),
                    "columns": columns,
                })

    return result


def parse_output_yaml(input_yaml_path: str, manifest_file: str = "output.yaml"):
    """Parse pipeline config, manifest (output.yaml or incremental_output.yaml), and schema.json.

    Returns ``(table_configs, data_path)`` where *table_configs* is a list
    of dicts with UC names, primary key, scd_type, source table identifiers,
    and the column list from schema.json for ``from_json`` parsing.

    When ``manifest_file`` is ``"incremental_output.yaml"``, falls back to
    ``output.yaml`` if the incremental file is missing or contains no tables.

    Args:
        input_yaml_path: Path to pipeline config YAML.
        manifest_file: Manifest filename under ingest_pipeline, e.g. "output.yaml"
            or "incremental_output.yaml". Default "output.yaml".
    """
    with open(input_yaml_path, "r") as f:
        config = yaml.safe_load(f) or {}

    storage = config.get("storage") or {}
    ingest_pipeline_path = storage.get("ingest_pipeline")
    if not ingest_pipeline_path:
        raise ValueError(f"Missing storage.ingest_pipeline in {input_yaml_path}")

    watermarks_path = os.path.join(ingest_pipeline_path, "watermarks")
    data_path = os.path.join(ingest_pipeline_path, "data")

    primary_path = os.path.join(ingest_pipeline_path, manifest_file)
    fallback_path = os.path.join(ingest_pipeline_path, "output.yaml")

    if manifest_file == "incremental_output.yaml":
        if not os.path.exists(primary_path):
            logger.warning(
                "incremental_output.yaml not found at %s, falling back to output.yaml",
                primary_path,
            )
            primary_path = fallback_path
        else:
            with open(primary_path, "r") as f:
                output_config = yaml.safe_load(f) or {}
            result = _parse_manifest_to_configs(output_config, watermarks_path)
            if not result:
                logger.warning(
                    "incremental_output.yaml contains no tables, falling back to output.yaml",
                )
                primary_path = fallback_path
            else:
                return result, data_path

    if not os.path.exists(primary_path):
        raise FileNotFoundError(f"{os.path.basename(primary_path)} not found at: {primary_path}")

    with open(primary_path, "r") as f:
        output_config = yaml.safe_load(f) or {}
    result = _parse_manifest_to_configs(output_config, watermarks_path)
    return result, data_path
