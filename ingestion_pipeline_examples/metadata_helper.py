import json
import os

import yaml


def parse_output_yaml(input_yaml_path: str):
    """Parse pipeline config, output.yaml, and per-table schema.json files.

    Returns ``(table_configs, data_path)`` where *table_configs* is a list
    of dicts with UC names, primary key, scd_type, source table identifiers,
    and the column list from schema.json for ``from_json`` parsing.
    """
    with open(input_yaml_path, "r") as f:
        config = yaml.safe_load(f) or {}

    storage = config.get("storage") or {}
    ingest_pipeline_path = storage.get("ingest_pipeline")
    if not ingest_pipeline_path:
        raise ValueError(f"Missing storage.ingest_pipeline in {input_yaml_path}")

    output_yaml_path = os.path.join(ingest_pipeline_path, "output.yaml")
    if not os.path.exists(output_yaml_path):
        raise FileNotFoundError(f"output.yaml not found at: {output_yaml_path}")

    watermarks_path = os.path.join(ingest_pipeline_path, "watermarks")
    data_path = os.path.join(ingest_pipeline_path, "data")

    with open(output_yaml_path, "r") as f:
        output_config = yaml.safe_load(f) or {}

    result = []

    databases = output_config.get("databases") or {}
    if not isinstance(databases, dict):
        raise ValueError("output.yaml: 'databases' must be a mapping/dict")

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
                    "primary_key": table_config.get("primary_key") or [],
                    "scd_type": table_config.get("scd_type", 1),
                    "file_path": table_config.get("file_path"),
                    "columns": columns,
                })

    return result, data_path
