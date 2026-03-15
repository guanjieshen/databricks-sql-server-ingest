import json
import logging
import os
import uuid

import yaml
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

# SQL Server type names (from schema.json) -> Spark types.
# Reference: https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/sql-server-reference
_DECIMAL_TYPES = frozenset({"decimal", "numeric", "money", "smallmoney"})

MSSQL_TO_SPARK = {
    "int": IntegerType(),
    "bigint": LongType(),
    "smallint": ShortType(),
    "tinyint": ShortType(),
    "bit": BooleanType(),
    "float": DoubleType(),
    "real": FloatType(),
    "decimal": DecimalType(38, 10),
    "numeric": DecimalType(38, 10),
    "money": DecimalType(19, 4),
    "smallmoney": DecimalType(10, 4),
    "nvarchar": StringType(),
    "varchar": StringType(),
    "nchar": StringType(),
    "char": StringType(),
    "ntext": StringType(),
    "text": StringType(),
    "xml": StringType(),
    "sql_variant": StringType(),
    "hierarchyid": StringType(),
    "uniqueidentifier": StringType(),
    "datetime": TimestampType(),
    "datetime2": TimestampType(),
    "smalldatetime": TimestampType(),
    "datetimeoffset": TimestampType(),
    "date": DateType(),
    "time": StringType(),
    "varbinary": BinaryType(),
    "binary": BinaryType(),
    "image": BinaryType(),
    "geography": BinaryType(),
    "geometry": BinaryType(),
    "rowversion": BinaryType(),
    "unknown": StringType(),
}


def build_spark_schema(columns):
    """Build a StructType from schema.json columns list.

    Uses ``precision`` and ``scale`` from schema.json when available
    for decimal/numeric/money types; otherwise falls back to the
    default in ``MSSQL_TO_SPARK``.
    """
    fields = []
    for c in columns:
        spark_type = MSSQL_TO_SPARK.get(c["type"], StringType())
        if c["type"] in _DECIMAL_TYPES and "precision" in c:
            spark_type = DecimalType(c["precision"], c.get("scale", 0))
        fields.append(StructField(c["name"], spark_type, nullable=True))
    return StructType(fields)


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
                    "soft_delete_column": table_config.get("soft_delete_column", "_is_deleted"),
                    "file_path": table_config.get("file_path"),
                    "columns": columns,
                })

    return result


def parse_output_yaml(input_yaml_path: str, manifest_file: str = "output.yaml"):
    """Parse pipeline config, manifest (output.yaml or incremental_output.yaml), and schema.json.

    Returns ``(table_configs, data_path, external_access)`` where
    *table_configs* is a list of dicts with UC names, primary key, scd_type,
    source table identifiers, and the column list from schema.json for
    ``from_json`` parsing.  *external_access* is a bool indicating whether
    UniForm Iceberg V3 table properties should be applied (defaults to
    ``False`` when the key is absent from the pipeline YAML).

    When ``manifest_file`` is ``"incremental_output.yaml"``, falls back to
    ``output.yaml`` if the incremental file is missing or contains no tables.

    Args:
        input_yaml_path: Path to pipeline config YAML.
        manifest_file: Manifest filename under ingest_pipeline, e.g. "output.yaml"
            or "incremental_output.yaml". Default "output.yaml".
    """
    with open(input_yaml_path, "r") as f:
        config = yaml.safe_load(f) or {}

    external_access = bool(config.get("external_access", False))

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
                return result, data_path, external_access

    if not os.path.exists(primary_path):
        raise FileNotFoundError(f"{os.path.basename(primary_path)} not found at: {primary_path}")

    with open(primary_path, "r") as f:
        output_config = yaml.safe_load(f) or {}
    result = _parse_manifest_to_configs(output_config, watermarks_path)
    return result, data_path, external_access
