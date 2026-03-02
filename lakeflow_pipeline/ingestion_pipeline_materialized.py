"""Materialized landing pipeline for the unified bronze schema.

Materializes landing_raw as a temporary Delta table instead of a view.
Cloud storage is read once; downstream views read from Delta with data
skipping. For large table counts (50+) this avoids re-reading all
Parquet N times.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
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

from metadata_helper import parse_output_yaml

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


def _build_spark_schema(columns):
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


# Load pipeline config and table metadata (including schema.json columns)
input_yaml = spark.conf.get("input_yaml")
manifest_file = spark.conf.get("manifest_file", "output.yaml")
table_configs, data_path = parse_output_yaml(input_yaml, manifest_file=manifest_file)

# BRONZE: Materialized landing (temporary Delta, not published to UC).
# Parquet read once; downstream views get Delta data skipping.

@dp.table(
    name="landing_raw",
    temporary=True,
    comment="Bronze: materialized unified envelope from all source tables",
    partition_cols=["_uoid"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def landing_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("recursiveFileLookup", "true")
        .load(data_path)
        .withColumn("_uoid", F.col("table_id.uoid"))
    )


# SILVER: Per-table view (filter + from_json) -> AUTO CDC silver table.

for tc in table_configs:
    schema_name = tc["schema"]
    table_name = tc["table"]
    uc_catalog = tc["uc_catalog"]
    uc_schema = tc["uc_schema"]
    uc_table = tc["uc_table"]
    primary_key = tc["primary_key"]
    scd_type = tc["scd_type"]
    soft_delete = tc.get("soft_delete", False)
    columns = tc["columns"]

    uoid = tc["uoid"]
    silver_table_name = f"{uc_catalog}.{uc_schema}.{uc_table}"
    view_name = f"_view_{uc_catalog}_{uc_schema}_{uc_table}"
    spark_schema = _build_spark_schema(columns)

    def create_view(v_name, uoid_val, schema, include_is_deleted):
        @dp.view(name=v_name)
        def _view():
            df = (
                dp.read_stream("landing_raw")
                .filter(F.col("_uoid") == uoid_val)
                .withColumn("parsed", F.from_json("data", schema))
            )
            select_cols = [
                F.col("cursor.seqNum").cast("long").alias("_seq_num"),
                "operation",
            ]
            if include_is_deleted:
                select_cols.append(
                    F.when(F.col("operation") == "DELETE", F.lit(True))
                    .otherwise(F.lit(False))
                    .alias("_is_deleted")
                )
            select_cols.append("parsed.*")
            return df.select(*select_cols)
        return _view

    create_view(view_name, uoid, spark_schema, soft_delete)

    dp.create_streaming_table(
        name=silver_table_name,
        comment=f"Silver: SCD Type {scd_type} from {table_name}",
        table_properties={
            "delta.feature.timestampNtz": "supported",
            "delta.enableChangeDataFeed": "true",
        },
    )

    if primary_key:
        keys = primary_key if isinstance(primary_key, list) else [primary_key]
        if soft_delete:
            dp.create_auto_cdc_flow(
                target=silver_table_name,
                source=view_name,
                keys=keys,
                sequence_by="_seq_num",
                stored_as_scd_type=scd_type,
                ignore_null_updates=True,
                except_column_list=["_seq_num", "operation"],
            )
        else:
            dp.create_auto_cdc_flow(
                target=silver_table_name,
                source=view_name,
                keys=keys,
                sequence_by="_seq_num",
                stored_as_scd_type=scd_type,
                ignore_null_updates=True,
                apply_as_deletes=F.expr("operation = 'DELETE'"),
                except_column_list=["_seq_num", "operation"],
            )
