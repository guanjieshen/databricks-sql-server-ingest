"""Materialized landing ingestion pipeline for the unified bronze schema.

This pipeline ingests unified Parquet files (bronze envelope schema)
produced by azsql_ct with ``output_format: unified``, then fans out
to per-table silver Delta tables via AUTO CDC (SCD Type 1 or 2).

Architecture:
    Auto Loader -> landing_raw (materialized temp Delta) -> per-table
    views (filter + from_json) -> silver streaming tables (AUTO CDC)

Key difference from ingestion_pipeline.py:
    The landing table is materialized as a temporary Delta table instead
    of a view. Parquet files are read from cloud storage once and written
    to Delta. Downstream per-table views then read from Delta, which
    supports data skipping on the table_id struct -- only rows matching
    each table are scanned.

    In ingestion_pipeline.py the landing is a view, so every per-table
    staging table re-reads ALL Parquet files from cloud storage and
    discards non-matching rows. With N tables, the source files are
    read N times.

Performance:
    For a small number of tables (< 5), the difference is negligible.
    For a large number of tables (50+), the materialized approach
    provides a significant performance improvement because the cloud
    storage read happens once rather than N times. The tradeoff is one
    extra Delta write for the landing table.

DAG structure:
    1 landing_raw (temporary table) + N views + N silver tables = 2N+1
    nodes. Same node count as ingestion_pipeline.py but different read
    characteristics.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from utilities.metadata_helper import parse_output_yaml

# SQL Server type names (from schema.json) -> Spark types
MSSQL_TO_SPARK = {
    "int": IntegerType(),
    "bigint": LongType(),
    "smallint": ShortType(),
    "tinyint": ShortType(),
    "bit": BooleanType(),
    "float": DoubleType(),
    "real": DoubleType(),
    "decimal": DecimalType(18, 2),
    "numeric": DecimalType(18, 2),
    "money": DecimalType(19, 4),
    "nvarchar": StringType(),
    "varchar": StringType(),
    "nchar": StringType(),
    "char": StringType(),
    "ntext": StringType(),
    "text": StringType(),
    "uniqueidentifier": StringType(),
    "datetime": TimestampType(),
    "datetime2": TimestampType(),
    "smalldatetime": TimestampType(),
    "date": DateType(),
    "time": StringType(),
    "varbinary": StringType(),
    "binary": StringType(),
    "unknown": StringType(),
}


def _build_spark_schema(columns):
    """Build a StructType from schema.json columns list."""
    return StructType([
        StructField(c["name"], MSSQL_TO_SPARK.get(c["type"], StringType()), nullable=True)
        for c in columns
    ])


# Load pipeline config and table metadata (including schema.json columns)
input_yaml = spark.conf.get("input_yaml")
table_configs, data_path = parse_output_yaml(input_yaml)

# ---------------------------------------------------------------------------
# BRONZE: Materialized landing table (1 DAG node)
#
# All unified Parquet files are ingested once via Auto Loader and written
# to a temporary Delta table. This avoids re-reading cloud storage N times
# (once per source table). Downstream views read from this Delta table
# and benefit from data skipping on the table_id struct columns.
#
# temporary=True keeps the table internal to the pipeline (not published
# to Unity Catalog). It is cleaned up when the pipeline is deleted.
# ---------------------------------------------------------------------------

@dp.table(
    name="landing_raw",
    temporary=True,
    comment="Bronze: materialized unified envelope from all source tables",
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
    )


# ---------------------------------------------------------------------------
# SILVER: Per-table view + AUTO CDC flow (2 DAG nodes per table)
#
# Each source table gets:
#   1. A view that filters landing_raw by table_id and parses the JSON
#      data column into typed columns using schema.json.
#   2. A streaming table with create_auto_cdc_flow that applies CDC
#      (SCD Type 1 or 2), sequenced by cursor.seqNum, with DELETE
#      support via the operation column.
#
# Views are lightweight (no storage) -- the filter + from_json logic
# is pushed down into the Delta read from landing_raw.
# ---------------------------------------------------------------------------

for tc in table_configs:
    schema_name = tc["schema"]
    table_name = tc["table"]
    uc_catalog = tc["uc_catalog"]
    uc_schema = tc["uc_schema"]
    uc_table = tc["uc_table"]
    primary_key = tc["primary_key"]
    scd_type = tc["scd_type"]
    columns = tc["columns"]

    silver_table_name = f"{uc_catalog}.{uc_schema}.{uc_table}"
    view_name = f"_view_{uc_catalog}_{uc_schema}_{uc_table}"
    spark_schema = _build_spark_schema(columns)

    def create_view(v_name, t_name, sc_name, schema):
        @dp.view(name=v_name)
        def _view():
            return (
                dp.read_stream("landing_raw")
                .filter(
                    (F.col("table_id.schema") == sc_name)
                    & (F.col("table_id.name") == t_name)
                )
                .withColumn("parsed", F.from_json("data", schema))
                .select(
                    F.col("cursor.seqNum").cast("long").alias("_seq_num"),
                    "operation",
                    "parsed.*",
                )
            )
        return _view

    create_view(view_name, table_name, schema_name, spark_schema)

    dp.create_streaming_table(
        name=silver_table_name,
        comment=f"Silver: SCD Type {scd_type} from {table_name}",
        table_properties={
            "delta.feature.timestampNtz": "supported",
            "delta.enableChangeDataFeed": "true",
        },
    )

    if primary_key:
        dp.create_auto_cdc_flow(
            target=silver_table_name,
            source=view_name,
            keys=primary_key if isinstance(primary_key, list) else [primary_key],
            sequence_by="_seq_num",
            stored_as_scd_type=scd_type,
            ignore_null_updates=True,
            apply_as_deletes=F.expr("operation = 'DELETE'"),
            except_column_list=["_seq_num", "operation"],
        )
