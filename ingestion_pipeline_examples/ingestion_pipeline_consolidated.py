"""Consolidated ingestion pipeline: materialized landing + per-table CDC.

Key difference from ingestion_pipeline.py: the landing table is
materialized as Delta (not a view), so data is read from cloud storage
once and downstream reads benefit from Delta data skipping.

DAG: 2N+1 nodes (same structure as ingestion_pipeline.py, but landing
is a table instead of a view for better read performance).

Compare against ingestion_pipeline.py to benchmark materialized landing
vs view-based landing.
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
# Data is written to Delta once; downstream reads use Delta data skipping
# instead of re-reading all Parquet files for each table.
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
# SILVER: Per-table view + create_auto_cdc_flow (2 DAG nodes each)
# Views filter + parse JSON from the materialized landing table.
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
