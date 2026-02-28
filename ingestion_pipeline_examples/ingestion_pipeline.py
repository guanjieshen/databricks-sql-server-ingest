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
# BRONZE: Single shared Auto Loader across all source tables (1 DAG node)
# ---------------------------------------------------------------------------

@dp.view(
    name="landing_raw",
    comment="Bronze: unified envelope schema from all source tables",
)
def landing_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("recursiveFileLookup", "true")
        .load(data_path)
    )


# ---------------------------------------------------------------------------
# SILVER: Per-table staging + CDC (1 staging + 1 target per table)
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
    staging_name = f"_staging_{uc_catalog}_{uc_schema}_{uc_table}"

    spark_schema = _build_spark_schema(columns)

    # Staging: filter landing to this table + parse JSON data column
    def create_staging(s_name, t_name, sc_name, schema):
        @dp.table(name=s_name, temporary=True)
        def _staging():
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
        return _staging

    create_staging(staging_name, table_name, schema_name, spark_schema)

    # Target silver streaming table
    dp.create_streaming_table(
        name=silver_table_name,
        comment=f"Silver: SCD Type {scd_type} from {table_name}",
        table_properties={
            "delta.feature.timestampNtz": "supported",
            "delta.enableChangeDataFeed": "true",
        },
    )

    # CDC flow: staging -> silver
    if primary_key:
        dp.create_auto_cdc_flow(
            target=silver_table_name,
            source=staging_name,
            keys=primary_key if isinstance(primary_key, list) else [primary_key],
            sequence_by="_seq_num",
            stored_as_scd_type=scd_type,
            ignore_null_updates=True,
            apply_as_deletes=F.expr("operation = 'DELETE'"),
            except_column_list=["_seq_num", "operation"],
        )
