"""Tests for build_spark_schema and MSSQL_TO_SPARK type mapping."""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
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

from lakeflow_pipeline.metadata_helper import MSSQL_TO_SPARK, build_spark_schema


class TestBuildSparkSchema:
    def test_basic_type_mapping(self):
        columns = [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "nvarchar"},
        ]
        schema = build_spark_schema(columns)
        assert schema == StructType([
            StructField("id", IntegerType(), nullable=True),
            StructField("name", StringType(), nullable=True),
        ])

    def test_unknown_type_defaults_to_string(self):
        columns = [{"name": "x", "type": "some_future_type"}]
        schema = build_spark_schema(columns)
        assert schema.fields[0].dataType == StringType()

    def test_decimal_uses_precision_scale(self):
        columns = [{"name": "amt", "type": "decimal", "precision": 19, "scale": 4}]
        schema = build_spark_schema(columns)
        assert schema.fields[0].dataType == DecimalType(19, 4)

    def test_decimal_without_precision_uses_default(self):
        columns = [{"name": "amt", "type": "decimal"}]
        schema = build_spark_schema(columns)
        assert schema.fields[0].dataType == DecimalType(38, 10)

    def test_schema_grows_with_new_columns(self):
        v1 = [{"name": "id", "type": "int"}]
        v2 = [{"name": "id", "type": "int"}, {"name": "email", "type": "nvarchar"}]
        s1 = build_spark_schema(v1)
        s2 = build_spark_schema(v2)
        assert len(s1.fields) == 1
        assert len(s2.fields) == 2
        assert s2.fields[1].name == "email"
        assert s2.fields[1].dataType == StringType()

    def test_type_widening_int_to_bigint(self):
        v1 = [{"name": "id", "type": "int"}]
        v2 = [{"name": "id", "type": "bigint"}]
        s1 = build_spark_schema(v1)
        s2 = build_spark_schema(v2)
        assert s1.fields[0].dataType == IntegerType()
        assert s2.fields[0].dataType == LongType()

    def test_all_mssql_types_have_mapping(self):
        for mssql_type, spark_type in MSSQL_TO_SPARK.items():
            assert spark_type is not None, f"MSSQL type {mssql_type!r} maps to None"
