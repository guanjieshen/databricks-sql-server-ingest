"""Shared constants for the azsql_ct package."""

VALID_MODES = frozenset({"full", "incremental", "full_incremental"})
VALID_SCD_TYPES = frozenset({1, 2})
DEFAULT_SCD_TYPE = 1
DEFAULT_SOFT_DELETE = False

DEFAULT_OUTPUT_DIR = "./data"
DEFAULT_WATERMARK_DIR = "./watermarks"
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_ROW_GROUP_SIZE = 500_000

VALID_OUTPUT_FORMATS = frozenset({"per_table", "unified"})
DEFAULT_OUTPUT_FORMAT = "per_table"

# Parquet compression: PyArrow accepts NONE, SNAPPY, GZIP, BROTLI, LZ4, ZSTD
VALID_PARQUET_COMPRESSION = frozenset({"none", "snappy", "gzip", "brotli", "lz4", "zstd"})
DEFAULT_PARQUET_COMPRESSION = "zstd"
