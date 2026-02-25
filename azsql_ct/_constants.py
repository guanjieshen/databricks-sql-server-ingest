"""Shared constants for the azsql_ct package."""

VALID_MODES = frozenset({"full", "incremental", "full_incremental"})

DEFAULT_OUTPUT_DIR = "./data"
DEFAULT_WATERMARK_DIR = "./watermarks"
DEFAULT_BATCH_SIZE = 10_000
