"""azsql_ct -- Incremental Azure SQL change-tracking sync to Parquet."""

from .client import ChangeTracker
from .config import expand_env, resolve_secrets, resolve_value
from .connection import AzureSQLConnection, get_connection
from .sync import sync_table
from .sync_log import write_sync_log
from .writer import OutputWriter, ParquetWriter, UnifiedParquetWriter, WriteResult

__all__ = [
    "ChangeTracker",
    "expand_env",
    "resolve_secrets",
    "resolve_value",
    "AzureSQLConnection",
    "get_connection",
    "sync_table",
    "write_sync_log",
    "ParquetWriter",
    "UnifiedParquetWriter",
    "OutputWriter",
    "WriteResult",
]
