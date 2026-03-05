# azsql_ct

Azure SQL Change Tracking → Parquet sync engine. Extracts data from SQL Server change tracking and writes Parquet files (per-table or unified bronze envelope), with watermark-based incremental sync.

## Overview

This package provides a Python SDK and CLI for incrementally syncing data from Azure SQL databases with [Change Tracking](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-tracking-sql-server) enabled. It supports:

- **Full and incremental sync modes** – full load on first run, incremental thereafter
- **Parallel sync** – configurable worker threads for syncing multiple tables concurrently
- **Schema evolution tracking** – detects and records schema changes over time
- **Two output formats**:
  - `per_table`: native Parquet with original columns plus CT metadata
  - `unified`: bronze envelope schema with JSON `data` column (Lakeflow-style)
- **Watermark-based state** – tracks sync position for reliable resume
- **Databricks integration** – sets task values for downstream workflows

## Quick Start

### Installation

```bash
# Requires Python 3.10+
python3.11 -m pip install mssql-python pyarrow pyyaml
```

### Configuration

Create a YAML config file:

```yaml
connection:
  server: myserver.database.windows.net
  sql_login: sqladmin
  password: ${ADMIN_PASSWORD}  # env var expansion

parallelism: 4
storage:
  ingest_pipeline: ./pipeline_1
  output_format: unified  # or "per_table"

databases:
  my_db:
    uc_catalog: my_catalog
    schemas:
      dbo:
        tables:
          orders:
            mode: full_incremental
            scd_type: 1
            soft_delete: false
```

### CLI Usage

```bash
# Run from project root with PYTHONPATH set
PYTHONPATH=/Users/guanjie.shen/sql_server_permissions python3.11 -m azsql_ct --config pipeline.yaml

# With verbose logging
PYTHONPATH=/Users/guanjie.shen/sql_server_permissions python3.11 -m azsql_ct --config pipeline.yaml -v
```

### SDK Usage

```python
from azsql_ct import ChangeTracker

# Initialize tracker
ct = ChangeTracker(
    "myserver.database.windows.net",
    "sqladmin",
    "password",
    max_workers=4
)

# Configure tables
ct.tables = {
    "my_db": {
        "dbo": {
            "orders": {"mode": "full_incremental", "scd_type": 1},
            "customers": {"mode": "full_incremental", "scd_type": 2}
        }
    }
}

# Run sync
results = ct.sync()
```

## Module Reference

| Module | Purpose |
|--------|---------|
| `client.py` | **Primary API.** `ChangeTracker` facade with connection pooling, parallel dispatch, and manifest updates |
| `sync.py` | Core sync engine – `sync_table()` orchestrates query → stream → write → watermark per table |
| `queries.py` | SQL query builders – CT version checks, PK discovery, full/incremental SELECT generation |
| `writer.py` | `ParquetWriter` (native columns) and `UnifiedParquetWriter` (bronze envelope with JSON `data` column) |
| `config.py` | Config loading, env-var expansion, table-map types, validation, and normalization |
| `watermark.py` | Persists sync state (`version`, `since_version`, `rows_synced`, `mode`, `files`) as JSON |
| `schema.py` | Append-only column history – tracks schema evolution across syncs via `schema.json` |
| `output_manifest.py` | `output.yaml` manifest – records which tables have been synced and their file locations |
| `connection.py` | `AzureSQLConnection` wrapper around `mssql-python` with context manager support |
| `_constants.py` | Enums/defaults: `VALID_MODES`, `VALID_SCD_TYPES`, `VALID_OUTPUT_FORMATS`, batch sizes |
| `__main__.py` | CLI entry point (`python -m azsql_ct --config pipeline.yaml`) |

## Sync Modes

- **`full`** – Reload entire table every time
- **`incremental`** – Strict incremental via `CHANGETABLE(CHANGES ...)`; fails if no watermark exists
- **`full_incremental`** – Full load on first run, incremental thereafter (recommended default)

## Output Formats

### Per-Table (`ParquetWriter`)

Native columns plus CT metadata:
- `SYS_CHANGE_VERSION`
- `SYS_CHANGE_OPERATION`
- `SYS_CHANGE_CREATION_VERSION`

### Unified (`UnifiedParquetWriter`)

Bronze envelope schema for Lakehouse ingestion:
- `table_id` – UOID struct identifying the source table
- `cursor` – change tracking position
- `operation` – I/U/D (insert/update/delete)
- `schemaVersion` – schema version number
- `data` – JSON string with original row data

## File Layout

```
<ingest_pipeline>/
├── data/<database>/<schema>/<table>/<date>/<prefix>_<timestamp>_part<N>.parquet
├── watermarks/<database>/<schema>/<table>/
│   ├── watermarks.json      # current sync state
│   ├── sync_history.jsonl   # append-only sync log
│   └── schema.json          # column history
└── output.yaml              # manifest of synced tables
```

## Concurrency & Safety

- **`_ConnectionPool`**: One connection per database, reused across tables
- **`_TableLock`**: Per-table file lock prevents concurrent syncs of the same table
- **Atomic writes**: Files written with `.tmp` suffix, renamed on completion
- **`ThreadPoolExecutor`**: Configurable `max_workers` for parallel table sync

## Testing

Tests live in `tests/` and use pytest. Key fixtures mock the database connection and exercise query builders, writers, watermark logic, and schema tracking independently.

## Integration with Downstream Pipelines

See `lakeflow_pipeline/` for Databricks integration patterns:
- `ingestion_pipeline_materialized.py` reads `output.yaml` + `schema.json` to create bronze Delta tables and per-table silver views with auto CDC (SCD Type 1/2)
- `metadata_helper.py` provides helpers for reading manifest and schema metadata
