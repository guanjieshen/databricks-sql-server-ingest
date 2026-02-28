# azsql_ct — Azure SQL Change Tracking Sync

Incrementally sync change-tracked tables from Azure SQL Server to local Parquet files. Built for batch ETL workflows that land Azure SQL data into a lakehouse (e.g. Databricks / Spark).

Uses [mssql-python](https://github.com/microsoft/mssql-python) — Microsoft's official driver that bundles its own TDS layer, so **no system ODBC driver is needed**.

---

## What This Repo Does

`azsql_ct` connects to one or more Azure SQL databases, reads tables that have SQL Server Change Tracking enabled, and writes the results as Parquet files to a local (or mounted) directory. On subsequent runs it uses change-tracking watermarks to fetch only the rows that changed, dramatically reducing data transfer and processing time.

Key capabilities:

- **Incremental extraction** via SQL Server Change Tracking (`CHANGETABLE`).
- **Parallel table sync** with configurable worker count.
- **Two output formats**: native per-table Parquet or a Lakeflow-Connect-style unified bronze envelope.
- **Watermark management** — persists sync state (version, rows, duration, files) per table so runs are resumable.
- **Schema tracking** — append-only column metadata with deterministic schema-version hashing.
- **Output manifest** — YAML manifest of synced tables for downstream orchestration.
- **SCD Type 1 & 2** support, configurable per table.
- **Soft-delete support** — optionally preserve deleted rows with an `_is_deleted` flag.
- **Atomic writes** — write-then-rename strategy prevents downstream readers from seeing partial files.

---

## Supported Features

### Sync Modes

| Mode | Behaviour |
|---|---|
| `full` | Reload the entire table every run |
| `incremental` | Only fetch rows changed since the last watermark (requires a prior sync) |
| `full_incremental` | Full load on first run, incremental on subsequent runs *(default)* |

If a watermark is older than `CHANGE_TRACKING_MIN_VALID_VERSION()`, the engine automatically falls back to a full sync.

### Output Formats

| Format | Description |
|---|---|
| `per_table` *(default)* | One Parquet file per table with original columns plus change-tracking metadata (`SYS_CHANGE_VERSION`, `SYS_CHANGE_CREATION_VERSION`, `SYS_CHANGE_OPERATION`) |
| `unified` | Lakeflow-Connect-style bronze envelope: `data` (JSON), `table_id` (struct), `cursor` (struct), `extractionTimestamp`, `operation`, `schemaVersion` |

### SCD Types

| Type | Behaviour |
|---|---|
| SCD Type 1 *(default)* | Overwrite — latest value wins |
| SCD Type 2 | Historical tracking — preserves prior versions of a row |

### Configuration

- **YAML** (recommended) and **JSON** config files.
- Environment-variable expansion (`${VAR}` syntax) and `.env` file support.
- Credential resolution order: explicit arguments → `${VAR}` expansion → environment variables → defaults.
- Legacy flat-format configs are still supported for backward compatibility.

### Other

- Snapshot isolation for point-in-time consistent reads.
- File-based advisory locking prevents concurrent syncs of the same table.
- Streaming row groups via `pyarrow.parquet.ParquetWriter` keep peak memory proportional to `row_group_size` (default 500 K rows), not total table size.
- Per-table sync history log (`sync_history.jsonl`).

---

## What Is **Not** Supported

| Limitation | Detail |
|---|---|
| **Change Data Capture (CDC)** | Only SQL Server Change Tracking is supported — not CDC, temporal tables, or transaction-log reading. |
| **Primary-key requirement** | Incremental sync requires a primary key on each table (used for the `CHANGETABLE` join). |
| **Strict incremental without prior state** | `incremental` mode raises an error if no watermark exists; use `full_incremental` for the first run. |
| **Column renames** | Schema tracking is append-only — renamed columns appear as a new column + the old column filled with nulls. |
| **Retry / back-off** | No automatic retry on transient failures; the caller is responsible for re-running. |
| **Streaming** | Processes result sets in batches, not a continuous streaming solution. |
| **Windows file locking** | File-based locking uses POSIX `fcntl.flock`; behaviour on Windows is untested. |
| **Parquet compression** | Configurable via `storage.parquet_compression` (default: `zstd`). Supports `none`, `snappy`, `gzip`, `brotli`, `lz4`, `zstd`. |
| **Monitoring / alerting** | No built-in metrics, dashboards, or alerting. |
| **Output formats other than Parquet** | The writer interface is pluggable (`OutputWriter` protocol), but only Parquet implementations ship today. |

---

## Installation

```bash
pip install -e .

# For YAML config support (recommended):
pip install -e ".[yaml]"

# For development / tests:
pip install -e ".[dev]"
```

> **Python 3.10+** is required (`mssql-python` has no wheel for earlier versions). Python 3.11 is recommended.

### Core Dependencies

| Package | Purpose |
|---|---|
| `mssql-python` | Azure SQL / SQL Server connectivity (bundles TDS — no ODBC driver needed) |
| `pyarrow` | Parquet file writing |
| `pyyaml` *(optional)* | YAML config parsing |
| `orjson` *(optional)* | Faster JSON serialization for unified output format; falls back to stdlib if absent |

---

## Configuration

### 1. Set up credentials

Copy `.env.example` to `.env` and fill in your connection details:

```bash
cp .env.example .env
```

### 2. Create a pipeline config

See `example_pipelines/pipeline_1.yaml` for a full example. The minimal structure is:

```yaml
connection:
  server: <your-server>.database.windows.net
  sql_login: <login>
  password: ${ADMIN_PASSWORD}        # resolved from environment

storage:
  ingest_pipeline: ./my_pipeline     # data, watermarks, and manifest go here
  # output_format: per_table         # (default) or "unified"
  # parquet_compression: zstd        # (default) or snappy, gzip, brotli, lz4, none
  # parquet_compression_level: 3     # optional; ZSTD 1-22, higher = smaller files
  # row_group_size: 500000           # (default) rows per Parquet row group; smaller = less memory

parallelism: 4                       # tables synced in parallel

databases:
  <database_name>:
    uc_catalog: <catalog>            # informational Unity Catalog mapping (not used by sync)
    schemas:
      dbo:
        uc_schema: <schema>          # informational Unity Catalog mapping (not used by sync)
        tables:
          orders:
            mode: full_incremental
            scd_type: 1
            soft_delete: true
          audit_log: incremental     # short-hand when no extra options needed
```

The `uc_catalog` and `uc_schema` fields are metadata for downstream Databricks Unity Catalog mapping — they are **not** consumed by the sync engine.

---

## Quick Start

### CLI

```bash
# Sync using a YAML config:
azsql-ct --config example_pipelines/pipeline_1.yaml

# Override workers and enable verbose logging:
azsql-ct --config example_pipelines/pipeline_1.yaml --workers 8 -v

# JSON config works too:
azsql-ct --config sync_config.json
```

### Python SDK

```python
from azsql_ct import ChangeTracker

# Load from a config file (YAML or JSON):
ct = ChangeTracker.from_config("example_pipelines/pipeline_1.yaml")
results = ct.sync()

# Or configure programmatically:
ct = ChangeTracker("<server>", "<login>", "<password>")
ct.tables = {
    "my_database": {
        "dbo": {
            "orders": "full_incremental",
            "customers": "full",
        }
    }
}
results = ct.sync()
```

---

## Output Structure

```
{ingest_pipeline}/
├── data/
│   └── {database}/
│       └── {schema}/
│           └── {table}/
│               └── {YYYY-MM-DD}/
│                   └── {schema}_{table}_{timestamp}_part{N}.parquet
├── watermarks/
│   └── {database}/
│       └── {schema}/
│           └── {table}/
│               ├── watermarks.json
│               ├── sync_history.jsonl
│               └── schema.json
└── output.yaml          # output manifest
```

---

## Change Tracking Permissions

If change tracking is enabled on tables in your database, the read-only user needs this one-time grant from an admin:

```sql
GRANT VIEW CHANGE TRACKING ON SCHEMA::dbo TO [<user>];
```

---

## Project Structure

```
azsql_ct/               Core package
  client.py               ChangeTracker facade + from_config()
  connection.py           AzureSQLConnection wrapper
  queries.py              SQL query builders
  sync.py                 Sync engine (full / incremental)
  watermark.py            JSON watermark store
  writer.py               Pluggable Parquet output writers
  schema.py               Append-only schema tracking
  output_manifest.py      YAML output manifest management
  _constants.py           Shared constants and defaults
  __main__.py             CLI entry point
scripts/                Runnable utility scripts
example_pipelines/      Sample YAML pipeline configs
tests/                  Unit and integration tests
```

---

## Testing

```bash
# Unit tests (no database required):
pytest

# Integration tests (requires a live Azure SQL database):
pytest -m integration -v
```

---

## License

Private.
