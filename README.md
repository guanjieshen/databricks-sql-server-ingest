# azsql_ct -- Azure SQL Change Tracking Sync

Incrementally sync change-tracked tables from Azure SQL Server to local Parquet files.

Supports three sync modes per table:

| Mode | Behaviour |
|---|---|
| `full` | Reload the entire table every run |
| `incremental` | Only fetch rows changed since the last watermark (requires a prior sync) |
| `full_incremental` | Full load on first run, incremental on subsequent runs *(default)* |

## Installation

```bash
pip install -e .

# For YAML config support (recommended):
pip install -e ".[yaml]"

# For development / tests:
pip install -e ".[dev]"
```

Uses [mssql-python](https://github.com/microsoft/mssql-python) -- Microsoft's official driver that bundles its own TDS layer, so no system ODBC driver is needed.

## Configuration

### 1. Set up credentials

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

```
SERVER=your-server.database.windows.net
ADMIN_USER=sqladmin
ADMIN_PASSWORD=<your-password>
```

Credentials are resolved in order: **explicit arguments > `${VAR}` expansion > environment variables > defaults**.

### 2. Create a config file

**YAML (recommended):** see `examples/tables.yaml`

```yaml
connection:
  server: your-server.database.windows.net
  sql_login: sqladmin
  password: ${ADMIN_PASSWORD}

storage:
  ingest_pipeline: ./ingest_pipeline   # data, watermarks, and output manifest go under this dir; or set data_dir, watermark_dir, output_manifest explicitly to override

# Tables to sync in parallel (optional; also supports key "max_workers")
parallelism: 8

databases:
  my_database:
    dbo:
      orders: full_incremental
      customers: full
      audit_log: incremental
```

When using `ingest_pipeline`, the manifest is written to `{ingest_pipeline}/output.yaml`. When `output_manifest` is set under `storage` (or derived from `ingest_pipeline`), sync updates that YAML file with the path and type of each table's output. Fields `uc_catalog_name`, `uc_schema_name`, and `uc_table_name` are left blank for you to fill (e.g. for Unity Catalog). The manifest is updated only when new databases, schemas, or tables are added; existing entries are never overwritten.

**JSON** is also supported (see `sync_config.json`).

## Quick Start

### CLI

```bash
# Sync using a YAML config:
azsql-ct --config examples/tables.yaml

# Override workers and enable verbose logging:
azsql-ct --config examples/tables.yaml --workers 8 -v

# JSON config works too:
azsql-ct --config sync_config.json
```

### SDK

```python
from azsql_ct import ChangeTracker

# Load from a config file (YAML or JSON):
ct = ChangeTracker.from_config("examples/tables.yaml")
results = ct.sync()

# Or configure programmatically:
ct = ChangeTracker("your-server.database.windows.net", "sqladmin", "secret")
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

### Verify connectivity

```bash
python examples/connect.py
```

## Change Tracking Permissions

If change tracking is enabled on tables in your database, the read-only user needs permission granted once by an admin:

```sql
-- Run in the target database as admin
GRANT VIEW CHANGE TRACKING ON SCHEMA::dbo TO <user>;
```

## Project Structure

```
azsql_ct/           Core package
  client.py           ChangeTracker SDK facade + from_config()
  connection.py       AzureSQLConnection wrapper
  queries.py          SQL query builders
  sync.py             Sync engine (full / incremental)
  watermark.py        JSON watermark store
  writer.py           Pluggable Parquet output writer
  __main__.py         CLI entry point
examples/           Runnable example scripts
tests/              Unit and integration tests
```

## Testing

```bash
# Unit tests (no database required):
pytest

# Integration tests (requires a live Azure SQL database):
pytest -m integration -v
```

## License

Private.
