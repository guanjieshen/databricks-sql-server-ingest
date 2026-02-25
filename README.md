# azsql_ct -- Azure SQL Change Tracking Sync

Incrementally sync change-tracked tables from Azure SQL Server to local CSV files.

Supports three sync modes per table:

| Mode | Behaviour |
|---|---|
| `full` | Reload the entire table every run |
| `incremental` | Only fetch rows changed since the last watermark (requires a prior sync) |
| `full_incremental` | Full load on first run, incremental on subsequent runs *(default)* |

## Installation

```bash
pip install -e .

# For YAML config support (used by examples):
pip install -e ".[yaml]"

# For development / tests:
pip install -e ".[dev]"
```

Uses [mssql-python](https://github.com/microsoft/mssql-python) -- Microsoft's official driver that bundles its own TDS layer, so no system ODBC driver is needed.

## Configuration

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

```
SERVER=your-server.database.windows.net
ADMIN_USER=sqladmin
ADMIN_PASSWORD=<your-password>
TEST_USER=test
TEST_PASSWORD=<your-password>
```

The library resolves configuration in order: **explicit arguments > environment variables > defaults**.

## Quick Start

### Verify connectivity

```bash
python examples/connect.py
```

### SDK usage

```python
from azsql_ct import ChangeTracker

ct = ChangeTracker("your-server.database.windows.net", "sqladmin", "secret")

# Configure tables -- list format (all default to full_incremental):
ct.tables = {"my_database": {"dbo": ["orders", "customers"]}}

# Or dict format with per-table modes:
ct.tables = {
    "my_database": {
        "dbo": {
            "orders": "full_incremental",
            "customers": "full",
            "audit_log": "incremental",
        }
    }
}

results = ct.sync()
```

### Parallel sync

```python
ct = ChangeTracker("server", "user", "pw", max_workers=8)
ct.tables = {"db": {"dbo": ["t1", "t2", "t3"]}}
ct.sync()
```

### CLI

```bash
python -m azsql_ct --config sync_config.json
python -m azsql_ct --config sync_config.json --output-dir ./data --watermark-dir ./watermarks -v
```

See `sync_config.json` for the config format.

## Change Tracking Permissions

If change tracking is enabled on tables in your database, the read-only user needs permission granted once by an admin:

```sql
-- Run in the target database as admin
GRANT VIEW CHANGE TRACKING ON SCHEMA::dbo TO <user>;
```

## Project Structure

```
azsql_ct/           Core package
  client.py           ChangeTracker SDK facade
  connection.py       AzureSQLConnection wrapper
  queries.py          SQL query builders
  sync.py             Sync engine (full / incremental)
  watermark.py        JSON watermark store
  writer.py           Pluggable CSV output writer
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
