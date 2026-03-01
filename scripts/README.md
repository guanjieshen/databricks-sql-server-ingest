# scripts — Runnable Utilities for azsql_ct

This directory contains executable scripts that use the `azsql_ct` package for Azure SQL Change Tracking sync. Use this README to navigate the scripts and understand how they fit into the azsql_ct workflow.

> **For deeper azsql_ct package navigation** (module map, data flow, key types), see `.cursor/rules/azsql-ct-package.mdc`. For Python version and run instructions, see `.cursor/rules/mssql-and-run.mdc`.

---

## Quick Reference

| Script | Purpose |
|--------|---------|
| `sync.py` | Run incremental sync from a YAML pipeline config |
| `connect.py` | Verify connectivity to the Azure SQL server |
| `parse_output.py` | Parse `output.yaml` manifest into a flat list of table records |

---

## Prerequisites

- **Python 3.10+** (3.11 recommended) — `mssql-python` has no wheel for earlier versions
- **Dependencies**: `mssql-python`, `pyarrow`, `pyyaml`
- **PYTHONPATH**: Set to project root so the local `azsql_ct` package is used

```bash
# From project root
cd /path/to/sql_server_permissions
PYTHONPATH=$(pwd) python3.11 -m pip install mssql-python pyarrow pyyaml
```

---

## Scripts in Detail

### sync.py

**Purpose**: Sync change-tracked tables from Azure SQL to local Parquet files using a YAML pipeline config.

**Usage**:
```bash
PYTHONPATH=/path/to/sql_server_permissions python3.11 scripts/sync.py [config]
```

- `config`: Path to YAML (or JSON) pipeline config. Default: `pipelines/pipeline_1.yaml`
- Loads config via `ChangeTracker.from_config()`, runs `ct.sync()`, prints per-table status and row counts

**Typical flow**:
1. Reads `connection`, `storage`, `parallelism`, `databases` from config
2. Connects per database, syncs tables in parallel
3. Writes Parquet under `{ingest_pipeline}/data/{database}/{schema}/{table}/{date}/`
4. Updates watermarks and `output.yaml` manifest

**Config locations**:
- `pipelines/pipeline_1.yaml` — project-specific pipeline (often gitignored)
- `example_pipelines/pipeline_1.yaml` — template with placeholders

**Databricks Jobs integration**: When run as a Databricks job task, the script sets `total_rows_changed` as a task value (sum of `rows_written` across all non-error results). Downstream tasks can reference it for conditional branching, e.g. in an If/else condition:

```
{{tasks.<sync_task_name>.values.total_rows_changed}} == 0
```

Replace `<sync_task_name>` with your sync task's key (e.g. `Gateway`). When run locally or when `dbutils` is unavailable, the helper is a no-op and the script completes normally.

---

### connect.py

**Purpose**: Test connectivity to the Azure SQL server defined in the pipeline config.

**Usage**:
```bash
PYTHONPATH=/path/to/sql_server_permissions python3.11 scripts/connect.py
```

- Uses `pipelines/pipeline_1.yaml` by default (hardcoded)
- Calls `ChangeTracker.from_config(...).test_connectivity()`
- Exits with code 1 on failure

---

### parse_output.py

**Purpose**: Parse the `output.yaml` manifest (produced by sync) into a flat list of table metadata dicts.

**Usage** (as a library):
```python
from scripts.parse_output import parse_yaml_to_table_dict

tables = parse_yaml_to_table_dict("pipeline_1/output.yaml")
# Returns: [{"database_name", "schema_name", "table_name", "file_path", "file_type", ...}, ...]
```

**Note**: This script does not have a CLI `__main__` block; it is intended for import. The ingestion pipeline examples use `metadata_helper.parse_output_yaml()` instead, which also loads `schema.json` and returns richer table configs for Databricks.

**Output structure expected**:
```yaml
databases:
  <database_name>:
    uc_catalog_name: ...
    <schema_name>:
      uc_schema_name: ...
      <table_name>:
        file_path: ...
        file_type: ...
        primary_key: [...]
```

---

## Relationship to azsql_ct

```
scripts/
├── sync.py          → ChangeTracker.from_config() → ct.sync()
├── connect.py       → ChangeTracker.from_config() → ct.test_connectivity()
└── parse_output.py  → Standalone parser for output.yaml (no azsql_ct import)
```

**azsql_ct package layout** (see `.cursor/rules/azsql-ct-package.mdc` for full map):
- `client.py` — `ChangeTracker` facade, config loading
- `sync.py` — Core sync engine
- `queries.py` — SQL builders
- `writer.py` — Parquet writers (per_table / unified)
- `output_manifest.py` — `output.yaml` management

---

## Downstream Integration

- **`ingestion_pipeline_examples/`** — Databricks bronze/silver patterns that consume `output.yaml` and `schema.json`
- **`metadata_helper.py`** — `parse_output_yaml()` for pipeline config + output manifest + schema.json

---

## Common Errors

| Error | Fix |
|-------|-----|
| `ModuleNotFoundError: No module named 'mssql_python'` | Install with Python 3.10+: `python3.11 -m pip install mssql-python` |
| `ChangeTracker has no attribute 'from_config'` | Run with `PYTHONPATH` set to project root (use local package, not installed copy) |
| `FileNotFoundError: pipelines/pipeline_1.yaml` | Create config or pass explicit path: `scripts/sync.py example_pipelines/pipeline_1.yaml` |
| `Table 'dbo.table_X' is not change-tracked in database_Y. Tracked tables: []` | Enable change tracking at the database and table level (see below) |

---

## Fix: Table is not change-tracked

If you see:

```
Failed to sync database_1.dbo.table_1: Table 'dbo.table_1' is not change-tracked in database_1. Tracked tables: []
```

Change tracking must be enabled at both the **database** and **table** level. Run the following as a database admin (e.g. in Azure Data Studio or SSMS):

**1. Enable change tracking on the database** (once per database):

```sql
ALTER DATABASE [database_1]
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
```

**2. Enable change tracking on each table** (once per table):

```sql
ALTER TABLE dbo.table_1 ENABLE CHANGE_TRACKING;
ALTER TABLE dbo.table_59 ENABLE CHANGE_TRACKING;
```

**3. Grant permissions** (if using a read-only user for sync):

```sql
GRANT VIEW CHANGE TRACKING ON SCHEMA::dbo TO [<your_sync_user>];
```

Then re-run the sync. See [Enable and Disable Change Tracking (SQL Server)](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-tracking-sql-server) for details.
