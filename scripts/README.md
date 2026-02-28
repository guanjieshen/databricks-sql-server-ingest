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
