# azsql_ct — Azure SQL Change Tracking Sync

Incrementally sync change-tracked tables from Azure SQL Server to Parquet files, then ingest into Databricks via Lakeflow DLT pipelines. Uses [mssql-python](https://github.com/microsoft/mssql-python) — Microsoft's official driver that bundles its own TDS layer, so **no system ODBC driver is needed**.

---

## How to Use This Repo

### Step 1 — Create a pipeline config

Each SQL Server you want to sync gets its own YAML config file under `pipelines/`. Start from the template in `example_pipelines/`:

```bash
cp example_pipelines/pipeline_1.yaml pipelines/my_server.yaml
```

Open `pipelines/my_server.yaml` and fill in the placeholders:

```yaml
connection:
  server: my-server.database.windows.net
  sql_login: sync_user
  password: ${ADMIN_PASSWORD}          # resolved from .env or environment

parallelism: 4                         # tables synced in parallel

storage:
  ingest_pipeline: /Volumes/my_catalog/my_schema/my_volume/my_server
  # output_format: per_table           # (default) or "unified" for Lakeflow-Connect-style bronze
  # parquet_compression: zstd          # (default) or snappy, gzip, brotli, lz4, none
  # row_group_size: 500000             # rows per Parquet row group; smaller = less memory

databases:
  my_database:
    uc_catalog: my_catalog             # Unity Catalog name (used by downstream DLT, not sync)
    schemas:
      dbo:
        uc_schema: my_schema           # Unity Catalog schema (used by downstream DLT, not sync)
        tables:
          orders:
            mode: full_incremental
            scd_type: 1
            soft_delete: true
          customers:
            mode: full_incremental
            scd_type: 2               # SCD Type II — track historical changes
```

> The `pipelines/` directory is gitignored because configs contain real connection info. Templates in `example_pipelines/` are safe to commit.

#### Config reference

| Field | Required | Description |
|-------|----------|-------------|
| `connection.server` | Yes | SQL Server hostname |
| `connection.sql_login` | Yes | Login name |
| `connection.password` | Yes | Password (supports `${VAR}` env expansion) |
| `parallelism` | No | Tables synced in parallel (default: 1) |
| `storage.ingest_pipeline` | Yes | Root directory for data, watermarks, and output manifest |
| `storage.output_format` | No | `per_table` (default) or `unified` |
| `databases.<db>.uc_catalog` | No | Unity Catalog name for downstream DLT |
| `databases.<db>.schemas.<s>.uc_schema` | No | Unity Catalog schema for downstream DLT |
| `tables.<t>.mode` | No | `full`, `incremental`, or `full_incremental` (default) |
| `tables.<t>.scd_type` | No | `1` (default, overwrite) or `2` (historical tracking) |
| `tables.<t>.soft_delete` | No | `true` to keep deleted rows with `_is_deleted` flag |

### Step 2 — Generate DAB resources

The `dab/` directory contains a [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/index.html) with a **1:1 mapping** — one DLT pipeline + one job per pipeline config. Resource files are generated automatically; never edit them by hand.

After adding, renaming, or removing a file in `pipelines/`, regenerate the DAB resources:

```bash
python dab/generate_jobs.py
```

This reads every `pipelines/*.yaml` / `*.yml` and writes two files per config:

| Generated file | Purpose |
|----------------|---------|
| `dab/resources/pipelines/sdp_<name>.yml` | DLT pipeline resource |
| `dab/resources/jobs/job_<name>.yml` | Job resource that runs sync then triggers the DLT pipeline |

Stale resource files (whose pipeline config no longer exists) are removed automatically.

Useful flags:

```bash
python dab/generate_jobs.py --dry-run      # preview without writing
python dab/generate_jobs.py --no-clean     # keep stale files
```

#### What gets generated

For a config file `pipelines/my_server.yaml`, the generator creates:

- **`dab/resources/pipelines/sdp_my_server.yml`** — a DLT pipeline that runs the Lakeflow ingestion code against the sync output.
- **`dab/resources/jobs/job_my_server.yml`** — a multi-task job with this flow:

```
gateway (sync.py)
  ├── check_record_changed ──→ ingestion (DLT pipeline)
  └── schema_change_detected
```

The `gateway` task runs `scripts/sync.py` with your pipeline config, sets task values (`total_rows_changed`, `schema_changes_detected`), and downstream tasks branch on those values.

### Step 3 — Configure the bundle variables

Open `dab/databricks.yml` and set the variables for your environment:

| Variable | Description |
|----------|-------------|
| `workspace_root` | Full workspace path to the repo (e.g. `/Workspace/Repos/my-org/sql_server_permissions`) |
| `catalog` | Unity Catalog name for DLT pipelines |
| `manifest_file` | Manifest filename (default: `incremental_output.yaml`) |

You can set `workspace_root` per target or via CLI (see Step 4).

### Step 4 — Validate and deploy

From inside the `dab/` directory:

```bash
cd dab

# Validate the bundle
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev \
  --var workspace_root=/Workspace/Repos/my-org/sql_server_permissions

# Deploy to prod
databricks bundle deploy -t prod \
  --var workspace_root=/Workspace/Repos/my-org/sql_server_permissions
```

### End-to-end example

Add a second SQL Server pipeline from scratch:

```bash
# 1. Create the config from the template
cp example_pipelines/pipeline_1.yaml pipelines/finance_db.yaml
# Edit pipelines/finance_db.yaml with your connection and table details

# 2. Generate DAB resources
python dab/generate_jobs.py
# Output:
#   Wrote dab/resources/pipelines/sdp_finance_db.yml
#   Wrote dab/resources/jobs/job_finance_db.yml

# 3. Deploy
cd dab
databricks bundle deploy -t dev \
  --var workspace_root=/Workspace/Repos/my-org/sql_server_permissions
```

---

## Running Sync Locally

You can run the sync outside of Databricks for testing. Requires **Python 3.10+** (3.11 recommended).

```bash
# Install dependencies
python3.11 -m pip install mssql-python pyarrow pyyaml

# Run sync (set PYTHONPATH so the local azsql_ct package is found)
PYTHONPATH=$(pwd) python3.11 scripts/sync.py pipelines/my_server.yaml
```

Or use the CLI entry point:

```bash
pip install -e .
azsql-ct --config pipelines/my_server.yaml
```

---

## Prerequisites — SQL Server

Change tracking must be enabled at both the **database** and **table** level. Run these as a database admin:

```sql
-- 1. Enable change tracking on the database (once per database)
ALTER DATABASE [my_database]
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

-- 2. Enable change tracking on each table
ALTER TABLE dbo.orders ENABLE CHANGE_TRACKING;
ALTER TABLE dbo.customers ENABLE CHANGE_TRACKING;

-- 3. Grant permissions to the sync user
GRANT VIEW CHANGE TRACKING ON SCHEMA::dbo TO [sync_user];
```

---

## Sync Modes

| Mode | Behaviour |
|------|-----------|
| `full` | Reload the entire table every run |
| `incremental` | Only fetch rows changed since the last watermark (requires a prior sync) |
| `full_incremental` | Full load on first run, incremental on subsequent runs *(default)* |

If a watermark is older than `CHANGE_TRACKING_MIN_VALID_VERSION()`, the engine automatically falls back to a full sync.

## Output Formats

| Format | Description |
|--------|-------------|
| `per_table` *(default)* | One Parquet file per table with original columns plus CT metadata |
| `unified` | Lakeflow-Connect-style bronze envelope: `data` (JSON), `table_id`, `cursor`, `extractionTimestamp`, `operation`, `schemaVersion` |

## SCD Types

| Type | Behaviour |
|------|-----------|
| SCD Type 1 *(default)* | Overwrite — latest value wins |
| SCD Type 2 | Historical tracking — preserves prior versions of a row |

---

## Project Structure

```
example_pipelines/      Template YAML configs (safe to commit)
pipelines/              Your pipeline configs (gitignored)
azsql_ct/               Core sync package
scripts/                Runnable utilities (sync.py, connect.py, parse_output.py)
lakeflow_pipeline/      Databricks DLT ingestion code
dab/                    Databricks Asset Bundle
  databricks.yml          Bundle definition, targets, variables
  generate_jobs.py        Generator: pipelines/ → DAB resources
  resources/
    pipelines/            Generated DLT pipeline resources
    jobs/                 Generated job resources
tests/                  Unit and integration tests
```

---

## Output Structure

```
{ingest_pipeline}/
├── data/
│   └── {database}/{schema}/{table}/{YYYY-MM-DD}/
│       └── {schema}_{table}_{timestamp}_part{N}.parquet
├── watermarks/
│   └── {database}/{schema}/{table}/
│       ├── watermarks.json
│       ├── sync_history.jsonl
│       └── schema.json
└── output.yaml          # output manifest for downstream DLT
```

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `ModuleNotFoundError: No module named 'mssql_python'` | Install with Python 3.10+: `python3.11 -m pip install mssql-python` |
| `ChangeTracker has no attribute 'from_config'` | Run with `PYTHONPATH` set to project root |
| `FileNotFoundError: pipelines/pipeline_1.yaml` | Create config from template or pass explicit path |
| `Table is not change-tracked` | Enable change tracking at both database and table level (see Prerequisites above) |

---

## License

Private.
