# ingestion_pipeline_examples — Databricks DLT Downstream Pipelines

Databricks Lakeflow Declarative Pipelines (DLT) that consume Parquet output from `azsql_ct` and materialize bronze + silver Delta tables with auto CDC.

> **Prerequisite**: Run `azsql_ct` sync with `output_format: unified` so Parquet files use the bronze envelope schema. See root `README.md` and `scripts/README.md`.

---

## Files

| File | Purpose |
|------|---------|
| `ingestion_pipeline_materialized.py` | Main DLT pipeline: bronze materialized temp table → per-table silver streaming tables with `create_auto_cdc_flow` |
| `metadata_helper.py` | Reads pipeline YAML → `output.yaml` → per-table `schema.json`; returns `(table_configs, data_path)` for downstream use |

---

## Architecture

```
Cloud Storage (Parquet from azsql_ct)
  └─ landing_raw  [Bronze — temporary materialized Delta table]
       └─ _view_{catalog}_{schema}_{table}  [per-table filter + from_json]
            └─ {catalog}.{schema}.{table}   [Silver — streaming Delta with auto CDC]
```

- **Bronze** (`landing_raw`): `@dp.table(temporary=True)` — materialized as Delta but not published to Unity Catalog. Reads all Parquet under `data_path` via Auto Loader (`cloudFiles`). Materializing once avoids re-reading cloud storage N times for N tables.
- **Silver**: Per-table streaming tables generated from `table_configs`. Each table gets a view (filter by `table_id.uoid`, `from_json` on `data`) and a streaming table with `create_auto_cdc_flow` for SCD Type 1 or 2.

---

## Config Flow

```
pipeline YAML (spark conf `input_yaml`)
  → storage.ingest_pipeline path
    → {ingest_pipeline}/output.yaml    (which tables, UC names, PKs, SCD types)
    → {ingest_pipeline}/watermarks/{db}/{schema}/{table}/schema.json  (column definitions)
    → {ingest_pipeline}/data/          (Parquet files — Auto Loader source)
```

`parse_output_yaml()` walks the nested `output.yaml` structure and joins each table with its `schema.json` columns.

---

## Unified Bronze Envelope Schema (Parquet)

The Parquet files from `azsql_ct` (unified format) use:

| Column | Type | Description |
|--------|------|-------------|
| `data` | `string` | JSON-serialized row (all non-CT columns) |
| `table_id` | `struct{catalog, schema, name, uoid}` | Source table identifier; `uoid` is a deterministic UUID5 |
| `cursor` | `struct{lsn, seqNum, sequence, timestamp}` | Change tracking position; `seqNum` = `SYS_CHANGE_VERSION` |
| `extractionTimestamp` | `int64` | Epoch milliseconds when sync ran |
| `operation` | `string` | `INSERT`, `UPDATE`, `DELETE`, or `LOAD` (full sync) |
| `schemaVersion` | `int64` | Deterministic hash of source column names/types |

---

## Running the Pipeline

1. Run `azsql_ct` sync to produce Parquet under `{ingest_pipeline}/data/`.
2. Set Spark config `input_yaml` to the path of your pipeline YAML (the same config used by sync, or one that points to the same `storage.ingest_pipeline`).
3. Run the DLT pipeline in Databricks (or compatible Spark environment).

---

## MSSQL-to-Spark Type Mapping

`MSSQL_TO_SPARK` in `ingestion_pipeline_materialized.py` maps SQL Server type names (from `schema.json`) to PySpark types for `from_json` parsing. Unknown types fall back to `StringType()`.

---

## Related

- `.cursor/rules/ingestion-pipeline-examples.mdc` — Detailed agent-oriented guide (UOID, soft-delete, etc.)
- `azsql_ct/writer.py` — `UnifiedParquetWriter` produces the bronze envelope
- `scripts/parse_output.py` — Alternative parser for `output.yaml` (flat table list)
