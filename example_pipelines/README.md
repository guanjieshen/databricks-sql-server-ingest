# example_pipelines — Template Pipeline Configs

Sample YAML pipeline configurations for `azsql_ct`. Use these as templates — copy and customize for your environment.

---

## Contents

| File | Description |
|------|-------------|
| `pipeline_1.yaml` | Template with placeholder values (`INSERT_SERVERNAME_HERE`, etc.) |

---

## Usage

1. Copy to your project (e.g. `pipelines/pipeline_1.yaml`).
2. Replace placeholders:
   - `connection.server`, `connection.sql_login`, `connection.password`
   - `storage.ingest_pipeline` — directory for data, watermarks, and `output.yaml`
   - `databases.<db>.uc_catalog`, `schemas.<schema>.uc_schema` — Unity Catalog names (informational; not used by sync)
3. Adjust tables, modes, SCD types, and `soft_delete` as needed.
4. Run sync: `python scripts/sync.py pipelines/pipeline_1.yaml` (or `azsql-ct --config ...`).

---

## Config Structure

```yaml
connection:
  server: <server>.database.windows.net
  sql_login: <login>
  password: ${ADMIN_PASSWORD}   # env var expansion supported

parallelism: 4

storage:
  ingest_pipeline: ./my_pipeline
  output_format: per_table   # or "unified"

databases:
  <database_name>:
    uc_catalog: <catalog>
    schemas:
      dbo:
        uc_schema: <schema>
        tables:
          <table_name>:
            mode: full_incremental
            scd_type: 1
            soft_delete: true
```

---

## Difference from `pipelines/`

- **`example_pipelines/`** — Version-controlled templates with placeholders. Safe to commit.
- **`pipelines/`** — Project-specific configs with real credentials. Often gitignored.

---

## Related

- Root `README.md` — Configuration section
- `scripts/README.md` — How to run sync
- `pipelines/README.md` — Project-specific configs
