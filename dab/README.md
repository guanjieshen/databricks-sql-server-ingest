# Databricks Asset Bundle (DAB) – SQL Server CT

This directory defines a Databricks Asset Bundle with **1:1 mapping**: one job per pipeline config under `pipelines/`. All jobs share a single DLT ingestion pipeline.

## Workspace path

Set **`workspace_root`** to the workspace path where this repo lives so job tasks and the pipeline resolve scripts and configs:

- Job gateway task: `scripts/sync.py`, `pipelines/pipeline_1.yaml`
- Pipeline: `ingestion_pipeline/` code and `root_path`

Examples:

- **Repos**: `/Workspace/Repos/<folder>/<repo_name>` (or your Repos path)
- **Synced workspace**: whatever path you use with bundle `sync` or manual upload

Override options:

- **Per target** in `databricks.yml`: under `targets.<name>.variables.workspace_root`
- **CLI**: `databricks bundle deploy -t dev --var workspace_root=/Workspace/Repos/...`

## Validate and deploy

From the **repo root** (parent of `dab/`):

```bash
cd dab
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Or with workspace_root set inline:

```bash
databricks bundle deploy -t dev --var workspace_root=/Workspace/Repos/my-org/sql_server_permissions
```

## Generating job resources from pipelines

Job files under `resources/job_*.yml` are **generated** from the pipeline configs in `pipelines/` so the bundle stays in sync (1:1). After adding or removing a file in `pipelines/`, run from the **repo root**:

```bash
python dab/generate_jobs.py
```

This discovers all `pipelines/*.yaml` and `pipelines/*.yml`, writes or overwrites `dab/resources/job_<base>.yml` for each, and removes any `job_*.yml` whose pipeline config no longer exists. Options: `--dry-run` (print only), `--no-clean` (do not remove stale job files), `--pipelines-dir`, `--resources-dir`.

You can run the generator in CI before `databricks bundle deploy` so the bundle always reflects `pipelines/`. Requires PyYAML (`pip install pyyaml`).

## Layout

- `databricks.yml` – bundle name, targets, `workspace_root` variable, `include` for resources
- `resources/ingestion_pipeline.yml` – shared DLT pipeline
- `resources/job_*.yml` – generated jobs (one per `pipelines/*.yaml`); do not edit by hand

To add another pipeline: add `pipelines/pipeline_2.yaml`, then run `python dab/generate_jobs.py` to create `resources/job_pipeline_2.yml`.
