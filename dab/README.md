# Databricks Asset Bundle (DAB) – SQL Server CT

This directory defines a Databricks Asset Bundle with **1:1 mapping**: one DLT pipeline + one job per pipeline config under `pipelines/`.

## Workspace path

Workspace paths in generated resources use the DAB built-in `${workspace.root_path}`, which resolves automatically to the bundle's sync destination at deploy time. No manual configuration is needed.

## Validate and deploy

From the **repo root** (parent of `dab/`):

```bash
cd dab
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## Generating job resources from pipelines

Job files under `resources/job_*.yml` are **generated** from the pipeline configs in `pipelines/` so the bundle stays in sync (1:1). After adding or removing a file in `pipelines/`, run from the **repo root**:

```bash
python dab/generate_jobs.py
```

This discovers all `pipelines/*.yaml` and `pipelines/*.yml` and writes two files per config:

- `dab/resources/pipelines/sdp_<base>.yml` — DLT pipeline resource
- `dab/resources/jobs/job_<base>.yml` — job resource referencing that pipeline

Stale files whose pipeline config no longer exists are removed automatically. Options: `--dry-run` (print only), `--no-clean` (do not remove stale files), `--pipelines-dir`, `--resources-dir`.

You can run the generator in CI before `databricks bundle deploy` so the bundle always reflects `pipelines/`. Requires PyYAML (`pip install pyyaml`).

## Layout

- `databricks.yml` – bundle name, targets, variables, `include` for resources
- `resources/pipelines/sdp_*.yml` – generated DLT pipeline resources; do not edit by hand
- `resources/jobs/job_*.yml` – generated job resources; do not edit by hand

To add another pipeline: add `pipelines/pipeline_2.yaml`, then run `python dab/generate_jobs.py` to create `resources/pipelines/sdp_pipeline_2.yml` and `resources/jobs/job_pipeline_2.yml`.
