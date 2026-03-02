#!/usr/bin/env python3
"""Generate DAB resources from pipeline configs in pipelines/.

Discover pipelines/*.yaml and pipelines/*.yml; for each config emit two files
under dab/resources/:
  resources/pipelines/sdp_<base>.yml  — DLT ingestion pipeline resource
  resources/jobs/job_<base>.yml       — job resource referencing that pipeline
Optionally remove stale files whose pipeline config no longer exists.

Run from repo root: python dab/generate_jobs.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    _SCRIPT_PATH: Path | None = Path(__file__)
except NameError:
    _SCRIPT_PATH = None


def _repo_root(start: Path | None = None) -> Path:
    """Infer repo root.

    When *start* is a file path (e.g. ``__file__`` inside ``dab/``), the repo
    root is its grandparent.  When running inside a Databricks notebook where
    ``__file__`` is unavailable, walk up from *cwd* looking for the ``dab/``
    directory as a landmark.
    """
    if start is not None:
        return start.resolve().parent.parent
    cwd = Path.cwd().resolve()
    for parent in (cwd, *cwd.parents):
        if (parent / "dab").is_dir():
            return parent
    return cwd


def _discover_pipeline_configs(pipelines_dir: Path) -> list[tuple[str, str]]:
    """Discover pipeline YAML configs; return list of (base_name, filename)."""
    if not pipelines_dir.is_dir():
        return []
    result: list[tuple[str, str]] = []
    for ext in ("*.yaml", "*.yml"):
        for path in sorted(pipelines_dir.glob(ext)):
            base = path.stem
            if base.startswith("_") or base.upper() == "README":
                continue
            result.append((base, path.name))
    seen: set[str] = set()
    unique: list[tuple[str, str]] = []
    for base, filename in result:
        if base not in seen:
            seen.add(base)
            unique.append((base, filename))
    return unique


def _job_key(base: str) -> str:
    safe = base.replace("-", "_")
    return f"sql_server_ct_{safe}"


def _pipeline_key(base: str) -> str:
    safe = base.replace("-", "_")
    return f"sdp_{safe}"


def _pipeline_resource(base_name: str, config_filename: str) -> dict:
    """Build the DLT pipeline resource dict wrapped in resources.pipelines."""
    pk = _pipeline_key(base_name)
    ws = "${var.workspace_root}"
    return {
        "resources": {
            "pipelines": {
                pk: {
                    "name": f"SQL Server CT – SDP – {base_name}",
                    "tags": {
                        "project": "sql-server-ct",
                        "pipeline_config": config_filename,
                        "generated_by": "dab/generate_jobs.py",
                    },
                    "configuration": {
                        "input_yaml": f"{ws}/pipelines/{config_filename}",
                        "manifest_file": "${var.manifest_file}",
                    },
                    "libraries": [
                        {"notebook": {"path": f"{ws}/lakeflow_pipeline/ingestion_pipeline_materialized"}},
                        {"notebook": {"path": f"{ws}/lakeflow_pipeline/metadata_helper.py"}},
                    ],
                    "schema": "default",
                    "photon": True,
                    "catalog": "${var.catalog}",
                    "serverless": True,
                    "root_path": f"{ws}/lakeflow_pipeline",
                }
            }
        }
    }


def _job_resource(base_name: str, config_filename: str) -> dict:
    """Build the job resource dict wrapped in resources.jobs."""
    jk = _job_key(base_name)
    pk = _pipeline_key(base_name)
    config_path = f"${{var.workspace_root}}/pipelines/{config_filename}"
    pipeline_ref = f"${{resources.pipelines.{pk}.id}}"
    return {
        "resources": {
            "jobs": {
                jk: {
                    "name": f"SQL Server CT – Job – {base_name}",
                    "tags": {
                        "project": "sql-server-ct",
                        "pipeline_config": config_filename,
                        "generated_by": "dab/generate_jobs.py",
                    },
                    "tasks": [
                        {
                            "task_key": "gateway",
                            "spark_python_task": {
                                "python_file": "${var.workspace_root}/scripts/sync.py",
                                "parameters": [config_path],
                            },
                            "environment_key": "Task_environment",
                        },
                        {
                            "task_key": "check_record_changed",
                            "depends_on": [{"task_key": "gateway"}],
                            "condition_task": {
                                "op": "GREATER_THAN",
                                "left": "{{tasks.gateway.values.total_rows_changed}}",
                                "right": "0",
                            },
                        },
                        {
                            "task_key": "schema_change_detected",
                            "depends_on": [{"task_key": "gateway"}],
                            "condition_task": {
                                "op": "EQUAL_TO",
                                "left": "{{tasks.gateway.values.schema_changes_detected}}",
                                "right": "true",
                            },
                        },
                        {
                            "task_key": "ingestion",
                            "depends_on": [
                                {"task_key": "check_record_changed", "outcome": "true"}
                            ],
                            "pipeline_task": {
                                "pipeline_id": pipeline_ref
                            },
                        },
                    ],
                    "queue": {"enabled": True},
                    "environments": [
                        {
                            "environment_key": "Task_environment",
                            "spec": {
                                "dependencies": ["mssql-python"],
                                "environment_version": "4",
                            },
                        }
                    ],
                    "performance_target": "PERFORMANCE_OPTIMIZED",
                }
            }
        }
    }


def _emit_yaml(data: dict) -> str:
    """Emit YAML string from dict. Requires PyYAML."""
    try:
        import yaml
    except ImportError as e:
        raise SystemExit(
            "PyYAML is required to run generate_jobs.py. "
            "Install with: pip install pyyaml"
        ) from e
    return yaml.dump(
        data,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        width=1000,
    )


def _write_file(path: Path, content: str, header: str, dry_run: bool) -> None:
    full_content = header + content
    if dry_run:
        print(f"Would write {path} ({len(full_content)} bytes)")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(full_content, encoding="utf-8")
    print(f"Wrote {path}")


def _stale_files(directory: Path, prefix: str, valid_bases: set[str]) -> list[Path]:
    """Return list of <prefix>_*.yml paths whose base is not in valid_bases."""
    stale: list[Path] = []
    if not directory.is_dir():
        return stale
    for path in directory.glob(f"{prefix}_*.yml"):
        base = path.stem.replace(f"{prefix}_", "", 1)
        if base not in valid_bases:
            stale.append(path)
    return stale


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate DAB pipeline + job resources from pipelines/ (1:1)."
    )
    repo = _repo_root(_SCRIPT_PATH)
    parser.add_argument(
        "--pipelines-dir",
        type=Path,
        default=repo / "pipelines",
        help="Directory containing pipeline YAML configs (default: repo/pipelines)",
    )
    parser.add_argument(
        "--resources-dir",
        type=Path,
        default=repo / "dab" / "resources",
        help="Root resources directory (default: repo/dab/resources)",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Do not remove stale generated files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written/removed without making changes",
    )
    args = parser.parse_args([] if _SCRIPT_PATH is None else None)

    pipelines_dir = args.pipelines_dir.resolve()
    resources_dir = args.resources_dir.resolve()
    pipelines_out = resources_dir / "pipelines"
    jobs_out = resources_dir / "jobs"

    configs = _discover_pipeline_configs(pipelines_dir)
    if not configs:
        print("No pipeline configs found in", pipelines_dir, file=sys.stderr)
        return 0

    header_tpl = "# Generated by dab/generate_jobs.py — do not edit by hand.\n\n"

    valid_bases: set[str] = set()
    for base_name, config_filename in configs:
        valid_bases.add(base_name)

        pl_data = _pipeline_resource(base_name, config_filename)
        _write_file(
            pipelines_out / f"sdp_{base_name}.yml",
            _emit_yaml(pl_data),
            header_tpl,
            args.dry_run,
        )

        job_data = _job_resource(base_name, config_filename)
        _write_file(
            jobs_out / f"job_{base_name}.yml",
            _emit_yaml(job_data),
            header_tpl,
            args.dry_run,
        )

    if not args.no_clean:
        for path in _stale_files(pipelines_out, "sdp", valid_bases):
            if args.dry_run:
                print(f"Would remove {path}")
            else:
                path.unlink()
                print(f"Removed {path}")
        for path in _stale_files(jobs_out, "job", valid_bases):
            if args.dry_run:
                print(f"Would remove {path}")
            else:
                path.unlink()
                print(f"Removed {path}")

    return 0


if __name__ == "__main__":
    _rc = main()
    if _SCRIPT_PATH is not None:
        raise SystemExit(_rc)
