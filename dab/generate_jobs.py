#!/usr/bin/env python3
"""Generate DAB job_*.yml files from pipeline configs in pipelines/.

Discover pipelines/*.yaml and pipelines/*.yml, emit dab/resources/job_<base>.yml
for each (1:1). Optionally remove job files whose pipeline config no longer exists.

Run from repo root: python dab/generate_jobs.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _repo_root(script_path: Path) -> Path:
    """Infer repo root as parent of the directory containing this script (dab/)."""
    return script_path.resolve().parent.parent


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
    # Deduplicate by base (first extension wins)
    seen: set[str] = set()
    unique: list[tuple[str, str]] = []
    for base, filename in result:
        if base not in seen:
            seen.add(base)
            unique.append((base, filename))
    return unique


def _job_key(base: str) -> str:
    """Derive job key from pipeline base name (e.g. pipeline_1 -> sql_server_ct_pipeline_1)."""
    safe = base.replace("-", "_")
    return f"sql_server_ct_{safe}"


def _job_template(base_name: str, config_filename: str) -> dict:
    """Build the job resource dict matching job_pipeline_1.yml structure."""
    job_key = _job_key(base_name)
    config_path = f"${{var.workspace_root}}/pipelines/{config_filename}"
    return {
        "resources": {
            "jobs": {
                job_key: {
                    "name": f"SQL Server CT – {base_name}",
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
                                "pipeline_id": "${resources.pipelines.ingestion_pipeline.id}"
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
    """Emit YAML string from dict. Requires PyYAML (used elsewhere in repo)."""
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


def _write_job_file(resources_dir: Path, base_name: str, content: str, dry_run: bool) -> None:
    out_path = resources_dir / f"job_{base_name}.yml"
    header = f"""# Job for pipelines/{base_name}.yaml (1:1 mapping).
# References shared ingestion pipeline via ${{resources.pipelines.ingestion_pipeline.id}}.
# Generated by dab/generate_jobs.py — do not edit by hand.

"""
    full_content = header + content
    if dry_run:
        print(f"Would write {out_path} ({len(full_content)} bytes)")
        return
    out_path.write_text(full_content, encoding="utf-8")
    print(f"Wrote {out_path}")


def _stale_job_files(resources_dir: Path, valid_bases: set[str]) -> list[Path]:
    """Return list of job_*.yml paths whose base is not in valid_bases."""
    stale: list[Path] = []
    for path in resources_dir.glob("job_*.yml"):
        base = path.stem.replace("job_", "", 1)
        if base not in valid_bases:
            stale.append(path)
    return stale


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate DAB job_*.yml from pipelines/ (1:1)."
    )
    script_dir = Path(__file__).resolve().parent
    repo = _repo_root(Path(__file__))
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
        help="Directory where to write job_*.yml (default: repo/dab/resources)",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Do not remove stale job_*.yml files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written/removed without making changes",
    )
    args = parser.parse_args()

    pipelines_dir = args.pipelines_dir.resolve()
    resources_dir = args.resources_dir.resolve()

    configs = _discover_pipeline_configs(pipelines_dir)
    if not configs:
        print("No pipeline configs found in", pipelines_dir, file=sys.stderr)
        return 0

    valid_bases: set[str] = set()
    for base_name, config_filename in configs:
        valid_bases.add(base_name)
        job_data = _job_template(base_name, config_filename)
        content = _emit_yaml(job_data)
        _write_job_file(resources_dir, base_name, content, args.dry_run)

    if not args.no_clean and resources_dir.is_dir():
        stale = _stale_job_files(resources_dir, valid_bases)
        for path in stale:
            if args.dry_run:
                print(f"Would remove {path}")
            else:
                path.unlink()
                print(f"Removed {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
