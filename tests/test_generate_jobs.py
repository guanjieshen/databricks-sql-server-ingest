"""Tests for dab/generate_jobs.py — DAB resource generation from pipeline configs."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import yaml

from dab.generate_jobs import (
    _discover_pipeline_configs,
    _emit_yaml,
    _job_key,
    _job_resource,
    _load_pipeline_config,
    _pipeline_key,
    _pipeline_resource,
    _repo_root,
    _stale_files,
    _write_file,
    main,
)


# ---------------------------------------------------------------------------
# _repo_root
# ---------------------------------------------------------------------------


class TestRepoRoot:
    def test_with_script_path(self, tmp_path: Path):
        dab_dir = tmp_path / "dab"
        dab_dir.mkdir()
        script = dab_dir / "generate_jobs.py"
        script.touch()
        assert _repo_root(script) == tmp_path

    def test_without_start_walks_cwd(self, tmp_path: Path, monkeypatch):
        (tmp_path / "dab").mkdir()
        monkeypatch.chdir(tmp_path)
        assert _repo_root() == tmp_path

    def test_without_start_walks_parents(self, tmp_path: Path, monkeypatch):
        (tmp_path / "dab").mkdir()
        child = tmp_path / "some" / "nested" / "dir"
        child.mkdir(parents=True)
        monkeypatch.chdir(child)
        assert _repo_root() == tmp_path

    def test_fallback_to_cwd_when_no_dab(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert _repo_root() == tmp_path


# ---------------------------------------------------------------------------
# _discover_pipeline_configs
# ---------------------------------------------------------------------------


class TestDiscoverPipelineConfigs:
    def test_discovers_yaml_and_yml(self, tmp_path: Path):
        (tmp_path / "a.yaml").touch()
        (tmp_path / "b.yml").touch()
        result = _discover_pipeline_configs(tmp_path)
        bases = [b for b, _ in result]
        assert "a" in bases
        assert "b" in bases

    def test_skips_underscore_prefix(self, tmp_path: Path):
        (tmp_path / "_internal.yaml").touch()
        (tmp_path / "good.yaml").touch()
        result = _discover_pipeline_configs(tmp_path)
        assert [("good", "good.yaml")] == result

    def test_skips_readme(self, tmp_path: Path):
        (tmp_path / "README.yaml").touch()
        (tmp_path / "README.yml").touch()
        (tmp_path / "pipeline.yaml").touch()
        result = _discover_pipeline_configs(tmp_path)
        assert [("pipeline", "pipeline.yaml")] == result

    def test_deduplicates_yaml_over_yml(self, tmp_path: Path):
        (tmp_path / "dup.yaml").touch()
        (tmp_path / "dup.yml").touch()
        result = _discover_pipeline_configs(tmp_path)
        assert len(result) == 1
        assert result[0] == ("dup", "dup.yaml")

    def test_empty_dir(self, tmp_path: Path):
        assert _discover_pipeline_configs(tmp_path) == []

    def test_nonexistent_dir(self, tmp_path: Path):
        assert _discover_pipeline_configs(tmp_path / "missing") == []

    def test_sorted_order(self, tmp_path: Path):
        (tmp_path / "z.yaml").touch()
        (tmp_path / "a.yaml").touch()
        (tmp_path / "m.yaml").touch()
        result = _discover_pipeline_configs(tmp_path)
        assert [b for b, _ in result] == ["a", "m", "z"]


# ---------------------------------------------------------------------------
# Key helpers
# ---------------------------------------------------------------------------


class TestKeyHelpers:
    def test_job_key(self):
        assert _job_key("pipeline_1") == "sql_server_ct_pipeline_1"

    def test_job_key_with_hyphens(self):
        assert _job_key("my-pipeline") == "sql_server_ct_my_pipeline"

    def test_pipeline_key(self):
        assert _pipeline_key("pipeline_1") == "sdp_pipeline_1"

    def test_pipeline_key_with_hyphens(self):
        assert _pipeline_key("my-pipeline") == "sdp_my_pipeline"


# ---------------------------------------------------------------------------
# _load_pipeline_config
# ---------------------------------------------------------------------------


class TestLoadPipelineConfig:
    def test_returns_tags_and_external_access(self, tmp_path: Path):
        (tmp_path / "p.yaml").write_text(
            "tags:\n  team: eng\nexternal_access: true\n"
        )
        cfg = _load_pipeline_config(tmp_path, "p.yaml")
        assert cfg["tags"] == {"team": "eng"}
        assert cfg["external_access"] is True

    def test_external_access_false(self, tmp_path: Path):
        (tmp_path / "p.yaml").write_text("external_access: false\n")
        cfg = _load_pipeline_config(tmp_path, "p.yaml")
        assert cfg["external_access"] is False

    def test_external_access_defaults_to_false(self, tmp_path: Path):
        (tmp_path / "p.yaml").write_text("tags:\n  team: eng\n")
        cfg = _load_pipeline_config(tmp_path, "p.yaml")
        assert cfg["external_access"] is False

    def test_missing_file_returns_defaults(self, tmp_path: Path):
        cfg = _load_pipeline_config(tmp_path, "nonexistent.yaml")
        assert cfg == {"tags": {}, "external_access": False, "schedule": None}

    def test_empty_yaml_returns_defaults(self, tmp_path: Path):
        (tmp_path / "p.yaml").write_text("")
        cfg = _load_pipeline_config(tmp_path, "p.yaml")
        assert cfg == {"tags": {}, "external_access": False, "schedule": None}

    def test_tags_values_coerced_to_str(self, tmp_path: Path):
        (tmp_path / "p.yaml").write_text("tags:\n  count: 42\n")
        cfg = _load_pipeline_config(tmp_path, "p.yaml")
        assert cfg["tags"] == {"count": "42"}

    def test_schedule_loaded(self, tmp_path: Path):
        (tmp_path / "p.yaml").write_text(
            "schedule:\n"
            "  quartz_cron_expression: '0 0 8 * * ?'\n"
            "  timezone_id: America/New_York\n"
            "  pause_status: PAUSED\n"
        )
        cfg = _load_pipeline_config(tmp_path, "p.yaml")
        assert cfg["schedule"] == {
            "quartz_cron_expression": "0 0 8 * * ?",
            "timezone_id": "America/New_York",
            "pause_status": "PAUSED",
        }

    def test_schedule_defaults_timezone_and_pause(self, tmp_path: Path):
        (tmp_path / "p.yaml").write_text(
            "schedule:\n  quartz_cron_expression: '0 30 6 * * ?'\n"
        )
        cfg = _load_pipeline_config(tmp_path, "p.yaml")
        assert cfg["schedule"]["timezone_id"] == "UTC"
        assert cfg["schedule"]["pause_status"] == "UNPAUSED"

    def test_schedule_missing_cron_raises(self, tmp_path: Path):
        (tmp_path / "p.yaml").write_text("schedule:\n  timezone_id: UTC\n")
        with pytest.raises(ValueError, match="quartz_cron_expression"):
            _load_pipeline_config(tmp_path, "p.yaml")

    def test_schedule_absent_returns_none(self, tmp_path: Path):
        (tmp_path / "p.yaml").write_text("tags:\n  team: eng\n")
        cfg = _load_pipeline_config(tmp_path, "p.yaml")
        assert cfg["schedule"] is None


# ---------------------------------------------------------------------------
# _pipeline_resource / _job_resource
# ---------------------------------------------------------------------------


class TestPipelineResource:
    def test_structure(self):
        data = _pipeline_resource("p1", "p1.yaml")
        pl = data["resources"]["pipelines"]["sdp_p1"]
        assert pl["name"] == "SQL Server CT – SDP – p1"
        assert pl["catalog"] == "${var.catalog}"
        assert pl["serverless"] is True
        assert pl["photon"] is True
        assert pl["schema"] == "default"

    def test_tags(self):
        data = _pipeline_resource("p1", "p1.yaml")
        tags = data["resources"]["pipelines"]["sdp_p1"]["tags"]
        assert tags["project"] == "sql-server-ct"
        assert tags["pipeline_config"] == "p1.yaml"
        assert tags["generated_by"] == "dab/generate_jobs.py"

    def test_workspace_root_path_used(self):
        data = _pipeline_resource("p1", "p1.yaml")
        pl = data["resources"]["pipelines"]["sdp_p1"]
        assert "${var.workspace_root}" in pl["configuration"]["input_yaml"]
        assert "${var.workspace_root}" in pl["root_path"]

    def test_config_filename_in_input_yaml(self):
        data = _pipeline_resource("p1", "custom.yaml")
        pl = data["resources"]["pipelines"]["sdp_p1"]
        assert pl["configuration"]["input_yaml"].endswith("/pipelines/custom.yaml")

    def test_libraries_point_to_lakeflow_pipeline(self):
        data = _pipeline_resource("p1", "p1.yaml")
        libs = data["resources"]["pipelines"]["sdp_p1"]["libraries"]
        paths = [lib["glob"]["include"] for lib in libs]
        assert any("ingestion_pipeline_materialized.py" in p for p in paths)
        assert any("metadata_helper.py" in p for p in paths)

    def test_iceberg_configs_omitted_by_default(self):
        data = _pipeline_resource("p1", "p1.yaml")
        cfg = data["resources"]["pipelines"]["sdp_p1"]["configuration"]
        assert "spark.databricks.delta.uniform.iceberg.v3.enabled" not in cfg
        assert "spark.databricks.delta.dbiManagedIcebergTable.v3.enabled" not in cfg

    def test_iceberg_configs_omitted_when_external_access_false(self):
        data = _pipeline_resource("p1", "p1.yaml", external_access=False)
        cfg = data["resources"]["pipelines"]["sdp_p1"]["configuration"]
        assert "spark.databricks.delta.uniform.iceberg.v3.enabled" not in cfg
        assert "spark.databricks.delta.dbiManagedIcebergTable.v3.enabled" not in cfg

    def test_iceberg_configs_present_when_external_access_true(self):
        data = _pipeline_resource("p1", "p1.yaml", external_access=True)
        cfg = data["resources"]["pipelines"]["sdp_p1"]["configuration"]
        assert cfg["spark.databricks.delta.uniform.iceberg.v3.enabled"] == "true"
        assert cfg["spark.databricks.delta.dbiManagedIcebergTable.v3.enabled"] == "true"

    def test_type_widening_always_present(self):
        for ea in (True, False):
            data = _pipeline_resource("p1", "p1.yaml", external_access=ea)
            cfg = data["resources"]["pipelines"]["sdp_p1"]["configuration"]
            assert cfg["pipelines.enableTypeWidening"] == "true"


class TestJobResource:
    def test_structure(self):
        data = _job_resource("p1", "p1.yaml")
        job = data["resources"]["jobs"]["sql_server_ct_p1"]
        assert job["name"] == "SQL Server CT – Job – p1"
        assert job["queue"]["enabled"] is True
        assert job["performance_target"] == "PERFORMANCE_OPTIMIZED"

    def test_tags(self):
        data = _job_resource("p1", "p1.yaml")
        tags = data["resources"]["jobs"]["sql_server_ct_p1"]["tags"]
        assert tags["project"] == "sql-server-ct"
        assert tags["pipeline_config"] == "p1.yaml"
        assert tags["generated_by"] == "dab/generate_jobs.py"

    def test_task_keys(self):
        data = _job_resource("p1", "p1.yaml")
        tasks = data["resources"]["jobs"]["sql_server_ct_p1"]["tasks"]
        keys = [t["task_key"] for t in tasks]
        assert keys == ["gateway", "check_record_changed", "schema_change_detected", "ingestion"]

    def test_gateway_references_sync_script(self):
        data = _job_resource("p1", "p1.yaml")
        gateway = data["resources"]["jobs"]["sql_server_ct_p1"]["tasks"][0]
        assert "${var.workspace_root}/scripts/sync.py" == gateway["spark_python_task"]["python_file"]

    def test_ingestion_references_pipeline(self):
        data = _job_resource("p1", "p1.yaml")
        ingestion = data["resources"]["jobs"]["sql_server_ct_p1"]["tasks"][3]
        assert ingestion["pipeline_task"]["pipeline_id"] == "${resources.pipelines.sdp_p1.id}"

    def test_gateway_passes_config_path(self):
        data = _job_resource("p1", "p1.yaml")
        gateway = data["resources"]["jobs"]["sql_server_ct_p1"]["tasks"][0]
        params = gateway["spark_python_task"]["parameters"]
        assert params == ["${var.workspace_root}/pipelines/p1.yaml"]

    def test_schedule_present(self):
        sched = {
            "quartz_cron_expression": "0 0 8 * * ?",
            "timezone_id": "UTC",
            "pause_status": "UNPAUSED",
        }
        data = _job_resource("p1", "p1.yaml", schedule=sched)
        job = data["resources"]["jobs"]["sql_server_ct_p1"]
        assert job["schedule"] == sched

    def test_schedule_absent(self):
        data = _job_resource("p1", "p1.yaml")
        job = data["resources"]["jobs"]["sql_server_ct_p1"]
        assert "schedule" not in job


# ---------------------------------------------------------------------------
# _emit_yaml
# ---------------------------------------------------------------------------


class TestEmitYaml:
    def test_produces_valid_yaml(self):
        data = {"key": "value", "nested": {"a": 1}}
        result = _emit_yaml(data)
        parsed = yaml.safe_load(result)
        assert parsed == data

    def test_preserves_key_order(self):
        data = {"z": 1, "a": 2, "m": 3}
        result = _emit_yaml(data)
        lines = [l.split(":")[0] for l in result.strip().splitlines()]
        assert lines == ["z", "a", "m"]


# ---------------------------------------------------------------------------
# _write_file
# ---------------------------------------------------------------------------


class TestWriteFile:
    def test_writes_with_header(self, tmp_path: Path):
        out = tmp_path / "test.yml"
        _write_file(out, "body\n", "# header\n", dry_run=False)
        assert out.read_text() == "# header\nbody\n"

    def test_creates_parent_dirs(self, tmp_path: Path):
        out = tmp_path / "a" / "b" / "test.yml"
        _write_file(out, "body\n", "", dry_run=False)
        assert out.exists()

    def test_dry_run_does_not_write(self, tmp_path: Path):
        out = tmp_path / "test.yml"
        _write_file(out, "body\n", "", dry_run=True)
        assert not out.exists()


# ---------------------------------------------------------------------------
# _stale_files
# ---------------------------------------------------------------------------


class TestStaleFiles:
    def test_finds_stale(self, tmp_path: Path):
        (tmp_path / "job_a.yml").touch()
        (tmp_path / "job_b.yml").touch()
        stale = _stale_files(tmp_path, "job", {"a"})
        assert [p.name for p in stale] == ["job_b.yml"]

    def test_no_stale(self, tmp_path: Path):
        (tmp_path / "sdp_x.yml").touch()
        assert _stale_files(tmp_path, "sdp", {"x"}) == []

    def test_nonexistent_directory(self, tmp_path: Path):
        assert _stale_files(tmp_path / "nope", "job", set()) == []

    def test_ignores_non_matching_prefix(self, tmp_path: Path):
        (tmp_path / "other_a.yml").touch()
        (tmp_path / "job_a.yml").touch()
        stale = _stale_files(tmp_path, "job", set())
        assert len(stale) == 1
        assert stale[0].name == "job_a.yml"


# ---------------------------------------------------------------------------
# main (end-to-end)
# ---------------------------------------------------------------------------


class TestMain:
    def _setup_dirs(self, tmp_path: Path):
        pipelines = tmp_path / "pipelines"
        pipelines.mkdir()
        (pipelines / "pipeline_1.yaml").write_text("tables: {}")
        resources = tmp_path / "dab" / "resources"
        resources.mkdir(parents=True)
        return pipelines, resources

    def test_generates_pipeline_and_job(self, tmp_path: Path, monkeypatch):
        pipelines, resources = self._setup_dirs(tmp_path)
        monkeypatch.setattr(
            "sys.argv",
            ["gen", "--pipelines-dir", str(pipelines), "--resources-dir", str(resources)],
        )
        import dab.generate_jobs as mod
        monkeypatch.setattr(mod, "_SCRIPT_PATH", Path(__file__))

        rc = main()
        assert rc == 0
        assert (resources / "pipelines" / "sdp_pipeline_1.yml").exists()
        assert (resources / "jobs" / "job_pipeline_1.yml").exists()

    def test_generated_yaml_is_valid(self, tmp_path: Path, monkeypatch):
        pipelines, resources = self._setup_dirs(tmp_path)
        monkeypatch.setattr(
            "sys.argv",
            ["gen", "--pipelines-dir", str(pipelines), "--resources-dir", str(resources)],
        )
        import dab.generate_jobs as mod
        monkeypatch.setattr(mod, "_SCRIPT_PATH", Path(__file__))

        main()
        for yml in (resources / "pipelines").glob("*.yml"):
            data = yaml.safe_load(yml.read_text())
            assert "resources" in data
        for yml in (resources / "jobs").glob("*.yml"):
            data = yaml.safe_load(yml.read_text())
            assert "resources" in data

    def test_cleans_stale_files(self, tmp_path: Path, monkeypatch):
        pipelines, resources = self._setup_dirs(tmp_path)
        stale_jobs = resources / "jobs"
        stale_jobs.mkdir(parents=True, exist_ok=True)
        (stale_jobs / "job_old.yml").touch()
        stale_pls = resources / "pipelines"
        stale_pls.mkdir(parents=True, exist_ok=True)
        (stale_pls / "sdp_old.yml").touch()

        monkeypatch.setattr(
            "sys.argv",
            ["gen", "--pipelines-dir", str(pipelines), "--resources-dir", str(resources)],
        )
        import dab.generate_jobs as mod
        monkeypatch.setattr(mod, "_SCRIPT_PATH", Path(__file__))

        main()
        assert not (stale_jobs / "job_old.yml").exists()
        assert not (stale_pls / "sdp_old.yml").exists()

    def test_no_clean_preserves_stale(self, tmp_path: Path, monkeypatch):
        pipelines, resources = self._setup_dirs(tmp_path)
        stale_jobs = resources / "jobs"
        stale_jobs.mkdir(parents=True, exist_ok=True)
        (stale_jobs / "job_old.yml").touch()

        monkeypatch.setattr(
            "sys.argv",
            ["gen", "--pipelines-dir", str(pipelines), "--resources-dir", str(resources), "--no-clean"],
        )
        import dab.generate_jobs as mod
        monkeypatch.setattr(mod, "_SCRIPT_PATH", Path(__file__))

        main()
        assert (stale_jobs / "job_old.yml").exists()

    def test_dry_run_writes_nothing(self, tmp_path: Path, monkeypatch):
        pipelines, resources = self._setup_dirs(tmp_path)
        monkeypatch.setattr(
            "sys.argv",
            ["gen", "--pipelines-dir", str(pipelines), "--resources-dir", str(resources), "--dry-run"],
        )
        import dab.generate_jobs as mod
        monkeypatch.setattr(mod, "_SCRIPT_PATH", Path(__file__))

        main()
        assert not (resources / "pipelines").exists() or not list((resources / "pipelines").glob("*.yml"))

    def test_dry_run_does_not_remove_stale(self, tmp_path: Path, monkeypatch):
        pipelines, resources = self._setup_dirs(tmp_path)
        stale_pls = resources / "pipelines"
        stale_pls.mkdir(parents=True, exist_ok=True)
        (stale_pls / "sdp_old.yml").touch()
        stale_jobs = resources / "jobs"
        stale_jobs.mkdir(parents=True, exist_ok=True)
        (stale_jobs / "job_old.yml").touch()

        monkeypatch.setattr(
            "sys.argv",
            ["gen", "--pipelines-dir", str(pipelines), "--resources-dir", str(resources), "--dry-run"],
        )
        import dab.generate_jobs as mod
        monkeypatch.setattr(mod, "_SCRIPT_PATH", Path(__file__))

        rc = main()
        assert rc == 0
        assert (stale_pls / "sdp_old.yml").exists()
        assert (stale_jobs / "job_old.yml").exists()

    def test_schedule_in_generated_job_yaml(self, tmp_path: Path, monkeypatch):
        pipelines = tmp_path / "pipelines"
        pipelines.mkdir()
        (pipelines / "scheduled.yaml").write_text(
            "schedule:\n"
            "  quartz_cron_expression: '0 0 12 * * ?'\n"
            "  timezone_id: America/Chicago\n"
            "  pause_status: PAUSED\n"
        )
        resources = tmp_path / "dab" / "resources"
        resources.mkdir(parents=True)
        monkeypatch.setattr(
            "sys.argv",
            ["gen", "--pipelines-dir", str(pipelines), "--resources-dir", str(resources)],
        )
        import dab.generate_jobs as mod
        monkeypatch.setattr(mod, "_SCRIPT_PATH", Path(__file__))

        rc = main()
        assert rc == 0
        job_file = resources / "jobs" / "job_scheduled.yml"
        assert job_file.exists()
        data = yaml.safe_load(job_file.read_text())
        job = data["resources"]["jobs"]["sql_server_ct_scheduled"]
        assert job["schedule"] == {
            "quartz_cron_expression": "0 0 12 * * ?",
            "timezone_id": "America/Chicago",
            "pause_status": "PAUSED",
        }

    def test_no_schedule_omits_key_in_generated_yaml(self, tmp_path: Path, monkeypatch):
        pipelines, resources = self._setup_dirs(tmp_path)
        monkeypatch.setattr(
            "sys.argv",
            ["gen", "--pipelines-dir", str(pipelines), "--resources-dir", str(resources)],
        )
        import dab.generate_jobs as mod
        monkeypatch.setattr(mod, "_SCRIPT_PATH", Path(__file__))

        main()
        job_file = resources / "jobs" / "job_pipeline_1.yml"
        data = yaml.safe_load(job_file.read_text())
        job = data["resources"]["jobs"]["sql_server_ct_pipeline_1"]
        assert "schedule" not in job

    def test_no_configs_returns_zero(self, tmp_path: Path, monkeypatch):
        empty_pipelines = tmp_path / "empty"
        empty_pipelines.mkdir()
        resources = tmp_path / "res"
        resources.mkdir()
        monkeypatch.setattr(
            "sys.argv",
            ["gen", "--pipelines-dir", str(empty_pipelines), "--resources-dir", str(resources)],
        )
        import dab.generate_jobs as mod
        monkeypatch.setattr(mod, "_SCRIPT_PATH", Path(__file__))

        assert main() == 0
