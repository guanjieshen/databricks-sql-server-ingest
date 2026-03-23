"""Smoke tests for scripts/sync.py and scripts/connect.py.

These verify argument handling, exit behavior, and basic wiring
without requiring a live database connection.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")


def _subprocess_env():
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = PROJECT_ROOT + (os.pathsep + pp if pp else "")
    return env


class TestSyncScript:
    def _run_subprocess(self, *args):
        cmd = [sys.executable, os.path.join(SCRIPTS_DIR, "sync.py")] + list(args)
        return subprocess.run(
            cmd, capture_output=True, text=True, timeout=10,
            env=_subprocess_env(),
        )

    def test_help_flag(self):
        result = self._run_subprocess("--help")
        assert result.returncode == 0
        assert "config" in result.stdout.lower()

    def test_nonexistent_config_errors(self, tmp_path):
        result = self._run_subprocess(str(tmp_path / "does_not_exist.yaml"))
        assert result.returncode != 0

    def test_invalid_yaml_errors(self, tmp_path):
        bad = tmp_path / "bad.yaml"
        bad.write_text("::not:valid:yaml:[[")
        result = self._run_subprocess(str(bad))
        assert result.returncode != 0

    def test_valid_config_sync_flow(self, tmp_path, capsys):
        """Verify sync.py's __main__ block wires from_config -> sync -> output."""
        cfg = tmp_path / "test.yaml"
        cfg.write_text(textwrap.dedent("""\
            connection:
              server: fake-server
              sql_login: testuser
              password: testpass
            databases:
              db1:
                dbo:
                  t1: full_incremental
        """))

        mock_ct = MagicMock()
        mock_ct.__str__ = lambda self: "ChangeTracker(fake-server)"
        mock_ct.sync.return_value = [
            {"database": "db1", "table": "dbo.t1", "mode": "full",
             "rows_written": 5, "current_version": 10},
        ]

        from azsql_ct import client as _client_mod
        original_from_config = _client_mod.ChangeTracker.from_config

        test_argv = ["sync.py", str(cfg)]
        with patch.object(sys, "argv", test_argv), \
             patch.object(_client_mod.ChangeTracker, "from_config", return_value=mock_ct) as mock_fc, \
             patch.object(_client_mod, "set_databricks_task_values", return_value=5):
            import runpy
            runpy.run_path(
                os.path.join(SCRIPTS_DIR, "sync.py"),
                run_name="__main__",
            )

        mock_fc.assert_called_once_with(str(cfg))
        mock_ct.sync.assert_called_once()
        captured = capsys.readouterr()
        assert "Total:" in captured.out

    def test_debug_flag_sets_debug_logging(self, tmp_path):
        cfg = tmp_path / "test.yaml"
        cfg.write_text(textwrap.dedent("""\
            connection:
              server: fake-server
              sql_login: u
              password: p
            databases:
              db1:
                dbo:
                  t1: full_incremental
        """))

        mock_ct = MagicMock()
        mock_ct.sync.return_value = [
            {"database": "db1", "table": "dbo.t1", "mode": "full",
             "rows_written": 0, "current_version": 1},
        ]

        from azsql_ct import client as _client_mod
        import logging

        test_argv = ["sync.py", "--debug", str(cfg)]
        with patch.object(sys, "argv", test_argv), \
             patch.object(_client_mod.ChangeTracker, "from_config", return_value=mock_ct), \
             patch.object(_client_mod, "set_databricks_task_values", return_value=0):
            import runpy
            runpy.run_path(
                os.path.join(SCRIPTS_DIR, "sync.py"),
                run_name="__main__",
            )

        assert logging.getLogger("azsql_ct").level == logging.DEBUG


class TestConnectScript:
    def test_fails_with_missing_config(self):
        cmd = [sys.executable, os.path.join(SCRIPTS_DIR, "connect.py")]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=10,
            env=_subprocess_env(),
        )
        assert result.returncode != 0

    def test_connect_script_is_importable(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "connect", os.path.join(SCRIPTS_DIR, "connect.py"),
        )
        assert spec is not None
