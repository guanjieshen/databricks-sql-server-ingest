"""Tests for azsql_ct.__main__ -- CLI argument parsing and error paths."""

from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

from azsql_ct.__main__ import _log_summary, main


class TestCli:
    def test_missing_config_flag_exits(self):
        with patch.object(sys, "argv", ["azsql_ct"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code != 0

    def test_nonexistent_config_exits(self, tmp_path):
        with patch.object(
            sys, "argv", ["azsql_ct", "--config", str(tmp_path / "nope.json")]
        ):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_invalid_json_exits(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json{{{")
        with patch.object(sys, "argv", ["azsql_ct", "--config", str(bad)]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    @patch("azsql_ct.__main__.ChangeTracker")
    def test_valid_config_calls_sync(self, MockCT, tmp_path, capsys):
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({
            "server": "srv", "user": "u", "password": "p",
            "tables": [{"database": "db1", "table": "dbo.T", "mode": "full"}],
        }))

        mock_ct = MagicMock()
        mock_ct.sync.return_value = [
            {"database": "db1", "table": "dbo.T", "mode": "full",
             "rows_written": 0, "current_version": 1, "files": [],
             "duration_seconds": 0.1, "since_version": None},
        ]
        MockCT.from_config.return_value = mock_ct

        with patch.object(sys, "argv", ["azsql_ct", "--config", str(cfg)]):
            main()

        MockCT.from_config.assert_called_once()
        mock_ct.sync.assert_called_once()
        captured = capsys.readouterr()
        assert "dbo.T" in captured.out

    @patch("azsql_ct.__main__.ChangeTracker")
    def test_sync_failure_exits(self, MockCT, tmp_path):
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({
            "server": "srv", "user": "u", "password": "p", "tables": [],
        }))

        mock_ct = MagicMock()
        mock_ct.sync.side_effect = RuntimeError("db error")
        MockCT.from_config.return_value = mock_ct

        with patch.object(sys, "argv", ["azsql_ct", "--config", str(cfg)]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    @patch("azsql_ct.__main__.ChangeTracker")
    def test_workers_flag_passed(self, MockCT, tmp_path, capsys):
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({
            "server": "srv", "user": "u", "password": "p", "tables": [],
        }))

        mock_ct = MagicMock()
        mock_ct.sync.return_value = []
        MockCT.from_config.return_value = mock_ct

        with patch.object(
            sys, "argv", ["azsql_ct", "--config", str(cfg), "--workers", "4"]
        ):
            main()

        call_kwargs = MockCT.from_config.call_args
        assert call_kwargs.kwargs.get("max_workers") == 4


class TestLogSummary:
    """Tests for _log_summary -- human-readable sync summary."""

    def _success_result(self, **overrides):
        base = {
            "database": "db1",
            "table": "dbo.T",
            "mode": "full",
            "rows_written": 10,
            "current_version": 5,
            "files": ["/data/f.parquet"],
            "duration_seconds": 1.5,
            "since_version": None,
        }
        base.update(overrides)
        return base

    def _error_result(self):
        return {
            "database": "db1",
            "table": "dbo.Bad",
            "status": "error",
            "error": "Connection reset",
        }

    def test_success_results_logged(self, caplog):
        import logging
        with caplog.at_level(logging.INFO, logger="azsql_ct.__main__"):
            _log_summary([self._success_result()])
        assert "db1.dbo.T" in caplog.text
        assert "10 rows" in caplog.text

    def test_error_results_logged(self, caplog):
        import logging
        with caplog.at_level(logging.ERROR, logger="azsql_ct.__main__"):
            _log_summary([self._error_result()])
        assert "ERROR" in caplog.text
        assert "Connection reset" in caplog.text

    def test_mixed_results(self, caplog):
        import logging
        with caplog.at_level(logging.INFO, logger="azsql_ct.__main__"):
            _log_summary([self._success_result(), self._error_result()])
        assert "Completed: 1 table(s)" in caplog.text
        assert "Failed: 1 table(s)" in caplog.text

    def test_empty_results(self, caplog):
        import logging
        with caplog.at_level(logging.INFO, logger="azsql_ct.__main__"):
            _log_summary([])
        assert "Completed: 0 table(s)" in caplog.text
