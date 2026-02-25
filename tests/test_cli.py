"""Tests for azsql_ct.__main__ -- CLI argument parsing and error paths."""

from __future__ import annotations

import json
import sys
from unittest.mock import patch

import pytest

from azsql_ct.__main__ import main


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

    @patch("azsql_ct.__main__.sync_from_config")
    def test_valid_config_calls_sync(self, mock_sync, tmp_path, capsys):
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({"tables": []}))
        mock_sync.return_value = [{"table": "dbo.T", "rows_written": 0}]

        with patch.object(sys, "argv", ["azsql_ct", "--config", str(cfg)]):
            main()

        mock_sync.assert_called_once()
        captured = capsys.readouterr()
        assert "dbo.T" in captured.out

    @patch("azsql_ct.__main__.sync_from_config", side_effect=RuntimeError("db error"))
    def test_sync_failure_exits(self, mock_sync, tmp_path):
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({"tables": []}))

        with patch.object(sys, "argv", ["azsql_ct", "--config", str(cfg)]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
