"""Tests for ChangeTracker.from_config(), UC catalog propagation, and Databricks task values."""

from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock, call, patch

from azsql_ct.client import (
    ChangeTracker,
    set_databricks_task_values,
)


class TestSyncPassesUcCatalog:
    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_sync_passes_uc_catalog_to_sync_table(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "databases": {
                "db1": {
                    "uc_catalog": "my_catalog",
                    "schemas": {"dbo": {"tables": {"t1": "full_incremental"}}},
                },
            },
        })
        ct.sync()

        _, kwargs = mock_sync.call_args
        assert kwargs["uc_catalog"] == "my_catalog"

    @patch("azsql_ct.client.sync_table")
    @patch("azsql_ct.client.AzureSQLConnection")
    def test_parallel_sync_passes_uc_catalog(self, MockAz, mock_sync):
        mock_conn = MagicMock()
        MockAz.return_value.connect.return_value = mock_conn
        mock_sync.return_value = {"ok": True}

        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "databases": {
                "db1": {
                    "uc_catalog": "my_catalog",
                    "schemas": {"dbo": {"tables": {"t1": "full_incremental"}}},
                },
            },
        })
        ct.max_workers = 2
        ct.sync()

        _, kwargs = mock_sync.call_args
        assert kwargs["uc_catalog"] == "my_catalog"


class TestSetDatabricksTaskValues:
    def test_sums_rows_written_excluding_errors(self):
        results = [
            {"database": "db1", "table": "dbo.t1", "rows_written": 10},
            {"database": "db1", "table": "dbo.t2", "rows_written": 5},
            {"database": "db1", "table": "dbo.t3", "status": "error", "error": "boom"},
        ]
        mock_main = MagicMock()
        mock_dbutils = MagicMock()
        mock_main.dbutils = mock_dbutils
        with patch.dict(sys.modules, {"__main__": mock_main}):
            total = set_databricks_task_values(results)

        assert total == 15
        mock_dbutils.jobs.taskValues.set.assert_has_calls([
            call(key="total_rows_changed", value=15),
            call(key="schema_changes_detected", value=False),
        ])

    def test_returns_total_when_no_dbutils(self):
        results = [
            {"database": "db1", "table": "dbo.t1", "rows_written": 42},
        ]
        total = set_databricks_task_values(results)
        assert total == 42

    def test_skipped_tables_contribute_zero(self):
        results = [
            {"database": "db1", "table": "dbo.t1", "status": "skipped", "reason": "no changes"},
            {"database": "db1", "table": "dbo.t2", "rows_written": 7},
        ]
        mock_main = MagicMock()
        mock_dbutils = MagicMock()
        mock_main.dbutils = mock_dbutils
        with patch.dict(sys.modules, {"__main__": mock_main}):
            total = set_databricks_task_values(results)

        assert total == 7
        mock_dbutils.jobs.taskValues.set.assert_has_calls([
            call(key="total_rows_changed", value=7),
            call(key="schema_changes_detected", value=False),
        ])

    def test_sets_schema_changes_detected_when_any_table_has_schema_changed(self):
        results = [
            {"database": "db1", "table": "dbo.t1", "rows_written": 10, "schema_changed": False},
            {"database": "db1", "table": "dbo.t2", "rows_written": 5, "schema_changed": True},
        ]
        mock_main = MagicMock()
        mock_dbutils = MagicMock()
        mock_main.dbutils = mock_dbutils
        with patch.dict(sys.modules, {"__main__": mock_main}):
            total = set_databricks_task_values(results)

        assert total == 15
        mock_dbutils.jobs.taskValues.set.assert_has_calls([
            call(key="total_rows_changed", value=15),
            call(key="schema_changes_detected", value=True),
        ])

    def test_returns_total_even_when_dbutils_raises(self):
        results = [{"database": "db1", "table": "dbo.t1", "rows_written": 10}]
        mock_main = MagicMock()
        mock_dbutils = MagicMock()
        mock_dbutils.jobs.taskValues.set.side_effect = RuntimeError("boom")
        mock_main.dbutils = mock_dbutils
        with patch.dict(sys.modules, {"__main__": mock_main}):
            total = set_databricks_task_values(results)
        assert total == 10


class TestFromConfig:
    """Tests for ChangeTracker.from_config() factory method."""

    def test_loads_flat_json_config(self, tmp_path):
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({
            "server": "srv.database.windows.net",
            "user": "admin",
            "password": "secret",
            "tables": [
                {"database": "db1", "table": "dbo.Foo", "mode": "full_incremental"},
                {"database": "db1", "table": "dbo.Bar", "mode": "incremental"},
            ],
        }))

        ct = ChangeTracker.from_config(str(cfg))
        assert ct.server == "srv.database.windows.net"
        assert ct.user == "admin"
        assert len(ct._flat_tables) == 2

    def test_loads_dict_config(self):
        ct = ChangeTracker.from_config({
            "server": "srv",
            "user": "u",
            "password": "p",
            "tables": [
                {"database": "db1", "table": "dbo.T", "mode": "full_incremental"},
            ],
        })
        assert ct.server == "srv"
        assert len(ct._flat_tables) == 1

    def test_loads_nested_dict_config(self):
        ct = ChangeTracker.from_config({
            "connection": {
                "server": "myserver",
                "sql_login": "admin",
                "password": "pw",
            },
            "databases": {
                "db1": {"dbo": {"t1": "full_incremental", "t2": "incremental"}},
            },
        })
        assert ct.server == "myserver"
        assert ct.user == "admin"
        assert len(ct._flat_tables) == 2

    def test_loads_structured_format_with_uc_metadata(self):
        ct = ChangeTracker.from_config({
            "connection": {
                "server": "myserver",
                "sql_login": "admin",
                "password": "pw",
            },
            "databases": {
                "db1": {
                    "uc_catalog": "my_catalog",
                    "schemas": {
                        "dbo": {
                            "uc_schema": "my_schema",
                            "tables": {
                                "t1": "full_incremental",
                                "t2": "incremental",
                            },
                        },
                    },
                },
            },
        })
        assert ct.server == "myserver"
        assert len(ct._flat_tables) == 2
        tables = {t[1] for t in ct._flat_tables}
        assert tables == {"dbo.t1", "dbo.t2"}
        assert ct._uc_metadata == {
            "db1": {"uc_catalog": "my_catalog", "schemas": {"dbo": "my_schema"}},
        }

    def test_cli_overrides_take_precedence(self):
        ct = ChangeTracker.from_config(
            {
                "connection": {
                    "server": "srv", "sql_login": "u", "password": "p",
                },
                "storage": {"data_dir": "/default"},
                "max_workers": 2,
                "databases": {"db1": {"dbo": ["t"]}},
            },
            max_workers=8,
            output_dir="/override",
        )
        assert ct.max_workers == 8
        assert ct.output_dir == "/override"

    def test_parallelism_config_key(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "srv", "sql_login": "u", "password": "p"},
            "parallelism": 4,
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.max_workers == 4

    def test_output_manifest_from_config(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "srv", "sql_login": "u", "password": "p"},
            "storage": {"data_dir": "/d", "watermark_dir": "/w", "output_manifest": "/out.yaml"},
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.output_manifest == "/out.yaml"

    def test_env_var_expansion(self, monkeypatch):
        monkeypatch.setenv("TEST_SERVER", "expanded.database.windows.net")
        monkeypatch.setenv("TEST_PW", "secret123")

        ct = ChangeTracker.from_config({
            "server": "${TEST_SERVER}",
            "user": "admin",
            "password": "${TEST_PW}",
            "tables": [],
        })
        assert ct.server == "expanded.database.windows.net"

    def test_storage_section_applied(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "storage": {"data_dir": "/mydata", "watermark_dir": "/mywm"},
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.output_dir == "/mydata"
        assert ct.watermark_dir == "/mywm"

    def test_ingest_pipeline_derives_paths(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "storage": {"ingest_pipeline": "/some/base"},
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.output_dir == "/some/base/data"
        assert ct.watermark_dir == "/some/base/watermarks"
        assert ct.output_manifest == "/some/base/output.yaml"

    def test_ingest_pipeline_explicit_data_dir_overrides(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "storage": {
                "ingest_pipeline": "/some/base",
                "data_dir": "/custom/data",
            },
            "databases": {"db1": {"dbo": ["t"]}},
        })
        assert ct.output_dir == "/custom/data"
        assert ct.watermark_dir == "/some/base/watermarks"
        assert ct.output_manifest == "/some/base/output.yaml"

    def test_structured_config_with_scd_type(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "databases": {
                "db1": {
                    "schemas": {
                        "dbo": {
                            "tables": {
                                "t1": {"mode": "full_incremental", "scd_type": 2},
                                "t2": "full_incremental",
                            },
                        },
                    },
                },
            },
        })
        assert len(ct._flat_tables) == 2
        by_table = {t[1]: t for t in ct._flat_tables}
        assert by_table["dbo.t1"][2] == "full_incremental"
        assert by_table["dbo.t1"][3] == 2
        assert by_table["dbo.t2"][2] == "full_incremental"
        assert by_table["dbo.t2"][3] == 1

    def test_structured_config_with_soft_delete(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "databases": {
                "db1": {
                    "schemas": {
                        "dbo": {
                            "tables": {
                                "t1": {"mode": "full_incremental", "scd_type": 1, "soft_delete": True},
                                "t2": "full_incremental",
                            },
                        },
                    },
                },
            },
        })
        assert len(ct._flat_tables) == 2
        by_table = {t[1]: t for t in ct._flat_tables}
        assert by_table["dbo.t1"][4] is True
        assert by_table["dbo.t2"][4] is False

    def test_structured_config_with_soft_delete_column(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "databases": {
                "db1": {
                    "schemas": {
                        "dbo": {
                            "tables": {
                                "t1": {"mode": "full_incremental", "soft_delete": True, "soft_delete_column": "is_removed"},
                                "t2": {"mode": "full_incremental", "soft_delete": True},
                            },
                        },
                    },
                },
            },
        })
        assert len(ct._flat_tables) == 2
        by_table = {t[1]: t for t in ct._flat_tables}
        assert by_table["dbo.t1"][5] == "is_removed"
        assert by_table["dbo.t2"][5] is None

    def test_pipeline_level_soft_delete_column(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "soft_delete_column": "deleted_flag",
            "databases": {
                "db1": {
                    "schemas": {
                        "dbo": {
                            "tables": {"t1": {"mode": "full_incremental", "soft_delete": True}},
                        },
                    },
                },
            },
        })
        assert ct.soft_delete_column == "deleted_flag"

    def test_default_soft_delete_column(self):
        ct = ChangeTracker.from_config({
            "connection": {"server": "s", "sql_login": "u", "password": "p"},
            "databases": {
                "db1": {
                    "schemas": {
                        "dbo": {
                            "tables": {"t1": {"mode": "full_incremental", "soft_delete": True}},
                        },
                    },
                },
            },
        })
        assert ct.soft_delete_column == "_is_deleted"

    def test_from_config_resolves_databricks_secrets(self):
        mock_main = MagicMock()
        mock_main.dbutils.secrets.get.return_value = "resolved_pw"
        with patch.dict(sys.modules, {"__main__": mock_main}):
            ct = ChangeTracker.from_config({
                "connection": {
                    "server": "srv",
                    "sql_login": "u",
                    "password": "{{secrets/my-scope/my-key}}",
                },
                "databases": {
                    "db1": {
                        "schemas": {
                            "dbo": {"tables": {"t1": "full_incremental"}},
                        },
                    },
                },
            })
        assert ct._password == "resolved_pw"
        mock_main.dbutils.secrets.get.assert_called_once_with(
            scope="my-scope", key="my-key",
        )
