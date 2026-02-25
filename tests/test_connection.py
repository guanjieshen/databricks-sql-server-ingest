"""Tests for azsql_ct.connection -- load_dotenv, AzureSQLConnection, get_connection."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from azsql_ct.connection import AzureSQLConnection, get_connection, load_dotenv


class TestLoadDotenv:
    def test_loads_vars_into_environ(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("FOO=bar\nBAZ=qux\n")
        with patch.dict(os.environ, {}, clear=True):
            load_dotenv(str(env_file))
            assert os.environ["FOO"] == "bar"
            assert os.environ["BAZ"] == "qux"

    def test_does_not_overwrite_existing(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("FOO=new\n")
        with patch.dict(os.environ, {"FOO": "old"}, clear=True):
            load_dotenv(str(env_file))
            assert os.environ["FOO"] == "old"

    def test_skips_comments_and_blanks(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("# comment\n\nKEY=val\n")
        with patch.dict(os.environ, {}, clear=True):
            load_dotenv(str(env_file))
            assert os.environ.get("KEY") == "val"

    def test_strips_quotes(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("A='quoted'\nB=\"double\"\n")
        with patch.dict(os.environ, {}, clear=True):
            load_dotenv(str(env_file))
            assert os.environ["A"] == "quoted"
            assert os.environ["B"] == "double"

    def test_noop_when_file_missing(self, tmp_path):
        load_dotenv(str(tmp_path / "no-such-file"))


class TestAzureSQLConnection:
    def test_resolves_explicit_params(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("")
        az = AzureSQLConnection(
            server="srv", database="db", user="u", password="p",
            dotenv_path=str(env_file),
        )
        assert az.server == "srv"
        assert az.database == "db"
        assert az.user == "u"

    def test_falls_back_to_env(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("SERVER=envhost\nADMIN_USER=envuser\nADMIN_PASSWORD=envpw\n")
        with patch.dict(os.environ, {}, clear=True):
            az = AzureSQLConnection(dotenv_path=str(env_file))
        assert az.server == "envhost"
        assert az.user == "envuser"

    def test_connect_raises_without_password(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("")
        with patch.dict(os.environ, {}, clear=True):
            az = AzureSQLConnection(dotenv_path=str(env_file))
            with pytest.raises(ValueError, match="No password supplied"):
                az.connect()

    @patch("azsql_ct.connection._mssql")
    def test_connect_returns_mssql_conn(self, mock_mssql, tmp_path):
        mock_mssql.connect.return_value = MagicMock()
        env_file = tmp_path / ".env"
        env_file.write_text("")
        az = AzureSQLConnection(password="pw", dotenv_path=str(env_file))
        conn = az.connect()
        assert conn is mock_mssql.connect.return_value
        mock_mssql.connect.assert_called_once()

    @patch("azsql_ct.connection._mssql")
    def test_connect_reuses_existing(self, mock_mssql, tmp_path):
        mock_mssql.connect.return_value = MagicMock()
        env_file = tmp_path / ".env"
        env_file.write_text("")
        az = AzureSQLConnection(password="pw", dotenv_path=str(env_file))
        c1 = az.connect()
        c2 = az.connect()
        assert c1 is c2
        mock_mssql.connect.assert_called_once()

    @patch("azsql_ct.connection._mssql")
    def test_close_closes_connection(self, mock_mssql, tmp_path):
        mock_conn = MagicMock()
        mock_mssql.connect.return_value = mock_conn
        env_file = tmp_path / ".env"
        env_file.write_text("")
        az = AzureSQLConnection(password="pw", dotenv_path=str(env_file))
        az.connect()
        az.close()
        mock_conn.close.assert_called_once()

    @patch("azsql_ct.connection._mssql")
    def test_close_is_idempotent(self, mock_mssql, tmp_path):
        mock_mssql.connect.return_value = MagicMock()
        env_file = tmp_path / ".env"
        env_file.write_text("")
        az = AzureSQLConnection(password="pw", dotenv_path=str(env_file))
        az.connect()
        az.close()
        az.close()

    @patch("azsql_ct.connection._mssql")
    def test_context_manager(self, mock_mssql, tmp_path):
        mock_conn = MagicMock()
        mock_mssql.connect.return_value = mock_conn
        env_file = tmp_path / ".env"
        env_file.write_text("")
        az = AzureSQLConnection(password="pw", dotenv_path=str(env_file))
        with az as conn:
            assert conn is mock_conn
        mock_conn.close.assert_called_once()

    @patch("azsql_ct.connection._mssql")
    def test_test_connectivity_success(self, mock_mssql, tmp_path):
        mock_conn = MagicMock()
        mock_mssql.connect.return_value = mock_conn
        env_file = tmp_path / ".env"
        env_file.write_text("")
        az = AzureSQLConnection(password="pw", dotenv_path=str(env_file))
        assert az.test_connectivity() is True

    @patch("azsql_ct.connection._mssql")
    def test_test_connectivity_failure(self, mock_mssql, tmp_path):
        mock_mssql.connect.side_effect = Exception("cannot connect")
        env_file = tmp_path / ".env"
        env_file.write_text("")
        az = AzureSQLConnection(password="pw", dotenv_path=str(env_file))
        assert az.test_connectivity() is False

    def test_repr(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("")
        az = AzureSQLConnection(
            server="s", database="d", user="u", password="p",
            dotenv_path=str(env_file),
        )
        r = repr(az)
        assert "server='s'" in r
        assert "database='d'" in r
        assert "user='u'" in r
        assert "p" not in r


class TestGetConnection:
    @patch("azsql_ct.connection._mssql")
    def test_returns_connection(self, mock_mssql, tmp_path):
        mock_conn = MagicMock()
        mock_mssql.connect.return_value = mock_conn
        env_file = tmp_path / ".env"
        env_file.write_text("ADMIN_PASSWORD=secret\n")

        conn = get_connection(dotenv_path=str(env_file))
        assert conn is mock_conn
        mock_mssql.connect.assert_called_once()
        kwargs = mock_mssql.connect.call_args[1]
        assert kwargs["pwd"] == "secret"

    @patch("azsql_ct.connection._mssql")
    def test_explicit_params_override_env(self, mock_mssql, tmp_path):
        mock_mssql.connect.return_value = MagicMock()
        env_file = tmp_path / ".env"
        env_file.write_text("ADMIN_PASSWORD=fromenv\n")

        get_connection(
            server="myhost", database="mydb", user="myuser", password="mypass",
            dotenv_path=str(env_file),
        )

        kwargs = mock_mssql.connect.call_args[1]
        assert kwargs["server"] == "myhost"
        assert kwargs["database"] == "mydb"
        assert kwargs["uid"] == "myuser"
        assert kwargs["pwd"] == "mypass"

    def test_raises_when_no_password(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("")
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="No password supplied"):
                get_connection(dotenv_path=str(env_file))
