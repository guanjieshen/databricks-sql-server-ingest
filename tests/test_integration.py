"""Integration test: verify the restricted login sees only allowed databases.

Requires a live Azure SQL database with TEST_PASSWORD set in the environment
or .env file. Skipped by default; run with ``pytest -m integration``.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.integration

pyodbc = pytest.importorskip("pyodbc")

from azsql_ct import AzureSQLConnection
from azsql_ct.connection import load_dotenv

ALLOWED_NAMES = {"master", "database_1"}


@pytest.fixture(scope="module")
def test_conn():
    """Open a connection as the restricted *test* user."""
    load_dotenv()
    user = os.environ.get("TEST_USER", "test")
    password = os.environ.get("TEST_PASSWORD")
    if not password:
        pytest.skip("TEST_PASSWORD not set -- skipping integration tests")
    az = AzureSQLConnection(user=user, password=password, database="master")
    conn = az.connect()
    yield conn
    az.close()


def test_allowed_databases_returns_expected(test_conn):
    cursor = test_conn.cursor()
    cursor.execute("SELECT name FROM dbo.AllowedDatabases ORDER BY name")
    rows = cursor.fetchall()
    names = {row[0].strip() for row in rows}
    assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
    assert names == ALLOWED_NAMES


def test_sys_databases_denied_or_limited(test_conn):
    cursor = test_conn.cursor()
    try:
        cursor.execute("SELECT name FROM sys.databases WHERE state = 0")
        rows = cursor.fetchall()
        names = {row[0].strip() for row in rows}
        assert names == ALLOWED_NAMES, (
            f"sys.databases should not expose extra databases; got {names}"
        )
    except pyodbc.Error:
        pass
