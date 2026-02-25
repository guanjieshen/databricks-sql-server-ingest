#!/usr/bin/env python3
"""Example: verify connectivity to the Azure SQL server.

Usage:
    # Set credentials in .env or as environment variables, then:
    python examples/connect.py

On Databricks serverless, just ``%pip install mssql-python`` — no ODBC
driver install required.
"""

from azsql_ct.connection import AzureSQLConnection, load_dotenv
import os
import sys

load_dotenv()

server = os.environ.get("SERVER", "guanjiesqldb.database.windows.net")
user = os.environ.get("ADMIN_USER", "sqladmin")
password = os.environ.get("ADMIN_PASSWORD", "")

if not password:
    print("ERROR: ADMIN_PASSWORD is not set in .env or environment.")
    sys.exit(1)

az = AzureSQLConnection(server=server, user=user, password=password, database="master")

try:
    conn = az.connect()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchone()
    print(f"Connected to {server} as {user}.")
except Exception as exc:
    print(f"FAILED to connect to {server} as {user}.")
    print(f"  Error type : {type(exc).__name__}")
    print(f"  Detail     : {exc}")
    sys.exit(1)
finally:
    az.close()
