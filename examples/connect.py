#!/usr/bin/env python3
"""Example: verify connectivity to the Azure SQL server.

Usage:
    # Set credentials in .env or as environment variables, then:
    python examples/connect.py
"""

from azsql_ct import ChangeTracker
from azsql_ct.connection import load_dotenv
import os

load_dotenv()

ct = ChangeTracker(
    os.environ.get("SERVER", "guanjiesqldb.database.windows.net"),
    os.environ.get("ADMIN_USER", "sqladmin"),
    os.environ["ADMIN_PASSWORD"],
)

if ct.test_connectivity():
    print(f"Connected to {ct.server} as {ct.user}.")
else:
    print(f"FAILED to connect to {ct.server}.")
