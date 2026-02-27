#!/usr/bin/env python3
"""Verify connectivity to the Azure SQL server.

Usage:
    python scripts/connect.py
"""

from azsql_ct import ChangeTracker

ct = ChangeTracker.from_config("pipelines/pipeline_1.yaml")

if ct.test_connectivity():
    print(f"Connected to {ct.server} as {ct.user}.")
else:
    print(f"FAILED to connect to {ct.server} as {ct.user}.")
    raise SystemExit(1)
