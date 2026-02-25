#!/usr/bin/env python3
"""Sync tables using configuration from tables.yaml.

Usage:
    python examples/sync.py
"""

import logging

from azsql_ct import ChangeTracker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

if __name__ == "__main__":
    ct = ChangeTracker.from_config("examples/tables.yaml")
    print(ct)
    results = ct.sync()
    for r in results:
        status = r.get("status", r.get("mode", ""))
        print(f"  {r['database']}.{r['table']}: {status} ({r.get('rows_written', 0)} rows)")
