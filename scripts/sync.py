#!/usr/bin/env python3
"""Sync tables using configuration from a YAML file.

Usage:
    python scripts/sync.py [config]
"""

import argparse
import logging

from azsql_ct import ChangeTracker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync tables from a YAML config.")
    parser.add_argument(
        "config",
        nargs="?",
        default="pipelines/pipeline_1.yaml",
        help="Path to YAML config file (default: pipelines/pipeline_1.yaml)",
    )
    args = parser.parse_args()

    ct = ChangeTracker.from_config(args.config)
    print(ct)
    results = ct.sync()
    for r in results:
        status = r.get("status", r.get("mode", ""))
        print(f"  {r['database']}.{r['table']}: {status} ({r.get('rows_written', 0)} rows)")
