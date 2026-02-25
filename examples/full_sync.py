#!/usr/bin/env python3
"""Example: sync tables with per-table mode using the ChangeTracker SDK.

Demonstrates both formats:
  - dict format: specify "full" or "incremental" per table
  - .sync() respects the per-table modes

Usage:
    python examples/full_sync.py
"""

import logging
import os
import time
from pathlib import Path

import yaml

from azsql_ct import ChangeTracker
from azsql_ct.connection import load_dotenv

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
load_dotenv()

ct = ChangeTracker(
    os.environ.get("SERVER", "guanjiesqldb.database.windows.net"),
    os.environ.get("ADMIN_USER", "sqladmin"),
    os.environ["ADMIN_PASSWORD"],
    max_workers=8,
)

tables_path = Path(__file__).with_name("tables.yaml")
with open(tables_path) as f:
    ct.tables = yaml.safe_load(f)

num_tables = sum(
    len(tbls) for schemas in ct.tables.values() for tbls in schemas.values()
)
logger.info("Tracker: %s", ct)
logger.info("Tables configured: %d across %d database(s)", num_tables, len(ct.tables))

wall_start = time.monotonic()
results = ct.sync()
wall_elapsed = time.monotonic() - wall_start

logger.info("=" * 80)
logger.info("SYNC SUMMARY")
logger.info("=" * 80)

total_rows = 0
total_files = 0
errors = []

for r in results:
    table_fqn = f"{r['database']}.{r['table']}"
    if r.get("status") == "error":
        errors.append(r)
        logger.error("  %-40s  ERROR: %s", table_fqn, r.get("error", "unknown"))
        continue

    rows = r["rows_written"]
    mode = r["mode"]
    version = r["current_version"]
    duration = r.get("duration_seconds", 0)
    files = r.get("files", [])
    since = r.get("since_version")
    total_rows += rows
    total_files += len(files)

    since_str = f" (since v{since})" if since is not None else ""
    logger.info(
        "  %-40s  %8d rows | %-12s | v%-8d | %6.1fs | %d file(s)%s",
        table_fqn, rows, mode, version, duration, len(files), since_str,
    )

logger.info("-" * 80)
logger.info(
    "Completed: %d table(s) | %d total rows | %d file(s) | %.1fs wall time | %d worker(s)",
    len(results) - len(errors), total_rows, total_files, wall_elapsed, ct.max_workers,
)
if errors:
    logger.warning("Failed: %d table(s)", len(errors))
logger.info("=" * 80)
