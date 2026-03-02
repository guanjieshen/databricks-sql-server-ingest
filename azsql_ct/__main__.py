"""CLI entry-point:  python -m azsql_ct [OPTIONS]

Examples:
    python -m azsql_ct --config tables.yaml
    python -m azsql_ct --config tables.yaml --workers 8 -v
"""

import argparse
import json
import logging
import sys

from .client import ChangeTracker, set_databricks_task_values

logger = logging.getLogger(__name__)


def _log_summary(results: list) -> None:
    """Print a human-readable summary of sync results."""
    total_rows = 0
    total_files = 0
    errors = []

    logger.info("=" * 72)
    logger.info("SYNC SUMMARY")
    logger.info("=" * 72)

    for r in results:
        table_fqn = f"{r['database']}.{r['table']}"

        if r.get("status") == "error":
            errors.append(r)
            logger.error(
                "  %-40s  ERROR: %s", table_fqn, r.get("error", "unknown"),
            )
            continue

        rows = r["rows_written"]
        files = r.get("files", [])
        since = r.get("since_version")
        total_rows += rows
        total_files += len(files)

        since_str = f" (since v{since})" if since is not None else ""
        logger.info(
            "  %-40s  %8d rows | %-12s | v%-8d | %6.1fs | %d file(s)%s",
            table_fqn, rows, r["mode"], r["current_version"],
            r.get("duration_seconds", 0), len(files), since_str,
        )

    logger.info("-" * 72)
    logger.info(
        "Completed: %d table(s) | %d total rows | %d file(s)",
        len(results) - len(errors), total_rows, total_files,
    )
    if errors:
        logger.warning("Failed: %d table(s)", len(errors))
    logger.info("=" * 72)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="azsql_ct",
        description="Incrementally sync Azure SQL change-tracked tables to CSV.",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to YAML or JSON config file",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Root directory for CSV data files (overrides config)",
    )
    parser.add_argument(
        "--watermark-dir",
        default=None,
        help="Root directory for watermark files (overrides config)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of tables to sync in parallel (overrides config)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug-level logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        ct = ChangeTracker.from_config(
            args.config,
            max_workers=args.workers,
            output_dir=args.output_dir,
            watermark_dir=args.watermark_dir,
        )
    except Exception as exc:
        logger.error("Failed to load config: %s", exc)
        sys.exit(1)

    logger.info("Tracker: %s", ct)

    try:
        results = ct.sync()
    except Exception as exc:
        logger.error("Sync failed: %s", exc)
        sys.exit(1)

    set_databricks_task_values(results)
    _log_summary(results)

    for r in results:
        print(json.dumps(r, indent=2))


if __name__ == "__main__":
    main()
