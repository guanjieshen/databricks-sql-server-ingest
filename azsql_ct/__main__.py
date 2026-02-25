"""CLI entry-point:  python -m azsql_ct [OPTIONS]

Examples:
    python -m azsql_ct --config sync_config.json
    python -m azsql_ct --config sync_config.json --output-dir ./data --watermark-dir ./watermarks
"""

import argparse
import json
import logging
import sys

from .sync import sync_from_config

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="azsql_ct",
        description="Incrementally sync Azure SQL change-tracked tables to CSV.",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to JSON config file listing databases/tables to sync",
    )
    parser.add_argument(
        "--output-dir",
        default="./data",
        help="Root directory for CSV data files (default: ./data)",
    )
    parser.add_argument(
        "--watermark-dir",
        default="./watermarks",
        help="Root directory for watermark files (default: ./watermarks)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug-level logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        with open(args.config) as f:
            config = json.load(f)
    except Exception as exc:
        logger.error("Failed to load config: %s", exc)
        sys.exit(1)

    try:
        results = sync_from_config(
            config, output_dir=args.output_dir, watermark_dir=args.watermark_dir
        )
    except Exception as exc:
        logger.error("Sync failed: %s", exc)
        sys.exit(1)

    for r in results:
        print(json.dumps(r, indent=2))


if __name__ == "__main__":
    main()
