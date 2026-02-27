#!/usr/bin/env python3
"""Parse output.yaml (manifest of synced tables) into a flat list of table records.

Usage:
    python scripts/parse_output.py [path_to_output.yaml]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

try:
    import yaml
except ImportError:
    print("PyYAML is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(1)


def parse_yaml_to_table_dict(yaml_file_path: str | Path) -> List[Dict[str, Any]]:
    """Parse output manifest YAML into a flat list of table metadata dicts.

    Expects the structure produced by the sync output manifest:
    databases -> database_name -> uc_catalog_name, schema_name -> uc_schema_name,
    table_name -> file_path, file_type.

    Args:
        yaml_file_path: Path to the YAML file (e.g. output.yaml).

    Returns:
        List of dicts with keys: database_name, schema_name, table_name,
        file_path, file_type, uc_catalog_name, uc_schema_name,
        primary_key (list of column names, if present in manifest).
    """
    path = Path(yaml_file_path)
    with path.open() as f:
        data = yaml.safe_load(f)

    if not data or not isinstance(data, dict):
        return []

    databases = data.get("databases")
    if not databases or not isinstance(databases, dict):
        return []

    table_list: List[Dict[str, Any]] = []

    for database_name, database_info in databases.items():
        if not isinstance(database_info, dict):
            continue
        uc_catalog_name = database_info.get("uc_catalog_name")

        for schema_name, schema_info in database_info.items():
            if schema_name == "uc_catalog_name":
                continue
            if not isinstance(schema_info, dict):
                continue
            uc_schema_name = schema_info.get("uc_schema_name")

            for table_name, table_info in schema_info.items():
                if table_name == "uc_schema_name":
                    continue
                if not isinstance(table_info, dict):
                    continue
                table_list.append({
                    "database_name": database_name,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "file_path": table_info.get("file_path"),
                    "file_type": table_info.get("file_type"),
                    "uc_catalog_name": uc_catalog_name,
                    "uc_schema_name": uc_schema_name,
                    "primary_key": table_info.get("primary_key"),
                })

    return table_list


