"""Output manifest: record where synced files are written.

The manifest is a YAML file listing each synced table with its output
directory (file_path), file type, and user-fillable UC fields
(uc_catalog_name, uc_schema_name, uc_table_name) left blank.

Update semantics: only new databases, schemas, or tables are added;
existing entries are never overwritten so user-filled values are preserved.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def load(path: str) -> Dict[str, Any]:
    """Load manifest from YAML file.

    Returns ``{"databases": {}}`` if the file is missing or empty.
    """
    if not os.path.isfile(path):
        return {"databases": {}}
    try:
        import yaml
    except ImportError:
        raise ImportError(
            "PyYAML is required for output manifest. "
            "Install it with: pip install azsql_ct[yaml]"
        )
    with open(path) as f:
        data = yaml.safe_load(f)
    if not data or not isinstance(data, dict):
        return {"databases": {}}
    if "databases" not in data or not isinstance(data["databases"], dict):
        return {"databases": {}}
    return data


def _is_success_result(result: dict) -> bool:
    """True if result is a successful sync (no error)."""
    if result.get("status") == "error":
        return False
    if "database" not in result or "table" not in result:
        return False
    return True


def merge_add(
    manifest: Dict[str, Any],
    sync_results: List[dict],
    output_dir: str,
    file_type: str,
) -> None:
    """Add new tables from sync results to manifest (in place).

    Only successful results are considered. For each (database, schema, table)
    that does not already exist in the manifest, add an entry with
    uc_catalog_name, uc_schema_name, uc_table_name left blank and
    file_path, file_type set. Existing entries are never overwritten.
    """
    databases = manifest.setdefault("databases", {})
    if not isinstance(databases, dict):
        manifest["databases"] = databases = {}

    for result in sync_results:
        if not _is_success_result(result):
            continue
        database = result["database"]
        full_table = result["table"]
        if "." not in full_table:
            continue
        schema, table = full_table.split(".", 1)

        # Table entry already exists: do not overwrite
        if database in databases and isinstance(databases[database], dict):
            db_node = databases[database]
            if schema in db_node and isinstance(db_node[schema], dict):
                schema_node = db_node[schema]
                if table in schema_node and isinstance(schema_node[table], dict):
                    continue

        file_path = os.path.join(output_dir, database, schema, table)

        if database not in databases:
            databases[database] = {"uc_catalog_name": None}
        db_node = databases[database]
        if not isinstance(db_node, dict):
            db_node = {"uc_catalog_name": None}
            databases[database] = db_node
        if "uc_catalog_name" not in db_node:
            db_node = {"uc_catalog_name": None, **db_node}
            databases[database] = db_node

        if schema not in db_node:
            db_node[schema] = {"uc_schema_name": None}
        schema_node = db_node[schema]
        if not isinstance(schema_node, dict):
            schema_node = {"uc_schema_name": None}
            db_node[schema] = schema_node
        if "uc_schema_name" not in schema_node:
            schema_node = {"uc_schema_name": None, **schema_node}
            db_node[schema] = schema_node

        if table in schema_node:
            continue
        schema_node[table] = _table_entry(file_path, file_type)


def _table_entry(file_path: str, file_type: str) -> Dict[str, Any]:
    """Build table node with uc_table_name first, then file_path, file_type."""
    return {
        "uc_table_name": None,
        "file_path": file_path,
        "file_type": file_type,
    }


def save(path: str, manifest: Dict[str, Any]) -> None:
    """Write manifest to YAML file."""
    try:
        import yaml
    except ImportError:
        raise ImportError(
            "PyYAML is required for output manifest. "
            "Install it with: pip install azsql_ct[yaml]"
        )
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump(manifest, f, default_flow_style=False, sort_keys=False)
