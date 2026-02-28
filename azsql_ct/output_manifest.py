"""Output manifest: record where synced files are written.

The manifest is a YAML file listing each synced table with its output
directory (file_path), file type, and UC catalog/schema names populated
from pipeline config metadata.

Update semantics: only new databases, schemas, or tables are added;
existing entries are never overwritten so user-filled values are preserved.

Column schemas are stored separately in per-table ``schema.json`` files
(see :mod:`azsql_ct.schema`).
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

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


def _has_output_files(result: dict) -> bool:
    """True if the sync result produced at least one output file."""
    if "files" in result:
        return bool(result["files"])
    if "rows_written" in result:
        return result["rows_written"] > 0
    return True


def merge_add(
    manifest: Dict[str, Any],
    sync_results: List[dict],
    output_dir: str,
    file_type: str,
    *,
    uc_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    """Add new tables from sync results to manifest (in place).

    Only successful results are considered. For each (database, schema, table)
    that does not already exist in the manifest, add an entry with
    file_path and file_type set.  ``uc_catalog_name`` and ``uc_schema_name``
    are populated from *uc_metadata* when available, otherwise ``None``.
    Existing entries are never overwritten.
    """
    databases = manifest.setdefault("databases", {})
    if not isinstance(databases, dict):
        manifest["databases"] = databases = {}

    uc = uc_metadata or {}

    for result in sync_results:
        if not _is_success_result(result):
            continue
        if not _has_output_files(result):
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

        db_uc = uc.get(database, {})
        catalog_value = db_uc.get("uc_catalog")
        schema_value = db_uc.get("schemas", {}).get(schema)

        if database not in databases:
            databases[database] = {"uc_catalog_name": catalog_value}
        db_node = databases[database]
        if not isinstance(db_node, dict):
            db_node = {"uc_catalog_name": catalog_value}
            databases[database] = db_node
        if "uc_catalog_name" not in db_node:
            db_node = {"uc_catalog_name": catalog_value, **db_node}
            databases[database] = db_node

        if schema not in db_node:
            db_node[schema] = {"uc_schema_name": schema_value}
        schema_node = db_node[schema]
        if not isinstance(schema_node, dict):
            schema_node = {"uc_schema_name": schema_value}
            db_node[schema] = schema_node
        if "uc_schema_name" not in schema_node:
            schema_node = {"uc_schema_name": schema_value, **schema_node}
            db_node[schema] = schema_node

        if table in schema_node:
            continue
        primary_key = result.get("primary_key")
        scd_type = result.get("scd_type")
        schema_node[table] = _table_entry(
            file_path, file_type, primary_key=primary_key,
            uc_table_name=table, scd_type=scd_type,
        )


def _table_entry(
    file_path: str,
    file_type: str,
    primary_key: Optional[List[str]] = None,
    uc_table_name: Optional[str] = None,
    scd_type: Optional[int] = None,
) -> Dict[str, Any]:
    """Build table node with file_path, file_type, scd_type, and optional primary_key."""
    entry: Dict[str, Any] = {
        "uc_table_name": uc_table_name,
        "file_path": file_path,
        "file_type": file_type,
    }
    if scd_type is not None:
        entry["scd_type"] = scd_type
    if primary_key is not None:
        entry["primary_key"] = primary_key
    return entry


def save(path: str, manifest: Dict[str, Any]) -> None:
    """Write manifest to YAML file."""
    try:
        import yaml
    except ImportError:
        raise ImportError(
            "PyYAML is required for output manifest. "
            "Install it with: pip install azsql_ct[yaml]"
        )

    class _IndentedDumper(yaml.SafeDumper):
        pass

    def _indented_increase_indent(self, flow=False, indentless=False):
        return yaml.SafeDumper.increase_indent(self, flow, False)

    _IndentedDumper.increase_indent = _indented_increase_indent

    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(
            manifest,
            f,
            Dumper=_IndentedDumper,
            default_flow_style=False,
            sort_keys=False,
        )
