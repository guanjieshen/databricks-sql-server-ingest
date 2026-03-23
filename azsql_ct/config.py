"""Config loading, table-map types, validation, and normalization."""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from ._constants import (
    DEFAULT_SCD_TYPE, DEFAULT_SOFT_DELETE, DEFAULT_SOFT_DELETE_COLUMN,
    VALID_MODES, VALID_SCD_TYPES,
)

_SECRETS_RE = re.compile(r"\{\{secrets/([^/]+)/([^}]+)\}\}")


def _get_dbutils() -> Any:
    """Return the Databricks ``dbutils`` object, or ``None`` outside a runtime."""
    main = sys.modules.get("__main__")
    return getattr(main, "dbutils", None) if main else None


def expand_env(value: str) -> str:
    """Expand ``${VAR}`` references in *value* with environment variables."""

    def _repl(m):
        name = m.group(1)
        if name not in os.environ:
            raise KeyError(
                f"Environment variable {name!r} is not set "
                f"(referenced in config as ${{{name}}})"
            )
        return os.environ[name]

    return re.sub(r"\$\{(\w+)}", _repl, str(value))


def resolve_secrets(value: str) -> str:
    """Resolve ``{{secrets/<scope>/<key>}}`` references via ``dbutils``.

    Raises :class:`RuntimeError` if a secret reference is found but
    ``dbutils`` is not available (i.e. running outside a Databricks runtime).
    """
    value = str(value)
    if not _SECRETS_RE.search(value):
        return value

    dbutils = _get_dbutils()
    if dbutils is None:
        raise RuntimeError(
            "{{secrets/…}} references require a Databricks runtime "
            "(dbutils is not available). Use ${ENV_VAR} syntax for "
            "local development, or run this job on Databricks."
        )

    def _repl(m: re.Match) -> str:
        scope, key = m.group(1), m.group(2)
        return dbutils.secrets.get(scope=scope, key=key)

    return _SECRETS_RE.sub(_repl, value)


def resolve_value(value: str) -> str:
    """Resolve both ``{{secrets/…}}`` and ``${VAR}`` references in *value*."""
    return expand_env(resolve_secrets(value))


def _load_config_file(path: Union[str, Path]) -> dict:
    """Load a YAML or JSON config file, chosen by extension."""
    p = Path(path)
    text = p.read_text()
    if p.suffix in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError:
            raise ImportError(
                "PyYAML is required for YAML config files. "
                "Install it with: pip install azsql_ct[yaml]"
            )
        return yaml.safe_load(text)
    return json.loads(text)


# Keys at the schema level that are metadata, not table names.
_SCHEMA_META_KEYS = frozenset({"uc_schema", "tables"})


def _normalize_table_map(raw: dict) -> dict:
    """Normalise a ``databases`` dict into the internal table-map format.

    Accepts both the **structured** layout (with ``schemas``/``tables`` keys
    and optional ``uc_catalog``/``uc_schema`` metadata) and the **legacy**
    flat layout where database keys map directly to schema dicts.

    Returns ``{db: {schema: {table: mode}}}`` (or list variant) with all
    metadata keys stripped.
    """
    result: dict = {}
    for db_name, db_value in raw.items():
        if not isinstance(db_value, dict):
            result[db_name] = db_value
            continue

        if "schemas" in db_value:
            schemas_section = db_value["schemas"]
            if not isinstance(schemas_section, dict):
                raise TypeError(
                    f"'schemas' for database '{db_name}' must be a dict, "
                    f"got {type(schemas_section).__name__}"
                )
            normalised_schemas: dict = {}
            for schema_name, schema_value in schemas_section.items():
                if not isinstance(schema_value, dict):
                    raise TypeError(
                        f"schema '{schema_name}' in database '{db_name}' "
                        f"must be a dict, got {type(schema_value).__name__}"
                    )
                if "tables" in schema_value:
                    normalised_schemas[schema_name] = schema_value["tables"]
                else:
                    normalised_schemas[schema_name] = {
                        k: v
                        for k, v in schema_value.items()
                        if k not in _SCHEMA_META_KEYS
                    }
            result[db_name] = normalised_schemas
        else:
            result[db_name] = db_value
    return result


def _extract_uc_metadata(raw: dict) -> Dict[str, Dict[str, Any]]:
    """Extract Unity Catalog metadata from a raw ``databases`` config dict.

    Returns ``{db: {"uc_catalog": value, "schemas": {schema: uc_schema_value}}}``
    for databases that use the structured format.  Returns an empty dict for
    legacy-format configs or databases without UC fields.
    """
    result: Dict[str, Dict[str, Any]] = {}
    for db_name, db_value in raw.items():
        if not isinstance(db_value, dict):
            continue
        uc_catalog = db_value.get("uc_catalog")
        schemas_section = db_value.get("schemas")
        if uc_catalog is None and schemas_section is None:
            continue
        schema_map: Dict[str, Any] = {}
        if isinstance(schemas_section, dict):
            for schema_name, schema_value in schemas_section.items():
                if isinstance(schema_value, dict):
                    uc_schema = schema_value.get("uc_schema")
                    if uc_schema is not None:
                        schema_map[schema_name] = uc_schema
        entry: Dict[str, Any] = {"schemas": schema_map}
        if uc_catalog is not None:
            entry["uc_catalog"] = uc_catalog
        result[db_name] = entry
    return result


def _flat_config_to_table_map(tables: List[dict]) -> Dict[str, Dict[str, Dict[str, str]]]:
    """Convert a flat ``[{"database": ..., "table": ..., "mode": ...}]`` list
    into the nested ``{db: {schema: {table: mode}}}`` format."""
    result: Dict[str, Dict[str, Dict[str, str]]] = {}
    for entry in tables:
        db = entry["database"]
        full_name = entry["table"]
        mode = entry.get("mode", "full_incremental")
        if "." in full_name:
            schema, table = full_name.split(".", 1)
        else:
            schema, table = "dbo", full_name
        result.setdefault(db, {}).setdefault(schema, {})[table] = mode
    return result


TableSpec = Union[List[str], Dict[str, Any]]
TableMap = Dict[str, Dict[str, TableSpec]]
FlatEntry = Tuple[str, str, Optional[str], int, bool, Optional[str]]


def _flatten_table_map(table_map: TableMap) -> List[FlatEntry]:
    """Convert a table map to ``[(db, "schema.table", mode, scd_type, soft_delete, soft_delete_column), ...]``.

    *mode_or_none* is ``None`` for list-format entries (no per-table mode).
    *scd_type* defaults to ``DEFAULT_SCD_TYPE`` (1) when not specified.
    *soft_delete* defaults to ``DEFAULT_SOFT_DELETE`` (False) when not specified.
    *soft_delete_column* is ``None`` when not specified (resolved later to
    the pipeline-level default or ``DEFAULT_SOFT_DELETE_COLUMN``).
    """
    entries: List[FlatEntry] = []
    for database, schemas in table_map.items():
        for schema, tables in schemas.items():
            if isinstance(tables, dict):
                for table, tbl_cfg in tables.items():
                    if isinstance(tbl_cfg, dict):
                        mode = tbl_cfg.get("mode")
                        scd_type = tbl_cfg.get("scd_type", DEFAULT_SCD_TYPE)
                        soft_delete = tbl_cfg.get("soft_delete", DEFAULT_SOFT_DELETE)
                        soft_delete_column = tbl_cfg.get("soft_delete_column")
                        entries.append((database, f"{schema}.{table}", mode, scd_type, soft_delete, soft_delete_column))
                    else:
                        entries.append((database, f"{schema}.{table}", tbl_cfg, DEFAULT_SCD_TYPE, DEFAULT_SOFT_DELETE, None))
            else:
                for table in tables:
                    entries.append((database, f"{schema}.{table}", None, DEFAULT_SCD_TYPE, DEFAULT_SOFT_DELETE, None))
    return entries


def _validate_table_map(value: object) -> TableMap:
    """Raise ``TypeError`` / ``ValueError`` if *value* is not a valid table map."""
    if not isinstance(value, dict):
        raise TypeError(f"tables must be a dict, got {type(value).__name__}")
    for db, schemas in value.items():
        if not isinstance(db, str):
            raise TypeError(f"database key must be str, got {type(db).__name__}")
        if not isinstance(schemas, dict):
            raise TypeError(
                f"schemas for database '{db}' must be a dict, "
                f"got {type(schemas).__name__}"
            )
        for schema, tables in schemas.items():
            if not isinstance(schema, str):
                raise TypeError(
                    f"schema key must be str, got {type(schema).__name__}"
                )
            if isinstance(tables, dict):
                if not tables:
                    raise ValueError(
                        f"table dict for '{db}'.'{schema}' must not be empty"
                    )
                for tbl, tbl_cfg in tables.items():
                    if not isinstance(tbl, str):
                        raise TypeError(
                            f"table name must be str, got {type(tbl).__name__}"
                        )
                    if isinstance(tbl_cfg, str):
                        if tbl_cfg not in VALID_MODES:
                            raise ValueError(
                                f"mode for '{db}'.'{schema}'.'{tbl}' must be one "
                                f"of {sorted(VALID_MODES)}, got {tbl_cfg!r}"
                            )
                    elif isinstance(tbl_cfg, dict):
                        mode = tbl_cfg.get("mode")
                        if mode not in VALID_MODES:
                            raise ValueError(
                                f"mode for '{db}'.'{schema}'.'{tbl}' must be one "
                                f"of {sorted(VALID_MODES)}, got {mode!r}"
                            )
                        scd_type = tbl_cfg.get("scd_type", DEFAULT_SCD_TYPE)
                        if scd_type not in VALID_SCD_TYPES:
                            raise ValueError(
                                f"scd_type for '{db}'.'{schema}'.'{tbl}' must be one "
                                f"of {sorted(VALID_SCD_TYPES)}, got {scd_type!r}"
                            )
                        soft_delete = tbl_cfg.get("soft_delete", DEFAULT_SOFT_DELETE)
                        if not isinstance(soft_delete, bool):
                            raise ValueError(
                                f"soft_delete for '{db}'.'{schema}'.'{tbl}' must be "
                                f"a bool, got {soft_delete!r}"
                            )
                        soft_delete_col = tbl_cfg.get("soft_delete_column")
                        if soft_delete_col is not None and not isinstance(soft_delete_col, str):
                            raise ValueError(
                                f"soft_delete_column for '{db}'.'{schema}'.'{tbl}' must be "
                                f"a string, got {soft_delete_col!r}"
                            )
                    else:
                        raise TypeError(
                            f"table config for '{db}'.'{schema}'.'{tbl}' must be "
                            f"a mode string or a dict, got {type(tbl_cfg).__name__}"
                        )
            elif isinstance(tables, list):
                if not tables:
                    raise ValueError(
                        f"table list for '{db}'.'{schema}' must not be empty"
                    )
            else:
                raise TypeError(
                    f"tables for '{db}'.'{schema}' must be a list or dict, "
                    f"got {type(tables).__name__}"
                )
    if not value:
        raise ValueError("tables dict must not be empty")
    return value  # type: ignore[return-value]
