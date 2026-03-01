"""Incremental output manifest: tables with changes from the current sync run.

Writes ``incremental_output.yaml`` containing only tables where
``rows_written > 0``, plus a ``generated_at`` timestamp. Created fresh on
each sync run (no merge semantics).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .output_manifest import merge_add, save

logger = logging.getLogger(__name__)


def _is_success_result(result: dict) -> bool:
    """True if result is a successful sync (no error)."""
    if result.get("status") == "error":
        return False
    if "database" not in result or "table" not in result:
        return False
    return True


def write(
    sync_results: List[dict],
    output_dir: str,
    file_type: str,
    path: str,
    *,
    uc_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
    ingest_pipeline: Optional[str] = None,
) -> None:
    """Write incremental manifest with only tables that had rows_written > 0.

    Args:
        sync_results: Per-table sync result dicts from sync_table.
        output_dir: Root directory for data files (used for file_path).
        file_type: Writer file type (e.g. "parquet").
        path: Output file path (e.g. {ingest_pipeline}/incremental_output.yaml).
        uc_metadata: Optional UC catalog/schema metadata per database.
        ingest_pipeline: Optional ingest pipeline path to include in manifest.
    """
    changed = [
        r for r in sync_results
        if _is_success_result(r) and r.get("rows_written", 0) > 0
    ]
    manifest: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "databases": {},
    }
    if ingest_pipeline is not None:
        manifest["ingest_pipeline"] = ingest_pipeline

    merge_add(manifest, changed, output_dir, file_type, uc_metadata=uc_metadata)
    save(path, manifest)
    logger.debug("Wrote incremental output manifest %s (%d table(s) with changes)", path, len(changed))
