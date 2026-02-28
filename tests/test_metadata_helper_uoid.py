"""Ensure metadata_helper._make_uoid matches azsql_ct.writer._make_uoid.

The DLT pipeline filters by table_id.uoid; both must produce identical UUIDs.
"""

from __future__ import annotations

import pytest

from azsql_ct.writer import _make_uoid as writer_uoid

# Import from ingestion_pipeline_examples (run pytest from project root)
from ingestion_pipeline_examples.metadata_helper import _make_uoid as metadata_uoid


class TestUoidConsistency:
    """metadata_helper and writer must produce identical UOIDs."""

    def test_same_uoid_for_simple_triple(self):
        a = writer_uoid("database_1", "dbo", "table_1")
        b = metadata_uoid("database_1", "dbo", "table_1")
        assert a == b

    def test_same_uoid_for_dotted_identifiers(self):
        """Dotted names must not collide; both use null-byte separator."""
        a = writer_uoid("a.b", "c", "d")
        b = metadata_uoid("a.b", "c", "d")
        assert a == b

    def test_different_triples_different_uoids(self):
        w1 = writer_uoid("db1", "dbo", "t1")
        w2 = writer_uoid("db2", "dbo", "t1")
        m1 = metadata_uoid("db1", "dbo", "t1")
        m2 = metadata_uoid("db2", "dbo", "t1")
        assert w1 != w2
        assert m1 != m2
        assert w1 == m1
        assert w2 == m2
