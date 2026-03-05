"""Canonical _make_uoid for the lakeflow_pipeline package.

Must stay identical to azsql_ct.writer._make_uoid — consistency is verified
by tests/test_metadata_helper_uoid.py.
"""

import uuid


def _make_uoid(database: str, schema: str, table: str) -> str:
    """Deterministic UUID5 from the (database, schema, table) triple.

    Uses a null-byte separator so that dotted identifiers like
    ``("a.b", "c", "d")`` and ``("a", "b.c", "d")`` never collide.
    """
    key = f"{database}\x00{schema}\x00{table}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, key))
