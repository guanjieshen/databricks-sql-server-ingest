"""Shared fixtures for azsql_ct tests."""

from __future__ import annotations

from typing import Any, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest


class FakeCursor:
    """Lightweight stand-in for a DB-API 2.0 cursor.

    Supply ``results`` as a list of lists -- each inner list is a set of rows
    returned by one successive ``execute()`` call.
    """

    def __init__(
        self,
        results: Optional[List[List[Tuple[Any, ...]]]] = None,
        descriptions: Optional[List[List[Tuple[str, ...]]]] = None,
    ) -> None:
        self._results = list(results or [])
        self._descriptions = list(descriptions or [])
        self._call_idx = -1
        self._rows: List[Tuple[Any, ...]] = []
        self.description: Optional[List[Tuple[str, ...]]] = None

    def execute(self, sql: str, params: Any = None) -> None:
        self._call_idx += 1
        if self._call_idx < len(self._results):
            self._rows = list(self._results[self._call_idx])
        else:
            self._rows = []
        if self._call_idx < len(self._descriptions):
            self.description = self._descriptions[self._call_idx]
        else:
            self.description = None

    def fetchall(self) -> List[Tuple[Any, ...]]:
        return self._rows

    def fetchmany(self, size: int = 1) -> List[Tuple[Any, ...]]:
        rows = self._rows[:size]
        self._rows = self._rows[size:]
        return rows

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return self._rows[0] if self._rows else None


@pytest.fixture()
def fake_cursor():
    """Return the ``FakeCursor`` *class* so tests can instantiate with custom data."""
    return FakeCursor


@pytest.fixture()
def mock_conn():
    """Return a ``MagicMock`` that looks like a DB-API 2.0 connection."""
    conn = MagicMock()
    return conn


