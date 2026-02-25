"""Tests for azsql_ct.queries -- SQL helpers with fake cursors."""

from __future__ import annotations

from azsql_ct import queries


class TestListTrackedTables:
    def test_returns_table_names(self, fake_cursor):
        cur = fake_cursor(results=[[("dbo.A",), ("dbo.B",)]])
        assert queries.list_tracked_tables(cur) == ["dbo.A", "dbo.B"]

    def test_returns_empty_when_none_tracked(self, fake_cursor):
        cur = fake_cursor(results=[[]])
        assert queries.list_tracked_tables(cur) == []


class TestResolveTable:
    def test_exact_match(self):
        assert queries.resolve_table("dbo.Foo", ["dbo.Foo", "dbo.Bar"]) == "dbo.Foo"

    def test_case_insensitive(self):
        assert queries.resolve_table("DBO.FOO", ["dbo.Foo"]) == "dbo.Foo"

    def test_unqualified_defaults_to_dbo(self):
        assert queries.resolve_table("Foo", ["dbo.Foo"]) == "dbo.Foo"

    def test_returns_none_when_not_found(self):
        assert queries.resolve_table("dbo.Missing", ["dbo.Foo"]) is None

    def test_strips_whitespace(self):
        assert queries.resolve_table("  dbo.Foo  ", ["dbo.Foo"]) == "dbo.Foo"


class TestCurrentVersion:
    def test_returns_version(self, fake_cursor):
        cur = fake_cursor(results=[[(42,)]])
        assert queries.current_version(cur) == 42


class TestMinValidVersion:
    def test_returns_version(self, fake_cursor):
        cur = fake_cursor(results=[[(10,)]])
        assert queries.min_valid_version_for_table(cur, "dbo.T") == 10

    def test_returns_zero_when_null(self, fake_cursor):
        cur = fake_cursor(results=[[(None,)]])
        assert queries.min_valid_version_for_table(cur, "dbo.T") == 0

    def test_returns_zero_when_no_row(self, fake_cursor):
        cur = fake_cursor(results=[[]])
        assert queries.min_valid_version_for_table(cur, "dbo.T") == 0


class TestPrimaryKeyColumns:
    def test_returns_columns(self, fake_cursor):
        cur = fake_cursor(results=[[("id",), ("tenant_id",)]])
        assert queries.primary_key_columns(cur, "dbo.T") == ["id", "tenant_id"]

    def test_returns_empty_when_no_pk(self, fake_cursor):
        cur = fake_cursor(results=[[]])
        assert queries.primary_key_columns(cur, "dbo.T") == []


class TestBuildFullQuery:
    def test_includes_ct_metadata_columns(self):
        sql = queries.build_full_query("dbo.Orders")
        assert "SYS_CHANGE_VERSION" in sql
        assert "SYS_CHANGE_CREATION_VERSION" in sql
        assert "SYS_CHANGE_OPERATION" in sql

    def test_uses_table_alias(self):
        sql = queries.build_full_query("dbo.Orders")
        assert "FROM dbo.Orders AS t" in sql

    def test_operation_is_L(self):
        sql = queries.build_full_query("dbo.Orders")
        assert "'L'" in sql


class TestBuildIncrementalQuery:
    def test_single_pk(self):
        sql = queries.build_incremental_query("dbo.Orders", ["id"])
        assert "CHANGETABLE(CHANGES dbo.Orders, ?)" in sql
        assert "t.[id] = ct.[id]" in sql

    def test_composite_pk(self):
        sql = queries.build_incremental_query("dbo.OrderLines", ["order_id", "line_id"])
        assert "t.[order_id] = ct.[order_id]" in sql
        assert "t.[line_id] = ct.[line_id]" in sql
