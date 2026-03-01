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


class TestTableColumns:
    def test_returns_column_names(self, fake_cursor):
        cur = fake_cursor(
            results=[[]],
            descriptions=[[("id",), ("name",), ("age",)]],
        )
        assert queries.table_columns(cur, "dbo.T") == ["id", "name", "age"]

    def test_executes_select_top_0(self, fake_cursor):
        cur = fake_cursor(
            results=[[]],
            descriptions=[[("id",)]],
        )
        queries.table_columns(cur, "dbo.T")
        assert cur.last_sql == "SELECT TOP 0 * FROM dbo.T"


class TestBuildIncrementalQuery:
    def test_single_pk(self):
        sql = queries.build_incremental_query("dbo.Orders", ["id"], ["id", "name", "total"])
        assert "CHANGETABLE(CHANGES dbo.Orders, ?)" in sql
        assert "t.[id] = ct.[id]" in sql
        assert "COALESCE(t.[id], ct.[id]) AS [id]" in sql
        assert "t.[name]" in sql
        assert "t.[total]" in sql

    def test_composite_pk(self):
        sql = queries.build_incremental_query(
            "dbo.OrderLines", ["order_id", "line_id"], ["order_id", "line_id", "qty"],
        )
        assert "t.[order_id] = ct.[order_id]" in sql
        assert "t.[line_id] = ct.[line_id]" in sql
        assert "COALESCE(t.[order_id], ct.[order_id]) AS [order_id]" in sql
        assert "COALESCE(t.[line_id], ct.[line_id]) AS [line_id]" in sql
        assert "t.[qty]" in sql

    def test_ct_columns_cast_to_match_full_query_schema(self):
        """Incremental query casts CT columns so Parquet schema matches full load."""
        sql = queries.build_incremental_query("dbo.T", ["id"], ["id"])
        assert "CAST(ct.SYS_CHANGE_CREATION_VERSION AS BIGINT)" in sql
        assert "CAST(ct.SYS_CHANGE_OPERATION AS NCHAR(1))" in sql
        assert "SYS_CHANGE_VERSION" in sql
        assert "SYS_CHANGE_CREATION_VERSION" in sql
        assert "SYS_CHANGE_OPERATION" in sql

    def test_coalesce_only_on_pk_columns(self):
        sql = queries.build_incremental_query("dbo.T", ["id"], ["id", "name", "status"])
        assert "COALESCE(t.[id], ct.[id])" in sql
        assert "COALESCE" not in sql.split("t.[name]")[0].split("AS [id]")[1]
        assert "t.[name]" in sql
        assert "t.[status]" in sql

    def test_no_t_star_in_incremental_query(self):
        sql = queries.build_incremental_query("dbo.T", ["id"], ["id", "col1"])
        assert "t.*" not in sql

    def test_pk_case_insensitive_match(self):
        sql = queries.build_incremental_query("dbo.T", ["Id"], ["id", "name"])
        assert "COALESCE(t.[id], ct.[id]) AS [id]" in sql
        assert "t.[name]" in sql


class TestSpecialColumnNames:
    """Edge cases for column names with spaces, dots, reserved words, and brackets."""

    def test_column_with_spaces(self):
        sql = queries.build_incremental_query(
            "dbo.T", ["id"], ["id", "first name", "last name"],
        )
        assert "t.[first name]" in sql
        assert "t.[last name]" in sql

    def test_column_with_dots(self):
        sql = queries.build_incremental_query(
            "dbo.T", ["id"], ["id", "user.email"],
        )
        assert "t.[user.email]" in sql

    def test_reserved_word_columns(self):
        sql = queries.build_incremental_query(
            "dbo.T", ["order"], ["order", "select", "from"],
        )
        assert "COALESCE(t.[order], ct.[order]) AS [order]" in sql
        assert "t.[select]" in sql
        assert "t.[from]" in sql

    def test_column_with_closing_bracket(self):
        """A ] inside a column name must be escaped as ]] in bracket quoting."""
        sql = queries.build_incremental_query(
            "dbo.T", ["id"], ["id", "val]ue"],
        )
        assert "t.[val]]ue]" in sql

    def test_pk_column_with_spaces(self):
        sql = queries.build_incremental_query(
            "dbo.T", ["order id"], ["order id", "qty"],
        )
        assert "COALESCE(t.[order id], ct.[order id]) AS [order id]" in sql
        assert "t.[order id] = ct.[order id]" in sql


class TestSpecialTableNames:
    """Edge cases for table names passed to query builders."""

    def test_build_full_query_with_bracket_quoted_table(self):
        sql = queries.build_full_query("dbo.[Order Details]")
        assert "FROM dbo.[Order Details] AS t" in sql

    def test_resolve_table_non_dbo_schema(self):
        assert queries.resolve_table("sales.Orders", ["sales.Orders"]) == "sales.Orders"

    def test_resolve_table_three_part_name_not_prefixed(self):
        """A name containing a dot is never prefixed with 'dbo.'."""
        result = queries.resolve_table("catalog.dbo.T", ["catalog.dbo.T"])
        assert result == "catalog.dbo.T"


class TestBuildChangeCheckQuery:
    def test_empty_dict_returns_empty_string(self):
        assert queries.build_change_check_query({}) == ""

    def test_single_table(self):
        sql = queries.build_change_check_query({"dbo.Orders": 2248})
        assert "SELECT 'dbo.Orders' AS table_name" in sql
        assert "CHANGETABLE(CHANGES dbo.Orders, 2248)" in sql

    def test_multiple_tables_different_versions(self):
        sql = queries.build_change_check_query({
            "dbo.orders": 2248,
            "dbo.customers": 2100,
        })
        assert "UNION ALL" in sql
        assert "dbo.orders" in sql and "2248" in sql
        assert "dbo.customers" in sql and "2100" in sql

    def test_uses_exists_for_short_circuit(self):
        sql = queries.build_change_check_query({"dbo.T": 1})
        assert "EXISTS(SELECT 1 FROM CHANGETABLE" in sql


class TestFetchTablesWithChanges:
    def test_returns_set_of_table_names(self, fake_cursor):
        cur = fake_cursor(results=[[("dbo.orders",), ("dbo.customers",)]])
        result = queries.fetch_tables_with_changes(
            cur, {"dbo.orders": 100, "dbo.customers": 200},
        )
        assert result == {"dbo.orders", "dbo.customers"}

    def test_empty_watermarks_returns_empty_set(self, fake_cursor):
        cur = fake_cursor()
        result = queries.fetch_tables_with_changes(cur, {})
        assert result == set()

    def test_empty_result_when_no_changes(self, fake_cursor):
        cur = fake_cursor(results=[[]])
        result = queries.fetch_tables_with_changes(cur, {"dbo.T": 100})
        assert result == set()


class TestMinValidVersionsBatch:
    def test_returns_dict(self, fake_cursor):
        cur = fake_cursor(results=[[("dbo.T1", 10), ("dbo.T2", 20)]])
        result = queries.min_valid_versions_batch(cur, ["dbo.T1", "dbo.T2"])
        assert result == {"dbo.T1": 10, "dbo.T2": 20}

    def test_handles_null_as_zero(self, fake_cursor):
        cur = fake_cursor(results=[[("dbo.T", None)]])
        result = queries.min_valid_versions_batch(cur, ["dbo.T"])
        assert result == {"dbo.T": 0}

    def test_empty_list_returns_empty_dict(self, fake_cursor):
        cur = fake_cursor()
        result = queries.min_valid_versions_batch(cur, [])
        assert result == {}

    def test_missing_tables_use_zero(self, fake_cursor):
        cur = fake_cursor(results=[[("dbo.T1", 5)]])
        result = queries.min_valid_versions_batch(cur, ["dbo.T1", "dbo.Missing"])
        assert result == {"dbo.T1": 5}
