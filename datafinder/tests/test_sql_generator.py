from datafinder import StringAttribute, build_query_operation, select_sql_to_string
from datafinder.sql_generator import SQLQueryGenerator
from model.relational import Table, Column, NoOperation


def _make_table_and_attr():
    col = Column("NAME", "VARCHAR")
    table = Table("accounts", [col])
    attr = StringAttribute("Name", "NAME", "VARCHAR", "accounts")
    return table, attr


class TestNoOperationFilter:

    def test_build_filter_returns_empty_string(self):
        gen = SQLQueryGenerator()
        assert gen.build_filter(NoOperation()) == ""

    def test_no_filter_produces_no_where_clause(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, NoOperation())
        sql = select_sql_to_string(select_op)
        assert "WHERE" not in sql

    def test_no_filter_still_produces_select_and_from(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, NoOperation())
        sql = select_sql_to_string(select_op)
        assert "SELECT" in sql
        assert "FROM" in sql
        assert "accounts" in sql

    def test_with_filter_produces_where_clause(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, attr.eq("Acme"))
        sql = select_sql_to_string(select_op)
        assert "WHERE" in sql
        assert "'Acme'" in sql


class TestStringOperations:

    def test_ne_produces_not_equal(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, attr.ne("Acme"))
        sql = select_sql_to_string(select_op)
        assert "<>" in sql
        assert "'Acme'" in sql

    def test_contains_produces_like_with_wildcards(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, attr.contains("corp"))
        sql = select_sql_to_string(select_op)
        assert "LIKE" in sql
        assert "'%corp%'" in sql

    def test_starts_with_produces_like_with_trailing_wildcard(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, attr.starts_with("Acme"))
        sql = select_sql_to_string(select_op)
        assert "LIKE" in sql
        assert "'Acme%'" in sql

    def test_ends_with_produces_like_with_leading_wildcard(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, attr.ends_with("Inc"))
        sql = select_sql_to_string(select_op)
        assert "LIKE" in sql
        assert "'%Inc'" in sql

    def test_ne_no_like_in_sql(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, attr.ne("Acme"))
        sql = select_sql_to_string(select_op)
        assert "LIKE" not in sql

    def test_contains_has_where_clause(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, attr.contains("corp"))
        sql = select_sql_to_string(select_op)
        assert "WHERE" in sql

    def test_starts_with_has_where_clause(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, attr.starts_with("Acme"))
        sql = select_sql_to_string(select_op)
        assert "WHERE" in sql

    def test_ends_with_has_where_clause(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, attr.ends_with("Inc"))
        sql = select_sql_to_string(select_op)
        assert "WHERE" in sql
