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
