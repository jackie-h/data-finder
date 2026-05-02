from datafinder import StringAttribute, build_query_operation, select_sql_to_string
from datafinder import IntegerAttribute
from datafinder.sql_generator import SQLQueryGenerator
from model.relational import Table, Column, NoOperation


def _make_table_and_attr():
    col = Column("NAME", "VARCHAR")
    table = Table("accounts", [col])
    attr = StringAttribute("Name", "NAME", "VARCHAR", "accounts")
    return table, attr


def _make_multi_col_table():
    name_col = Column("NAME", "VARCHAR")
    id_col = Column("ID", "INT")
    table = Table("accounts", [name_col, id_col])
    name_attr = StringAttribute("Name", "NAME", "VARCHAR", "accounts")
    id_attr = IntegerAttribute("Id", "ID", "INT", "accounts")
    return table, name_attr, id_attr


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


class TestOrderBy:

    def test_no_order_by_produces_no_order_clause(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, NoOperation())
        sql = select_sql_to_string(select_op)
        assert "ORDER BY" not in sql

    def test_asc_produces_order_by_clause(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, NoOperation(), [attr.ascending()])
        sql = select_sql_to_string(select_op)
        assert "ORDER BY" in sql
        assert "ASC" in sql

    def test_desc_produces_order_by_clause(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, NoOperation(), [attr.descending()])
        sql = select_sql_to_string(select_op)
        assert "ORDER BY" in sql
        assert "DESC" in sql

    def test_asc_does_not_produce_desc(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, NoOperation(), [attr.ascending()])
        sql = select_sql_to_string(select_op)
        assert "DESC" not in sql

    def test_order_by_references_correct_column(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, NoOperation(), [attr.ascending()])
        sql = select_sql_to_string(select_op)
        assert "NAME" in sql.split("ORDER BY")[1]

    def test_multi_column_order_by(self):
        table, name_attr, id_attr = _make_multi_col_table()
        select_op = build_query_operation(None, None, [name_attr, id_attr], table, NoOperation(),
                                          [name_attr.ascending(), id_attr.descending()])
        sql = select_sql_to_string(select_op)
        order_clause = sql.split("ORDER BY")[1]
        assert "ASC" in order_clause
        assert "DESC" in order_clause

    def test_order_by_with_filter(self):
        table, attr = _make_table_and_attr()
        select_op = build_query_operation(None, None, [attr], table, attr.eq("Acme"), [attr.ascending()])
        sql = select_sql_to_string(select_op)
        assert "WHERE" in sql
        assert "ORDER BY" in sql
        assert sql.index("WHERE") < sql.index("ORDER BY")

    def test_finder_result_order_by_chaining(self):
        from datafinder import FinderResult, convert_inputs_and_select
        table, attr = _make_table_and_attr()
        result = convert_inputs_and_select(None, None, [attr], table, NoOperation())
        assert isinstance(result, FinderResult)
        chained = result.order_by(attr.ascending())
        assert chained is result
        assert len(result._order_by) == 1
