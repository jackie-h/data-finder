from datafinder import StringAttribute, IntegerAttribute
from datafinder_databricks.databricks_engine import _to_databricks_sql
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


class TestDatabricksSqlTranspilation:

    def test_produces_select_and_from(self):
        table, attr = _make_table_and_attr()
        sql = _to_databricks_sql(None, None, [attr], table, NoOperation())
        assert "SELECT" in sql
        assert "FROM" in sql

    def test_filter_produces_where_clause(self):
        table, attr = _make_table_and_attr()
        sql = _to_databricks_sql(None, None, [attr], table, attr.eq("Acme"))
        assert "WHERE" in sql
        assert "'Acme'" in sql

    def test_order_by_is_preserved(self):
        table, attr = _make_table_and_attr()
        sql = _to_databricks_sql(None, None, [attr], table, NoOperation(), order_by=[attr.ascending()])
        assert "ORDER BY" in sql
        assert "ASC" in sql

    def test_limit_is_preserved(self):
        table, attr = _make_table_and_attr()
        sql = _to_databricks_sql(None, None, [attr], table, NoOperation(), limit=10)
        assert "LIMIT 10" in sql

    def test_backtick_quoting_for_aliases(self):
        table, attr = _make_table_and_attr()
        sql = _to_databricks_sql(None, None, [attr], table, NoOperation())
        assert "`Name`" in sql

    def test_returns_string(self):
        table, attr = _make_table_and_attr()
        sql = _to_databricks_sql(None, None, [attr], table, NoOperation())
        assert isinstance(sql, str)
