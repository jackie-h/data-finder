from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from datafinder import QueryRunnerBase, StringAttribute, IntegerAttribute
from datafinder_databricks.databricks_engine import _to_databricks_sql, DatabricksConnect
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


class TestDatabricksConnectRegistryIntegration:
    """DatabricksConnect must work the same way every other QueryRunnerBase implementation
    does: registered as a class (QueryRunnerBase.register(DatabricksConnect)), with
    get_runner() calling .select() unbound on that class — proving it's genuinely usable
    through the registry, not just type-compatible with it."""

    def setup_method(self):
        DatabricksConnect._server_hostname = None
        DatabricksConnect._http_path = None
        DatabricksConnect._access_token = None
        QueryRunnerBase.clear()

    def test_select_without_configure_raises(self):
        table, attr = _make_table_and_attr()
        QueryRunnerBase.register(DatabricksConnect)
        with pytest.raises(RuntimeError, match="configure"):
            QueryRunnerBase.get_runner().select(None, None, [attr], table, NoOperation())

    @patch("databricks.sql.connect")
    def test_select_after_configure_returns_output(self, mock_connect):
        arrow_table = pa.table({"Name": ["Acme"]})
        mock_cursor = MagicMock()
        mock_cursor.fetchall_arrow.return_value = arrow_table
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn

        DatabricksConnect.configure("host", "path", "token")
        QueryRunnerBase.register(DatabricksConnect)

        table, attr = _make_table_and_attr()
        output = QueryRunnerBase.get_runner().select(None, None, [attr], table, NoOperation())
        df = output.to_pandas()
        assert list(df["Name"]) == ["Acme"]
        mock_connect.assert_called_once_with(server_hostname="host", http_path="path", access_token="token")
