import datetime
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from datafinder import QueryRunnerBase, StringAttribute, IntegerAttribute
from datafinder_databricks.databricks_engine import _to_databricks_sql, DatabricksConnect
from model.relational import Table, Column, NoOperation
from model.relational import Table as _OwnerTable
_OWNER_ACCOUNTS = _OwnerTable('accounts', [])


def _make_table_and_attr():
    col = Column("NAME", "VARCHAR")
    table = Table("accounts", [col])
    attr = StringAttribute("Name", "NAME", "VARCHAR", _OWNER_ACCOUNTS)
    return table, attr


def _make_multi_col_table():
    name_col = Column("NAME", "VARCHAR")
    id_col = Column("ID", "INT")
    table = Table("accounts", [name_col, id_col])
    name_attr = StringAttribute("Name", "NAME", "VARCHAR", _OWNER_ACCOUNTS)
    id_attr = IntegerAttribute("Id", "ID", "INT", _OWNER_ACCOUNTS)
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


class TestTimeoutCancelsTheQuery:
    """A query that times out is cancelled on Databricks, and not only no longer waited for."""

    def _select(self, cursor, timeout_ms=50):
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        table, attr = _make_table_and_attr()
        DatabricksConnect.configure("host", "path", "token")
        QueryRunnerBase.register(DatabricksConnect)
        with patch("databricks.sql.connect") as connect:
            connect.return_value.__enter__.return_value = conn
            return QueryRunnerBase.get_runner().select(None, None, [attr], table, NoOperation(), timeout_ms=timeout_ms)

    def test_a_query_that_times_out_is_cancelled(self):
        import threading
        import time
        cancelled = threading.Event()
        cursor = MagicMock()
        cursor.active_command_id = object()  # the server has taken the query
        cursor.execute.side_effect = lambda query: cancelled.wait(10)  # runs until it is cancelled
        cursor.cancel.side_effect = cancelled.set
        started = time.monotonic()
        with pytest.raises(TimeoutError, match="50ms"):
            self._select(cursor)
        assert cancelled.is_set() and cursor.cancel.called
        assert time.monotonic() - started < 5

    def test_the_cancel_waits_until_the_server_has_the_query(self):
        import threading
        cancelled = threading.Event()
        cursor = MagicMock()
        cursor.active_command_id = None  # a cancel now would do nothing, and be logged

        def execute(query):
            threading.Timer(0.2, lambda: setattr(cursor, "active_command_id", object())).start()
            cancelled.wait(10)

        cursor.execute.side_effect = execute
        cursor.cancel.side_effect = cancelled.set
        with pytest.raises(TimeoutError):
            self._select(cursor)
        assert cursor.cancel.call_count >= 1 and cancelled.is_set()

    def test_a_cancel_that_fails_does_not_hide_the_timeout(self):
        import threading
        finished = threading.Event()
        cursor = MagicMock()
        cursor.active_command_id = object()

        def cancel():
            finished.set()
            raise RuntimeError("the connection is gone")

        cursor.execute.side_effect = lambda query: finished.wait(10)
        cursor.cancel.side_effect = cancel
        with pytest.raises(TimeoutError):
            self._select(cursor)

    def test_a_query_that_finishes_in_time_is_not_cancelled(self):
        cursor = MagicMock()
        cursor.fetchall_arrow.return_value = pa.table({"Name": ["Acme"]})
        self._select(cursor, timeout_ms=60_000)
        cursor.cancel.assert_not_called()


class TestTheGeneratedSqlIsTranslatedFromPostgres:
    """The SQL the finders generate is Postgres SQL, and what Databricks gets is sqlglot's translation of it."""

    def _sql(self, column):
        from model.relational import Table as _Table
        table = _Table("events", [])
        return _to_databricks_sql(None, None, [column], table, NoOperation())

    def test_a_regular_expression_is_a_databricks_function(self):
        from datafinder import StringAttribute
        name = StringAttribute("Name", "NAME", "VARCHAR", _OWNER_ACCOUNTS)
        sql = self._sql(name.matches("a.*b"))
        assert "REGEXP_LIKE(" in sql and " ~ " not in sql

    def test_an_interval_is_in_a_unit_that_databricks_has_in_a_literal(self):
        from datafinder import DateTimeAttribute
        created = DateTimeAttribute("Created", "CREATED_AT", "TIMESTAMP", _OWNER_ACCOUNTS)
        sql = self._sql(created.add_microseconds(5))
        assert "INTERVAL '0.000005' SECOND" in sql and "MICROSECOND" not in sql

    def test_a_date_difference_is_the_days_by_datediff_not_an_interval(self):
        """date - date is an interval in Databricks (as in Spark): the days are DATEDIFF(DAY, start, end), and not DuckDB's DATE_DIFF. The rows it gives are
        specced against Spark in datafinder_spark (``diff_days``), which Databricks shares the dialect changes of."""
        from datafinder import DateAttribute
        start = DateAttribute("Start", "START_AT", "DATE", _OWNER_ACCOUNTS)
        sql = self._sql(start.diff_days(datetime.date(2024, 6, 15)))
        assert "DATEDIFF(DAY, CAST(t1.START_AT AS DATE), CAST('2024-06-15' AS DATE))" in sql and "DATE_DIFF" not in sql


class TestPercentile:
    """sqlglot's Spark writes a percentile as an approximation with no order; Databricks has the exact one."""

    def _sql(self, *columns):
        from model.relational import Table as _Table
        return _to_databricks_sql(None, None, list(columns), _Table("events", []), NoOperation())

    def test_a_percentile_is_exact_and_keeps_its_order(self):
        from datafinder import IntegerAttribute
        qty = IntegerAttribute("Qty", "QTY", "INT", _OWNER_ACCOUNTS)
        sql = self._sql(qty.percentile(0.5, continuous=False), qty.percentile(0.25, ascending=False))
        import re
        assert re.search(r"PERCENTILE_DISC\(0\.5\) WITHIN GROUP \(ORDER BY t\d\.QTY ASC", sql)
        assert re.search(r"PERCENTILE_CONT\(0\.25\) WITHIN GROUP \(ORDER BY CAST\(t\d\.QTY AS DOUBLE\) DESC", sql)
        assert "PERCENTILE_APPROX" not in sql


class TestSharedWithSpark:
    """Databricks is Spark: the changes that the Spark specs need are made for it too (spec'd against Spark in datafinder_spark, as there is no
    Databricks to run them on)."""

    def _sql(self, *columns):
        from model.relational import Table as _Table
        return _to_databricks_sql(None, None, list(columns), _Table("events", []), NoOperation())

    def test_the_iso_day_of_the_week_is_dow_iso(self):
        from datafinder import DateAttribute
        sql = self._sql(DateAttribute("Start", "START_AT", "DATE", _OWNER_ACCOUNTS).day_of_week())
        assert "ISODOW" not in sql and "DOW_ISO" in sql

    def test_a_date_plus_an_interval_is_a_timestamp(self):
        from datafinder import DateAttribute
        sql = self._sql(DateAttribute("Start", "START_AT", "DATE", _OWNER_ACCOUNTS).add_days(5))
        assert "CAST(t1.START_AT AS TIMESTAMP) + INTERVAL" in sql


class TestVectorDistances:
    """Databricks has no live runner here: the SQL of a distance is checked as text. It is the one written for Spark, whose rows are specced against a
    local Spark in datafinder_spark (``cosine_distance_of_each_company`` and the other vector specs)."""

    def _sql(self, *columns):
        from model.relational import Table as _Table
        return _to_databricks_sql(None, None, list(columns), _Table("companies", []), NoOperation())

    def _embedding(self):
        from datafinder.typed_attributes import VectorAttribute
        return VectorAttribute("Embedding", "embedding", "VECTOR(2)", _OWNER_ACCOUNTS)

    def test_a_distance_is_computed_over_the_arrays(self):
        sql = self._sql(self._embedding().l2_distance([1, 2]), self._embedding().inner_product([1, 2]), self._embedding().cosine_distance([1, 2]))
        assert "SQRT(AGGREGATE(ZIP_WITH(t1.embedding, CAST(ARRAY(1.0, 2.0) AS ARRAY<FLOAT>)" in sql
        assert "COSINE_DISTANCE" not in sql and "EUCLIDEAN_DISTANCE" not in sql and "VECTOR" not in sql
