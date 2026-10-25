import datetime

import duckdb
import ibis
from ibis.backends.duckdb import Backend as DuckDBBackend
import numpy as np
import pytest

from datafinder import QueryRunnerBase, ViewResult
from datafinder.typed_attributes import StringAttribute, IntegerAttribute, DoubleAttribute, BooleanAttribute
from datafinder_examples_tests.spec import TestExpectation
from datafinder_ibis.ibis_engine import IbisConnect, sql_for_backend
from model.milestoning import MilestonedTable, ProcessingTemporalColumns
from model.relational import Alias, Column, JoinOperation, JoinTreeNodeOperation, NoOperation, Table
from model.view import View

_BUSINESS_DATE = datetime.date(2024, 1, 1)
_PROCESSING_DATETIME = datetime.datetime(2024, 1, 1, 0, 0, 0)


class TestIbisEngine:

    def test_initialization(self):
        QueryRunnerBase.clear()
        QueryRunnerBase.register(IbisConnect)
        out = QueryRunnerBase.get_runner()
        assert out == IbisConnect


class TestSqlForBackend:

    DIFF = "SELECT DATE_DIFF('day', a, b) FROM t"
    TRUNC = "SELECT CAST(DATE_TRUNC('month', d) AS DATE) FROM t"

    def test_duckdb_sql_is_unchanged(self):
        assert sql_for_backend(self.DIFF, "duckdb") == self.DIFF

    def test_a_date_difference_is_written_as_the_backend_spells_it(self):
        assert sql_for_backend(self.DIFF, "snowflake") == "SELECT DATEDIFF(DAY, a, b) FROM t"
        assert sql_for_backend(self.DIFF, "mssql") == "SELECT DATEDIFF(DAY, a, b) FROM t"
        assert sql_for_backend(self.DIFF, "bigquery") == "SELECT DATE_DIFF(b, a, DAY) FROM t"

    def test_a_date_truncation_is_written_as_the_backend_spells_it(self):
        assert sql_for_backend(self.TRUNC, "bigquery") == "SELECT CAST(TIMESTAMP_TRUNC(d, MONTH) AS DATE) FROM t"
        assert sql_for_backend(self.TRUNC, "postgres") == "SELECT CAST(DATE_TRUNC('MONTH', d) AS DATE) FROM t"

    def test_an_interval_is_written_as_the_backend_spells_it(self):
        assert sql_for_backend("SELECT d + INTERVAL '7' DAY FROM t", "postgres") == "SELECT d + INTERVAL '7 DAY' FROM t"


class TestIbisTimeout:
    """IbisConnect.select() only builds the (unexecuted) Ibis expression — the timeout can
    only fire once something actually materializes it, via to_pandas() or to_numpy()."""

    def _output(self, monkeypatch):
        import datafinder_ibis.ibis_engine as engine_module

        conn = ibis.connect('duckdb://:memory:')
        monkeypatch.setattr(ibis, 'connect', lambda *a, **kw: conn)
        monkeypatch.setattr(engine_module, 'to_sql',
                            lambda *a, **kw: "SELECT sum(i) FROM range(1000000000) t(i)")
        return IbisConnect.select(
            _BUSINESS_DATE, _PROCESSING_DATETIME, [], Table("t", []), NoOperation(), timeout_ms=1)

    def test_select_itself_does_not_execute_or_raise(self, monkeypatch):
        self._output(monkeypatch)

    def test_to_pandas_raises_timeout_error(self, monkeypatch):
        with pytest.raises(TimeoutError, match="1ms"):
            self._output(monkeypatch).to_pandas()

    def test_to_numpy_raises_timeout_error(self, monkeypatch):
        with pytest.raises(TimeoutError, match="1ms"):
            self._output(monkeypatch).to_numpy()


class TestIbisOutputTypes:
    """Verify IbisOutput.to_pandas()/to_numpy() carry correct per-column types, and that
    to_numpy() alone never builds a pandas DataFrame."""

    @pytest.fixture
    def con(self, monkeypatch) -> DuckDBBackend:
        conn: DuckDBBackend = ibis.connect('duckdb://:memory:')  # type: ignore[assignment]
        conn.con.execute(
            "CREATE TABLE widgets (name VARCHAR, quantity INTEGER, price DOUBLE, active BOOLEAN)"
        )
        conn.con.execute("INSERT INTO widgets VALUES ('Widget', 10, 4.5, true)")
        monkeypatch.setattr(ibis, 'connect', lambda *a, **kw: conn)
        return conn

    def _columns(self):
        table = Table("widgets", [])
        columns = [
            StringAttribute("Name", "name", "VARCHAR", "widgets"),
            IntegerAttribute("Quantity", "quantity", "INTEGER", "widgets"),
            DoubleAttribute("Price", "price", "DOUBLE", "widgets"),
            BooleanAttribute("Active", "active", "BOOLEAN", "widgets"),
        ]
        return table, columns

    def test_pandas_dtypes_are_nullable_and_correct(self, con):
        table, columns = self._columns()
        df = IbisConnect.select(_BUSINESS_DATE, _PROCESSING_DATETIME, columns, table, NoOperation()).to_pandas()
        assert list(df.columns) == ["Name", "Quantity", "Price", "Active"]
        assert str(df["Name"].dtype) == "string[pyarrow]"
        assert str(df["Quantity"].dtype) == "int32[pyarrow]"
        assert str(df["Price"].dtype) == "double[pyarrow]"
        assert str(df["Active"].dtype) == "bool[pyarrow]"

    def test_numpy_cell_types_match_model_types(self, con):
        table, columns = self._columns()
        row = IbisConnect.select(_BUSINESS_DATE, _PROCESSING_DATETIME, columns, table, NoOperation()).to_numpy()[0]
        name, quantity, price, active = row
        assert isinstance(name, str) and name == "Widget"
        assert isinstance(quantity, (int, np.integer)) and quantity == 10
        assert isinstance(price, (float, np.floating)) and price == 4.5
        assert isinstance(active, (bool, np.bool_)) and active is True

    def test_to_numpy_alone_never_builds_a_pandas_dataframe(self, con):
        table, columns = self._columns()
        output = IbisConnect.select(_BUSINESS_DATE, _PROCESSING_DATETIME, columns, table, NoOperation())
        output.to_numpy()
        assert output._IbisOutput__arrow is not None  # type: ignore[attr-defined]

    def test_null_value_is_pd_na_in_both_pandas_and_numpy(self, con):
        import pandas as pd
        con.con.execute("INSERT INTO widgets VALUES (NULL, NULL, NULL, NULL)")
        table, columns = self._columns()
        output = IbisConnect.select(_BUSINESS_DATE, _PROCESSING_DATETIME, columns, table, NoOperation())
        df = output.to_pandas()
        assert df["Quantity"].iloc[1] is pd.NA
        arr = output.to_numpy()
        assert arr[1][1] is pd.NA


class TestIbisSelectView:
    """A view is created on the backend and its rows read back, through the same expectation type as any query."""

    @pytest.fixture
    def view(self, monkeypatch) -> View:
        conn: DuckDBBackend = ibis.connect('duckdb://:memory:')  # type: ignore[assignment]
        monkeypatch.setattr(ibis, 'connect', lambda *a, **kw: conn)
        for statement in [
            "CREATE SCHEMA vt",
            "CREATE TABLE vt.trades (sym VARCHAR, account_id INT, in_z TIMESTAMP, out_z TIMESTAMP)",
            "CREATE TABLE vt.account (id INT, name VARCHAR, in_z TIMESTAMP, out_z TIMESTAMP)",
            "INSERT INTO vt.account VALUES (1, 'Acme Old', '2020-01-01', '2022-01-01'), "
            "(1, 'Acme New', '2022-01-01', '9999-12-31')",
            "INSERT INTO vt.trades VALUES ('AAPL', 1, '2020-06-01', '9999-12-31'), "
            "('IBM', 1, '2023-01-01', '9999-12-31')",
        ]:
            conn.raw_sql(statement)

        def milestoning():
            return ProcessingTemporalColumns(Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP"))

        join = JoinOperation("Account", MilestonedTable("vt.account", [], milestoning()),
                             Column("account_id", "INT", "vt.trades"), Column("id", "INT", "vt.account"))
        node = JoinTreeNodeOperation(join)
        return View("trade_account", "reporting", MilestonedTable("vt.trades", [], milestoning()),
                    [Alias(StringAttribute("Symbol", "sym", "VARCHAR", "vt.trades"), "symbol"),
                     Alias(StringAttribute("Account Name", "name", "VARCHAR", "vt.account", node), "account_name")])

    def test_select_view_reads_the_rows_of_a_view_that_was_created(self, view):
        IbisConnect.create_view(view)
        df = IbisConnect.select_view(view).to_pandas()
        assert list(df.columns) == ["symbol", "account_name", "in_z", "out_z"]
        assert df[["symbol", "account_name"]].values.tolist() == [["AAPL", "Acme Old"], ["IBM", "Acme New"]]

    def test_select_view_does_not_create_the_view(self, view):
        with pytest.raises(duckdb.CatalogException):
            IbisConnect.select_view(view).to_pandas()

    def test_rows_can_be_ordered_by_view_columns(self, view):
        IbisConnect.create_view(view)
        df = IbisConnect.select_view(view, order_by=["account_name", "symbol"]).to_pandas()
        assert df["symbol"].tolist() == ["IBM", "AAPL"] and df["account_name"].tolist() == ["Acme New", "Acme Old"]

    def test_creating_the_same_view_twice_replaces_it(self, view):
        IbisConnect.create_view(view)
        IbisConnect.create_view(view)
        assert len(IbisConnect.select_view(view).to_pandas()) == 2

    def test_a_view_result_works_in_the_existing_test_expectation(self, view):
        QueryRunnerBase.clear()
        QueryRunnerBase.register(IbisConnect)
        TestExpectation(
            name="each_trade_sees_the_account_applicable_at_its_time",
            query=lambda _: ViewResult(view),
            expected_columns=["symbol", "account_name", "in_z", "out_z"],
            expected_result=np.array([
                ["AAPL", "Acme Old", datetime.datetime(2020, 6, 1), datetime.datetime(9999, 12, 31)],
                ["IBM", "Acme New", datetime.datetime(2023, 1, 1), datetime.datetime(9999, 12, 31)],
            ], dtype=object),
            expected_sql={"duckdb": "t1.in_z <= t0.in_z"},
            unexpected_sql={"duckdb": "GREATEST"},
        ).run(None, backend="duckdb")
