import ibis
import numpy as np
import pytest

from datafinder import QueryRunnerBase
from datafinder.typed_attributes import StringAttribute, IntegerAttribute, DoubleAttribute, BooleanAttribute
from datafinder_ibis.ibis_engine import IbisConnect
from model.relational import NoOperation, Table


class TestIbisEngine:

    def test_initialization(self):
        QueryRunnerBase.clear()
        QueryRunnerBase.register(IbisConnect)
        out = QueryRunnerBase.get_runner()
        assert out == IbisConnect


class TestIbisTimeout:
    """IbisConnect.select() only builds the (unexecuted) Ibis expression — the timeout can
    only fire once something actually materializes it, via to_pandas() or to_numpy()."""

    def _output(self, monkeypatch):
        import datafinder_ibis.ibis_engine as engine_module

        conn = ibis.connect('duckdb://:memory:')
        monkeypatch.setattr(ibis, 'connect', lambda *a, **kw: conn)
        monkeypatch.setattr(engine_module, 'to_sql',
                            lambda *a, **kw: "SELECT sum(i) FROM range(1000000000) t(i)")
        return IbisConnect.select(None, None, [], None, None, timeout_ms=1)  # type: ignore[arg-type]

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
    def con(self, monkeypatch):
        conn = ibis.connect('duckdb://:memory:')
        conn.con.execute(  # type: ignore[attr-defined]
            "CREATE TABLE widgets (name VARCHAR, quantity INTEGER, price DOUBLE, active BOOLEAN)"
        )
        conn.con.execute("INSERT INTO widgets VALUES ('Widget', 10, 4.5, true)")  # type: ignore[attr-defined]
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

    def test_pandas_dtypes_match_model_types(self, con):
        table, columns = self._columns()
        df = IbisConnect.select(None, None, columns, table, NoOperation()).to_pandas()  # type: ignore[arg-type]
        assert list(df.columns) == ["Name", "Quantity", "Price", "Active"]
        assert df["Name"].dtype == object
        assert np.issubdtype(df["Quantity"].dtype, np.integer)  # type: ignore[arg-type]
        assert np.issubdtype(df["Price"].dtype, np.floating)  # type: ignore[arg-type]
        assert df["Active"].dtype == bool

    def test_numpy_cell_types_match_model_types(self, con):
        table, columns = self._columns()
        row = IbisConnect.select(None, None, columns, table, NoOperation()).to_numpy()[0]  # type: ignore[arg-type]
        name, quantity, price, active = row
        assert isinstance(name, str) and name == "Widget"
        assert isinstance(quantity, (int, np.integer)) and quantity == 10
        assert isinstance(price, (float, np.floating)) and price == 4.5
        assert isinstance(active, (bool, np.bool_)) and active is True

    def test_to_numpy_alone_never_builds_a_pandas_dataframe(self, con):
        table, columns = self._columns()
        output = IbisConnect.select(None, None, columns, table, NoOperation())  # type: ignore[arg-type]
        output.to_numpy()
        assert output._IbisOutput__arrow is not None  # type: ignore[attr-defined]
