import math
import os
import shutil
import sys
import tempfile

import duckdb
import pytest

from datafinder import QueryRunnerBase
from datafinder_generator.generator import generate
from datafinder_ibis.ibis_engine import IbisConnect
from mapping_markdown.markdown_mapping import load

_MAPPING_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "mapping_markdown", "tests", "finance_mapping.md")
)

_FINDER_MODULES = ["account_finder", "instrument_finder", "trade_finder"]

_ACCOUNTS = [(1, "Alpha Fund")]

_TRADES = [
    # (sym, price, is_settled, account_id, in_z, out_z)
    ("AAPL",  -25.5, True,  1, "2020-01-01", "9999-12-31"),
    ("GOOG",   36.0, True,  1, "2020-01-01", "9999-12-31"),
    ("MSFT",  100.4, True,  1, "2020-01-01", "9999-12-31"),
    ("TSLA",    9.0, False, 1, "2020-01-01", "9999-12-31"),
]


def _build_repository():
    from model.relational import Repository, Schema, Table, Column
    repo = Repository("finance_db", "duckdb://test.db")
    ref_data = Schema("ref_data", repo)
    trading = Schema("trading", repo)
    Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR")], ref_data)
    Table("price", [Column("SYM", "VARCHAR"), Column("PRICE", "DOUBLE"),
                    Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], ref_data)
    Table("trades", [Column("sym", "VARCHAR"), Column("price", "DOUBLE"), Column("is_settled", "BOOLEAN"),
                     Column("account_id", "INT"), Column("in_z", "TIMESTAMP"),
                     Column("out_z", "TIMESTAMP")], trading)
    return repo


def _seed_test_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS trading CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ref_data CASCADE")
    conn.execute("CREATE SCHEMA trading")
    conn.execute("CREATE SCHEMA ref_data")
    conn.execute("CREATE TABLE ref_data.account_master (ID INT, ACCT_NAME VARCHAR)")
    for row in _ACCOUNTS:
        conn.execute("INSERT INTO ref_data.account_master VALUES (?, ?)", row)
    conn.execute("CREATE TABLE ref_data.price (SYM VARCHAR, PRICE DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP)")
    conn.execute(
        "CREATE TABLE trading.trades "
        "(sym VARCHAR, price DOUBLE, is_settled BOOLEAN, account_id INT, in_z TIMESTAMP, out_z TIMESTAMP)"
    )
    for row in _TRADES:
        conn.execute("INSERT INTO trading.trades VALUES (?, ?, ?, ?, ?, ?)", row)
    conn.close()


@pytest.fixture(scope="module")
def TradeFinder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)

    temp_dir = tempfile.mkdtemp()
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    sys.path.insert(0, temp_dir)

    import datetime
    repo = _build_repository()
    mapping = load(_MAPPING_FILE, repo)
    generate(mapping, temp_dir)
    _seed_test_db()

    from trade_finder import TradeFinder as TF
    yield TF

    sys.path.remove(temp_dir)
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestAbs:

    def test_abs_returns_absolute_values(self, TradeFinder):
        import datetime
        price = -25.5
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.symbol(), TradeFinder.price().abs()],
            TradeFinder.symbol().eq("AAPL"),
        ).to_pandas()
        assert result.iloc[0]["Abs Price"] == pytest.approx(abs(price))

    def test_abs_column_name(self, TradeFinder):
        import datetime
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.price().abs()],
        ).to_pandas()
        assert "Abs Price" in result.columns


class TestCeil:

    def test_ceil_rounds_up(self, TradeFinder):
        import datetime
        price = 36.0
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.symbol(), TradeFinder.price().ceil()],
            TradeFinder.symbol().eq("GOOG"),
        ).to_pandas()
        assert result.iloc[0]["Ceil Price"] == math.ceil(price)

    def test_ceil_column_name(self, TradeFinder):
        import datetime
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.price().ceil()],
        ).to_pandas()
        assert "Ceil Price" in result.columns


class TestFloor:

    def test_floor_rounds_down(self, TradeFinder):
        import datetime
        price = 100.4
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.symbol(), TradeFinder.price().floor()],
            TradeFinder.symbol().eq("MSFT"),
        ).to_pandas()
        assert result.iloc[0]["Floor Price"] == math.floor(price)

    def test_floor_column_name(self, TradeFinder):
        import datetime
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.price().floor()],
        ).to_pandas()
        assert "Floor Price" in result.columns


class TestSqrt:

    def test_sqrt_returns_square_root(self, TradeFinder):
        import datetime
        price = 9.0
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.symbol(), TradeFinder.price().sqrt()],
            TradeFinder.symbol().eq("TSLA"),
        ).to_pandas()
        assert result.iloc[0]["Sqrt Price"] == pytest.approx(math.sqrt(price))


class TestMod:

    def test_mod_returns_remainder(self, TradeFinder):
        import datetime
        price, divisor = 36.0, 10
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.symbol(), TradeFinder.price().mod(divisor)],
            TradeFinder.symbol().eq("GOOG"),
        ).to_pandas()
        assert result.iloc[0]["Mod Price"] == pytest.approx(price % divisor)

    def test_mod_operator_matches_mod(self, TradeFinder):
        import datetime
        price, divisor = 36.0, 10
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.symbol(), TradeFinder.price() % divisor],
            TradeFinder.symbol().eq("GOOG"),
        ).to_pandas()
        assert result.iloc[0]["Mod Price"] == pytest.approx(price % divisor)


class TestPower:

    def test_power_raises_to_exponent(self, TradeFinder):
        import datetime
        price, exp = 9.0, 2
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.symbol(), TradeFinder.price().power(exp)],
            TradeFinder.symbol().eq("TSLA"),
        ).to_pandas()
        assert result.iloc[0]["Power Price"] == pytest.approx(price ** exp)

    def test_pow_operator_matches_power(self, TradeFinder):
        import datetime
        price, exp = 9.0, 2
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.symbol(), TradeFinder.price() ** exp],
            TradeFinder.symbol().eq("TSLA"),
        ).to_pandas()
        assert result.iloc[0]["Power Price"] == pytest.approx(price ** exp)


class TestRound:

    def test_round_without_decimals(self, TradeFinder):
        import datetime
        price = 100.4
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.symbol(), TradeFinder.price().round()],
            TradeFinder.symbol().eq("MSFT"),
        ).to_pandas()
        assert result.iloc[0]["Round Price"] == pytest.approx(round(price))

    def test_round_with_decimals(self, TradeFinder):
        import datetime
        price, d = -25.5, 1
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.symbol(), TradeFinder.price().round(d)],
            TradeFinder.symbol().eq("AAPL"),
        ).to_pandas()
        assert result.iloc[0]["Round Price"] == pytest.approx(round(price, d))
