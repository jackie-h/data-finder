import datetime
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
from model.relational import Repository, Schema, Table, Column

_MAPPING_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "mapping_markdown", "tests", "finance_mapping.md")
)

_FINDER_MODULES = [
    "finance", "finance.reference_data", "finance.trade",
    "finance.reference_data.account_finder",
    "finance.reference_data.instrument_finder",
    "finance.trade.trade_finder",
]

_ACCOUNTS = [
    (1, "Alpha Fund"),
    (2, "Beta Fund"),
    (3, "Gamma Fund"),
]

_TRADES = [
    # (sym, price, is_settled, account_id, in_z, out_z)
    ("AAPL", 100.0, True,  1, "2020-01-01", "9999-12-31"),
    ("GOOG", 200.0, True,  1, "2020-01-01", "9999-12-31"),
    ("MSFT", 300.0, True,  2, "2020-01-01", "9999-12-31"),
    ("TSLA", 400.0, False, 2, "2020-01-01", "9999-12-31"),
    ("GS",   500.0, True,  3, "2020-01-01", "9999-12-31"),
]


def _build_repository() -> Repository:
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

    repo = _build_repository()
    mapping = load(_MAPPING_FILE, repo)
    generate(mapping, temp_dir)
    _seed_test_db()

    from finance.trade.trade_finder import TradeFinder as TF
    yield TF

    sys.path.remove(temp_dir)
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestGroupByAccountAveragePrice:

    def test_average_price_per_account(self, TradeFinder):
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.account().name(), TradeFinder.price().average()],
        ).group_by(TradeFinder.account().name()).to_pandas()
        avgs = dict(zip(result["Account Name"], result["Average Price"]))
        assert avgs["Alpha Fund"] == pytest.approx(150.0)
        assert avgs["Beta Fund"] == pytest.approx(350.0)
        assert avgs["Gamma Fund"] == pytest.approx(500.0)

    def test_average_price_returns_one_row_per_account(self, TradeFinder):
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.account().name(), TradeFinder.price().average()],
        ).group_by(TradeFinder.account().name()).to_pandas()
        assert len(result) == 3

    def test_average_price_with_filter(self, TradeFinder):
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.account().name(), TradeFinder.price().average()],
            TradeFinder.is_settled().is_true(),
        ).group_by(TradeFinder.account().name()).to_pandas()
        avgs = dict(zip(result["Account Name"], result["Average Price"]))
        assert avgs["Alpha Fund"] == pytest.approx(150.0)
        assert avgs["Beta Fund"] == pytest.approx(300.0)
        assert avgs["Gamma Fund"] == pytest.approx(500.0)

    def test_average_price_ordered_by_account(self, TradeFinder):
        result = TradeFinder.find_all(
            datetime.datetime.now(),
            [TradeFinder.account().name(), TradeFinder.price().average()],
        ).group_by(TradeFinder.account().name()).order_by(
            TradeFinder.account().name().ascending()
        ).to_pandas()
        names = result["Account Name"].tolist()
        assert names == sorted(names)
