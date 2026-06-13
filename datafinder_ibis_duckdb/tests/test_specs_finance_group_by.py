"""
Runs TRADE_GROUP_BY_FINDER_SPECS (group_by average, group_by count per account)
against the ibis/DuckDB backend.
"""
import shutil
import sys
import tempfile

import duckdb
import pytest

from datafinder import QueryRunnerBase
from datafinder_generator.generator import generate
from datafinder_ibis.ibis_engine import IbisConnect
from mapping_markdown.markdown_mapping import load
from model.relational import Database, Schema, Table, Column
from datafinder_examples import example_path

from datafinder_examples_tests.finance_group_by_specs import (
    TRADE_GROUP_BY_FINDER_SPECS,
)

_MODS = [
    "finance", "finance.reference_data", "finance.trade",
    "finance.reference_data.account_finder",
    "finance.reference_data.instrument_finder",
    "finance.trade.trade_finder",
]


def _build_repo():
    repo = Database("finance_db", "duckdb://test.db")
    ref = Schema("ref_data", repo)
    trd = Schema("trading", repo)
    Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR")], ref)
    Table("price", [Column("SYM", "VARCHAR"), Column("PRICE", "DOUBLE"),
                    Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], ref)
    Table("trades", [Column("sym", "VARCHAR"), Column("price", "DOUBLE"),
                     Column("is_settled", "BOOLEAN"), Column("account_id", "INT"),
                     Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], trd)
    return repo


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS trading CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ref_data CASCADE")
    conn.execute("CREATE SCHEMA trading")
    conn.execute("CREATE SCHEMA ref_data")
    conn.execute("CREATE TABLE ref_data.account_master (ID INT, ACCT_NAME VARCHAR)")
    conn.execute(f"INSERT INTO ref_data.account_master SELECT * FROM read_csv_auto('{str(example_path('finance_accounts_group_by.csv'))}')")
    conn.execute("CREATE TABLE ref_data.price (SYM VARCHAR, PRICE DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP)")
    conn.execute(
        "CREATE TABLE trading.trades "
        "(sym VARCHAR, price DOUBLE, is_settled BOOLEAN, account_id INT, in_z TIMESTAMP, out_z TIMESTAMP)"
    )
    conn.execute(f"INSERT INTO trading.trades SELECT * FROM read_csv_auto('{str(example_path('finance_trades_group_by.csv'))}')")
    conn.close()


@pytest.fixture(scope="module")
def trade_group_by_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    repo = _build_repo()
    mapping = load(str(example_path(TRADE_GROUP_BY_FINDER_SPECS.mapping_file)), repo)
    generate(mapping, tmp)
    _seed_db()

    from finance.trade.trade_finder import TradeFinder  # type: ignore[import]
    yield TradeFinder()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "expectation", TRADE_GROUP_BY_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_trade_group_by(expectation, trade_group_by_finder):
    expectation.run(trade_group_by_finder, backend="duckdb")
