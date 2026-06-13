"""
Runs ACCOUNT_FINDER_SPECS, TRADE_FINDER_SPECS, and CONTRACTUAL_POSITION_FINDER_SPECS
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

from datafinder_examples_tests.finance_specs import (
    ACCOUNT_FINDER_SPECS,
    TRADE_FINDER_SPECS,
    CONTRACTUAL_POSITION_FINDER_SPECS,
    ACCOUNTS,
    PRICES,
    TRADES,
    POSITIONS,
)

_MODS = [
    "finance", "finance.reference_data", "finance.trade",
    "finance.reference_data.account_finder",
    "finance.reference_data.instrument_finder",
    "finance.trade.trade_finder",
    "finance.trade.contractualposition_finder",
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
    Table("contractualposition", [Column("DATE", "DATE"), Column("QUANTITY", "DOUBLE"),
                                  Column("NPV", "DOUBLE"), Column("in_z", "TIMESTAMP"),
                                  Column("out_z", "TIMESTAMP")], trd)
    return repo


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS trading CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ref_data CASCADE")
    conn.execute("CREATE SCHEMA trading")
    conn.execute("CREATE SCHEMA ref_data")
    conn.execute("CREATE TABLE ref_data.account_master (ID INT, ACCT_NAME VARCHAR)")
    for r in ACCOUNTS:
        conn.execute("INSERT INTO ref_data.account_master VALUES (?, ?)", r)
    conn.execute("CREATE TABLE ref_data.price (SYM VARCHAR, PRICE DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP)")
    for r in PRICES:
        conn.execute("INSERT INTO ref_data.price VALUES (?, ?, ?, ?)", r)
    conn.execute(
        "CREATE TABLE trading.trades "
        "(sym VARCHAR, price DOUBLE, is_settled BOOLEAN, account_id INT, in_z TIMESTAMP, out_z TIMESTAMP)"
    )
    for r in TRADES:
        conn.execute("INSERT INTO trading.trades VALUES (?, ?, ?, ?, ?, ?)", r)
    conn.execute(
        "CREATE TABLE trading.contractualposition "
        "(DATE DATE, QUANTITY DOUBLE, NPV DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP)"
    )
    for r in POSITIONS:
        conn.execute("INSERT INTO trading.contractualposition VALUES (?, ?, ?, ?, ?)", r)
    conn.close()


@pytest.fixture(scope="module")
def finance_finders():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    repo = _build_repo()
    mapping = load(str(example_path(ACCOUNT_FINDER_SPECS.mapping_file)), repo)
    generate(mapping, tmp)
    _seed_db()

    from finance.reference_data.account_finder import AccountFinder  # type: ignore[import]
    from finance.trade.trade_finder import TradeFinder  # type: ignore[import]
    from finance.trade.contractualposition_finder import ContractualPositionFinder  # type: ignore[import]

    yield AccountFinder(), TradeFinder(), ContractualPositionFinder()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "expectation", ACCOUNT_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_account(expectation, finance_finders):
    expectation.run(finance_finders[0], backend="duckdb")


@pytest.mark.parametrize(
    "expectation", TRADE_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_trade(expectation, finance_finders):
    expectation.run(finance_finders[1], backend="duckdb")


@pytest.mark.parametrize(
    "expectation", CONTRACTUAL_POSITION_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_position(expectation, finance_finders):
    expectation.run(finance_finders[2], backend="duckdb")
