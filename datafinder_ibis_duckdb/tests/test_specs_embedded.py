"""
Runs EMBEDDED_TRADE_FINDER_SPECS against the ibis/DuckDB backend, proving that a query
selecting only embedded (denormalized) properties of Trade.account (one-hop) or
Trade.account.branch (two-hop) elides the corresponding join(s), while a query that also
touches a non-embedded property of the same related object forces the real join(s) for
all of them.
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

from datafinder_examples_tests.embedded_specs import EMBEDDED_TRADE_FINDER_SPECS

_MODS = [
    "finance", "finance.reference_data", "finance.trade",
    "finance.reference_data.account_finder", "finance.reference_data.account_finder_base",
    "finance.reference_data.branch_finder", "finance.reference_data.branch_finder_base",
    "finance.reference_data.instrument_finder", "finance.reference_data.instrument_finder_base",
    "finance.trade.trade_finder", "finance.trade.trade_finder_base",
    "finance.trade.contractualposition_finder", "finance.trade.contractualposition_finder_base",
]


def _build_repo():
    repo = Database("finance_db", "duckdb://test.db")
    ref = Schema("ref_data", repo)
    trd = Schema("trading", repo)
    Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR"), Column("BRANCH_ID", "INT")], ref)
    Table("branch_master", [Column("ID", "INT"), Column("CITY", "VARCHAR")], ref)
    Table("price", [Column("SYM", "VARCHAR"), Column("PRICE", "DOUBLE"),
                    Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], ref)
    Table("trades", [Column("sym", "VARCHAR"), Column("price", "DOUBLE"),
                     Column("is_settled", "BOOLEAN"), Column("account_id", "INT"),
                     Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP"),
                     Column("acct_name", "VARCHAR"), Column("branch_city", "VARCHAR")], trd)
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
    conn.execute("CREATE TABLE ref_data.branch_master (ID INT, CITY VARCHAR)")
    conn.execute(f"INSERT INTO ref_data.branch_master SELECT * FROM read_csv_auto('{str(example_path('finance_branches_embedded.csv'))}')")
    conn.execute("CREATE TABLE ref_data.account_master (ID INT, ACCT_NAME VARCHAR, BRANCH_ID INT)")
    conn.execute(f"INSERT INTO ref_data.account_master SELECT * FROM read_csv_auto('{str(example_path('finance_accounts_embedded.csv'))}')")
    conn.execute(
        "CREATE TABLE trading.trades "
        "(sym VARCHAR, price DOUBLE, is_settled BOOLEAN, account_id INT, in_z TIMESTAMP, out_z TIMESTAMP, "
        "acct_name VARCHAR, branch_city VARCHAR)"
    )
    conn.execute(f"INSERT INTO trading.trades SELECT * FROM read_csv_auto('{str(example_path('finance_trades_embedded.csv'))}')")
    conn.close()


@pytest.fixture(scope="module")
def trade_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    repo = _build_repo()
    mapping = load(str(example_path(EMBEDDED_TRADE_FINDER_SPECS.mapping_file)), repo)
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
    "expectation", EMBEDDED_TRADE_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_trade_embedded(expectation, trade_finder):
    expectation.run(trade_finder, backend="duckdb")
