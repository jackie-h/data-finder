"""
Runs ACCOUNT_FINDER_SPECS, TRADE_FINDER_SPECS, TRADE_MULTI_HOP_FINDER_SPECS, TRADE_EXISTS_FINDER_SPECS,
TRADE_IS_NONE_FINDER_SPECS, TRADE_VIEW_FINDER_SPECS, and CONTRACTUAL_POSITION_FINDER_SPECS against the ibis/DuckDB
backend.
"""
import datetime
import random
import shutil
import sys
import tempfile

import duckdb
import pandas as pd
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
    TRADE_MULTI_HOP_FINDER_SPECS,
    TRADE_EXISTS_FINDER_SPECS,
    TRADE_OR_FINDER_SPECS,
    TRADE_IS_NONE_FINDER_SPECS,
    TRADE_VIEW_FINDER_SPECS,
    CONTRACTUAL_POSITION_FINDER_SPECS,
)

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
    Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR"), Column("BRANCH_ID", "INT"),
                             Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], ref)
    Table("branch", [Column("ID", "INT"), Column("CITY", "VARCHAR")], ref)
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
    conn.execute("DROP SCHEMA IF EXISTS reporting CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS trading CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ref_data CASCADE")
    conn.execute("CREATE SCHEMA trading")
    conn.execute("CREATE SCHEMA ref_data")
    conn.execute("CREATE TABLE ref_data.account_master "
                 "(ID INT, ACCT_NAME VARCHAR, BRANCH_ID INT, in_z TIMESTAMP, out_z TIMESTAMP)")
    conn.execute(f"INSERT INTO ref_data.account_master SELECT * FROM read_csv_auto('{str(example_path('finance_accounts.csv'))}')")
    conn.execute("CREATE TABLE ref_data.branch (ID INT, CITY VARCHAR)")
    conn.execute(f"INSERT INTO ref_data.branch SELECT * FROM read_csv_auto('{str(example_path('finance_branches.csv'))}')")
    conn.execute("CREATE TABLE ref_data.price (SYM VARCHAR, PRICE DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP)")
    conn.execute(f"INSERT INTO ref_data.price SELECT * FROM read_csv_auto('{str(example_path('finance_prices.csv'))}')")
    conn.execute(
        "CREATE TABLE trading.trades "
        "(sym VARCHAR, price DOUBLE, is_settled BOOLEAN, account_id INT, in_z TIMESTAMP, out_z TIMESTAMP)"
    )
    conn.execute(f"INSERT INTO trading.trades SELECT * FROM read_csv_auto('{str(example_path('finance_trades.csv'))}')")
    conn.execute(
        "CREATE TABLE trading.contractualposition "
        "(DATE DATE, QUANTITY DOUBLE, NPV DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP)"
    )
    conn.execute(f"INSERT INTO trading.contractualposition SELECT * FROM read_csv_auto('{str(example_path('finance_positions.csv'))}')")
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
    "expectation", TRADE_MULTI_HOP_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_trade_multi_hop(expectation, finance_finders):
    expectation.run(finance_finders[1], backend="duckdb")


@pytest.mark.parametrize(
    "expectation", TRADE_EXISTS_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_trade_exists(expectation, finance_finders):
    expectation.run(finance_finders[1], backend="duckdb")


@pytest.mark.parametrize(
    "expectation", TRADE_OR_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_trade_or(expectation, finance_finders):
    expectation.run(finance_finders[1], backend="duckdb")


@pytest.mark.parametrize(
    "expectation", TRADE_IS_NONE_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_trade_is_none(expectation, finance_finders):
    expectation.run(finance_finders[1], backend="duckdb")


@pytest.mark.parametrize(
    "expectation", TRADE_VIEW_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_trade_view(expectation, finance_finders):
    expectation.run(finance_finders[1], backend="duckdb")


def test_trade_view_is_created_in_the_database(finance_finders):
    """The view outlives the call that created it, which is what deploying it relies on."""
    tf = finance_finders[1]
    tf.find_all(None, None, [tf.symbol()]).to_view("trade_symbols", "reporting").result().to_pandas()
    conn = duckdb.connect("test.db")
    try:
        assert conn.execute("SELECT symbol FROM reporting.trade_symbols ORDER BY symbol").fetchall() == \
            [("AAPL",), ("GOOG",), ("MSFT",)]
    finally:
        conn.close()


def test_trade_view_rows_do_not_change_when_reference_data_changes(finance_finders):
    """Reference data moves on and a new version of a trade is recorded: the existing rows stay as they were,
    and only the new trade version sees the new account."""
    tf = finance_finders[1]

    def rows():
        view = tf.find_all(None, None, [tf.symbol(), tf.account().name()]).to_view("trade_account", "reporting")
        df = view.result().order_by("symbol", "in_z").to_pandas()
        return [(r.symbol, None if pd.isna(r.account_name) else r.account_name, r.in_z) for r in df.itertuples()]

    before = rows()
    conn = duckdb.connect("test.db")
    try:
        conn.execute("UPDATE ref_data.account_master SET out_z = '2024-02-01' WHERE ACCT_NAME = 'Acme Corp'")
        conn.execute("INSERT INTO ref_data.account_master VALUES (1, 'Acme Newest', 11, '2024-02-01', '9999-12-31 23:59:59')")
        conn.execute("UPDATE trading.trades SET out_z = '2024-07-01' WHERE sym = 'MSFT'")
        conn.execute("INSERT INTO trading.trades VALUES ('MSFT', 310.0, true, 2, '2024-07-01', '9999-12-31 23:59:59')")
        conn.close()
        after = rows()
    finally:
        _seed_db()

    assert before == [row for row in after if row[2] < datetime.datetime(2024, 7, 1)]
    assert after[-1][:2] == ("MSFT", "Beta Inc")


@pytest.mark.parametrize(
    "expectation", CONTRACTUAL_POSITION_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_position(expectation, finance_finders):
    expectation.run(finance_finders[2], backend="duckdb")


# Related finders, and the join nodes under them, are shared between queries. A query that is missing a join
# condition can pass only because an earlier query left its condition on a shared node, so each spec must also
# pass when it runs in a different order.
_TRADE_EXPECTATIONS = [
    *TRADE_FINDER_SPECS.expectations,
    *TRADE_MULTI_HOP_FINDER_SPECS.expectations,
    *TRADE_EXISTS_FINDER_SPECS.expectations,
    *TRADE_OR_FINDER_SPECS.expectations,
    *TRADE_IS_NONE_FINDER_SPECS.expectations,
]


def _shuffled(seed):
    order = list(_TRADE_EXPECTATIONS)
    random.Random(seed).shuffle(order)
    return order


@pytest.mark.parametrize(
    "order",
    [list(reversed(_TRADE_EXPECTATIONS)), _shuffled(1), _shuffled(2), _shuffled(3)],
    ids=["reversed", "shuffled-1", "shuffled-2", "shuffled-3"],
)
def test_trade_specs_do_not_depend_on_order(order, finance_finders):
    for expectation in order:
        expectation.run(finance_finders[1], backend="duckdb")


def test_trade_specs_pass_when_each_query_is_rendered_after_the_next_is_built(finance_finders):
    """Build every query before running any of them, so none can rely on a condition left by the one before."""
    tf = finance_finders[1]
    queries = [(expectation, expectation.query(tf)) for expectation in _TRADE_EXPECTATIONS]
    for expectation, result in reversed(queries):
        result.to_sql()
        df = result.to_pandas()
        assert list(df.columns) == expectation.expected_columns, expectation.name
