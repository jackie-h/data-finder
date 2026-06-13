import datetime
import os
import shutil
import sys
import tempfile

import duckdb
import numpy as np
import pytest
from numpy.testing import assert_array_equal

from datafinder import QueryRunnerBase
from datafinder_generator.generator import generate
from datafinder_ibis.ibis_engine import IbisConnect
from mapping_markdown.markdown_mapping import load
from model.relational import Database, Schema, Table, Column

_MAPPING_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "datafinder_examples", "src", "datafinder_examples", "finance_mapping.md")
)

_FINDER_MODULES = [
    "finance", "finance.reference_data", "finance.trade",
    "finance.reference_data.account_finder",
    "finance.reference_data.instrument_finder",
    "finance.trade.trade_finder",
    "finance.trade.contractualposition_finder",
]


def _build_repository() -> Database:
    repo = Database("finance_db", "duckdb://test.db")
    ref_data = Schema("ref_data", repo)
    trading = Schema("trading", repo)
    Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR")], ref_data)
    Table("price", [Column("SYM", "VARCHAR"), Column("PRICE", "DOUBLE"),
                    Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], ref_data)
    Table("trades", [Column("sym", "VARCHAR"), Column("price", "DOUBLE"), Column("is_settled", "BOOLEAN"),
                     Column("account_id", "INT"), Column("in_z", "TIMESTAMP"),
                     Column("out_z", "TIMESTAMP")], trading)
    Table("contractualposition", [Column("DATE", "DATE"), Column("QUANTITY", "DOUBLE"),
                                  Column("NPV", "DOUBLE"), Column("in_z", "TIMESTAMP"),
                                  Column("out_z", "TIMESTAMP")], trading)
    return repo


def _seed_test_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS trading CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ref_data CASCADE")
    conn.execute("CREATE SCHEMA trading")
    conn.execute("CREATE SCHEMA ref_data")
    conn.execute("CREATE TABLE ref_data.account_master(ID INT, ACCT_NAME VARCHAR)")
    conn.execute("INSERT INTO ref_data.account_master VALUES (1, 'Acme Corp')")
    conn.execute("CREATE TABLE ref_data.price(SYM VARCHAR, PRICE DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP)")
    conn.execute("INSERT INTO ref_data.price VALUES ('AAPL', 150.0, '2020-01-01', '9999-12-31')")
    conn.execute("INSERT INTO ref_data.price VALUES ('GOOG', 2800.0, '2020-01-01', '2022-01-01')")
    conn.execute(
        "CREATE TABLE trading.trades(sym VARCHAR, price DOUBLE, is_settled BOOLEAN, account_id INT, in_z TIMESTAMP, out_z TIMESTAMP)"
    )
    # AAPL trade: currently active, settled
    conn.execute("INSERT INTO trading.trades VALUES ('AAPL', 84.11, true, 1, '2020-01-01', '9999-12-31')")
    # GOOG trade: expired before 2022, not settled
    conn.execute("INSERT INTO trading.trades VALUES ('GOOG', 200.0, false, 1, '2020-01-01', '2022-01-01')")
    conn.execute(
        "CREATE TABLE trading.contractualposition(DATE DATE, QUANTITY DOUBLE, NPV DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP)"
    )
    # Row 1: business_date=2023-01-15, processed from 2023-01-15 onward (active as of now)
    conn.execute("INSERT INTO trading.contractualposition VALUES ('2023-01-15', 100.0, 500.0, '2023-01-15', '9999-12-31')")
    # Row 2: same business_date=2023-01-15, superseded by row 1 (processing expired at 2023-01-15)
    conn.execute("INSERT INTO trading.contractualposition VALUES ('2023-01-15', 90.0, 450.0, '2023-01-10', '2023-01-15')")
    # Row 3: different business_date=2023-01-16, active as of now
    conn.execute("INSERT INTO trading.contractualposition VALUES ('2023-01-16', 200.0, 1000.0, '2023-01-16', '9999-12-31')")
    conn.close()


@pytest.fixture(scope="module")
def finders():
    """Generate finders from markdown, seed test DB, yield finder classes, then clean up."""
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

    from finance.reference_data.account_finder import AccountFinder  # type: ignore[import]
    from finance.trade.trade_finder import TradeFinder  # type: ignore[import]
    from finance.reference_data.instrument_finder import InstrumentFinder  # type: ignore[import]
    from finance.trade.contractualposition_finder import ContractualPositionFinder  # type: ignore[import]

    yield {"Account": AccountFinder(), "Trade": TradeFinder(), "Instrument": InstrumentFinder(),
           "ContractualPosition": ContractualPositionFinder()}

    sys.path.remove(temp_dir)
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestE2EMarkdownIbisDuckDb:

    def test_account_query(self, finders):
        AccountFinder = finders["Account"]
        result = AccountFinder.find_all(
            None, None, [AccountFinder.id_(), AccountFinder.name()],
            AccountFinder.id_().eq(1),
        ).to_numpy()
        assert_array_equal(result, np.array([[1, "Acme Corp"]], dtype=object))

    def test_account_to_pandas_no_filter(self, finders):
        AccountFinder = finders["Account"]
        df = AccountFinder.find_all(
            None, None, [AccountFinder.id_(), AccountFinder.name()],
        ).to_pandas()
        assert list(df.columns) == ["Id", "Name"]
        assert df.iloc[0]["Name"] == "Acme Corp"

    def test_trade_query_with_milestoning(self, finders):
        TradeFinder = finders["Trade"]
        # Both AAPL and GOOG are active in 2021
        result = TradeFinder.find_all(
            None, "2021-06-01 12:00:00",
            [TradeFinder.symbol(), TradeFinder.price()],
        ).to_pandas()
        assert len(result) == 2
        assert set(result["Symbol"].tolist()) == {"AAPL", "GOOG"}

    def test_trade_milestoning_filters_expired_records(self, finders):
        TradeFinder = finders["Trade"]
        # After 2022-01-01, GOOG record expired — only AAPL visible
        result = TradeFinder.find_all(
            None, "2023-01-01 12:00:00",
            [TradeFinder.symbol(), TradeFinder.price()],
        ).to_numpy()
        assert_array_equal(result, np.array([["AAPL", 84.11]], dtype=object))

    def test_trade_query_with_account_join(self, finders):
        TradeFinder = finders["Trade"]
        result = TradeFinder.find_all(
            None, datetime.datetime.now(),
            [TradeFinder.account().name(), TradeFinder.symbol(), TradeFinder.price()],
            TradeFinder.symbol().eq("AAPL"),
        ).to_numpy()
        assert_array_equal(result, np.array([["Acme Corp", "AAPL", 84.11]], dtype=object))

    def test_trade_filter_by_symbol(self, finders):
        TradeFinder = finders["Trade"]
        result = TradeFinder.find_all(
            None, datetime.datetime.now(),
            [TradeFinder.symbol(), TradeFinder.price()],
            TradeFinder.symbol().eq("AAPL"),
        ).to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["Price"] == 84.11

    def test_instrument_milestoning_returns_active_rows(self, finders):
        InstrumentFinder = finders["Instrument"]
        # Both AAPL and GOOG active in 2021
        result = InstrumentFinder.find_all(
            None, "2021-06-01 12:00:00",
            [InstrumentFinder.symbol(), InstrumentFinder.price()],
        ).to_pandas()
        assert set(result["Symbol"].tolist()) == {"AAPL", "GOOG"}

    def test_instrument_milestoning_filters_expired(self, finders):
        InstrumentFinder = finders["Instrument"]
        # GOOG record expired before 2023 — only AAPL visible
        result = InstrumentFinder.find_all(
            None, "2023-01-01 12:00:00",
            [InstrumentFinder.symbol(), InstrumentFinder.price()],
        ).to_pandas()
        assert result["Symbol"].tolist() == ["AAPL"]

    def test_instrument_synthetic_milestoning_attrs_accessible(self, finders):
        # valid_from / valid_to are synthetic (not in the model) but should be
        # accessible as finder attributes since they appear in the mapping markdown
        InstrumentFinder = finders["Instrument"]
        assert InstrumentFinder.valid_from() is not None
        assert InstrumentFinder.valid_to() is not None

    def test_trade_filter_by_boolean(self, finders):
        TradeFinder = finders["Trade"]
        result = TradeFinder.find_all(
            None, datetime.datetime.now(),
            [TradeFinder.symbol(), TradeFinder.is_settled()],
            TradeFinder.is_settled().is_true(),
        ).to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["Symbol"] == "AAPL"

    # --- Bidirectional association traversal ---

    # --- Bidirectional association traversal with milestoning ---

    def test_forward_trade_to_account(self, finders):
        """Forward direction: Trade → Account via TradeFinder.account().
        Root table is Trade; Account is the join so its columns are prefixed 'Account'.
        """
        TradeFinder = finders["Trade"]
        result = TradeFinder.find_all(
            None, datetime.datetime.now(),
            [TradeFinder.symbol(), TradeFinder.account().name()],
            TradeFinder.symbol().eq("AAPL"),
        ).to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["Symbol"] == "AAPL"
        assert result.iloc[0]["Account Name"] == "Acme Corp"

    def test_forward_milestoning_on_root_filters_join_results(self, finders):
        """Forward direction: Trade root milestoning is applied as a WHERE clause.
        After GOOG expired (2022-01-01), only the AAPL trade survives, so only one
        account row is produced by the join.
        """
        TradeFinder = finders["Trade"]
        after_goog_expired = "2023-01-01 12:00:00"
        result = TradeFinder.find_all(
            None, after_goog_expired,
            [TradeFinder.symbol(), TradeFinder.account().name()],
        ).to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["Symbol"] == "AAPL"
        assert result.iloc[0]["Account Name"] == "Acme Corp"

    def test_forward_milestoning_both_trades_visible_before_expiry(self, finders):
        """Forward direction: before GOOG expired both trades are milestoning-active,
        so both appear in the join result."""
        TradeFinder = finders["Trade"]
        before_goog_expired = "2021-06-01 12:00:00"
        result = TradeFinder.find_all(
            None, before_goog_expired,
            [TradeFinder.symbol(), TradeFinder.account().name()],
        ).to_pandas()
        assert len(result) == 2
        assert set(result["Symbol"].tolist()) == {"AAPL", "GOOG"}
        assert set(result["Account Name"].tolist()) == {"Acme Corp"}

    def test_reverse_account_to_trades_no_milestoning(self, finders):
        """Reverse direction: Account → Trades via AccountFinder.trades().
        Account is non-milestoned so find_all passes no processing timestamp.
        The Trade join therefore carries no milestoning ON-clause filter,
        and all trade rows (including expired GOOG) are returned.
        """
        AccountFinder = finders["Account"]
        result = AccountFinder.find_all(
            None, None, [AccountFinder.name(), AccountFinder.trades().symbol()],
        ).to_pandas()
        assert set(result["Name"].tolist()) == {"Acme Corp"}
        # GOOG (expired 2022) is present because no milestoning filter is applied
        assert set(result["Trade Symbol"].tolist()) == {"AAPL", "GOOG"}

    def test_reverse_account_to_trades_filter_by_symbol(self, finders):
        """Reverse direction with filter applied on the joined trade column."""
        AccountFinder = finders["Account"]
        result = AccountFinder.find_all(
            None, None, [AccountFinder.name(), AccountFinder.trades().symbol()],
            AccountFinder.trades().symbol().eq("AAPL"),
        ).to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["Name"] == "Acme Corp"
        assert result.iloc[0]["Trade Symbol"] == "AAPL"


class TestE2EBusinessDateProcessingMilestoning:

    def test_query_by_business_date_and_processing_time(self, finders):
        CPFinder = finders["ContractualPosition"]
        # business_date=2023-01-15, processing_valid_at=now → should return row 1 (quantity=100)
        result = CPFinder.find_all(
            "2023-01-15",
            datetime.datetime.now(),
            [CPFinder.quantity(), CPFinder.npv()],
        ).to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["Quantity"] == 100.0

    def test_different_business_date_excluded(self, finders):
        CPFinder = finders["ContractualPosition"]
        # business_date=2023-01-15 should not return the row for 2023-01-16
        result = CPFinder.find_all(
            "2023-01-15",
            datetime.datetime.now(),
            [CPFinder.quantity()],
        ).to_pandas()
        quantities = result["Quantity"].tolist()
        assert 200.0 not in quantities

    def test_processing_expired_row_excluded(self, finders):
        CPFinder = finders["ContractualPosition"]
        # Row 2 (quantity=90) was superseded at 2023-01-15; should not appear at processing_valid_at=now
        result = CPFinder.find_all(
            "2023-01-15",
            datetime.datetime.now(),
            [CPFinder.quantity()],
        ).to_pandas()
        quantities = result["Quantity"].tolist()
        assert 90.0 not in quantities

    def test_processing_expired_row_visible_before_expiry(self, finders):
        CPFinder = finders["ContractualPosition"]
        # Row 2 (quantity=90) was active before 2023-01-15; should appear at processing_valid_at=2023-01-12
        result = CPFinder.find_all(
            "2023-01-15",
            "2023-01-12 12:00:00",
            [CPFinder.quantity()],
        ).to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["Quantity"] == 90.0
