"""Verify that a LEFT OUTER JOIN is used so the primary object (trade) is always
returned even when the associated object (account) does not exist."""
import datetime

import duckdb
import numpy as np
import pytest
from numpy.testing import assert_array_equal

from datafinder import to_sql
from datafinder.typed_attributes import StringAttribute, IntegerAttribute, FloatAttribute
from model.relational import Column, Table, JoinOperation, JoinTreeNodeOperation, NoOperation
from model.milestoning import ProcessingTemporalColumns, MilestonedTable


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA trading")
    conn.execute("CREATE SCHEMA ref_data")
    conn.execute(
        "CREATE TABLE trading.trades "
        "(id INTEGER, account_id INTEGER, sym VARCHAR, price DOUBLE, start_at TIMESTAMP, end_at TIMESTAMP)"
    )
    conn.execute(
        "CREATE TABLE ref_data.account_master (ID INTEGER, ACCT_NAME VARCHAR)"
    )
    # Two accounts exist
    conn.execute("INSERT INTO ref_data.account_master VALUES (100, 'Existing Account')")
    # Three trades: two belong to account 100 (exists), one belongs to account 999 (missing)
    ts_open = "9999-12-31 23:59:59"
    conn.execute(
        "INSERT INTO trading.trades VALUES "
        f"(1, 100, 'AAPL', 84.11, '2024-01-01 09:00:00', '{ts_open}'), "
        f"(2, 100, 'IBM',  200.5, '2024-01-01 09:00:00', '{ts_open}'), "
        f"(3, 999, 'GS',   45.7, '2024-01-01 09:00:00', '{ts_open}')"
    )
    return conn


def _build_finder():
    """Mirror the structure produced by the code generator for TradeFinder."""
    milestoning = ProcessingTemporalColumns(
        Column("start_at", "TIMESTAMP"),
        Column("end_at", "TIMESTAMP"),
        infinite_datetime="9999-12-31 23:59:59",
    )
    trade_table = MilestonedTable("trading.trades", [], milestoning)

    account_table = Table("ref_data.account_master", [])

    account_join = JoinOperation(
        "Account",
        account_table,
        Column("account_id", "INT", "trading.trades"),
        Column("ID", "INT", "ref_data.account_master"),
    )
    account_node = JoinTreeNodeOperation(account_join)

    sym_attr = StringAttribute("Symbol", "sym", "VARCHAR", "trading.trades")
    price_attr = FloatAttribute("Price", "price", "DOUBLE", "trading.trades")
    acct_name_attr = StringAttribute("Account Name", "ACCT_NAME", "VARCHAR", "ref_data.account_master", account_node)

    return trade_table, sym_attr, price_attr, acct_name_attr


class TestLeftOuterJoinMissingAssociation:

    def test_all_trades_returned_when_account_missing(self, con):
        """A trade whose account_id has no matching row in account_master must
        still appear in results — the join must be LEFT OUTER, not INNER."""
        trade_table, sym_attr, price_attr, acct_name_attr = _build_finder()
        processing_dt = datetime.datetime(2024, 6, 1, 12, 0, 0)

        sql = to_sql(None, processing_dt, [sym_attr], trade_table, NoOperation())
        rows = con.sql(sql).fetchall()
        syms = sorted(r[0] for r in rows)

        assert syms == ["AAPL", "GS", "IBM"], (
            f"Expected all 3 trades but got: {syms}. "
            "Check that the join is LEFT OUTER and not INNER."
        )

    def test_trade_with_missing_account_has_null_account_name(self, con):
        """The trade whose account doesn't exist should return NULL for account fields."""
        trade_table, sym_attr, price_attr, acct_name_attr = _build_finder()
        processing_dt = datetime.datetime(2024, 6, 1, 12, 0, 0)

        sql = to_sql(None, processing_dt, [sym_attr, acct_name_attr], trade_table, NoOperation())
        rows = con.sql(sql).fetchall()
        by_sym = {r[0]: r[1] for r in rows}

        assert by_sym["AAPL"] == "Existing Account"
        assert by_sym["IBM"] == "Existing Account"
        assert by_sym["GS"] is None, (
            f"Expected NULL account name for GS (account 999 doesn't exist) but got: {by_sym['GS']}"
        )

    def test_trade_count_is_not_reduced_by_missing_account(self, con):
        """Row count must equal the number of trades, not the number of matched accounts."""
        trade_table, sym_attr, price_attr, acct_name_attr = _build_finder()
        processing_dt = datetime.datetime(2024, 6, 1, 12, 0, 0)

        sql = to_sql(None, processing_dt, [sym_attr, acct_name_attr], trade_table, NoOperation())
        rows = con.sql(sql).fetchall()

        assert len(rows) == 3, (
            f"Expected 3 rows (one per trade) but got {len(rows)}. "
            "An INNER JOIN would silently drop trade with missing account."
        )
