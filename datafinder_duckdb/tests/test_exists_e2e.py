"""E2E tests for RelatedFinder.exists() / not_exists().

Two scenarios are covered:
- Non-milestoned table: plain Table with no temporal columns; all rows always visible.
- Milestoned table: MilestonedTable with ProcessingTemporalColumns; the milestoning
  filter (WHERE end_at > :processing_dt) runs alongside exists/not_exists so only
  rows current at the requested processing_datetime are considered.
"""
import datetime

import duckdb
import pytest

from datafinder import to_sql
from datafinder.finder import RelatedFinder
from datafinder.typed_attributes import StringAttribute, IntegerAttribute
from model.relational import Column, Table, JoinOperation, JoinTreeNodeOperation, NoOperation
from model.milestoning import ProcessingTemporalColumns, MilestonedTable

INFINITE = "9999-12-31 23:59:59"
PROCESSING_DT = datetime.datetime(2024, 6, 1, 12, 0, 0)


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA trading")
    conn.execute("CREATE SCHEMA ref_data")
    conn.execute(
        "CREATE TABLE trading.trades "
        "(id INTEGER, account_id INTEGER, sym VARCHAR, start_at TIMESTAMP, end_at TIMESTAMP)"
    )
    conn.execute("CREATE TABLE ref_data.account_master (ID INTEGER, ACCT_NAME VARCHAR)")
    conn.execute("INSERT INTO ref_data.account_master VALUES (100, 'Acme Fund')")

    rows = [
        # (id, account_id, sym, start_at, end_at, description)
        (1, 100, "AAPL", "2024-01-01 09:00:00", INFINITE),   # current, account exists
        (2, 999, "GS",   "2024-01-01 09:00:00", INFINITE),   # current, account missing
        (3, 100, "IBM",  "2020-01-01 09:00:00", "2021-01-01 09:00:00"),  # expired, account exists
        (4, 999, "MSFT", "2020-01-01 09:00:00", "2021-01-01 09:00:00"),  # expired, account missing
    ]
    values = ", ".join(f"({r[0]}, {r[1]}, '{r[2]}', '{r[3]}', '{r[4]}')" for r in rows)
    conn.execute(f"INSERT INTO trading.trades VALUES {values}")
    return conn


def _build_finder():
    milestoning = ProcessingTemporalColumns(
        Column("start_at", "TIMESTAMP"),
        Column("end_at", "TIMESTAMP"),
        infinite_datetime=INFINITE,
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
    account_finder = RelatedFinder(account_node)

    id_attr = IntegerAttribute("Id", "id", "INTEGER", "trading.trades")
    sym_attr = StringAttribute("Symbol", "sym", "VARCHAR", "trading.trades")
    acct_name_attr = StringAttribute("Account Name", "ACCT_NAME", "VARCHAR", "ref_data.account_master", account_node)

    return trade_table, id_attr, sym_attr, acct_name_attr, account_finder


class TestExistsWithMilestoning:

    def test_exists_returns_only_current_trades_with_matching_account(self, con):
        """exists() must apply both the milestoning filter and the join-key IS NOT NULL check.
        Only trade 1 (current + account 100 exists) should be returned."""
        trade_table, id_attr, sym_attr, acct_name_attr, account_finder = _build_finder()
        sql = to_sql(None, PROCESSING_DT, [id_attr, sym_attr], trade_table, account_finder.exists())
        rows = con.sql(sql).fetchall()
        ids = [r[0] for r in rows]
        assert ids == [1], f"Expected only trade 1 but got ids: {ids}"

    def test_not_exists_returns_only_current_trades_with_no_account(self, con):
        """not_exists() must apply both the milestoning filter and the join-key IS NULL check.
        Only trade 2 (current + account 999 missing) should be returned."""
        trade_table, id_attr, sym_attr, acct_name_attr, account_finder = _build_finder()
        sql = to_sql(None, PROCESSING_DT, [id_attr, sym_attr], trade_table, account_finder.not_exists())
        rows = con.sql(sql).fetchall()
        ids = [r[0] for r in rows]
        assert ids == [2], f"Expected only trade 2 but got ids: {ids}"

    def test_expired_trades_excluded_even_when_account_exists(self, con):
        """Trade 3 is expired (end_at in past) and has a matching account.
        Milestoning must exclude it from exists() results."""
        trade_table, id_attr, sym_attr, acct_name_attr, account_finder = _build_finder()
        sql = to_sql(None, PROCESSING_DT, [id_attr, sym_attr], trade_table, account_finder.exists())
        rows = con.sql(sql).fetchall()
        ids = [r[0] for r in rows]
        assert 3 not in ids, "Expired trade (id=3) must not appear in exists() results"

    def test_expired_trades_excluded_even_when_account_missing(self, con):
        """Trade 4 is expired and has no matching account.
        Milestoning must exclude it from not_exists() results."""
        trade_table, id_attr, sym_attr, acct_name_attr, account_finder = _build_finder()
        sql = to_sql(None, PROCESSING_DT, [id_attr, sym_attr], trade_table, account_finder.not_exists())
        rows = con.sql(sql).fetchall()
        ids = [r[0] for r in rows]
        assert 4 not in ids, "Expired trade (id=4) must not appear in not_exists() results"

    def test_exists_correct_account_name_returned(self, con):
        """When exists() matches, the joined account columns should be populated."""
        trade_table, id_attr, sym_attr, acct_name_attr, account_finder = _build_finder()
        sql = to_sql(None, PROCESSING_DT, [sym_attr, acct_name_attr], trade_table, account_finder.exists())
        rows = con.sql(sql).fetchall()
        assert rows == [("AAPL", "Acme Fund")]

    def test_exists_at_historical_processing_dt_includes_expired_rows(self, con):
        """When processing_datetime is set to a point in the past, expired rows that
        were valid at that time are included.  Trade 3 (account 100, expired 2021-01-01)
        should appear when queried with a processing_datetime inside its validity window."""
        trade_table, id_attr, sym_attr, acct_name_attr, account_finder = _build_finder()
        historical_dt = datetime.datetime(2020, 6, 1, 12, 0, 0)
        sql = to_sql(None, historical_dt, [id_attr], trade_table, account_finder.exists())
        rows = con.sql(sql).fetchall()
        ids = sorted(r[0] for r in rows)
        # Trade 1 starts 2024-01-01, which is after the historical_dt, so it is not visible.
        # Trade 3 started 2020-01-01 and ended 2021-01-01, valid at 2020-06-01.
        assert ids == [3], f"Only trade 3 should be visible at historical processing_dt; got: {ids}"


# ---------------------------------------------------------------------------
# Non-milestoned scenario
# ---------------------------------------------------------------------------

@pytest.fixture
def con_plain():
    """Plain (non-milestoned) orders table with a nullable category FK."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE orders (id INTEGER, category_id INTEGER, sym VARCHAR)")
    conn.execute("CREATE TABLE categories (ID INTEGER, NAME VARCHAR)")
    conn.execute("INSERT INTO categories VALUES (10, 'Equity'), (20, 'Fixed Income')")
    conn.execute(
        "INSERT INTO orders VALUES "
        "(1, 10,   'AAPL'), "   # category exists
        "(2, 20,   'TLT'),  "   # category exists
        "(3, 999,  'GS'),   "   # category missing
        "(4, NULL, 'MSFT')"     # FK is NULL → also no matching category
    )
    return conn


def _build_plain_finder():
    orders_table = Table("orders", [])
    categories_table = Table("categories", [])

    cat_join = JoinOperation(
        "Category",
        categories_table,
        Column("category_id", "INT", "orders"),
        Column("ID", "INT", "categories"),
    )
    cat_node = JoinTreeNodeOperation(cat_join)
    cat_finder = RelatedFinder(cat_node)

    id_attr = IntegerAttribute("Id", "id", "INTEGER", "orders")
    sym_attr = StringAttribute("Symbol", "sym", "VARCHAR", "orders")
    cat_name_attr = StringAttribute("Category", "NAME", "VARCHAR", "categories", cat_node)

    return orders_table, id_attr, sym_attr, cat_name_attr, cat_finder


class TestExistsWithoutMilestoning:

    def test_exists_returns_rows_with_matching_category(self, con_plain):
        """exists() on a plain table returns every row whose FK matches a category row."""
        orders_table, id_attr, sym_attr, cat_name_attr, cat_finder = _build_plain_finder()
        sql = to_sql(None, None, [id_attr], orders_table, cat_finder.exists())
        rows = con_plain.sql(sql).fetchall()
        ids = sorted(r[0] for r in rows)
        assert ids == [1, 2], f"Expected orders with matching category; got: {ids}"

    def test_not_exists_returns_rows_with_no_matching_category(self, con_plain):
        """not_exists() on a plain table returns rows whose FK has no match (including NULL FK)."""
        orders_table, id_attr, sym_attr, cat_name_attr, cat_finder = _build_plain_finder()
        sql = to_sql(None, None, [id_attr], orders_table, cat_finder.not_exists())
        rows = con_plain.sql(sql).fetchall()
        ids = sorted(r[0] for r in rows)
        assert ids == [3, 4], f"Expected orders with no matching category; got: {ids}"

    def test_exists_correct_category_name_returned(self, con_plain):
        """When exists() matches, the joined category name should be populated correctly."""
        orders_table, id_attr, sym_attr, cat_name_attr, cat_finder = _build_plain_finder()
        sql = to_sql(None, None, [sym_attr, cat_name_attr], orders_table, cat_finder.exists())
        rows = con_plain.sql(sql).fetchall()
        by_sym = {r[0]: r[1] for r in rows}
        assert by_sym["AAPL"] == "Equity"
        assert by_sym["TLT"] == "Fixed Income"

    def test_all_rows_present_without_filter(self, con_plain):
        """Baseline: without any filter all 4 orders are returned (LEFT OUTER JOIN)."""
        orders_table, id_attr, sym_attr, cat_name_attr, cat_finder = _build_plain_finder()
        sql = to_sql(None, None, [id_attr], orders_table, NoOperation())
        rows = con_plain.sql(sql).fetchall()
        assert len(rows) == 4
