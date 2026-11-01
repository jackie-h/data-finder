"""E2E tests for exists() / not_exists() on a to-many association.

A parent with several matching rows must still be one row: ``exists()`` asks whether a row exists, not for a row per
match. As in Legend, the child is joined as ``SELECT DISTINCT <join column> ... WHERE <milestoning>``. A to-many step
the query also reads a value through stays a plain join, since those values are one row per child.
"""
import datetime

import duckdb
import pytest

from datafinder import to_sql, count_all
from datafinder.finder import RelatedFinder
from datafinder.typed_attributes import IntegerAttribute, StringAttribute
from model.milestoning import MilestonedTable, ProcessingTemporalColumns
from model.relational import Column, JoinOperation, JoinTreeNodeOperation, Table

INFINITE = "9999-12-31 23:59:59"
NOW = datetime.datetime(2024, 6, 1, 12, 0, 0)
PAST = datetime.datetime(2020, 6, 1, 12, 0, 0)


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE accounts (ID INTEGER, NAME VARCHAR)")
    conn.execute("CREATE TABLE trades (id INTEGER, account_id INTEGER, sym VARCHAR, start_at TIMESTAMP, end_at TIMESTAMP)")
    conn.execute("CREATE TABLE positions (id INTEGER, account_id INTEGER, qty INTEGER)")
    conn.execute("INSERT INTO accounts VALUES (1, 'Acme'), (2, 'Beta'), (3, 'Gamma'), (4, 'Delta')")
    conn.execute(
        "INSERT INTO trades VALUES "
        f"(11, 1, 'AAPL', '2024-01-01 09:00:00', '{INFINITE}'), "  # Acme: two current trades
        f"(12, 1, 'MSFT', '2024-01-01 09:00:00', '{INFINITE}'), "
        "(13, 1, 'GS',   '2020-01-01 09:00:00', '2021-01-01 09:00:00'), "  # Acme: an expired one
        f"(14, 2, 'IBM',  '2024-01-01 09:00:00', '{INFINITE}'), "  # Beta: one current trade
        "(15, 4, 'OLD',  '2020-01-01 09:00:00', '2021-01-01 09:00:00')"  # Delta: only an expired trade
    )
    conn.execute("INSERT INTO positions VALUES (21, 1, 5), (22, 1, 6), (23, 3, 7), (24, 3, 8)")
    return conn


def _accounts():
    """Accounts with two to-many navigations: trades (milestoned) and positions."""
    accounts = Table("accounts", [])
    trades_table = MilestonedTable(
        "trades", [], ProcessingTemporalColumns(Column("start_at", "TIMESTAMP"), Column("end_at", "TIMESTAMP"),
                                                infinite_datetime=INFINITE))
    positions_table = Table("positions", [])
    trades_node = JoinTreeNodeOperation(
        JoinOperation("Trades", trades_table, Column("ID", "INT", "accounts", primary_key=True),
                      Column("account_id", "INT", "trades")), many=True)
    positions_node = JoinTreeNodeOperation(
        JoinOperation("Positions", positions_table, Column("ID", "INT", "accounts", primary_key=True),
                      Column("account_id", "INT", "positions")), many=True)
    account_id = IntegerAttribute("Id", "ID", "INT", "accounts")
    trade_sym = StringAttribute("Symbol", "sym", "VARCHAR", "trades", trades_node)
    return accounts, account_id, trade_sym, RelatedFinder(trades_node), RelatedFinder(positions_node)


def _ids(con, sql):
    return sorted(row[0] for row in con.sql(sql).fetchall())


def test_exists_returns_a_parent_once_however_many_rows_match(con):
    accounts, account_id, _, trades, _ = _accounts()
    sql = to_sql(None, NOW, [account_id], accounts, trades.exists())
    # Acme has two current trades but is one row; Delta's only trade has expired
    assert _ids(con, sql) == [1, 2]


def test_exists_counts_a_parent_once(con):
    accounts, _, _, trades, _ = _accounts()
    sql = to_sql(None, NOW, [count_all()], accounts, trades.exists())
    assert con.sql(sql).fetchall() == [(2,)]


def test_not_exists_returns_parents_with_no_current_row(con):
    accounts, account_id, _, trades, _ = _accounts()
    sql = to_sql(None, NOW, [account_id], accounts, trades.not_exists())
    assert _ids(con, sql) == [3, 4]  # Delta has only an expired trade


def test_the_child_is_milestoned_inside_the_derived_table(con):
    accounts, account_id, _, trades, _ = _accounts()
    sql = to_sql(None, PAST, [account_id], accounts, trades.exists())
    assert _ids(con, sql) == [1, 4]  # the trades that were valid in 2020: Acme's GS and Delta's OLD
    inner = sql[sql.index("(SELECT DISTINCT"):sql.index(") AS ")]
    assert "start_at <=" in inner and "end_at >" in inner
    assert "start_at" not in sql[sql.index(") AS "):]  # and not in the ON clause


def test_exists_and_not_exists_on_the_same_step_cover_every_parent_once(con):
    accounts, account_id, _, trades, _ = _accounts()
    sql = to_sql(None, NOW, [account_id], accounts, trades.exists().or_(trades.not_exists()))
    assert _ids(con, sql) == [1, 2, 3, 4]


def test_two_to_many_steps_do_not_multiply_each_other(con):
    accounts, account_id, _, trades, positions = _accounts()
    # Acme has two current trades and two positions: four rows if both were plain joins
    sql = to_sql(None, NOW, [account_id], accounts, trades.exists().and_(positions.exists()))
    assert _ids(con, sql) == [1]


def test_a_derived_table_is_used_only_for_exists(con):
    accounts, account_id, _, trades, _ = _accounts()
    sql = to_sql(None, NOW, [account_id], accounts, trades.exists())
    # the table inside the sub-select and the sub-select itself have their own aliases
    assert "LEFT OUTER JOIN (SELECT DISTINCT t2.account_id FROM trades AS t2 WHERE " in sql
    assert ") AS t1 ON (t0.ID = t1.account_id)" in sql
    assert "WHERE t1.account_id IS NOT NULL" in sql


def test_reading_a_value_through_the_step_keeps_a_row_per_child(con):
    accounts, account_id, trade_sym, trades, _ = _accounts()
    sql = to_sql(None, NOW, [account_id, trade_sym], accounts, trades.exists())
    assert "SELECT DISTINCT" not in sql
    assert sorted(con.sql(sql).fetchall()) == [(1, "AAPL"), (1, "MSFT"), (2, "IBM")]


def test_exists_through_a_to_one_step(con):
    # positions -> account (to-one) -> trades (to-many): the derived table joins on the account's alias
    positions_table = Table("positions", [])
    account_node = JoinTreeNodeOperation(
        JoinOperation("Account", Table("accounts", []), Column("account_id", "INT", "positions"),
                      Column("ID", "INT", "accounts", primary_key=True)))
    trades_table = MilestonedTable(
        "trades", [], ProcessingTemporalColumns(Column("start_at", "TIMESTAMP"), Column("end_at", "TIMESTAMP"),
                                                infinite_datetime=INFINITE))
    trades_node = JoinTreeNodeOperation(
        JoinOperation("Trades", trades_table, Column("ID", "INT", "accounts", primary_key=True),
                      Column("account_id", "INT", "trades")), parent=account_node, many=True)
    position_id = IntegerAttribute("Id", "id", "INT", "positions")
    sql = to_sql(None, NOW, [position_id], positions_table, RelatedFinder(trades_node).exists())
    assert _ids(con, sql) == [21, 22]  # both positions of Acme, once each
    assert "SELECT DISTINCT" in sql
