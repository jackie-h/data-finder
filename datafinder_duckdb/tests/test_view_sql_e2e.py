"""A View rendered as CREATE VIEW DDL and created in DuckDB.

Trade (milestoned) -> Account (milestoned) -> Branch (not milestoned), built by hand the way the generated
finders build them. Account 1 has two versions that point at different branches.
"""
import datetime

import duckdb
import pytest

from datafinder import to_sql
from datafinder.sql_generator import view_sql_to_string
from datafinder.typed_attributes import StringAttribute, FloatAttribute
from model.milestoning import MilestonedTable, ProcessingTemporalColumns
from model.relational import Column, Table, Alias, JoinOperation, JoinTreeNodeOperation, NoOperation, \
    AggregateOperation, AggregateOperator, WindowFunction, WindowFunctionOperation, WindowSpecification
from model.view import View

_OPEN = "9999-12-31 23:59:59"


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA vt")
    conn.execute("CREATE SCHEMA reporting")
    conn.execute("CREATE TABLE vt.trades (sym VARCHAR, price DOUBLE, account_id INT, in_z TIMESTAMP, out_z TIMESTAMP)")
    conn.execute("CREATE TABLE vt.account (id INT, name VARCHAR, branch_id INT, in_z TIMESTAMP, out_z TIMESTAMP)")
    conn.execute("CREATE TABLE vt.branch (id INT, city VARCHAR)")
    conn.execute("INSERT INTO vt.branch VALUES (10, 'London'), (11, 'Paris')")
    conn.execute(f"INSERT INTO vt.account VALUES (1, 'Acme Old', 10, '2020-01-01', '2022-01-01'), "
                 f"(1, 'Acme New', 11, '2022-01-01', '{_OPEN}')")
    conn.execute(f"INSERT INTO vt.trades VALUES "
                 f"('AAPL', 10.0, 1, '2020-06-01', '{_OPEN}'), "
                 f"('GOOG', 20.0, 1, '2020-06-01', '{_OPEN}'), "
                 f"('IBM', 5.0, 1, '2023-01-01', '{_OPEN}')")
    return conn


def _milestoning():
    return ProcessingTemporalColumns(Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP"))


class _Model:
    """The tables, joins and attributes shared by the view and the selects in a test."""

    def __init__(self):
        self.trades = MilestonedTable("vt.trades", [], _milestoning())
        account_join = JoinOperation("Account", MilestonedTable("vt.account", [], _milestoning()),
                                     Column("account_id", "INT", "vt.trades"), Column("id", "INT", "vt.account"))
        account_node = JoinTreeNodeOperation(account_join)
        branch_join = JoinOperation("Branch", Table("vt.branch", []), Column("branch_id", "INT", "vt.account"),
                                    Column("id", "INT", "vt.branch"))
        branch_node = JoinTreeNodeOperation(branch_join, account_node)
        self.symbol = StringAttribute("Symbol", "sym", "VARCHAR", "vt.trades")
        self.price = FloatAttribute("Price", "price", "DOUBLE", "vt.trades")
        self.account_name = StringAttribute("Account Name", "name", "VARCHAR", "vt.account", account_node)
        self.city = StringAttribute("Branch City", "city", "VARCHAR", "vt.branch", branch_node)


def _rows(con, sql):
    return con.execute(sql).fetchall()


def test_each_trade_version_sees_the_account_applicable_at_its_time(con):
    m = _Model()
    view = View("trade_account", "reporting", m.trades,
                [Alias(m.symbol, "symbol"), Alias(m.account_name, "account_name")])
    sql = view_sql_to_string(view)
    assert "GREATEST" not in sql and "LEAST" not in sql and "ORDER BY" not in sql and "LIMIT" not in sql
    con.execute(sql)
    assert _rows(con, "SELECT symbol, account_name FROM reporting.trade_account ORDER BY symbol") == \
        [("AAPL", "Acme Old"), ("GOOG", "Acme Old"), ("IBM", "Acme New")]
    assert [c[0] for c in con.execute("DESCRIBE reporting.trade_account").fetchall()] == \
        ["symbol", "account_name", "in_z", "out_z"]


def test_view_does_not_change_when_reference_data_changes(con):
    m = _Model()
    view = View("trade_account", "reporting", m.trades,
                [Alias(m.symbol, "symbol"), Alias(m.account_name, "account_name")])
    con.execute(view_sql_to_string(view))
    query = "SELECT symbol, account_name FROM reporting.trade_account ORDER BY symbol, in_z"
    before = _rows(con, query)

    con.execute("UPDATE vt.account SET out_z = '2024-01-01' WHERE name = 'Acme New'")
    con.execute(f"INSERT INTO vt.account VALUES (1, 'Acme Newest', 11, '2024-01-01', '{_OPEN}')")
    assert _rows(con, query) == before


def test_an_ancestor_that_nothing_projects_is_pinned_too(con):
    m = _Model()
    view = View("trade_city", "reporting", m.trades, [Alias(m.symbol, "symbol"), Alias(m.city, "city")])
    con.execute(view_sql_to_string(view))
    assert _rows(con, "SELECT symbol, city FROM reporting.trade_city ORDER BY symbol") == \
        [("AAPL", "London"), ("GOOG", "London"), ("IBM", "Paris")]


def test_filter(con):
    m = _Model()
    view = View("goog", "reporting", m.trades, [Alias(m.account_name, "account_name")], filter=m.symbol.eq("GOOG"))
    con.execute(view_sql_to_string(view))
    assert _rows(con, "SELECT account_name FROM reporting.goog") == [("Acme Old",)]


def test_grouped_view_groups_by_the_root_validity_too(con):
    m = _Model()
    total = Alias(AggregateOperation(m.price, AggregateOperator.SUM), "total_price")
    view = View("account_totals", "reporting", m.trades, [Alias(m.account_name, "account_name"), total],
                group_by=[m.account_name])
    sql = view_sql_to_string(view)
    assert "GROUP BY" in sql
    con.execute(sql)
    # AAPL and GOOG share a validity and an account version, so they total together; IBM is its own group
    assert _rows(con, "SELECT account_name, total_price FROM reporting.account_totals ORDER BY total_price DESC") == \
        [("Acme Old", 30.0), ("Acme New", 5.0)]
    assert [c[0] for c in con.execute("DESCRIBE reporting.account_totals").fetchall()] == \
        ["account_name", "total_price", "in_z", "out_z"]


def test_window_function_view_keeps_every_row(con):
    m = _Model()
    numbered = Alias(WindowFunctionOperation(None, WindowFunction.ROW_NUMBER,
                                             window=WindowSpecification([], [m.symbol.ascending()])),
                     "n")
    view = View("numbered", "reporting", m.trades, [Alias(m.symbol, "symbol"), numbered])
    sql = view_sql_to_string(view)
    assert "GROUP BY" not in sql
    con.execute(sql)
    assert _rows(con, "SELECT symbol, n FROM reporting.numbered ORDER BY n") == [("AAPL", 1), ("GOOG", 2), ("IBM", 3)]


def test_rendering_is_not_affected_by_selects_run_on_the_same_joins_in_between(con):
    m = _Model()
    view = View("trade_account", "reporting", m.trades,
                [Alias(m.symbol, "symbol"), Alias(m.account_name, "account_name")])
    first = view_sql_to_string(view)
    # a select over the same shared join nodes leaves its own date on the Account join
    to_sql(None, datetime.datetime(2021, 6, 1, 12), [m.symbol, m.account_name], m.trades, NoOperation())
    assert view_sql_to_string(view) == first


def test_databricks_dialect():
    m = _Model()
    view = View("trade_account", "reporting", m.trades,
                [Alias(m.symbol, "symbol"), Alias(m.account_name, "account_name")])
    sql = view_sql_to_string(view, dialect="databricks")
    assert sql.startswith("CREATE OR REPLACE VIEW reporting.trade_account AS SELECT")
    assert "`account_name`" in sql
