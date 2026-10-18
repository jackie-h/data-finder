"""Apply a generated Liquibase changelog to a real (file-backed) DuckDB database and query the view."""
import re

import duckdb
import pytest

from datafinder import FinderResult
from datafinder.typed_attributes import StringAttribute
from datafinder_liquibase import write_changelog
from model.milestoning import MilestonedTable, ProcessingTemporalColumns
from model.relational import Column, JoinOperation, JoinTreeNodeOperation, NoOperation

_OPEN = "9999-12-31 23:59:59"


def _trade_account_view():
    milestoning = lambda: ProcessingTemporalColumns(Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP"))
    trades = MilestonedTable("vt.trades", [], milestoning())
    account = MilestonedTable("vt.account", [], milestoning())
    join = JoinOperation("Account", account, Column("account_id", "INT", "vt.trades"),
                         Column("id", "INT", "vt.account"))
    node = JoinTreeNodeOperation(join, None)
    symbol = StringAttribute("Symbol", "sym", "VARCHAR", "vt.trades")
    account_name = StringAttribute("Account Name", "name", "VARCHAR", "vt.account", node)
    return FinderResult(None, None, [account_name, symbol], trades, NoOperation()).to_view("trade_account", "reporting")


def _changeset_statements(changelog: str) -> list[str]:
    """What Liquibase would run: the body of each changeset, without comment lines such as --rollback."""
    bodies = re.split(r"^--changeset .*$", changelog, flags=re.MULTILINE)[1:]
    return ["\n".join(l for l in body.splitlines() if l.strip() and not l.startswith("--")) for body in bodies]


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / "local.duckdb")
    con = duckdb.connect(path)
    con.execute("CREATE SCHEMA vt")
    con.execute("CREATE SCHEMA reporting")
    con.execute("CREATE TABLE vt.trades (sym VARCHAR, account_id INT, in_z TIMESTAMP, out_z TIMESTAMP)")
    con.execute("CREATE TABLE vt.account (id INT, name VARCHAR, in_z TIMESTAMP, out_z TIMESTAMP)")
    con.execute(f"INSERT INTO vt.account VALUES (1, 'Acme Old', '2020-01-01', '2022-01-01'), "
                f"(1, 'Acme New', '2022-01-01', '{_OPEN}')")
    con.execute(f"INSERT INTO vt.trades VALUES ('AAPL', 1, '2021-06-01', '{_OPEN}'), "
                f"('GOOG', 1, '2023-01-01', '{_OPEN}')")
    con.close()
    return path


def test_generated_changelog_creates_a_working_view_on_duckdb(tmp_path, db_path):
    changelog = write_changelog([_trade_account_view()], tmp_path / "views.sql", author="jackie").read_text()

    con = duckdb.connect(db_path)
    for statement in _changeset_statements(changelog):
        con.execute(statement)
    con.close()

    # a fresh connection sees the persisted view
    con = duckdb.connect(db_path)
    rows = con.execute("SELECT symbol, account_name FROM reporting.trade_account ORDER BY symbol").fetchall()
    assert rows == [("AAPL", "Acme Old"), ("GOOG", "Acme New")]
    assert [c[0] for c in con.execute("DESCRIBE reporting.trade_account").fetchall()] == \
        ["account_name", "symbol", "in_z", "out_z"]


def test_changeset_is_rerunnable_and_rollback_drops_the_view(tmp_path, db_path):
    changelog = write_changelog([_trade_account_view()], tmp_path / "views.sql", author="jackie").read_text()
    con = duckdb.connect(db_path)
    (statement,) = _changeset_statements(changelog)
    con.execute(statement)
    con.execute(statement)  # runOnChange redeploys over the existing view

    rollback = re.search(r"^--rollback (.*)$", changelog, flags=re.MULTILINE)
    assert rollback is not None
    con.execute(rollback.group(1))
    with pytest.raises(duckdb.CatalogException):
        con.execute("SELECT * FROM reporting.trade_account")
