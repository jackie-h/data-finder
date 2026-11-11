"""join: the rows of two results put together, as Legend's join and as pandas, polars and ibis have it: a condition that is a function
of a row of each, and the kind of join as ``how``."""
import pytest

from datafinder import DoubleAttribute, StringAttribute
from datafinder.runner import FinderResult
from model.relational import (ComparisonOperation, JoinKind, JoinOperation, NoOperation, QuerySpecification, Table,
                              TableAlias)


def _results() -> tuple[FinderResult, FinderResult]:
    trades = Table("trades", [("ACCOUNT", "VARCHAR"), ("SYM", "VARCHAR"), ("PRICE", "DOUBLE")])
    accounts = Table("accounts", [("ID", "VARCHAR"), ("NAME", "VARCHAR")])
    left = FinderResult(None, None, [StringAttribute("Account", "ACCOUNT", "VARCHAR", trades).alias("account"),
                                     StringAttribute("Sym", "SYM", "VARCHAR", trades).alias("sym"),
                                     DoubleAttribute("Price", "PRICE", "DOUBLE", trades).alias("price")], trades, NoOperation())
    right = FinderResult(None, None, [StringAttribute("Id", "ID", "VARCHAR", accounts).alias("id"),
                                      StringAttribute("Name", "NAME", "VARCHAR", accounts).alias("name")], accounts, NoOperation())
    return left, right


def _from(sql: str) -> str:
    return sql[sql.index(" FROM "):]


@pytest.mark.parametrize("how, word", [("inner", "INNER JOIN"), ("left", "LEFT OUTER JOIN"), ("right", "RIGHT OUTER JOIN"),
                                       ("full", "FULL OUTER JOIN")])
def test_a_join_is_written_as_the_join_of_the_two_queries(how, word):
    trades, accounts = _results()
    sql = trades.join(accounts, lambda t, a: t.account == a.id, how=how).to_sql()
    # the join is a query of its own, which the result reads from, as Legend wraps a joined relation
    assert sql.startswith('SELECT t0.account AS "account",t0.sym AS "sym",t0.price AS "price",t0.id AS "id",t0.name AS "name" FROM (SELECT t1.account AS "account",')
    assert f") AS t1 {word} (SELECT " in sql and sql.endswith(") AS t2 ON (t1.account = t2.id)) AS t0")


def test_how_is_required_as_legends_join_has_no_default_kind():
    trades, accounts = _results()
    with pytest.raises(TypeError, match="how"):
        trades.join(accounts, lambda t, a: t.account == a.id)  # type: ignore[call-arg]


def test_a_condition_of_several_comparisons_is_joined_with_and_or_or():
    trades, accounts = _results()
    sql = trades.join(accounts, lambda t, a: ((t.account == a.id) & (t.sym != a.name)) | (t.price > 5.0), how="inner").to_sql()
    assert "ON ((t1.account = t2.id AND t1.sym <> t2.name) OR t1.price > 5.0)) AS t0" in sql


def test_a_joined_result_can_be_filtered_extended_ordered_and_aggregated_like_any_other():
    trades, accounts = _results()
    joined = trades.join(accounts, lambda t, a: t.account == a.id, how="inner")
    totals = joined.filter(lambda r: r.price > 1.0).group_by(lambda r: r.name).aggregate(total=lambda r: r.price.sum())
    sql = totals.order_by(totals["total"].descending()).to_sql()
    assert "INNER JOIN" in sql and "GROUP BY t0.name" in sql and sql.endswith("ORDER BY SUM(t0.price) DESC")


def test_a_join_can_be_joined_again():
    trades, accounts = _results()
    other = FinderResult(None, None, [StringAttribute("Region", "REGION", "VARCHAR", Table("regions", [("REGION", "VARCHAR")]))
                                      .alias("region")], Table("regions", [("REGION", "VARCHAR")]), NoOperation())
    twice = trades.join(accounts, lambda t, a: t.account == a.id, how="inner").join(other, lambda j, r: j.name == r.region, how="left")
    assert twice.columns() == ["account", "sym", "price", "id", "name", "region"]
    assert twice.to_sql().count("JOIN") == 2


def test_the_columns_of_a_join_are_the_columns_of_both():
    trades, accounts = _results()
    assert trades.join(accounts, lambda t, a: t.account == a.id, how="inner").columns() == ["account", "sym", "price", "id", "name"]


def test_the_model_of_a_join_is_a_query_of_the_left_with_a_join_to_the_right():
    trades, accounts = _results()
    alias = trades.join(accounts, lambda t, a: t.account == a.id, how="left")._table
    assert isinstance(alias, TableAlias) and isinstance(alias.relation, QuerySpecification)
    query = alias.relation
    (join,) = query.joins
    assert isinstance(join, JoinOperation) and join.kind is JoinKind.LEFT and isinstance(join.operation, ComparisonOperation)
    assert isinstance(query.table, TableAlias) and isinstance(join.target, TableAlias)
    assert len(query.columns) == len(query.table.columns) + len(join.target.columns) == 5


def test_the_two_sides_columns_must_be_named_differently():
    trades, _ = _results()
    with pytest.raises(ValueError, match="account"):
        trades.join(trades, lambda a, b: a.price == b.price, how="inner")


def test_how_is_one_of_the_ansi_join_types():
    trades, accounts = _results()
    for how in ("outer", "cross", "semi", "anti"):
        with pytest.raises(ValueError, match="one of"):
            trades.join(accounts, lambda t, a: t.account == a.id, how=how)  # type: ignore[arg-type]


def test_a_condition_is_a_function_that_gives_a_comparison():
    trades, accounts = _results()
    with pytest.raises(TypeError, match="function of a row"):
        trades.join(accounts, trades["account"] == accounts["id"], how="inner")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="comparison"):
        trades.join(accounts, lambda t, a: t.account, how="inner")


def test_a_column_of_a_row_that_is_not_there_says_which_are():
    trades, accounts = _results()
    with pytest.raises(AttributeError, match="id"):
        trades.join(accounts, lambda t, a: t.account == a.account, how="inner")
