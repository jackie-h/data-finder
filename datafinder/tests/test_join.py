"""join: the rows of two results put together, as Legend's join and as pandas, polars and ibis have it: a condition that is a function
of a row of each, and the kind of join as ``how``."""
import pytest

from datafinder import DoubleAttribute, StringAttribute
from datafinder.runner import FinderResult
from model.relational import ComparisonOperation, JoinedTable, JoinKind, NoOperation, Table


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
    assert sql.startswith('SELECT t0.account AS "account",t0.sym AS "sym",t0.price AS "price",t1.id AS "id",t1.name AS "name" FROM (SELECT ')
    assert f") AS t0 {word} (SELECT " in sql and sql.endswith(") AS t1 ON (t0.account = t1.id)")


def test_inner_is_the_default():
    trades, accounts = _results()
    assert " INNER JOIN " in trades.join(accounts, lambda t, a: t.account == a.id).to_sql()


def test_a_cross_join_has_no_condition_and_no_on():
    trades, accounts = _results()
    sql = trades.join(accounts, how="cross").to_sql()
    assert ") AS t0 CROSS JOIN (SELECT " in sql and " ON " not in sql and "WHERE" not in sql


def test_a_semi_join_keeps_the_rows_that_have_a_match_and_only_the_left_columns():
    trades, accounts = _results()
    sql = trades.join(accounts, lambda t, a: t.account == a.id, how="semi").to_sql()
    assert sql.startswith('SELECT t0.account AS "account",t0.sym AS "sym",t0.price AS "price" FROM (SELECT ')
    assert " WHERE EXISTS (SELECT 1 FROM (SELECT " in sql and sql.endswith("WHERE t0.account = t1.id)")
    assert " JOIN " not in sql


def test_an_anti_join_keeps_the_rows_that_have_none():
    trades, accounts = _results()
    sql = trades.join(accounts, lambda t, a: t.account == a.id, how="anti").to_sql()
    assert " WHERE NOT EXISTS (SELECT 1 FROM (SELECT " in sql


def test_a_condition_of_several_comparisons_is_joined_with_and_or_or():
    trades, accounts = _results()
    sql = trades.join(accounts, lambda t, a: ((t.account == a.id) & (t.sym != a.name)) | (t.price > 5.0)).to_sql()
    assert sql.endswith("ON (t0.account = t1.id AND t0.sym <> t1.name OR t0.price > 5.0)") \
        or sql.endswith("ON ((t0.account = t1.id AND t0.sym <> t1.name) OR t0.price > 5.0)")


def test_a_filtered_left_result_is_filtered_inside_it_and_the_existence_test_is_the_outer_where():
    trades, accounts = _results()
    sql = trades.filter(lambda r: r.price > 1.0).join(accounts, lambda t, a: t.account == a.id, how="semi").to_sql()
    assert "WHERE t3.price > 1.0" in sql and " WHERE EXISTS (SELECT 1 FROM " in sql


def test_a_joined_result_can_be_filtered_extended_ordered_and_aggregated_like_any_other():
    trades, accounts = _results()
    joined = trades.join(accounts, lambda t, a: t.account == a.id)
    totals = joined.filter(lambda r: r.price > 1.0).group_by(lambda r: r.name).aggregate(total=lambda r: r.price.sum())
    sql = totals.order_by(totals["total"].descending()).to_sql()
    assert "INNER JOIN" in sql and "GROUP BY t0.name" in sql and sql.endswith("ORDER BY SUM(t0.price) DESC")


def test_a_join_can_be_joined_again():
    trades, accounts = _results()
    other = FinderResult(None, None, [StringAttribute("Region", "REGION", "VARCHAR", Table("regions", [("REGION", "VARCHAR")]))
                                      .alias("region")], Table("regions", [("REGION", "VARCHAR")]), NoOperation())
    twice = trades.join(accounts, lambda t, a: t.account == a.id).join(other, lambda j, r: j.name == r.region, how="left")
    assert twice.columns() == ["account", "sym", "price", "id", "name", "region"]
    assert twice.to_sql().count("JOIN") == 2


def test_the_columns_of_a_join_are_the_columns_of_both_or_the_left_for_semi_and_anti():
    trades, accounts = _results()
    assert trades.join(accounts, how="cross").columns() == ["account", "sym", "price", "id", "name"]
    assert trades.join(accounts, lambda t, a: t.account == a.id, how="anti").columns() == ["account", "sym", "price"]


def test_the_model_of_a_join_is_a_joined_table_of_two_relations():
    trades, accounts = _results()
    joined = trades.join(accounts, lambda t, a: t.account == a.id, how="left")._table
    assert isinstance(joined, JoinedTable) and joined.kind is JoinKind.LEFT
    assert isinstance(joined.condition, ComparisonOperation)
    assert len(joined.columns) == len(joined.left.columns) + len(joined.right.columns) == 5


def test_the_two_sides_columns_must_be_named_differently_for_a_join_that_shows_both():
    trades, _ = _results()
    with pytest.raises(ValueError, match="account"):
        trades.join(trades, lambda a, b: a.price == b.price)
    trades.join(trades, lambda a, b: a.price == b.price, how="semi")  # only the left's columns are shown: no clash


def test_a_join_that_is_not_a_cross_join_needs_a_condition_and_a_cross_join_has_none():
    trades, accounts = _results()
    with pytest.raises(ValueError, match="needs a condition"):
        trades.join(accounts)
    with pytest.raises(ValueError, match="needs a condition"):
        trades.join(accounts, how="left")
    with pytest.raises(ValueError, match="no condition"):
        trades.join(accounts, lambda t, a: t.account == a.id, how="cross")


def test_how_is_one_of_the_kinds():
    trades, accounts = _results()
    with pytest.raises(ValueError, match="one of"):
        trades.join(accounts, lambda t, a: t.account == a.id, how="outer")  # type: ignore[arg-type]


def test_a_condition_is_a_function_that_gives_a_comparison():
    trades, accounts = _results()
    with pytest.raises(TypeError, match="function of a row"):
        trades.join(accounts, trades["account"] == accounts["id"])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="comparison"):
        trades.join(accounts, lambda t, a: t.account)


def test_a_column_of_a_row_that_is_not_there_says_which_are():
    trades, accounts = _results()
    with pytest.raises(AttributeError, match="id"):
        trades.join(accounts, lambda t, a: t.account == a.account)
