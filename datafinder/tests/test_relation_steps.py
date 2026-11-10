"""A result as a relation: ``filter``, ``extend`` and ``select`` each take the result before them and return one, as Legend's
relation functions do, and the SQL of a step is a query over the query before it."""
import pytest

from datafinder import DoubleAttribute, StringAttribute
from datafinder.runner import FinderResult
from model.relational import NoOperation, Table


def _trades() -> FinderResult:
    table = Table("trades", [("ACCOUNT", "VARCHAR"), ("SYM", "VARCHAR"), ("PRICE", "DOUBLE")])
    account = StringAttribute("Account", "ACCOUNT", "VARCHAR", table)
    sym = StringAttribute("Sym", "SYM", "VARCHAR", table)
    price = DoubleAttribute("Price", "PRICE", "DOUBLE", table)
    return FinderResult(None, None, [account.alias("account"), sym.alias("sym"), price.alias("price")], table, NoOperation())


def test_filter_reads_the_columns_of_the_query_before_it():
    trades = _trades()
    sql = trades.filter(lambda r: r.price > 250.0).to_sql()
    assert sql.startswith('SELECT t0.account AS "account",t0.sym AS "sym",t0.price AS "price" FROM (SELECT ')
    assert "AS t0 WHERE t0.price > 250.0" in sql


def test_several_conditions_are_all_required():
    trades = _trades()
    sql = trades.filter(lambda r: r.price > 1.0, lambda r: r.price < 9.0).to_sql()
    assert "WHERE t0.price > 1.0 AND t0.price < 9.0" in sql


def test_a_window_over_a_result_can_be_filtered_on_which_where_alone_cannot_do():
    trades = _trades()
    with_top = trades.extend(top=lambda r: r.price.max().over(partition_by=[r.account]))
    sql = with_top.filter(lambda r: r.price == r.top).to_sql()
    assert "OVER (PARTITION BY t" in sql and 'AS "top"' in sql
    assert sql.endswith("WHERE t0.price = t0.top")


def test_a_window_does_not_group_the_rows_it_is_added_to():
    trades = _trades()
    sql = trades.extend(top=lambda r: r.price.max().over()).to_sql()
    assert "GROUP BY" not in sql and "OVER ()" in sql


def test_extend_adds_a_column_worked_out_from_the_others():
    trades = _trades()
    sql = trades.extend(double=lambda r: r.price * 2.0).select("sym", "double").to_sql()
    assert 't0.sym AS "sym",t0.double AS "double"' in sql and '* 2.0 AS "double"' in sql


def test_select_keeps_only_the_columns_asked_for_and_extend_names_a_new_one():
    trades = _trades()
    sql = trades.extend(cost=lambda r: r.price).select("sym", "cost").to_sql()
    assert sql.startswith('SELECT t0.sym AS "sym",t0.cost AS "cost" FROM (SELECT')


def test_a_step_can_be_ordered_by_a_column_of_it():
    trades = _trades()
    step = trades.filter(lambda r: r.price > 1.0)
    assert step.order_by(step["price"].descending()).to_sql().endswith("ORDER BY t0.price DESC")


def test_the_steps_of_a_chain_number_their_tables_from_the_outside_in():
    trades = _trades()
    step = trades.filter(lambda r: r.price > 1.0)
    step = step.filter(lambda r: r.price < 9.0)
    sql = step.to_sql()
    assert sql.count("AS t") >= 3 and "AS t0 WHERE" in sql


def test_a_column_that_is_not_there_says_which_are():
    with pytest.raises(KeyError, match="account"):
        _trades()["missing"]


def test_a_column_of_a_row_that_is_not_there_says_which_are():
    with pytest.raises(AttributeError, match="account"):
        _trades().filter(lambda r: r.prise > 1.0)


def test_a_condition_is_a_function_of_a_row():
    trades = _trades()
    with pytest.raises(TypeError, match="function of a row"):
        trades.filter(trades["price"] > 1.0)
    with pytest.raises(TypeError, match="function of a row"):
        trades.extend(double=trades["price"] * 2.0)


def test_select_of_a_name_that_is_not_there_says_which_are():
    with pytest.raises(KeyError, match="account"):
        _trades().select("nope")
