"""A result as a relation: ``filter``, ``extend``, ``select``, ``join`` and ``concatenate`` each take the result before them and
return one, as Legend's relation functions do, and the SQL of a step is a query over the query before it."""
import pytest

from datafinder import DoubleAttribute, IntegerAttribute, StringAttribute, count_all
from datafinder.runner import FinderResult
from model.relational import Column, NoOperation, Table
from model.view import contains_aggregate


def _trades() -> tuple[FinderResult, tuple]:
    table = Table("trades", [Column("ACCOUNT", "VARCHAR"), Column("SYM", "VARCHAR"), Column("PRICE", "DOUBLE")])
    account = StringAttribute("Account", "ACCOUNT", "VARCHAR", "trades")
    sym = StringAttribute("Sym", "SYM", "VARCHAR", "trades")
    price = DoubleAttribute("Price", "PRICE", "DOUBLE", "trades")
    return FinderResult(None, None, [account.alias("account"), sym.alias("sym"), price.alias("price")], table, NoOperation()), \
        (account, sym, price)


def test_the_columns_of_a_result_are_named_for_the_step_after_it():
    trades, _ = _trades()
    assert trades.columns() == ["account", "sym", "price"]
    assert trades["price"].column().type == "DOUBLE"


def test_a_column_that_is_not_there_says_which_are():
    trades, _ = _trades()
    with pytest.raises(KeyError, match="account"):
        trades["missing"]


def test_a_name_is_made_from_the_display_name_when_there_is_no_alias():
    table = Table("trades", [Column("PRICE", "DOUBLE")])
    price = DoubleAttribute("Trade Price", "PRICE", "DOUBLE", "trades")
    assert FinderResult(None, None, [price], table, NoOperation()).columns() == ["trade_price"]


def test_filter_reads_the_columns_of_the_query_before_it():
    trades, _ = _trades()
    sql = trades.filter(trades["price"] > 250.0).to_sql()
    assert sql.startswith('SELECT t0.account AS "account",t0.sym AS "sym",t0.price AS "price" FROM (SELECT ')
    assert "AS t0 WHERE t0.price > 250.0" in sql


def test_several_conditions_are_all_required():
    trades, _ = _trades()
    sql = trades.filter(trades["price"] > 1.0, trades["price"] < 9.0).to_sql()
    assert "WHERE t0.price > 1.0 AND t0.price < 9.0" in sql


def test_a_window_over_a_result_can_be_filtered_on_which_where_alone_cannot_do():
    trades, _ = _trades()
    with_top = trades.extend(top=trades["price"].max().over(partition_by=[trades["account"]]))
    sql = with_top.filter(with_top["price"] == with_top["top"]).to_sql()
    assert "OVER (PARTITION BY t" in sql and ".account) AS \"top\"" in sql
    assert sql.endswith("WHERE t0.price = t0.top")


def test_a_window_does_not_group_the_rows_it_is_added_to():
    trades, _ = _trades()
    sql = trades.extend(top=trades["price"].max().over()).to_sql()
    assert "GROUP BY" not in sql and "OVER ()" in sql


def test_a_window_aggregate_keeps_every_row_where_an_aggregate_collapses_them():
    _, (_, _, price) = _trades()
    assert contains_aggregate(price.max()) and not contains_aggregate(price.max().over())


def test_extend_adds_a_column_worked_out_from_the_others():
    trades, _ = _trades()
    sql = trades.extend(double=trades["price"] * 2.0).select("sym", "double").to_sql()
    assert 't0.sym AS "sym",t0.double AS "double"' in sql and "t1.price * 2.0 AS \"double\"" in sql


def test_select_keeps_only_the_columns_asked_for_and_can_rename_one():
    trades, _ = _trades()
    sql = trades.select("sym", trades["price"].alias("cost")).to_sql()
    assert sql.startswith('SELECT t0.sym AS "sym",t0.price AS "cost" FROM (SELECT')


def test_ordering_a_step_by_a_column_of_it_orders_by_that_column():
    trades, _ = _trades()
    step = trades.filter(trades["price"] > 1.0)
    assert step.order_by(step["price"].descending()).to_sql().endswith("ORDER BY t0.price DESC")


def test_a_step_can_be_grouped_and_aggregated():
    trades, _ = _trades()
    sql = trades.select(trades["account"], trades["price"].sum().alias("total")).group_by(trades["account"]).to_sql()
    assert "SUM(t0.price) AS \"total\"" in sql and "GROUP BY t0.account" in sql


def test_two_columns_of_the_same_name_in_a_result_are_refused():
    table = Table("trades", [Column("PRICE", "DOUBLE")])
    price = DoubleAttribute("Price", "PRICE", "DOUBLE", "trades")
    with pytest.raises(ValueError, match="alias"):
        FinderResult(None, None, [price, price], table, NoOperation()).columns()


def _totals(name: str):
    trades, (account, _, price) = _trades()
    return FinderResult(None, None, [price.sum().alias(name)], trades._table, NoOperation())


def test_join_with_no_condition_pairs_every_row_with_every_row():
    sql = _totals("a").join(_totals("b")).to_sql()
    assert ") AS t0,(SELECT" in sql and "WHERE" not in sql.split("AS t1")[-1]


def test_join_on_a_condition_keeps_the_rows_that_match():
    trades, _ = _trades()
    left = trades.select(trades["account"].alias("left_account"), trades["price"])
    right = trades.select(trades["account"].alias("right_account"))
    sql = left.join(right, on=left["left_account"] == right["right_account"]).to_sql()
    assert "WHERE t0.left_account = t1.right_account" in sql


def test_left_join_keeps_every_row_of_the_left_and_puts_the_condition_in_on():
    trades, _ = _trades()
    left = trades.select(trades["account"].alias("left_account"))
    right = trades.select(trades["account"].alias("right_account"))
    sql = left.join(right, on=left["left_account"] == right["right_account"], left=True).to_sql()
    assert "LEFT OUTER JOIN (SELECT" in sql and ") AS t1 ON (t0.left_account = t1.right_account)" in sql
    assert " WHERE " not in sql.split("LEFT OUTER JOIN")[-1].split(") AS t1")[-1]


def test_a_join_needs_its_sides_columns_named_differently():
    trades, _ = _trades()
    with pytest.raises(ValueError, match="account"):
        trades.join(trades)


def test_concatenate_is_a_union_of_the_two_queries():
    trades, _ = _trades()
    first = trades.select(trades["sym"].alias("symbol"))
    second = trades.select(trades["account"].alias("symbol"))
    sql = first.concatenate(second).to_sql()
    assert " UNION SELECT" in sql and "UNION ALL" not in sql
    assert " UNION ALL " in first.concatenate(second, all=True).to_sql()


def test_concatenated_results_need_the_same_columns():
    trades, _ = _trades()
    with pytest.raises(ValueError, match="same columns"):
        trades.select("sym").concatenate(trades.select("account"))


def test_the_steps_of_a_chain_number_their_tables_from_the_outside_in():
    trades, _ = _trades()
    step = trades.filter(trades["price"] > 1.0)
    step = step.filter(step["price"] < 9.0)
    sql = step.to_sql()
    assert sql.count("AS t") >= 3 and "AS t0 WHERE" in sql


def test_an_expression_of_a_function_can_be_tested_against_a_list_of_values():
    table = Table("customer", [Column("PHONE", "VARCHAR")])
    phone = StringAttribute("Phone", "PHONE", "VARCHAR", "customer")
    result = FinderResult(None, None, [phone], table, phone.substring(0, 2).is_in(["13", "31"]))
    assert "SUBSTRING(t0.PHONE, 1, 2) IN ('13', '31')" in result.to_sql()


def test_contains_matches_text_literally():
    _, (account, _, _) = _trades()
    table = Table("trades", [Column("ACCOUNT", "VARCHAR")])
    assert "LIKE '%a!%b%' ESCAPE '!'" in FinderResult(None, None, [account], table, account.contains("a%b")).to_sql()