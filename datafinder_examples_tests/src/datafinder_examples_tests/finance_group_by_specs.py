"""
Test specifications for group_by aggregate queries using finance_mapping.md.

Uses a different dataset from finance_specs.py — multiple accounts and trades
to exercise group_by with average().

    ref_data.account_master — ID INT, ACCT_NAME VARCHAR
    ref_data.price          — SYM VARCHAR, PRICE DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP
    trading.trades          — sym VARCHAR, price DOUBLE, is_settled BOOLEAN,
                              account_id INT, in_z TIMESTAMP, out_z TIMESTAMP
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

FINANCE_MAPPING = "finance_mapping.md"

_AT = "2030-01-01 00:00:00"

_NO_CHAIN = {"graphql": "The GraphQL finder cannot read a query as a relation yet"}


def _trades(f):
    """Each trade as a row of a relation: ``account``, ``sym`` and ``price``."""
    return f.find_all(None, _AT, [f.account().name().alias("account"), f.symbol().alias("sym"), f.price().alias("price")])


def _expensive_trades(f):
    trades = _trades(f)
    expensive = trades.filter(lambda r: r.price > 250.0)
    return expensive.order_by(expensive["price"].ascending())


def _top_trade_per_account(f):
    """A window function adds the highest price of the account to each trade, and a filter keeps the trade at that price."""
    trades = _trades(f)
    with_top = trades.extend(top=lambda r: r.price.max().over(partition_by=[r.account]))
    top = with_top.filter(lambda r: r.price == r.top).select("account", "sym", "price")
    return top.order_by(top["account"].ascending())


def _most_expensive_trade(f):
    """A window over all the rows (an aggregate for the whole relation, as a scalar subquery would give) compared with each row."""
    trades = _trades(f)
    with_top = trades.extend(top=lambda r: r.price.max().over())
    return with_top.filter(lambda r: r.price == r.top).select("account", "sym", "price")


def _price_as_a_multiple(f):
    trades = _trades(f)
    doubled = trades.extend(double=lambda r: r.price * 2.0).select("sym", "double")
    return doubled.order_by(doubled["double"].descending()).limit(2)


def _totals_per_account(f):
    """The rows of a relation grouped by one of its columns, and an aggregate of each group: Legend's groupBy."""
    totals = _trades(f).group_by(lambda r: r.account).aggregate(total=lambda r: r.price.sum(), trades=lambda r: r.price.count())
    return totals.order_by(totals["account"].ascending())


def _accounts_with_a_large_total(f):
    totals = _trades(f).group_by(lambda r: r.account).aggregate(total=lambda r: r.price.sum())
    large = totals.filter(lambda r: r.total > 400.0)
    return large.order_by(large["total"].descending())


def _account_totals(f):
    """One row for each account: ``total_account`` and ``total``."""
    return _trades(f).group_by(lambda r: r.account).aggregate(total=lambda r: r.price.sum()).extend(
        total_account=lambda r: r.account).select("total_account", "total")


def _dearer_trades(f):
    """The trades over 350, as a relation of their own: ``dear_account``, ``dear_sym``."""
    dear = _trades(f).filter(lambda r: r.price > 350.0)
    return dear.extend(dear_account=lambda r: r.account, dear_sym=lambda r: r.sym).select("dear_account", "dear_sym")


def _each_trade_with_its_account_total(f):
    trades = _trades(f)
    joined = trades.join(_account_totals(f), lambda t, a: t.account == a.total_account)
    result = joined.select("sym", "price", "total")
    return result.order_by(result["price"].ascending())


def _accounts_with_the_trades_over_350(f, how):
    accounts = _account_totals(f).select("total_account")
    joined = accounts.join(_dearer_trades(f), lambda a, t: a.total_account == t.dear_account, how=how)
    result = joined.select("total_account", "dear_sym") if how in ("left", "inner") else joined
    return result.order_by(result["total_account"].ascending())


def _trades_at_the_highest_price_by_a_cross_join(f):
    """Every trade paired with the one-row relation of the highest price: a scalar subquery as a join."""
    highest = _trades(f).aggregate(highest=lambda r: r.price.max())
    joined = _trades(f).join(highest, how="cross")
    return joined.filter(lambda r: r.price == r.highest).select("account", "sym", "price")


def _right_join_of_the_trades_over_350_to_the_accounts(f):
    """The accounts are the right side: every account is kept, with NULL where it has no trade over 350."""
    joined = _dearer_trades(f).join(_account_totals(f).select("total_account"),
                                    lambda t, a: t.dear_account == a.total_account, how="right")
    result = joined.select("total_account", "dear_sym")
    return result.order_by(result["total_account"].ascending())


def _full_join_of_two_accounts_to_the_trades_over_350(f):
    """Alpha and Beta, with the trades over 350 (Beta's and Gamma's): Alpha has none, and Gamma's is of an account not listed."""
    accounts = _account_totals(f).filter(lambda r: r.total_account != "Gamma Fund").select("total_account")
    joined = accounts.join(_dearer_trades(f), lambda a, t: a.total_account == t.dear_account, how="full")
    result = joined.select("total_account", "dear_sym")
    return result.order_by(result["total_account"].ascending(), result["dear_sym"].ascending())


TRADE_GROUP_BY_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="average_price_per_account",
            query=lambda f: f.find_all(
                None, _AT,
                [f.account().name(), f.price().average()],
            ).group_by(f.account().name()).order_by(f.account().name().ascending()),
            expected_columns=["Account Name", "Average Price"],
            expected_result=np.array(
                [["Alpha Fund", 150.0], ["Beta Fund", 350.0], ["Gamma Fund", 500.0]],
                dtype=object,
            ),
        ),
        # With no group_by, the columns that are not aggregated are what the aggregates are grouped by.
        TestExpectation(
            name="average_price_per_account_grouped_by_the_plain_columns",
            query=lambda f: f.find_all(
                None, _AT,
                [f.account().name(), f.price().average()],
            ).order_by(f.account().name().ascending()),
            expected_columns=["Account Name", "Average Price"],
            expected_result=np.array(
                [["Alpha Fund", 150.0], ["Beta Fund", 350.0], ["Gamma Fund", 500.0]],
                dtype=object,
            ),
            expected_sql={"duckdb": "GROUP BY t1.ACCT_NAME"},
        ),
        TestExpectation(
            name="average_price_settled_only",
            query=lambda f: f.find_all(
                None, _AT,
                [f.account().name(), f.price().average()],
                f.is_settled().is_true(),
            ).group_by(f.account().name()).order_by(f.account().name().ascending()),
            expected_columns=["Account Name", "Average Price"],
            expected_result=np.array(
                [["Alpha Fund", 150.0], ["Beta Fund", 300.0], ["Gamma Fund", 500.0]],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="group_count_per_account",
            skip_backends={"graphql": "The GraphQL finder has no count() yet"},
            query=lambda f: f.find_all(
                None, _AT, [f.account().name(), f.count()],
            ).group_by(f.account().name()).order_by(f.account().name().ascending()),
            expected_columns=["Account Name", "Count"],
            expected_result=np.array(
                [["Alpha Fund", 2], ["Beta Fund", 2], ["Gamma Fund", 1]],
                dtype=object,
            ),
        ),
        # --- each step is a relation: filter, extend and select take the result before them and return one ---
        TestExpectation(
            name="chain_filter_on_a_column",
            skip_backends=_NO_CHAIN,
            query=_expensive_trades,
            expected_columns=["account", "sym", "price"],
            expected_result=np.array(
                [["Beta Fund", "MSFT", 300.0], ["Beta Fund", "TSLA", 400.0], ["Gamma Fund", "GS", 500.0]], dtype=object),
            expected_sql={"duckdb": "FROM (SELECT"},
        ),
        TestExpectation(
            name="chain_window_then_filter_on_it",
            skip_backends=_NO_CHAIN,
            query=_top_trade_per_account,
            expected_columns=["account", "sym", "price"],
            expected_result=np.array(
                [["Alpha Fund", "GOOG", 200.0], ["Beta Fund", "TSLA", 400.0], ["Gamma Fund", "GS", 500.0]], dtype=object),
        ),
        TestExpectation(
            name="chain_window_over_all_rows",
            skip_backends=_NO_CHAIN,
            query=_most_expensive_trade,
            expected_columns=["account", "sym", "price"],
            expected_result=np.array([["Gamma Fund", "GS", 500.0]], dtype=object),
        ),
        TestExpectation(
            name="chain_extend_an_expression_and_select",
            skip_backends=_NO_CHAIN,
            query=_price_as_a_multiple,
            expected_columns=["sym", "double"],
            expected_result=np.array([["GS", 1000.0], ["TSLA", 800.0]], dtype=object),
        ),
        TestExpectation(
            name="chain_group_by_a_column_and_aggregate",
            skip_backends=_NO_CHAIN,
            query=_totals_per_account,
            expected_columns=["account", "total", "trades"],
            expected_result=np.array(
                [["Alpha Fund", 300.0, 2], ["Beta Fund", 700.0, 2], ["Gamma Fund", 500.0, 1]], dtype=object),
        ),
        TestExpectation(
            name="chain_filter_the_groups",
            skip_backends=_NO_CHAIN,
            query=_accounts_with_a_large_total,
            expected_columns=["account", "total"],
            expected_result=np.array([["Beta Fund", 700.0], ["Gamma Fund", 500.0]], dtype=object),
        ),
        TestExpectation(
            name="chain_aggregate_the_whole_relation",
            skip_backends=_NO_CHAIN,
            query=lambda f: _trades(f).aggregate(total=lambda r: r.price.sum(), highest=lambda r: r.price.max(),
                                                 average_of_five=lambda r: r.price.sum() / 5.0),
            expected_columns=["total", "highest", "average_of_five"],
            expected_result=np.array([[1500.0, 500.0, 300.0]], dtype=object),
        ),
        TestExpectation(
            name="join_inner_on_a_column_of_each",
            skip_backends=_NO_CHAIN,
            query=_each_trade_with_its_account_total,
            expected_columns=["sym", "price", "total"],
            expected_result=np.array(
                [["AAPL", 100.0, 300.0], ["GOOG", 200.0, 300.0], ["MSFT", 300.0, 700.0], ["TSLA", 400.0, 700.0],
                 ["GS", 500.0, 500.0]], dtype=object),
        ),
        TestExpectation(
            name="join_left_keeps_the_rows_that_find_no_match",
            skip_backends=_NO_CHAIN,
            query=lambda f: _accounts_with_the_trades_over_350(f, "left"),
            expected_columns=["total_account", "dear_sym"],
            expected_result=np.array([["Alpha Fund", None], ["Beta Fund", "TSLA"], ["Gamma Fund", "GS"]], dtype=object),
        ),
        TestExpectation(
            name="join_cross_pairs_every_row_with_a_one_row_aggregate",
            skip_backends=_NO_CHAIN,
            query=_trades_at_the_highest_price_by_a_cross_join,
            expected_columns=["account", "sym", "price"],
            expected_result=np.array([["Gamma Fund", "GS", 500.0]], dtype=object),
        ),
        TestExpectation(
            name="join_semi_keeps_the_rows_that_have_a_match",
            skip_backends=_NO_CHAIN,
            query=lambda f: _accounts_with_the_trades_over_350(f, "semi"),
            expected_columns=["total_account"],
            expected_result=np.array([["Beta Fund"], ["Gamma Fund"]], dtype=object),
        ),
        TestExpectation(
            name="join_anti_keeps_the_rows_that_have_none",
            skip_backends=_NO_CHAIN,
            query=lambda f: _accounts_with_the_trades_over_350(f, "anti"),
            expected_columns=["total_account"],
            expected_result=np.array([["Alpha Fund"]], dtype=object),
        ),
        TestExpectation(
            name="join_right_keeps_every_row_of_the_right",
            skip_backends=_NO_CHAIN,
            query=_right_join_of_the_trades_over_350_to_the_accounts,
            expected_columns=["total_account", "dear_sym"],
            expected_result=np.array([["Alpha Fund", None], ["Beta Fund", "TSLA"], ["Gamma Fund", "GS"]], dtype=object),
        ),
        TestExpectation(
            name="join_full_keeps_the_rows_of_both_that_find_no_match",
            skip_backends=_NO_CHAIN,
            query=_full_join_of_two_accounts_to_the_trades_over_350,
            expected_columns=["total_account", "dear_sym"],
            expected_result=np.array([["Alpha Fund", None], ["Beta Fund", "TSLA"], [None, "GS"]], dtype=object),
        ),
    ],
)
