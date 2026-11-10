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
    ],
)
