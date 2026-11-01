"""
Test specifications for the finance domain (finance_mapping.md).

Backends must seed their database with ACCOUNTS, PRICES, TRADES, and POSITIONS
before running these specs.  Column ordering matches the tables in finance_mapping.md:

    ref_data.account_master  — ID INT, ACCT_NAME VARCHAR, BRANCH_ID INT, in_z TIMESTAMP, out_z TIMESTAMP
    ref_data.branch          — ID INT, CITY VARCHAR
    ref_data.price           — SYM VARCHAR, PRICE DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP
    trading.trades           — sym VARCHAR, price DOUBLE, is_settled BOOLEAN,
                               account_id INT, in_z TIMESTAMP, out_z TIMESTAMP
    trading.contractualposition — DATE DATE, QUANTITY DOUBLE, NPV DOUBLE,
                                  in_z TIMESTAMP, out_z TIMESTAMP

Trade data:
    AAPL: price=84.11, is_settled=True,  in_z=2020-01-01 00:00:00, out_z=9999-12-31 23:59:59
    GOOG: price=200.0, is_settled=False, in_z=2020-01-01 00:00:00, out_z=2022-01-01 00:00:00

    MSFT: price=300.0, is_settled=True,  in_z=2024-01-01 00:00:00, out_z=9999-12-31 23:59:59, account 2

Account 1 has two versions, which point at different branches:
    'Acme Ltd'  branch 10 (London)    in_z=2020-01-01 00:00:00, out_z=2022-01-01 00:00:00
    'Acme Corp' branch 11 (New York)  in_z=2022-01-01 00:00:00, out_z=9999-12-31 23:59:59
Account 2 only becomes live after MSFT does, so for a while MSFT has an account row but no live account:
    'Beta Inc'  branch 11 (New York)  in_z=2024-06-01 00:00:00, out_z=9999-12-31 23:59:59
"""
import datetime

import numpy as np

from datafinder.expressions import count_all
from datafinder_examples_tests.spec import FinderSpec, TestExpectation

FINANCE_MAPPING = "finance_mapping.md"

_AT = "2023-12-01 12:00:00"
# Processing time at which both AAPL and GOOG are active (before GOOG expires 2022-01-01)
_BOTH_AT = "2021-06-01 12:00:00"
# valid_from for both trades = 2020-01-01 00:00:00
_VALID_FROM = datetime.datetime(2020, 1, 1, 0, 0, 0)
# MSFT is live from 2024-01-01 but its account (2) only from 2024-06-01
_MSFT_WITHOUT_LIVE_ACCOUNT_AT = "2024-03-01 12:00:00"
_MSFT_WITH_LIVE_ACCOUNT_AT = "2024-09-01 12:00:00"

ACCOUNT_FINDER_SPECS = FinderSpec(
    finder_name="AccountFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="account_by_id",
            query=lambda f: f.find_all(None, _AT, [f.id_(), f.name()], f.id_().eq(1)),
            expected_columns=["Id", "Name"],
            expected_result=np.array([[1, "Acme Corp"]], dtype=object),
        ),
    ],
)

TRADE_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="aapl_with_account",
            query=lambda f: f.find_all(
                None, _AT,
                [f.account().name(), f.symbol(), f.price()],
                f.symbol().eq("AAPL"),
            ),
            expected_columns=["Account Name", "Symbol", "Price"],
            expected_result=np.array([["Acme Corp", "AAPL", 84.11]], dtype=object),
        ),
        TestExpectation(
            name="goog_expired",
            query=lambda f: f.find_all(None, _AT, [f.symbol()], f.symbol().eq("GOOG")),
            expected_columns=["Symbol"],
            expected_result=np.empty((0, 1), dtype=object),
        ),
        TestExpectation(
            name="price_sum_single_active_trade",
            query=lambda f: f.find_all(None, _AT, [f.price().sum()]),
            expected_columns=["Sum Price"],
            expected_result=np.array([[84.11]], dtype=object),
        ),
        TestExpectation(
            name="both_trades_active_in_2021",
            query=lambda f: f.find_all(
                None, "2021-06-01 12:00:00", [f.symbol()]
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"], ["GOOG"]], dtype=object),
        ),
        TestExpectation(
            name="filter_by_is_settled_true",
            query=lambda f: f.find_all(None, _AT, [f.symbol()], f.is_settled().eq(True)),
            expected_columns=["Symbol"],
            # At _AT only AAPL is active; GOOG expired 2022-01-01 and is_settled=False anyway
            expected_result=np.array([["AAPL"]], dtype=object),
        ),
        # --- BooleanAttribute.is_false() ---
        TestExpectation(
            name="is_settled_is_false",
            query=lambda f: f.find_all(None, _BOTH_AT, [f.symbol()], f.is_settled().is_false()),
            expected_columns=["Symbol"],
            expected_result=np.array([["GOOG"]], dtype=object),
        ),
        # --- DateTimeAttribute comparisons (using valid_to; both trades active at _BOTH_AT) ---
        TestExpectation(
            name="valid_to_eq",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol()],
                f.valid_to().eq(datetime.datetime(2022, 1, 1, 0, 0, 0)),
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["GOOG"]], dtype=object),
        ),
        TestExpectation(
            name="valid_to_gt",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol()],
                f.valid_to() > datetime.datetime(2022, 6, 1, 0, 0, 0),
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"]], dtype=object),
        ),
        TestExpectation(
            name="valid_to_lt",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol()],
                f.valid_to() < datetime.datetime(2022, 6, 1, 0, 0, 0),
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["GOOG"]], dtype=object),
        ),
        TestExpectation(
            name="valid_to_ge",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol()],
                f.valid_to() >= datetime.datetime(2022, 1, 1, 0, 0, 0),
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"], ["GOOG"]], dtype=object),
        ),
        TestExpectation(
            name="valid_to_le",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol()],
                f.valid_to() <= datetime.datetime(2022, 1, 1, 0, 0, 0),
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["GOOG"]], dtype=object),
        ),
        # --- DateTimeAttribute extract (valid_from=2020-01-01 00:00:00, only AAPL at _AT) ---
        TestExpectation(
            name="valid_from_year",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().year()]),
            expected_columns=["Year Valid From"],
            expected_result=np.array([[2020]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_month",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().month()]),
            expected_columns=["Month Valid From"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_day",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().day()]),
            expected_columns=["Day Valid From"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_hour",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().hour()]),
            expected_columns=["Hour Valid From"],
            expected_result=np.array([[0]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_minute",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().minute()]),
            expected_columns=["Minute Valid From"],
            expected_result=np.array([[0]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_second",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().second()]),
            expected_columns=["Second Valid From"],
            expected_result=np.array([[0]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_quarter",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().quarter()]),
            expected_columns=["Quarter Valid From"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_week",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().week()]),
            expected_columns=["Week Valid From"],
            # 2020-01-01 is ISO week 1
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_day_of_week",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().day_of_week()]),
            expected_columns=["Day Of Week Valid From"],
            # 2020-01-01 is Wednesday; DuckDB DOW: 0=Sunday → Wednesday=3
            expected_result=np.array([[3]], dtype=object),
        ),
        # --- DateTimeAttribute arithmetic ---
        TestExpectation(
            name="valid_from_add_hours",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().add_hours(2)]),
            expected_columns=["Add Hours Valid From"],
            expected_result=np.array([[np.datetime64("2020-01-01T02:00:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_add_minutes",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().add_minutes(30)]),
            expected_columns=["Add Minutes Valid From"],
            expected_result=np.array([[np.datetime64("2020-01-01T00:30:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_add_seconds",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().add_seconds(60)]),
            expected_columns=["Add Seconds Valid From"],
            expected_result=np.array([[np.datetime64("2020-01-01T00:01:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_subtract_hours",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().subtract_hours(1)]),
            expected_columns=["Subtract Hours Valid From"],
            expected_result=np.array([[np.datetime64("2019-12-31T23:00:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_subtract_minutes",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().subtract_minutes(5)]),
            expected_columns=["Subtract Minutes Valid From"],
            expected_result=np.array([[np.datetime64("2019-12-31T23:55:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_subtract_seconds",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().subtract_seconds(30)]),
            expected_columns=["Subtract Seconds Valid From"],
            expected_result=np.array([[np.datetime64("2019-12-31T23:59:30", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_add_months",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().add_months(2)]),
            expected_columns=["Add Months Valid From"],
            expected_result=np.array([[np.datetime64("2020-03-01T00:00:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_add_years",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().add_years(1)]),
            expected_columns=["Add Years Valid From"],
            expected_result=np.array([[np.datetime64("2021-01-01T00:00:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_subtract_months",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().subtract_months(1)]),
            expected_columns=["Subtract Months Valid From"],
            expected_result=np.array([[np.datetime64("2019-12-01T00:00:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_subtract_years",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().subtract_years(1)]),
            expected_columns=["Subtract Years Valid From"],
            expected_result=np.array([[np.datetime64("2019-01-01T00:00:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_add_days",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().add_days(1)]),
            expected_columns=["Add Days Valid From"],
            expected_result=np.array([[np.datetime64("2020-01-02T00:00:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_subtract_days",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().subtract_days(1)]),
            expected_columns=["Subtract Days Valid From"],
            expected_result=np.array([[np.datetime64("2019-12-31T00:00:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_add_timedelta",
            query=lambda f: f.find_all(None, _AT, [f.valid_from() + datetime.timedelta(hours=2)]),
            expected_columns=["Add Hours Valid From"],
            expected_result=np.array([[np.datetime64("2020-01-01T02:00:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_sub_timedelta",
            query=lambda f: f.find_all(None, _AT, [f.valid_from() - datetime.timedelta(hours=1)]),
            expected_columns=["Subtract Hours Valid From"],
            expected_result=np.array([[np.datetime64("2019-12-31T23:00:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_add_timedelta_days",
            query=lambda f: f.find_all(None, _AT, [f.valid_from() + datetime.timedelta(days=1)]),
            expected_columns=["Add Days Valid From"],
            expected_result=np.array([[np.datetime64("2020-01-02T00:00:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_add_timedelta_minutes",
            query=lambda f: f.find_all(None, _AT, [f.valid_from() + datetime.timedelta(minutes=30)]),
            expected_columns=["Add Minutes Valid From"],
            expected_result=np.array([[np.datetime64("2020-01-01T00:30:00", "us")]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_add_timedelta_seconds",
            query=lambda f: f.find_all(None, _AT, [f.valid_from() + datetime.timedelta(seconds=30)]),
            expected_columns=["Add Seconds Valid From"],
            expected_result=np.array([[np.datetime64("2020-01-01T00:00:30", "us")]], dtype=object),
        ),
        # --- DateTimeAttribute.__eq__ dunder (vs .eq() method) ---
        TestExpectation(
            name="valid_from_eq_operator",
            query=lambda f: f.find_all(None, _AT, [f.symbol()], f.valid_from() == _VALID_FROM),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"]], dtype=object),
        ),
        # --- BooleanAttribute.is_true() and __eq__ dunder ---
        TestExpectation(
            name="is_settled_is_true",
            query=lambda f: f.find_all(None, _AT, [f.symbol()], f.is_settled().is_true()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"]], dtype=object),
        ),
        TestExpectation(
            name="is_settled_eq_operator",
            query=lambda f: f.find_all(None, _AT, [f.symbol()], f.is_settled() == True),  # noqa: E712
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"]], dtype=object),
        ),
        # --- DateTimeAttribute diff ---
        TestExpectation(
            name="valid_from_diff_days",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().diff_days(datetime.datetime(2020, 2, 1))]),
            expected_columns=["Diff Days Valid From"],
            expected_result=np.array([[31]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_diff_months",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().diff_months(datetime.datetime(2020, 4, 1))]),
            expected_columns=["Diff Months Valid From"],
            expected_result=np.array([[3]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_diff_years",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().diff_years(datetime.datetime(2022, 1, 1))]),
            expected_columns=["Diff Years Valid From"],
            expected_result=np.array([[2]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_diff_hours",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().diff_hours(datetime.datetime(2020, 1, 2, 0, 0, 0))]),
            expected_columns=["Diff Hours Valid From"],
            expected_result=np.array([[24]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_diff_minutes",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().diff_minutes(datetime.datetime(2020, 1, 1, 2, 0, 0))]),
            expected_columns=["Diff Minutes Valid From"],
            expected_result=np.array([[120]], dtype=object),
        ),
        TestExpectation(
            name="valid_from_diff_seconds",
            query=lambda f: f.find_all(None, _AT, [f.valid_from().diff_seconds(datetime.datetime(2020, 1, 1, 0, 1, 0))]),
            expected_columns=["Diff Seconds Valid From"],
            expected_result=np.array([[60]], dtype=object),
        ),
    ],
)

# Trade -> Account -> Branch, where Account is milestoned and the Branch column is the only thing projected from
# it: the Account join is an ancestor that no projected column hangs off directly, and it must still be
# constrained to the version applicable at the query time. The Account node is shared between queries, so the
# order matters: account_name_in_2021 leaves a 2021 filter on it that the later 2023 queries must not inherit.
TRADE_MULTI_HOP_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="account_name_in_2021",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol(), f.account().name()]
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol", "Account Name"],
            expected_result=np.array([["AAPL", "Acme Ltd"], ["GOOG", "Acme Ltd"]], dtype=object),
        ),
        TestExpectation(
            name="ancestor_join_is_milestoned",
            query=lambda f: f.find_all(
                None, _AT, [f.symbol(), f.account().branch().city()], f.symbol().eq("AAPL")
            ),
            expected_columns=["Symbol", "Account Branch City"],
            # Without a milestoning condition on the Account join both account versions match: London and New York
            expected_result=np.array([["AAPL", "New York"]], dtype=object),
        ),
        TestExpectation(
            name="ancestor_join_follows_the_query_time",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol(), f.account().branch().city()]
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol", "Account Branch City"],
            expected_result=np.array([["AAPL", "London"], ["GOOG", "London"]], dtype=object),
        ),
        TestExpectation(
            name="ancestor_join_keeps_its_filter_kwargs",
            skip_backends={"graphql": "A to-one relationship cannot be filtered on a server: only a list takes a where argument"},
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol(), f.account(name="Acme Ltd").branch().city()], f.symbol().eq("AAPL")
            ),
            expected_columns=["Symbol", "Account Branch City"],
            expected_result=np.array([["AAPL", "London"]], dtype=object),
        ),
        TestExpectation(
            name="ancestor_join_filter_kwargs_apply_at_the_query_time",
            skip_backends={"graphql": "A to-one relationship cannot be filtered on a server: only a list takes a where argument"},
            query=lambda f: f.find_all(
                None, _AT, [f.symbol(), f.account(name="Acme Ltd").branch().city()], f.symbol().eq("AAPL")
            ),
            expected_columns=["Symbol", "Account Branch City"],
            # no version of the account named 'Acme Ltd' is live in 2023, so the branch isn't found
            expected_result=np.array([["AAPL", None]], dtype=object),
        ),
    ],
)

# exists() / not_exists() join the related table, so the join has to be constrained to the version of the related
# row applicable at the query time, like any other join. Account 1 has two versions, so an unconstrained join
# returns each of its trades twice; account 2 only becomes live after MSFT does, so an unconstrained join
# wrongly finds an account for MSFT while it has none.
TRADE_EXISTS_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    skip_backends={"graphql": "exists/not_exists are not yet generated for GraphQL finders"},
    expectations=[
        TestExpectation(
            name="exists_returns_each_trade_once",
            query=lambda f: f.find_all(None, _AT, [f.symbol()], f.account().exists()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"]], dtype=object),
        ),
        TestExpectation(
            name="exists_returns_each_trade_once_in_2021",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol()], f.account().exists()
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"], ["GOOG"]], dtype=object),
        ),
        TestExpectation(
            name="exists_excludes_trade_whose_account_is_not_yet_live",
            query=lambda f: f.find_all(None, _MSFT_WITHOUT_LIVE_ACCOUNT_AT, [f.symbol()], f.account().exists()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"]], dtype=object),
        ),
        TestExpectation(
            name="not_exists_finds_trade_whose_account_is_not_yet_live",
            query=lambda f: f.find_all(None, _MSFT_WITHOUT_LIVE_ACCOUNT_AT, [f.symbol()], f.account().not_exists()),
            expected_columns=["Symbol"],
            expected_result=np.array([["MSFT"]], dtype=object),
        ),
        TestExpectation(
            name="exists_once_the_account_is_live",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()], f.account().exists()
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"], ["MSFT"]], dtype=object),
        ),
        TestExpectation(
            name="not_exists_is_empty_once_the_account_is_live",
            query=lambda f: f.find_all(None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()], f.account().not_exists()),
            expected_columns=["Symbol"],
            expected_result=np.empty((0, 1), dtype=object),
        ),
        TestExpectation(
            name="exists_through_a_milestoned_ancestor",
            query=lambda f: f.find_all(None, _MSFT_WITHOUT_LIVE_ACCOUNT_AT, [f.symbol()], f.account().branch().exists()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"]], dtype=object),
        ),
        TestExpectation(
            name="exists_combined_with_a_filter",
            query=lambda f: f.find_all(
                None, _MSFT_WITHOUT_LIVE_ACCOUNT_AT, [f.symbol()],
                f.account().exists().and_(f.symbol().eq("AAPL")),
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"]], dtype=object),
        ),
    ],
)

# or_() combines filters like and_(), and a chain is read left to right: each call takes everything before it as
# its left side. With both trades live (AAPL 84.11 on account 1, MSFT 300.0 on account 2), a wrong grouping or an OR
# treated as an AND changes which symbols come back.
TRADE_OR_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="or_returns_rows_matching_either_side",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()],
                f.symbol().eq("AAPL").or_(f.symbol().eq("MSFT")),
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"], ["MSFT"]], dtype=object),
            expected_sql={"duckdb": " OR "},
        ),
        TestExpectation(
            name="or_where_only_the_right_side_matches",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()],
                f.symbol().eq("NOSUCH").or_(f.symbol().eq("MSFT")),
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["MSFT"]], dtype=object),
        ),
        TestExpectation(
            name="or_where_neither_side_matches",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()],
                f.symbol().eq("NOSUCH").or_(f.symbol().eq("ALSONOSUCH")),
            ),
            expected_columns=["Symbol"],
            expected_result=np.empty((0, 1), dtype=object),
        ),
        TestExpectation(
            name="or_on_different_properties",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()],
                f.symbol().eq("NOSUCH").or_(f.price() > 200.0),
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["MSFT"]], dtype=object),
        ),
        TestExpectation(
            name="or_then_and_applies_the_and_to_the_whole_or",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()],
                f.symbol().eq("AAPL").or_(f.symbol().eq("MSFT")).and_(f.price() > 200.0),
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["MSFT"]], dtype=object),
            expected_sql={"duckdb": "(t0.sym = 'AAPL' OR t0.sym = 'MSFT') AND t0.price > 200.0"},
        ),
        TestExpectation(
            name="and_then_or_applies_the_or_to_the_whole_and",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()],
                f.symbol().eq("AAPL").and_(f.price() > 1000.0).or_(f.symbol().eq("MSFT")),
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["MSFT"]], dtype=object),
            expected_sql={"duckdb": "(t0.sym = 'AAPL' AND t0.price > 1000.0) OR t0.sym = 'MSFT'"},
        ),
        TestExpectation(
            name="or_chain_of_three",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()],
                f.symbol().eq("AAPL").or_(f.symbol().eq("GOOG")).or_(f.symbol().eq("MSFT")),
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"], ["MSFT"]], dtype=object),
            expected_sql={"duckdb": "t0.sym = 'AAPL' OR t0.sym = 'GOOG' OR t0.sym = 'MSFT'"},
        ),
        TestExpectation(
            name="or_across_a_property_and_a_related_property",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()],
                f.account().name().eq("Beta Inc").or_(f.symbol().eq("AAPL")),
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"], ["MSFT"]], dtype=object),
        ),
        TestExpectation(
            name="or_where_only_the_related_property_matches",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()],
                f.account().name().eq("Beta Inc").or_(f.symbol().eq("NOSUCH")),
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["MSFT"]], dtype=object),
        ),
        TestExpectation(
            name="exists_or_a_filter",
            skip_backends={"graphql": "exists/not_exists are not yet generated for GraphQL finders"},
            query=lambda f: f.find_all(
                None, _MSFT_WITHOUT_LIVE_ACCOUNT_AT, [f.symbol()],
                f.account().exists().or_(f.symbol().eq("MSFT")),
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"], ["MSFT"]], dtype=object),
        ),
        TestExpectation(
            name="not_exists_or_a_filter",
            skip_backends={"graphql": "exists/not_exists are not yet generated for GraphQL finders"},
            query=lambda f: f.find_all(
                None, _MSFT_WITHOUT_LIVE_ACCOUNT_AT, [f.symbol()],
                f.account().not_exists().or_(f.symbol().eq("NOSUCH")),
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["MSFT"]], dtype=object),
        ),
    ],
)

# is_none() / is_not_none() read a column through the join, so the join has to be constrained to the version of
# the related row applicable at the query time. With the same data as the exists() specs: an unconstrained join
# returns each of account 1's trades twice and finds a name for MSFT while its account is not yet live.
TRADE_IS_NONE_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="is_not_none_returns_each_trade_once",
            query=lambda f: f.find_all(None, _AT, [f.symbol()], f.account().name().is_not_none()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"]], dtype=object),
        ),
        TestExpectation(
            name="is_none_finds_trade_whose_account_is_not_yet_live",
            skip_backends={"graphql": "A condition on a nested field never matches a missing nested object on a server, so is_none cannot mean 'no live account'"},
            query=lambda f: f.find_all(None, _MSFT_WITHOUT_LIVE_ACCOUNT_AT, [f.symbol()], f.account().name().is_none()),
            expected_columns=["Symbol"],
            expected_result=np.array([["MSFT"]], dtype=object),
        ),
        TestExpectation(
            name="is_not_none_excludes_trade_whose_account_is_not_yet_live",
            query=lambda f: f.find_all(
                None, _MSFT_WITHOUT_LIVE_ACCOUNT_AT, [f.symbol()], f.account().name().is_not_none()
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"]], dtype=object),
        ),
        TestExpectation(
            name="is_none_is_empty_once_the_account_is_live",
            query=lambda f: f.find_all(None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()], f.account().name().is_none()),
            expected_columns=["Symbol"],
            expected_result=np.empty((0, 1), dtype=object),
        ),
        TestExpectation(
            name="is_not_none_once_the_account_is_live",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()], f.account().name().is_not_none()
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"], ["MSFT"]], dtype=object),
        ),
        TestExpectation(
            name="is_none_through_a_milestoned_ancestor",
            skip_backends={"graphql": "A condition on a nested field never matches a missing nested object on a server, so is_none cannot mean 'no live account'"},
            query=lambda f: f.find_all(
                None, _MSFT_WITHOUT_LIVE_ACCOUNT_AT, [f.symbol()], f.account().branch().city().is_none()
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["MSFT"]], dtype=object),
        ),
        TestExpectation(
            name="is_not_none_through_a_milestoned_ancestor",
            query=lambda f: f.find_all(
                None, _MSFT_WITHOUT_LIVE_ACCOUNT_AT, [f.symbol()], f.account().branch().city().is_not_none()
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"]], dtype=object),
        ),
        TestExpectation(
            name="is_none_combined_with_a_filter",
            skip_backends={"graphql": "A condition on a nested field never matches a missing nested object on a server, so is_none cannot mean 'no live account'"},
            query=lambda f: f.find_all(
                None, _MSFT_WITHOUT_LIVE_ACCOUNT_AT, [f.symbol()],
                f.account().name().is_none().and_(f.symbol().eq("MSFT")),
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["MSFT"]], dtype=object),
        ),
    ],
)

# A view has no dates: it exposes the root's validity columns (in_z, out_z) and pins every joined table to the
# version that applied when the root row was recorded. So AAPL and GOOG (recorded 2020-01-01) see 'Acme Ltd' in
# London, not today's 'Acme Corp', and MSFT (recorded 2024-01-01, before its account went live in June) has no
# account at all, even though that account exists now. Rows are read back ordered by symbol, as a view has no order.
_FOREVER = datetime.datetime(9999, 12, 31, 23, 59, 59)
_START_OF_2020 = datetime.datetime(2020, 1, 1)
_START_OF_2022 = datetime.datetime(2022, 1, 1)
_START_OF_2024 = datetime.datetime(2024, 1, 1)

# Trade.instrument comes from the one-way TradeInstrument association: the trade's instrument is a navigation from the
# trade only, and the instrument has no accessor back to its trades. The instrument table is milestoned, so the join is
# anchored to the processing time of the query like the other association joins are.
TRADE_ONE_WAY_ASSOCIATION_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    skip_backends={"graphql": "The finance GraphQL mapping does not map the TradeInstrument association"},
    expectations=[
        TestExpectation(
            name="one_way_association_reads_through_to_the_instrument",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol(), f.instrument().price()]
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol", "Instrument Price"],
            expected_result=np.array([["AAPL", 150.0], ["GOOG", 2800.0]], dtype=object),
            expected_sql={"duckdb": "JOIN"},
        ),
        TestExpectation(
            name="one_way_association_join_is_anchored_to_the_query_time",
            # GOOG's trade and its instrument both expired on 2022-01-01, so only AAPL is live
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.instrument().price()]),
            expected_columns=["Symbol", "Instrument Price"],
            expected_result=np.array([["AAPL", 150.0]], dtype=object),
        ),
        TestExpectation(
            name="one_way_association_can_be_filtered_on",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol()], f.instrument().price() > 1000.0
            ),
            expected_columns=["Symbol"],
            expected_result=np.array([["GOOG"]], dtype=object),
        ),
        TestExpectation(
            name="one_way_association_is_read_with_the_association",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol(), f.instrument().price(), f.account().name()]
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol", "Instrument Price", "Account Name"],
            expected_result=np.array([["AAPL", 150.0, "Acme Ltd"], ["GOOG", 2800.0, "Acme Ltd"]], dtype=object),
        ),
    ],
)

# A navigation can be given its own processing time, which it and the navigations below it use instead of the query's.
# At _BOTH_AT, account 1 is 'Acme Ltd' (London); at _AT it is 'Acme Corp' (New York).
TRADE_NAVIGATION_DATE_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    skip_backends={"graphql": "The GraphQL finders do not take a date on a navigation"},
    expectations=[
        TestExpectation(
            name="navigation_given_its_own_processing_time_uses_it",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol(), f.account(processing_time=_AT).name()]
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol", "Account Name"],
            expected_result=np.array([["AAPL", "Acme Corp"], ["GOOG", "Acme Corp"]], dtype=object),
            expected_sql={"duckdb": "2023-12-01"},
        ),
        TestExpectation(
            name="navigation_without_a_time_of_its_own_uses_the_querys",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol(), f.account().name()]
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol", "Account Name"],
            expected_result=np.array([["AAPL", "Acme Ltd"], ["GOOG", "Acme Ltd"]], dtype=object),
            unexpected_sql={"duckdb": "2023-12-01"},
        ),
        TestExpectation(
            name="sibling_navigation_keeps_the_querys_time",
            # the instrument is read at _BOTH_AT while the account is read at _AT
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol(), f.account(processing_time=_AT).name(), f.instrument().price()]
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol", "Account Name", "Instrument Price"],
            expected_result=np.array([["AAPL", "Acme Corp", 150.0], ["GOOG", "Acme Corp", 2800.0]], dtype=object),
        ),
        TestExpectation(
            name="a_call_with_a_time_does_not_change_the_plain_accessor",
            query=lambda f: _call_with_a_time_then_plain(f),
            expected_columns=["Symbol", "Account Name"],
            expected_result=np.array([["AAPL", "Acme Ltd"], ["GOOG", "Acme Ltd"]], dtype=object),
        ),
        TestExpectation(
            name="navigation_time_given_as_a_datetime",
            query=lambda f: f.find_all(
                None, _BOTH_AT, [f.symbol(), f.account(processing_time=datetime.datetime(2023, 12, 1, 12, 0, 0)).name()]
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol", "Account Name"],
            expected_result=np.array([["AAPL", "Acme Corp"], ["GOOG", "Acme Corp"]], dtype=object),
        ),
    ],
)


def _call_with_a_time_then_plain(f):
    f.account(processing_time=_AT)  # the finder's own accessor is shared, so this must not leave a date on it
    return f.find_all(None, _BOTH_AT, [f.symbol(), f.account().name()]).order_by(f.symbol().ascending())


# Branch has no milestoning of its own, but the accounts it reaches do. A date given for the query reaches them, so the
# accounts come back as of that date, as when the query starts from a milestoned class.
BRANCH_FINDER_SPECS = FinderSpec(
    finder_name="BranchFinder",
    mapping_file=FINANCE_MAPPING,
    skip_backends={"graphql": "The finance GraphQL mapping does not map the accounts of a branch"},
    expectations=[
        TestExpectation(
            name="non_milestoned_root_reads_a_milestoned_navigation_as_of_the_query_time",
            # At _AT, 'Acme Corp' is New York's only account: London's 'Acme Ltd' expired on 2022-01-01
            query=lambda f: f.find_all(None, _AT, [f.city(), f.accounts().name()]).order_by(f.city().ascending()),
            expected_columns=["City", "Accounts Name"],
            expected_result=np.array([["London", None], ["New York", "Acme Corp"]], dtype=object),
            expected_sql={"duckdb": "in_z <="},
        ),
        TestExpectation(
            name="non_milestoned_root_reads_the_earlier_version_at_an_earlier_time",
            query=lambda f: f.find_all(None, _BOTH_AT, [f.city(), f.accounts().name()]).order_by(f.city().ascending()),
            expected_columns=["City", "Accounts Name"],
            expected_result=np.array([["London", "Acme Ltd"], ["New York", None]], dtype=object),
        ),
    ],
)

# exists() and not_exists() on a to-many association ask whether a row exists, so a branch with several accounts is one
# row, and counts once. Branch has no milestoning of its own, and the accounts it reaches are milestoned: at
# _MSFT_WITH_LIVE_ACCOUNT_AT New York has two accounts, 'Acme Corp' and 'Beta Inc', and London has none.
_GRAPHQL_NO_BRANCH_ACCOUNTS = "The finance GraphQL mapping does not map the accounts of a branch"

BRANCH_EXISTS_FINDER_SPECS = FinderSpec(
    finder_name="BranchFinder",
    mapping_file=FINANCE_MAPPING,
    skip_backends={"graphql": _GRAPHQL_NO_BRANCH_ACCOUNTS},
    expectations=[
        TestExpectation(
            name="exists_to_many_is_one_row_however_many_rows_match",
            query=lambda f: f.find_all(None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.city()], f.accounts().exists()),
            expected_columns=["City"],
            expected_result=np.array([["New York"]], dtype=object),
            expected_sql={"duckdb": 'SELECT DISTINCT t2.BRANCH_ID AS "BRANCH_ID" FROM ref_data.account_master AS t2'},
        ),
        TestExpectation(
            name="exists_to_many_counts_a_row_once",
            query=lambda f: f.find_all(None, _MSFT_WITH_LIVE_ACCOUNT_AT, [count_all()], f.accounts().exists()),
            expected_columns=["Count"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="not_exists_to_many_is_a_row_with_no_match",
            query=lambda f: f.find_all(None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.city()], f.accounts().not_exists()),
            expected_columns=["City"],
            expected_result=np.array([["London"]], dtype=object),
        ),
        TestExpectation(
            name="exists_and_not_exists_on_one_step_cover_every_row_once",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.city()], f.accounts().exists().or_(f.accounts().not_exists())
            ).order_by(f.city().ascending()),
            expected_columns=["City"],
            expected_result=np.array([["London"], ["New York"]], dtype=object),
        ),
        TestExpectation(
            # London's account at _BOTH_AT, 'Acme Ltd', has expired by now: the milestoning is inside the sub-select
            name="the_step_is_milestoned_inside_the_sub_select",
            query=lambda f: f.find_all(None, _BOTH_AT, [f.city()], f.accounts().exists()),
            expected_columns=["City"],
            expected_result=np.array([["London"]], dtype=object),
            expected_sql={"duckdb": "FROM ref_data.account_master AS t2 WHERE t2.in_z <="},
            unexpected_sql={"duckdb": "t1.in_z"},
        ),
        TestExpectation(
            name="a_step_with_its_own_processing_time_uses_it_inside_the_sub_select",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.city()], f.accounts(processing_time=_BOTH_AT).exists()
            ),
            expected_columns=["City"],
            expected_result=np.array([["London"]], dtype=object),
            expected_sql={"duckdb": "2021-06-01"},
        ),
        TestExpectation(
            # A value read through the step is a row for each account, and exists() then holds for an account in the result
            name="reading_a_value_through_the_step_keeps_a_row_for_each_match",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.city(), f.accounts().name()], f.accounts().exists()
            ).order_by(f.accounts().name().ascending()),
            expected_columns=["City", "Accounts Name"],
            expected_result=np.array([["New York", "Acme Corp"], ["New York", "Beta Inc"]], dtype=object),
            unexpected_sql={"duckdb": "SELECT DISTINCT"},
        ),
    ],
)

TRADE_TO_MANY_EXISTS_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    skip_backends={"graphql": _GRAPHQL_NO_BRANCH_ACCOUNTS},
    expectations=[
        TestExpectation(
            # trade -> account -> branch are to-one; the accounts of the branch are to-many. New York has two accounts,
            # so a join would repeat each of its trades.
            name="exists_to_many_through_to_one_steps_is_one_row_per_trade",
            query=lambda f: f.find_all(
                None, _MSFT_WITH_LIVE_ACCOUNT_AT, [f.symbol()], f.account().branch().accounts().exists()
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"], ["MSFT"]], dtype=object),
            expected_sql={"duckdb": "SELECT DISTINCT"},
        ),
    ],
)

TRADE_VIEW_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    skip_backends={"graphql": "Views are not supported by the GraphQL backend"},
    expectations=[
        TestExpectation(
            name="view_of_root_columns_adds_the_validity_columns",
            query=lambda f: f.find_all(None, None, [f.symbol(), f.price()])
            .to_view("trade_prices", "reporting").result().order_by("symbol"),
            expected_columns=["symbol", "price", "in_z", "out_z"],
            expected_result=np.array([
                ["AAPL", 84.11, _START_OF_2020, _FOREVER],
                ["GOOG", 200.0, _START_OF_2020, _START_OF_2022],
                ["MSFT", 300.0, _START_OF_2024, _FOREVER],
            ], dtype=object),
        ),
        TestExpectation(
            name="each_trade_sees_the_account_applicable_when_it_was_recorded",
            query=lambda f: f.find_all(None, None, [f.symbol(), f.account().name()])
            .to_view("trade_account", "reporting").result().order_by("symbol"),
            expected_columns=["symbol", "account_name", "in_z", "out_z"],
            expected_result=np.array([
                ["AAPL", "Acme Ltd", _START_OF_2020, _FOREVER],
                ["GOOG", "Acme Ltd", _START_OF_2020, _START_OF_2022],
                ["MSFT", None, _START_OF_2024, _FOREVER],
            ], dtype=object),
            expected_sql={"duckdb": "t1.in_z <= t0.in_z"},
            unexpected_sql={"duckdb": "GREATEST"},
        ),
        TestExpectation(
            name="an_ancestor_nothing_projects_is_pinned_too",
            query=lambda f: f.find_all(None, None, [f.symbol(), f.account().branch().city()])
            .to_view("trade_branch", "reporting").result().order_by("symbol"),
            expected_columns=["symbol", "account_branch_city", "in_z", "out_z"],
            expected_result=np.array([
                ["AAPL", "London", _START_OF_2020, _FOREVER],
                ["GOOG", "London", _START_OF_2020, _START_OF_2022],
                ["MSFT", None, _START_OF_2024, _FOREVER],
            ], dtype=object),
        ),
        TestExpectation(
            name="filter_on_a_root_column",
            query=lambda f: f.find_all(None, None, [f.account().name()], f.symbol().eq("GOOG"))
            .to_view("goog_account", "reporting").result(),
            expected_columns=["account_name", "in_z", "out_z"],
            expected_result=np.array([["Acme Ltd", _START_OF_2020, _START_OF_2022]], dtype=object),
        ),
        TestExpectation(
            name="filter_on_a_joined_column",
            query=lambda f: f.find_all(None, None, [f.symbol()], f.account().name().is_not_none())
            .to_view("trades_with_account", "reporting").result().order_by("symbol"),
            expected_columns=["symbol", "in_z", "out_z"],
            expected_result=np.array([
                ["AAPL", _START_OF_2020, _FOREVER],
                ["GOOG", _START_OF_2020, _START_OF_2022],
            ], dtype=object),
        ),
        # Aggregates and group_by come from the same builder. A grouped view also groups by the validity columns,
        # so AAPL and GOOG, which are valid over different periods, are separate groups.
        TestExpectation(
            name="grouped_view_groups_by_the_validity_columns_too",
            query=lambda f: f.find_all(None, None, [f.account().name(), f.price().sum()])
            .group_by(f.account().name()).to_view("account_totals", "reporting").result().order_by("sum_price"),
            expected_columns=["account_name", "sum_price", "in_z", "out_z"],
            expected_result=np.array([
                ["Acme Ltd", 84.11, _START_OF_2020, _FOREVER],
                ["Acme Ltd", 200.0, _START_OF_2020, _START_OF_2022],
                [None, 300.0, _START_OF_2024, _FOREVER],
            ], dtype=object),
            expected_sql={"duckdb": "GROUP BY t1.ACCT_NAME, t0.in_z, t0.out_z"},
        ),
    ],
)

CONTRACTUAL_POSITION_FINDER_SPECS = FinderSpec(
    finder_name="ContractualPositionFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="position_jan15_current",
            query=lambda f: f.find_all("2023-01-15", _AT, [f.quantity(), f.npv()]),
            expected_columns=["Quantity", "Npv"],
            expected_result=np.array([[100.0, 500.0]], dtype=object),
        ),
        TestExpectation(
            name="position_jan16_current",
            query=lambda f: f.find_all("2023-01-16", _AT, [f.quantity(), f.npv()]),
            expected_columns=["Quantity", "Npv"],
            expected_result=np.array([[200.0, 1000.0]], dtype=object),
        ),
        TestExpectation(
            name="position_jan15_before_update",
            query=lambda f: f.find_all("2023-01-15", "2023-01-12 12:00:00", [f.quantity()]),
            expected_columns=["Quantity"],
            expected_result=np.array([[90.0]], dtype=object),
        ),
        TestExpectation(
            name="range_jan15_jan16",
            skip_backends={"graphql": "A sort across a date range cannot be pushed: each date is a separate query"},
            query=lambda f: f.find_for_date_range(
                "2023-01-15", "2023-01-16", _AT, [f.quantity()]
            ).order_by(f.quantity().ascending()),
            expected_columns=["Quantity"],
            expected_result=np.array([[100.0], [200.0]], dtype=object),
        ),
        TestExpectation(
            name="range_jan15_only",
            query=lambda f: f.find_for_date_range(
                "2023-01-15", "2023-01-15", _AT, [f.quantity()]
            ),
            expected_columns=["Quantity"],
            expected_result=np.array([[100.0]], dtype=object),
        ),
        TestExpectation(
            name="range_before_data",
            query=lambda f: f.find_for_date_range(
                "2023-01-01", "2023-01-14", _AT, [f.quantity()]
            ),
            expected_columns=["Quantity"],
            expected_result=np.empty((0, 1), dtype=object),
        ),
    ],
)
