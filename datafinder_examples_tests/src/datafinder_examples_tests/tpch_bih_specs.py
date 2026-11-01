"""
Test specifications for the TPC-BiH benchmark (Kaufmann et al., "TPC-BiH: A Benchmark for Bitemporal Databases",
TPCTC 2013), using tpch_bih_mapping.md on a small data set.

Appendix B benchmark queries are either implemented as specified or represented by an ``_unsupported`` expectation
with the reason the current finder API cannot express them. Supplemental finder examples remain useful, but are named as
such so they cannot be mistaken for benchmark coverage. The TPC-H query below follows Appendix B.2's H.1 definition and
time parameters.

Schema (the TPC-H tables; comment columns and a few descriptive ones are left out):

    region, nation   not versioned
    supplier         degenerate: system time only
    customer         bitemporal, application time visible_time
    part             bitemporal, application time availability_time
    partsupp         bitemporal, application time validity_time
    orders           bitemporal, active_time as application time, receivable_time as ordinary dates
    lineitem         bitemporal, application time active_time

The CSVs are written by datafinder_examples/tools/generate_tpch_bih_data.py, which applies the history below. System
times are S0 = 2020-01-01 (the initial load), S1 = 2021-01-01, S2 = 2022-01-01 and S3 = 2023-01-01. The dates below are
application dates as the history was designed, 2019 to 2024; the data has them moved back 26 years, into TPC-H's range,
and the year of an application date in a query is moved the same way.

    S1  customer 1 pays (balance 100 -> 6000 from 2021-01-01); part 2 price 1000 -> 1050 from 2021-03-01;
        partsupp (1,1) cost 100 -> 110 from 2021-06-01; order 4 and its lines placed
    S2  order 5 placed; order 2 delivered (status F, active time ends 2021-12-30);
        customer 2 moves to the US from 2022-01-01; supplier 1 balance 1000 -> 1200;
        part 4 availability delayed to 2022-06-01; partsupp (1,2) cost 80 -> 82 from 2022-03-01,
        (2,1) stock 30 -> 80 from 2022-01-01; supplier 3 starts supplying part 1 from 2022-01-01
    S3  order 6 placed; order 5 cancelled; order 3 price 500 -> 700 with the same application time;
        customer 3 balance 4000 -> 2000 from 2023-01-01; partsupp (1,1) cost 110 -> 125 from 2023-03-01;
        supplier 2 balance 500 -> 450; supplier 3's rows for part 4 corrected to end 2021-06-01

The queries are given a business date (application time) and a processing time (system time). None means all versions.
"""
import datetime
import decimal

import numpy as np

from datafinder.conditional import when
from datafinder.expressions import constant, count_all
from datafinder_examples_tests.spec import FinderSpec, TestExpectation

TPCH_BIH_MAPPING = "tpch_bih_mapping.md"

_SHIFT = 26


def _D(year: int, month: int, day: int) -> datetime.date:
    """An application date. The history was designed on 2019 to 2024 and the data has it moved back 26 years, into TPC-H's
    range (see generate_tpch_bih_data.py), so the years here are as designed. The end of time is not moved."""
    return datetime.date(year if year > 3000 else year - _SHIFT, month, day)


_S0 = datetime.datetime(2020, 1, 1)
_S1 = datetime.datetime(2021, 1, 1)
_S2 = datetime.datetime(2022, 1, 1)
_S3 = datetime.datetime(2023, 1, 1)
_NOW = datetime.datetime(2024, 1, 1)
_FOREVER = datetime.datetime(9999, 12, 31, 23, 59, 59)
_END_OF_TIME = _D(9999, 12, 31)

_BACKENDS = ("duckdb",)


def _skip(reason: str) -> dict[str, str]:
    return {backend: reason for backend in _BACKENDS}


def _not_supported(reason: str):
    def query(_finder):
        raise NotImplementedError(reason)
    return query


def _unsupported(name: str, reason: str, columns: list[str] | None = None) -> TestExpectation:
    """A benchmark query that is not run as defined. Keep its gap visible rather than substituting a similar query;
    the reason states whether it is an API limitation or simply not implemented here yet."""
    columns = columns or []
    return TestExpectation(
        name=name,
        query=_not_supported(reason),
        expected_columns=columns,
        expected_result=np.empty((0, len(columns)), dtype=object),
        skip_backends=_skip(reason),
    )


def _rows(*rows) -> np.ndarray:
    result = np.empty((len(rows), len(rows[0]) if rows else 0), dtype=object)
    for i, row in enumerate(rows):
        for j, value in enumerate(row):
            result[i, j] = value
    return result


def _overlap(a_begin, a_end, b_begin, b_end):
    """The periods a and b overlap: each begins before the other ends."""
    return (a_begin < b_end).and_(a_end > b_begin)


def _discounted(extended_price: float, discount: float) -> float:
    """l_extendedprice * (1 - l_discount), written as the SQL evaluates it so the doubles match."""
    return extended_price * (1 - discount)


# ---------------------------------------------------------------------------------------------------------------------
# Pure-timeslice queries (time travel) and key-in-time queries on ORDERS and CUSTOMER
# ---------------------------------------------------------------------------------------------------------------------

_ORDER_COLUMNS = ["Order Key", "Total Price"]


def _price_changed(f):
    """Orders whose price now differs from the price recorded at S0, with both: the order at two system times."""
    at_s0 = f.other_version(processing_time=_S0)
    return f.find_all(
        "1996-06-01", _NOW, [f.order_key(), f.total_price(), at_s0.total_price()],
        f.total_price() != at_s0.total_price(),
    )

ORDER_FINDER_SPECS = FinderSpec(
    finder_name="OrderFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        # T.2: point in both time dimensions; benchmark query and aggregate output.
        TestExpectation(
            name="T2_open_order_revenue_at_two_time_points",
            query=lambda f: f.find_all(
                "1996-06-01", "2022-06-01",
                [f.total_price().sum().alias("revenue"), count_all().alias("count")],
                f.order_status().eq("O"),
            ),
            expected_columns=["revenue", "count"],
            expected_result=_rows([4300.0, 3]),
        ),
        _unsupported(
            "T3sys_compare_open_order_revenue_between_system_times",
            "Requires comparing two independently aggregated time-travel subqueries, which finder specs cannot express",
            ["old_revenue", "new_revenue"],
        ),
        _unsupported(
            "T3app_compare_open_order_revenue_between_application_times",
            "Requires comparing two independently aggregated time-travel subqueries, which finder specs cannot express",
            ["old_revenue", "new_revenue"],
        ),
        _unsupported(
            "T4sys_top_ten_customers_by_open_to_filled_revenue_change",
            "Requires joining two grouped time-travel subqueries and ordering by their aggregate difference",
            ["inc_revenue", "o_custkey"],
        ),
        _unsupported(
            "T4app_top_ten_customers_by_open_order_revenue_change",
            "Requires joining two grouped time-travel subqueries and ordering by their aggregate difference",
            ["inc_revenue", "o_custkey"],
        ),
        _unsupported(
            "T5_maximum_open_order_price_per_customer_over_system_history",
            "The order finder exposes o_custkey only through a temporal CUSTOMER navigation; grouping by that joined "
            "relation duplicates historical order rows, so it cannot reproduce the benchmark's direct o_custkey group",
            ["o_custkey", "max(o_totalprice)", "supp_revenue"],
        ),
        TestExpectation(
            name="T6sys_average_open_order_price_per_customer_at_system_time",
            query=lambda f: f.find_all(
                None, "2022-06-01", [f.customer().customer_key(), f.total_price().average().alias("supp_revenue")],
                f.order_status().eq("O"),
            ).group_by(f.customer().customer_key()).order_by(f.customer().customer_key().ascending()),
            expected_columns=["Customer Customer Key", "supp_revenue"],
            expected_result=_rows([1, 800.0], [3, 500.0], [4, 3000.0]),
        ),
        TestExpectation(
            name="T6app_average_open_order_price_per_customer_at_application_time",
            query=lambda f: f.find_all(
                "1996-06-01", None,
                [f.customer().customer_key(), f.total_price().average().alias("supp_revenue")],
                f.order_status().eq("O"),
            ).group_by(f.customer().customer_key()).order_by(f.customer().customer_key().ascending()),
            expected_columns=["Customer Customer Key", "supp_revenue"],
            expected_result=_rows([1, 800.0], [2, 2000.0], [3, 600.0], [4, 3000.0]),
        ),
        TestExpectation(
            name="T8_count_orders_payable_at_application_time_as_recorded_at_system_time",
            query=lambda f: f.find_all(
                "1996-06-01", "2022-06-01", [count_all().alias("count")],
                (f.receivable_time_begin() <= datetime.date(1996, 6, 1))
                .and_(f.receivable_time_end() > datetime.date(1996, 6, 1)),
            ),
            expected_columns=["count"],
            expected_result=_rows([3]),
        ),
        TestExpectation(
            name="T9_count_orders_in_transit_at_two_independent_times",
            query=lambda f: f.find_all(
                None, "2022-06-01", [count_all().alias("count")],
                (f.receivable_time_begin() <= datetime.date(1996, 6, 1))
                .and_(f.receivable_time_end() > datetime.date(1996, 6, 1)),
            ),
            expected_columns=["count"],
            expected_result=_rows([5]),
        ),
        # Supplemental point queries; T.2 is the aggregate specification below.
        TestExpectation(
            name="order_point_point_before_cancel",
            query=lambda f: f.find_all("1996-06-01", "2022-06-01", [f.order_key(), f.total_price()])
            .order_by(f.order_key().ascending()),
            expected_columns=_ORDER_COLUMNS,
            expected_result=_rows([3, 500.0], [4, 3000.0], [5, 800.0]),
            expected_sql={"duckdb": "t0.active_time_begin <= '1996-06-01'"},
        ),
        TestExpectation(
            name="order_point_point_after_cancel_and_price_manipulation",
            query=lambda f: f.find_all("1996-06-01", _NOW, [f.order_key(), f.total_price()])
            .order_by(f.order_key().ascending()),
            expected_columns=_ORDER_COLUMNS,
            expected_result=_rows([3, 700.0], [4, 3000.0]),
        ),
        TestExpectation(
            name="order_point_point_early",
            query=lambda f: f.find_all("1993-12-12", "2020-06-01", [f.order_key(), f.total_price()])
            .order_by(f.order_key().ascending()),
            expected_columns=_ORDER_COLUMNS,
            expected_result=_rows([1, 1000.0], [2, 2000.0]),
        ),
        # Supplemental version comparisons; these are not benchmark T.3/T.4 aggregates.
        TestExpectation(
            name="order_price_changed_between_system_times",
            query=lambda f: _price_changed(f),
            expected_columns=["Order Key", "Total Price", "Other Version Total Price"],
            expected_result=_rows([3, 700.0, 500.0]),
        ),
        TestExpectation(
            name="order_largest_price_change_between_system_times_top_n",
            query=lambda f: _price_changed(f).order_by(
                (f.total_price() - f.other_version(processing_time=_S0).total_price()).descending()
            ).limit(1),
            expected_columns=["Order Key", "Total Price", "Other Version Total Price"],
            expected_result=_rows([3, 700.0, 500.0]),
        ),
        # Supplemental history listing; benchmark T.5 requires grouped MAX and COUNT.
        TestExpectation(
            name="order_all_versions",
            query=lambda f: f.find_all(None, None, [f.order_key(), f.order_status(), f.total_price()])
            .order_by(f.order_key().ascending(), f.sys_time_begin().ascending()),
            expected_columns=["Order Key", "Order Status", "Total Price"],
            expected_result=_rows(
                [1, "F", 1000.0], [2, "O", 2000.0], [2, "F", 2000.0], [3, "O", 500.0], [3, "O", 700.0],
                [4, "O", 3000.0], [5, "O", 800.0], [6, "O", 1500.0],
            ),
            unexpected_sql={"duckdb": "WHERE"},
        ),
        # Supplemental slices; benchmark T.6 requires grouped averages.
        TestExpectation(
            name="order_application_time_point_all_system_time",
            query=lambda f: f.find_all("1994-06-01", None, [f.order_key(), f.order_status(), f.total_price()])
            .order_by(f.order_key().ascending(), f.sys_time_begin().ascending()),
            expected_columns=["Order Key", "Order Status", "Total Price"],
            expected_result=_rows([2, "O", 2000.0], [2, "F", 2000.0], [3, "O", 500.0], [3, "O", 700.0]),
        ),
        TestExpectation(
            name="order_system_time_point_all_application_time",
            query=lambda f: f.find_all(None, "2022-06-01", [f.order_key(), f.order_status()])
            .order_by(f.order_key().ascending()),
            expected_columns=["Order Key", "Order Status"],
            expected_result=_rows([1, "F"], [2, "F"], [3, "O"], [4, "O"], [5, "O"]),
        ),
        # Supplemental explicit current-system-time point query; benchmark T.7 is a PARTSUPP aggregate.
        TestExpectation(
            name="order_current_system_time",
            query=lambda f: f.find_all("1996-06-01", datetime.datetime.now(), [f.order_key(), f.total_price()])
            .order_by(f.order_key().ascending()),
            expected_columns=_ORDER_COLUMNS,
            expected_result=_rows([3, 700.0], [4, 3000.0]),
        ),
        # Supplemental secondary-date filters; benchmark T.8/T.9 return counts.
        TestExpectation(
            name="order_second_application_time_point",
            query=lambda f: f.find_all(
                None, "2020-06-01", [f.order_key()],
                (f.receivable_time_begin() <= _D(2020, 1, 10)).and_(f.receivable_time_end() > _D(2020, 1, 10)),
            ).order_by(f.order_key().ascending()),
            expected_columns=["Order Key"],
            expected_result=_rows([1], [2], [3]),
        ),
        TestExpectation(
            name="order_second_application_time_slice",
            query=lambda f: f.find_all(
                None, None, [f.order_key()],
                (f.receivable_time_begin() <= _D(2021, 12, 31)).and_(f.receivable_time_end() > _D(2021, 1, 1)),
            ).order_by(f.order_key().ascending(), f.sys_time_begin().ascending()),
            expected_columns=["Order Key"],
            expected_result=_rows([1], [2], [2], [3], [3], [4]),
        ),
        # Supplemental ORDERS audit examples. Appendix B's K.1-K.6 are CUSTOMER queries.
        TestExpectation(
            name="order_audit_all_versions",
            query=lambda f: f.find_all(
                None, None,
                [f.order_key(), f.order_status(), f.total_price(), f.sys_time_begin(), f.sys_time_end()],
                f.order_key().eq(3),
            ).order_by(f.sys_time_begin().ascending()),
            expected_columns=["Order Key", "Order Status", "Total Price", "Sys Time Begin", "Sys Time End"],
            expected_result=_rows([3, "O", 500.0, _S0, _S3], [3, "O", 700.0, _S3, _FOREVER]),
        ),
        TestExpectation(
            name="order_audit_system_time_range",
            query=lambda f: f.find_all(
                None, None,
                [f.order_key(), f.order_status(), f.total_price(), f.sys_time_begin(), f.sys_time_end()],
                f.order_key().eq(3).and_(f.sys_time_begin() <= _NOW).and_(f.sys_time_end() > datetime.datetime(2023, 6, 1)),
            ).order_by(f.sys_time_begin().ascending()),
            expected_columns=["Order Key", "Order Status", "Total Price", "Sys Time Begin", "Sys Time End"],
            expected_result=_rows([3, "O", 700.0, _S3, _FOREVER]),
        ),
        TestExpectation(
            name="order_audit_range_one_column",
            query=lambda f: f.find_all(
                None, None, [f.total_price()],
                f.order_key().eq(3).and_(f.sys_time_begin() <= datetime.datetime(2023, 6, 1))
                .and_(f.sys_time_end() > datetime.datetime(2022, 6, 1)),
            ).order_by(f.sys_time_begin().ascending()),
            expected_columns=["Total Price"],
            expected_result=_rows([500.0], [700.0]),
        ),
        TestExpectation(
            name="order_audit_latest_versions_top_n",
            query=lambda f: f.find_all(None, None, [f.total_price()], f.order_key().eq(3))
            .order_by(f.sys_time_begin().descending()).limit(2),
            expected_columns=["Total Price"],
            expected_result=_rows([700.0], [500.0]),
        ),
        TestExpectation(
            # The version before the current one, found as the paper says: with no TOP N, by correlating timestamps. A
            # version ended when the next one began, so the version before the one that began at S3 is the one that ended
            # at S3. (The first version has no version before it.)
            name="order_audit_previous_version",
            query=lambda f: f.find_all(
                None, None, [f.other_version().total_price()],
                f.order_key().eq(3).and_(f.other_version().sys_time_end() == f.sys_time_begin()),
            ),
            expected_columns=["Other Version Total Price"],
            expected_result=_rows([500.0]),
        ),
        TestExpectation(
            name="order_audit_history_selected_by_a_value",
            query=lambda f: f.find_all(None, None, [f.order_key(), f.total_price()], f.total_price() > 1500.0)
            .order_by(f.order_key().ascending(), f.sys_time_begin().ascending()),
            expected_columns=_ORDER_COLUMNS,
            expected_result=_rows([2, 2000.0], [2, 2000.0], [4, 3000.0]),
        ),
        TestExpectation(
            name="order_audit_history_selected_by_a_value_high_selectivity",
            query=lambda f: f.find_all(None, None, [f.order_key(), f.total_price()], f.total_price() > 2500.0),
            expected_columns=_ORDER_COLUMNS,
            expected_result=_rows([4, 3000.0]),
        ),
        # --- R.3b, R.5, R.6 ---
        _unsupported(
            "R3b_temporal_aggregation_maximum_value_of_unshipped_orders",
            "Temporal aggregation groups by the points where the data changes (an interval join on the period "
            "boundaries). The finders aggregate over the versions a date selects, not over change points",
            ["Max Total Price"],
        ),
        _unsupported(
            # Appendix B.4.6 of the local TPCTC 2013 report gives the SQL. This implementation gap is in the finder,
            # not missing query text or benchmark output.
            "R6_temporal_aggregation_with_a_join_of_two_temporal_tables",
            "Combines a temporal aggregation (a result row for each timestamp where the data changed, from an interval "
            "join on the period boundaries) with a join of two temporal tables, which the finders cannot do",
            [],
        ),
        _unsupported(
            "R5_count_distinct_open_orders_overlapping_low_balance_customer_system_history",
            "COUNT DISTINCT is supported, but the finder joins a related temporal row at the root version's time; it "
            "cannot enumerate every customer/order version pair whose system intervals overlap",
            ["count(distinct(o_orderkey))"],
        ),
        TestExpectation(
            # Supplemental overlap query; the benchmark R.5 has no application-time overlap predicate.
            name="supplemental_temporal_join_low_balance_customers_and_orders_in_both_dimensions",
            query=lambda f: f.find_all(
                None, None, [f.customer().customer_key(), count_all().alias("count")],
                (f.customer().acct_bal() < 5000.0).and_(f.total_price() > 10.0)
                .and_(_overlap(f.active_time_begin(), f.active_time_end(),
                               f.customer().visible_time_begin(), f.customer().visible_time_end()))
                .and_(_overlap(f.sys_time_begin(), f.sys_time_end(),
                               f.customer().sys_time_begin(), f.customer().sys_time_end())),
            ).order_by(f.customer().customer_key().ascending()),
            expected_columns=["Customer Customer Key", "count"],
            expected_result=_rows([1, 2], [2, 3], [3, 3]),
        ),
        TestExpectation(
            # Supplemental point query, not a benchmark temporal join.
            name="supplemental_at_a_point_low_balance_customers_and_orders",
            query=lambda f: f.find_all(
                "1996-06-01", "2022-06-01", [f.customer().customer_key()],
                (f.customer().acct_bal() < 5000.0).and_(f.total_price() > 10.0),
            ).distinct(),
            expected_columns=["Customer Customer Key"],
            expected_result=_rows([3]),
        ),
    ],
)

_CUSTOMER_COLUMNS = ["Customer Key", "Acct Bal", "Nation Name"]


def _moved_to_us(f):
    """Customers in the US at 2022-06-01 who were not on 2021-06-01: the customer at two application dates."""
    before = f.other_version(business_date=_D(2021, 6, 1))
    return f.find_all(
        "1996-06-01", _NOW, [f.customer_key(), f.name()],
        f.nation().name().eq("UNITED STATES").and_(before.nation().name() != "UNITED STATES"),
    )


def _customers_at(f, business_date, processing_time):
    return f.find_all(business_date, processing_time, [f.customer_key(), f.acct_bal(), f.nation().name()]) \
        .order_by(f.customer_key().ascending())


CUSTOMER_FINDER_SPECS = FinderSpec(
    finder_name="CustomerFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        # K.1: all system versions at one application-time point; all application versions as recorded now.
        TestExpectation(
            name="K1sys_customer_system_history_at_application_point",
            query=lambda f: f.find_all(
                "1995-06-01", None, [f.customer_key(), f.acct_bal(), f.sys_time_begin()], f.customer_key().eq(1),
            ).order_by(f.sys_time_begin().ascending()),
            expected_columns=["Customer Key", "Acct Bal", "Sys Time Begin"],
            expected_result=_rows([1, 100.0, _S0], [1, 6000.0, _S1]),
        ),
        TestExpectation(
            name="K1app_customer_application_history_as_recorded",
            query=lambda f: f.find_all(
                None, _NOW,
                [f.customer_key(), f.acct_bal(), f.visible_time_begin(), f.visible_time_end()], f.customer_key().eq(1),
            ).order_by(f.visible_time_begin().ascending()),
            expected_columns=["Customer Key", "Acct Bal", "Visible Time Begin", "Visible Time End"],
            expected_result=_rows([1, 100.0, _D(2019, 1, 1), _D(2021, 1, 1)],
                                  [1, 6000.0, _D(2021, 1, 1), _END_OF_TIME]),
        ),
        # K.2: temporal ranges are expressible by filtering version bounds or using the business-range finder.
        TestExpectation(
            name="K2sys_customer_system_history_range_at_application_point",
            query=lambda f: f.find_all(
                "1995-06-01", None, [f.customer_key(), f.acct_bal(), f.sys_time_begin()],
                f.customer_key().eq(1).and_(f.sys_time_begin() < _S2).and_(f.sys_time_end() > _S0),
            ).order_by(f.sys_time_begin().ascending()),
            expected_columns=["Customer Key", "Acct Bal", "Sys Time Begin"],
            expected_result=_rows([1, 100.0, _S0], [1, 6000.0, _S1]),
        ),
        TestExpectation(
            name="K2app_customer_application_history_range_as_recorded",
            query=lambda f: f.find_for_date_range(
                "1993-01-01", "2022-01-01", _NOW,
                [f.customer_key(), f.acct_bal(), f.visible_time_begin()], f.customer_key().eq(1),
            ).order_by(f.visible_time_begin().ascending()),
            expected_columns=["Customer Key", "Acct Bal", "Visible Time Begin"],
            expected_result=_rows([1, 100.0, _D(2019, 1, 1)], [1, 6000.0, _D(2021, 1, 1)]),
        ),
        # K.3: MIN over either kind of temporal range.
        TestExpectation(
            name="K3sys_minimum_balance_over_system_history",
            query=lambda f: f.find_all(
                "1995-06-01", None, [f.acct_bal().min().alias("min_balance")],
                f.customer_key().eq(1).and_(f.sys_time_begin() < _S2).and_(f.sys_time_end() > _S0),
            ),
            expected_columns=["min_balance"],
            expected_result=_rows([100.0]),
        ),
        TestExpectation(
            name="K3app_minimum_balance_over_application_history",
            query=lambda f: f.find_for_date_range(
                "1993-01-01", "2022-01-01", _NOW, [f.acct_bal().min().alias("min_balance")],
                f.customer_key().eq(1),
            ),
            expected_columns=["min_balance"],
            expected_result=_rows([100.0]),
        ),
        # K.4: latest-version retrieval. The chosen key has only two versions, below HSIZE=10.
        TestExpectation(
            name="K4sys_latest_ten_customer_system_versions",
            query=lambda f: f.find_all(
                "1995-06-01", None,
                [f.customer_key(), f.acct_bal(), f.sys_time_begin(), f.sys_time_end()], f.customer_key().eq(1),
            ).order_by(f.sys_time_begin().ascending()).limit(10),
            expected_columns=["Customer Key", "Acct Bal", "Sys Time Begin", "Sys Time End"],
            expected_result=_rows([1, 100.0, _S0, _S1], [1, 6000.0, _S1, _FOREVER]),
        ),
        TestExpectation(
            name="K4app_latest_ten_customer_application_versions",
            query=lambda f: f.find_for_date_range(
                "0001-01-01", "9999-12-30", _NOW,
                [f.customer_key(), f.acct_bal(), f.visible_time_begin(), f.visible_time_end()], f.customer_key().eq(1),
            ).order_by(f.visible_time_begin().ascending()).limit(10),
            expected_columns=["Customer Key", "Acct Bal", "Visible Time Begin", "Visible Time End"],
            expected_result=_rows([1, 100.0, _D(2019, 1, 1), _D(2021, 1, 1)],
                                  [1, 6000.0, _D(2021, 1, 1), _END_OF_TIME]),
        ),
        # K.5 uses adjacent versions here. The generated history is contiguous; arbitrary gaps would need correlated MAX.
        TestExpectation(
            name="K5sys_preceding_customer_system_version",
            query=lambda f: f.find_all(
                "1995-06-01", None, [f.other_version().acct_bal(), f.other_version().sys_time_begin()],
                f.customer_key().eq(1).and_(f.sys_time_begin() <= _S2)
                .and_(f.other_version().sys_time_end() == f.sys_time_begin()),
            ),
            expected_columns=["Other Version Acct Bal", "Other Version Sys Time Begin"],
            expected_result=_rows([100.0, _S0]),
        ),
        TestExpectation(
            name="K5app_preceding_customer_application_version",
            query=lambda f: f.find_all(
                None, _NOW, [f.other_version().acct_bal(), f.other_version().visible_time_begin()],
                f.customer_key().eq(1).and_(f.visible_time_begin() <= _D(2022, 1, 1))
                .and_(f.other_version().visible_time_end() == f.visible_time_begin()),
            ),
            expected_columns=["Other Version Acct Bal", "Other Version Visible Time Begin"],
            expected_result=_rows([100.0, _D(2019, 1, 1)]),
        ),
        # K.6: a value predicate over the selected history is an ordinary finder filter.
        TestExpectation(
            name="K6sys_customer_history_filtered_by_balance_at_application_point",
            query=lambda f: f.find_all(
                "1995-06-01", None, [f.customer_key(), f.acct_bal(), f.sys_time_begin()], f.acct_bal() > 5000.0,
            ).order_by(f.customer_key().ascending(), f.sys_time_begin().ascending()),
            expected_columns=["Customer Key", "Acct Bal", "Sys Time Begin"],
            expected_result=_rows([1, 6000.0, _S1], [4, 9000.0, _S0]),
        ),
        TestExpectation(
            name="K6app_customer_application_history_filtered_by_balance",
            query=lambda f: f.find_for_date_range(
                "0001-01-01", "9999-12-30", _NOW,
                [f.customer_key(), f.acct_bal(), f.visible_time_begin()], f.acct_bal() > 5000.0,
            ).order_by(f.customer_key().ascending(), f.acct_bal().ascending(), f.visible_time_begin().ascending()),
            expected_columns=["Customer Key", "Acct Bal", "Visible Time Begin"],
            expected_result=_rows([1, 6000.0, _D(2021, 1, 1)], [4, 9000.0, _D(2019, 1, 1)]),
        ),
        _unsupported("R1sys_customers_moving_to_us_in_system_time_interval",
                     "Requires grouped temporal subqueries and a range join over system-time transitions",
                     ["Name"]),
        _unsupported("R1app_customers_moving_to_us_in_application_time_interval",
                     "Requires grouped temporal subqueries and a range join over application-time transitions",
                     ["Name"]),
        _unsupported("R2sys_mean_order_to_ship_time_over_system_history",
                     "Requires grouped earliest system-time transitions and a join",
                     ["AVG"]),
        _unsupported("R2app_mean_order_to_ship_time_as_currently_recorded",
                     "Requires grouped earliest application-time transitions and a join",
                     ["AVG"]),
        # Supplemental CUSTOMER point queries. Benchmark T.1 is a PARTSUPP aggregate.
        TestExpectation(
            name="customer_point_point_current",
            query=lambda f: _customers_at(f, "1996-06-01", "2022-06-01"),
            expected_columns=_CUSTOMER_COLUMNS,
            expected_result=_rows(
                [1, 6000.0, "UNITED STATES"], [2, 3000.0, "UNITED STATES"], [3, 4000.0, "FRANCE"],
                [4, 9000.0, "UNITED STATES"],
            ),
        ),
        TestExpectation(
            name="customer_point_point_past",
            query=lambda f: _customers_at(f, "1994-06-01", "2020-06-01"),
            expected_columns=_CUSTOMER_COLUMNS,
            expected_result=_rows(
                [1, 100.0, "UNITED STATES"], [2, 3000.0, "GERMANY"], [3, 4000.0, "FRANCE"],
                [4, 9000.0, "UNITED STATES"],
            ),
        ),
        TestExpectation(
            # Tomorrow's state in application time, as recorded yesterday: the 2021 payment was not known in 2020.
            name="customer_future_application_time_as_recorded_in_the_past",
            query=lambda f: _customers_at(f, "1995-06-01", "2020-06-01"),
            expected_columns=_CUSTOMER_COLUMNS,
            expected_result=_rows(
                [1, 100.0, "UNITED STATES"], [2, 3000.0, "GERMANY"], [3, 4000.0, "FRANCE"],
                [4, 9000.0, "UNITED STATES"],
            ),
        ),
        TestExpectation(
            name="customer_same_application_time_as_recorded_later",
            query=lambda f: _customers_at(f, "1995-06-01", "2021-06-01"),
            expected_columns=_CUSTOMER_COLUMNS,
            expected_result=_rows(
                [1, 6000.0, "UNITED STATES"], [2, 3000.0, "GERMANY"], [3, 4000.0, "FRANCE"],
                [4, 9000.0, "UNITED STATES"],
            ),
        ),
        # Supplemental CUSTOMER audit query; it is not one of the benchmark's K query forms.
        TestExpectation(
            name="customer_audit_history_in_application_time",
            query=lambda f: f.find_all(
                None, _NOW, [f.customer_key(), f.acct_bal(), f.visible_time_begin(), f.visible_time_end()],
                f.customer_key().eq(1),
            ).order_by(f.visible_time_begin().ascending()),
            expected_columns=["Customer Key", "Acct Bal", "Visible Time Begin", "Visible Time End"],
            expected_result=_rows(
                [1, 100.0, _D(2019, 1, 1), _D(2021, 1, 1)], [1, 6000.0, _D(2021, 1, 1), _END_OF_TIME],
            ),
        ),
        # Supplemental state-change query; Appendix B's R.1 is interval-bounded and separate for each time dimension.
        TestExpectation(
            name="customer_moved_to_us_at_two_application_points",
            query=lambda f: _moved_to_us(f),
            expected_columns=["Customer Key", "Name"],
            expected_result=_rows([2, "Customer#2"]),
        ),
    ],
)

# ---------------------------------------------------------------------------------------------------------------------
# Bitemporal self-join (Appendix B.5): the other parts supplied by suppliers who supply part 1
# ---------------------------------------------------------------------------------------------------------------------

_B5_COLUMNS = ["Part Part Key"]


def _other_parts(f, business_date, processing_time, correlate=()):
    """PARTSUPP rows of other parts, for suppliers that also have a PARTSUPP row for part 1: a self-join of PARTSUPP
    through SUPPLIER. A navigation takes the dates of the root, so a dimension that is a point (or agnostic) for both
    rows is a date of the query, and one that is correlated is left open (None) and the periods of the two rows
    are compared: ``correlate`` names "application" and "system"."""
    other = f.supplier().part_supps()
    condition = other.part().part_key().eq(1).and_(f.part().part_key().ne(1))
    if "application" in correlate:
        condition = condition.and_(_overlap(f.validity_time_begin(), f.validity_time_end(),
                                            other.validity_time_begin(), other.validity_time_end()))
    if "system" in correlate:
        condition = condition.and_(_overlap(f.sys_time_begin(), f.sys_time_end(),
                                            other.sys_time_begin(), other.sys_time_end()))
    return f.find_all(business_date, processing_time, [f.part().part_key()], condition) \
        .distinct().order_by(f.part().part_key().ascending())


def _other_parts_at_distinct_business_times(f, anchor_business_date, other_business_date, processing_time):
    """B.5.2: use one business-time point for the anchor part and another for candidate parts."""
    anchor = f.supplier().part_supps(business_date=anchor_business_date, processing_time=processing_time)
    condition = anchor.part().part_key().eq(1).and_(f.part().part_key().ne(1))
    return f.find_all(other_business_date, processing_time, [f.part().part_key()], condition) \
        .distinct().order_by(f.part().part_key().ascending())


PARTSUPP_FINDER_SPECS = FinderSpec(
    finder_name="PartSuppFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        # T.1: one application-time and system-time point on the stable PARTSUPP relation.
        TestExpectation(
            name="T1_average_supply_cost_and_count_at_two_time_points",
            query=lambda f: f.find_all(
                "1994-06-01", "2022-06-01",
                [f.supply_cost().average().alias("avg_supplycost"), count_all().alias("count")],
            ),
            expected_columns=["avg_supplycost", "count"],
            expected_result=_rows([215.0, 6]),
        ),
        TestExpectation(
            name="T7_current_average_availability_by_part",
            query=lambda f: f.find_all(
                "1998-06-01", _NOW, [f.part().part_key(), f.avail_qty().average().alias("avail")],
            ).group_by(f.part().part_key()).order_by(f.part().part_key().ascending()),
            expected_columns=["Part Part Key", "avail"],
            expected_result=_rows([1, 130.0 / 3], [2, 60.0], [3, 60.0]),
        ),
        # --- B.5.1: current application and system times ---
        TestExpectation(
            # Supplier 3's rows for part 4 ended in 2021 (recorded at S3), so part 4 is not found at 2023-06-01.
            name="B5_1_application_point_system_point_current",
            query=lambda f: _other_parts(f, "1997-06-01", _NOW),
            expected_columns=_B5_COLUMNS,
            expected_result=_rows([2], [3]),
        ),
        TestExpectation(
            name="B5_2_app_and_system_points_with_distinct_business_times",
            query=lambda f: _other_parts_at_distinct_business_times(
                f, "1995-06-01", "1997-06-01", "2022-06-01",
            ),
            expected_columns=_B5_COLUMNS,
            expected_result=_rows([2], [3]),
        ),
        TestExpectation(
            name="B5_6_current_system_as_known_all_application_time",
            query=lambda f: _other_parts(f, None, _NOW),
            expected_columns=_B5_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        # --- B.5.7: application time agnostic / past system point ---
        TestExpectation(
            name="B5_7_application_agnostic_system_point_past",
            query=lambda f: _other_parts(f, None, "2020-06-01"),
            expected_columns=_B5_COLUMNS,
            expected_result=_rows([2], [3]),
        ),
        # --- B.5.9, B.5.11: system time agnostic ---
        TestExpectation(
            name="B5_9_application_point_system_agnostic_at_earlier_application_date",
            query=lambda f: _other_parts(f, "1997-06-01", None),
            expected_columns=_B5_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        TestExpectation(
            name="B5_9_application_point_system_agnostic_before_new_supplier",
            query=lambda f: _other_parts(f, "1995-06-01", None),
            expected_columns=_B5_COLUMNS,
            expected_result=_rows([2], [3]),
        ),
        TestExpectation(
            name="B5_11_application_agnostic_system_agnostic",
            query=lambda f: _other_parts(f, None, None, correlate=("system",)),
            expected_columns=_B5_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        # --- B.5.3, B.5.4, B.5.5, B.5.8, B.5.10: row periods correlate in a dimension ---
        # Only B.5.3 loses part 4: the periods of supplier 3's rows for part 4 (ended in 2021) and for part 1 (from
        # 2022) are disjoint in application time. In system time they overlapped until S3, when the first was corrected.
        TestExpectation(
            name="B5_3_application_correlation_system_point_current",
            query=lambda f: _other_parts(f, None, _NOW, correlate=("application",)),
            expected_columns=_B5_COLUMNS,
            expected_result=_rows([2], [3]),
        ),
        TestExpectation(
            name="B5_4_application_point_system_correlation",
            query=lambda f: _other_parts(f, "1997-06-01", None, correlate=("system",)),
            expected_columns=_B5_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        TestExpectation(
            name="B5_5_application_correlation_system_correlation",
            query=lambda f: _other_parts(f, None, None, correlate=("application", "system")),
            expected_columns=_B5_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        TestExpectation(
            name="B5_8_application_agnostic_system_correlation",
            query=lambda f: _other_parts(f, None, None, correlate=("system",)),
            expected_columns=_B5_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        TestExpectation(
            name="B5_10_application_correlation_system_agnostic",
            query=lambda f: _other_parts(f, None, None, correlate=("application",)),
            expected_columns=_B5_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        # --- Supplemental supply-cost history queries ---
        TestExpectation(
            name="supply_cost_change_effective_in_the_future_as_recorded",
            query=lambda f: f.find_all(
                "1995-07-01", "2021-06-01", [f.supply_cost()],
                f.part().part_key().eq(1).and_(f.supplier().supplier_key().eq(1)),
            ),
            expected_columns=["Supply Cost"],
            expected_result=_rows([110.0]),
        ),
        TestExpectation(
            name="supply_cost_before_the_change_was_recorded",
            query=lambda f: f.find_all(
                "1995-07-01", "2020-06-01", [f.supply_cost()],
                f.part().part_key().eq(1).and_(f.supplier().supplier_key().eq(1)),
            ),
            expected_columns=["Supply Cost"],
            expected_result=_rows([100.0]),
        ),
        TestExpectation(
            name="supply_cost_current",
            query=lambda f: f.find_all(
                "1997-06-01", _NOW, [f.supply_cost()],
                f.part().part_key().eq(1).and_(f.supplier().supplier_key().eq(1)),
            ),
            expected_columns=["Supply Cost"],
            expected_result=_rows([125.0]),
        ),
        TestExpectation(
            name="supply_cost_as_it_was_before_the_last_change_was_recorded",
            query=lambda f: f.find_all(
                "1997-06-01", "2022-06-01", [f.supply_cost()],
                f.part().part_key().eq(1).and_(f.supplier().supplier_key().eq(1)),
            ),
            expected_columns=["Supply Cost"],
            expected_result=_rows([110.0]),
        ),
        # --- R.4, R.7 ---
        _unsupported(
            "R4_products_with_the_smallest_difference_in_stock_over_the_history",
            "Needs every version of a key compared with the others (max minus min of the stock, then the smallest "
            "difference over all keys) as a nested aggregate over versions of the same table",
            ["Part Part Key"],
        ),
        TestExpectation(
            # The fixture has contiguous validity intervals, so the immediate predecessor is the row ending when this
            # version begins. For non-contiguous histories the benchmark's correlated MAX would be needed.
            name="R7_business_time_price_increase_from_latest_preceding_version",
            query=lambda f: f.find_all(
                None, _NOW,
                [f.part().part_key(), f.supplier().supplier_key(), f.supply_cost(),
                 f.validity_time_begin(), f.validity_time_end(),
                 f.same_part().validity_time_begin(), f.same_part().validity_time_end(), f.same_part().supply_cost()],
                f.supplier().supplier_key().eq(f.same_part().supplier().supplier_key())
                .and_(f.same_part().validity_time_end() == f.validity_time_begin())
                .and_(f.supply_cost() > f.same_part().supply_cost() * 1.075),
            ).order_by(f.validity_time_end().ascending(), f.same_part().validity_time_end().descending()),
            expected_columns=["Part Part Key", "Supplier Supplier Key", "Supply Cost", "Validity Time Begin",
                              "Validity Time End", "Same Part Validity Time Begin", "Same Part Validity Time End",
                              "Same Part Supply Cost"],
            expected_result=_rows(
                [1, 1, 110.0, datetime.date(1995, 6, 1), datetime.date(1997, 3, 1),
                 datetime.date(1993, 1, 1), datetime.date(1995, 6, 1), 100.0],
                [1, 1, 125.0, datetime.date(1997, 3, 1), datetime.date(9999, 12, 31),
                 datetime.date(1995, 6, 1), datetime.date(1997, 3, 1), 110.0],
            ),
        ),
    ],
)

# ---------------------------------------------------------------------------------------------------------------------
# TPC-H queries with time travel, as the TPC-H specification (3.0.1, clause 2.4) defines them: its columns, filters, grouping,
# ordering and row limit, and values for its substitution parameters. A query the finders cannot express as defined is
# written as _unsupported, which is skipped with the reason.
# ---------------------------------------------------------------------------------------------------------------------

# The supplemental queries are asked at points that have the orders they need:
#   _P1  1993-12-12 as at 2020-06-01: orders 1 and 2 and their lines
#   _PC  1996-01-15 as at 2022-06-01: orders 3, 4 and 5
#   _PD  1997-01-15 as at 2024-01-01: orders 3, 4 and 6
_P1 = ("1993-12-12", "2020-06-01")
_PC = ("1996-01-15", "2022-06-01")
_PD = ("1997-01-15", _NOW)

# 1 - 0.05 and the like are inexact doubles, so the expected values use the same operations the SQL does.
_O1L1 = _discounted(9000.0, 0.05)    # part 1 (green, PROMO), supplier 1 (GERMANY), customer 1 (US), R F, MAIL
_O1L2 = _discounted(5000.0, 0.10)    # part 2, supplier 1, customer 1, A F, SHIP
_O2L1 = _discounted(18000.0, 0.00)   # part 1, supplier 2 (FRANCE), customer 2 (GERMANY), N O, MAIL
_O2L2 = _discounted(8800.0, 0.07)    # part 3 (green, PROMO), supplier 2, customer 2, R O, AIR
_O3L1 = _discounted(28800.0, 0.02)   # part 4, supplier 3 (US), customer 3 (FRANCE), N O, AIR
_O4L1 = _discounted(12000.0, 0.04)   # part 2, supplier 3 (US), customer 4 (US), N O, AIR
_O4L2 = _discounted(2700.0, 0.06)    # part 1, supplier 1 (GERMANY), customer 4 (US), N O, AIR
_O6L1 = _discounted(9450.0, 0.03)    # part 2, supplier 1 (GERMANY), customer 2 (US), N O, SHIP
_O6L2 = _discounted(11000.0, 0.05)   # part 2, supplier 3 (US), customer 2 (US), N O, MAIL


def _revenue(l):
    return l.extended_price() * (constant(1) - l.discount())


LINEITEM_FINDER_SPECS = FinderSpec(
    finder_name="LineItemFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        TestExpectation(
            # Appendix B.2 H.1: current version in each temporal dimension; exact 1998-11-01 cutoff.
            name="Q1_pricing_summary_report",
            query=lambda f: f.find_all(
                datetime.date(9999, 12, 30), _NOW,
                [f.return_flag().alias("l_returnflag"), f.line_status().alias("l_linestatus"),
                 f.quantity().sum().alias("sum_qty"), f.extended_price().sum().alias("sum_base_price"),
                 _revenue(f).sum().alias("sum_disc_price"),
                 (_revenue(f) * (constant(1) + f.tax())).sum().alias("sum_charge"),
                 f.quantity().average().alias("avg_qty"), f.extended_price().average().alias("avg_price"),
                 f.discount().average().alias("avg_disc"), count_all().alias("count_order")],
                f.ship_date() <= datetime.date(1998, 11, 1),
            ).group_by(f.return_flag(), f.line_status())
            .order_by(f.return_flag().ascending(), f.line_status().ascending()),
            expected_columns=["l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price",
                              "sum_charge", "avg_qty", "avg_price", "avg_disc", "count_order"],
            expected_result=_rows(
                ["N", "O", 59.0, 63950.0, 61898.5,
                 _O3L1 * (1 + 0.01) + _O4L1 * (1 + 0.08) + _O4L2 * (1 + 0.08)
                 + _O6L1 * (1 + 0.04) + _O6L2 * (1 + 0.04), 11.8, 63950.0 / 5,
                 (0.02 + 0.04 + 0.06 + 0.03 + 0.05) / 5, 5],
            ),
        ),
        TestExpectation(
            # Q3, SEGMENT MACHINERY, DATE 1995-03-15: the first 10 rows
            name="Q3_shipping_priority",
            query=lambda f: f.find_all(
                *_PC,
                [f.order().order_key().alias("l_orderkey"), _revenue(f).sum().alias("revenue"),
                 f.order().order_date().alias("o_orderdate"), f.order().ship_priority().alias("o_shippriority")],
                f.order().customer().mkt_segment().eq("MACHINERY")
                .and_(f.order().order_date() < datetime.date(1995, 3, 15))
                .and_(f.ship_date() > datetime.date(1995, 3, 15)),
            ).group_by(f.order().order_key(), f.order().order_date(), f.order().ship_priority())
            .order_by(_revenue(f).sum().descending(), f.order().order_date().ascending()).limit(10),
            expected_columns=["l_orderkey", "revenue", "o_orderdate", "o_shippriority"],
            expected_result=_rows([4, _O4L1 + _O4L2, datetime.date(1994, 12, 15), 0]),
        ),
        TestExpectation(
            # Q5 with REGION AMERICA and order dates 1995-01-01 through 1996-12-31. Comparing nation names through
            # each navigation is equivalent to comparing the keys because the nation key/name mapping is unique.
            name="Q5_local_supplier_volume",
            query=lambda f: f.find_all(
                *_PD,
                [f.order().customer().nation().name().alias("n_name"), _revenue(f).sum().alias("revenue")],
                f.order().customer().nation().region().name().eq("AMERICA")
                .and_(f.order().order_date() >= datetime.date(1995, 1, 1))
                .and_(f.order().order_date() < datetime.date(1997, 1, 1))
                .and_(f.order().customer().nation().name().eq(f.supplier().nation().name())),
            ).group_by(f.order().customer().nation().name())
            .order_by(_revenue(f).sum().descending()),
            expected_columns=["n_name", "revenue"],
            expected_result=_rows(["UNITED STATES", _O6L2]),
        ),
        TestExpectation(
            # Q6, DATE 1993-01-01, DISCOUNT 0.06, QUANTITY 24
            name="Q6_forecasting_revenue_change",
            query=lambda f: f.find_all(
                *_P1, [(f.extended_price() * f.discount()).sum().alias("revenue")],
                (f.ship_date() >= datetime.date(1993, 1, 1)).and_(f.ship_date() < datetime.date(1994, 1, 1))
                .and_(f.discount() >= float(decimal.Decimal("0.06") - decimal.Decimal("0.01")))
                .and_(f.discount() <= float(decimal.Decimal("0.06") + decimal.Decimal("0.01")))
                .and_(f.quantity() < 24.0),
            ),
            expected_columns=["revenue"],
            expected_result=_rows([9000.0 * 0.05 + 8800.0 * 0.07]),
        ),
        TestExpectation(
            # Q7, NATION1 GERMANY, NATION2 UNITED STATES
            name="Q7_volume_shipping",
            query=lambda f: f.find_all(
                *_PD,
                [f.supplier().nation().name().alias("supp_nation"),
                 f.order().customer().nation().name().alias("cust_nation"),
                 f.ship_date().year().alias("l_year"), _revenue(f).sum().alias("revenue")],
                f.supplier().nation().name().eq("GERMANY").and_(f.order().customer().nation().name().eq("UNITED STATES"))
                .or_(f.supplier().nation().name().eq("UNITED STATES")
                     .and_(f.order().customer().nation().name().eq("GERMANY")))
                .and_(f.ship_date() >= datetime.date(1995, 1, 1)).and_(f.ship_date() <= datetime.date(1996, 12, 31)),
            ).order_by(f.supplier().nation().name().ascending(), f.order().customer().nation().name().ascending(),
                       f.ship_date().year().ascending()),
            expected_columns=["supp_nation", "cust_nation", "l_year", "revenue"],
            expected_result=_rows(["GERMANY", "UNITED STATES", 1995, _O4L2], ["GERMANY", "UNITED STATES", 1996, _O6L1]),
        ),
        TestExpectation(
            # Q8, NATION UNITED STATES, REGION AMERICA, TYPE STANDARD POLISHED BRASS
            name="Q8_national_market_share",
            query=lambda f: f.find_all(
                *_PD,
                [f.order().order_date().year().alias("o_year"),
                 (when(f.supplier().nation().name().eq("UNITED STATES")).then(_revenue(f)).otherwise(0).sum()
                  / _revenue(f).sum()).alias("mkt_share")],
                f.order().customer().nation().region().name().eq("AMERICA")
                .and_(f.order().order_date() >= datetime.date(1995, 1, 1))
                .and_(f.order().order_date() <= datetime.date(1996, 12, 31))
                .and_(f.part().type_().eq("STANDARD POLISHED BRASS")),
            ).group_by(f.order().order_date().year()).order_by(f.order().order_date().year().ascending()),
            expected_columns=["o_year", "mkt_share"],
            expected_result=_rows([1996, _O6L2 / (_O6L1 + _O6L2)]),
        ),
        _unsupported(
            "Q9_product_type_profit_measure",
            "Joins PARTSUPP on a composite key: ps_partkey = l_partkey and ps_suppkey = l_suppkey. An association joins "
            "on one column",
            ["nation", "o_year", "sum_profit"],
        ),
        TestExpectation(
            # Q12, SHIPMODE1 MAIL, SHIPMODE2 SHIP, DATE 1993-01-01
            name="Q12_shipping_modes_and_order_priority",
            query=lambda f: f.find_all(
                *_P1,
                [f.ship_mode().alias("l_shipmode"),
                 when(f.order().order_priority().eq("1-URGENT").or_(f.order().order_priority().eq("2-HIGH")))
                 .then(1).otherwise(0).sum().alias("high_line_count"),
                 when(f.order().order_priority().ne("1-URGENT").and_(f.order().order_priority().ne("2-HIGH")))
                 .then(1).otherwise(0).sum().alias("low_line_count")],
                f.ship_mode().is_in(["MAIL", "SHIP"]).and_(f.commit_date() < f.receipt_date())
                .and_(f.ship_date() < f.commit_date())
                .and_(f.receipt_date() >= datetime.date(1993, 1, 1)).and_(f.receipt_date() < datetime.date(1994, 1, 1)),
            ).order_by(f.ship_mode().ascending()),
            expected_columns=["l_shipmode", "high_line_count", "low_line_count"],
            expected_result=_rows(["MAIL", 1, 1], ["SHIP", 1, 0]),
        ),
        TestExpectation(
            # Q14, DATE 1993-11-01: 100.00 * sum(promo) / sum(all)
            name="Q14_promotion_effect",
            query=lambda f: f.find_all(
                *_P1,
                [((constant(100.0) * when(f.part().type_().starts_with("PROMO")).then(_revenue(f)).otherwise(0).sum())
                  / _revenue(f).sum()).alias("promo_revenue")],
                (f.ship_date() >= datetime.date(1993, 11, 1)).and_(f.ship_date() < datetime.date(1993, 12, 1)),
            ),
            expected_columns=["promo_revenue"],
            expected_result=_rows([(100.0 * _O1L1) / (_O1L1 + _O1L2)]),
        ),
        TestExpectation(
            # Q19, QUANTITY1 1, QUANTITY2 10, QUANTITY3 20, BRAND1 Brand#13, BRAND2 Brand#24, BRAND3 Brand#35
            name="Q19_discounted_revenue",
            query=lambda f: f.find_all(
                *_PD, [_revenue(f).sum().alias("revenue")],
                _q19_branch(f, "Brand#13", ["SM CASE", "SM BOX", "SM PACK", "SM PKG"], 1.0, 5)
                .or_(_q19_branch(f, "Brand#24", ["MED BAG", "MED BOX", "MED PKG", "MED PACK"], 10.0, 10))
                .or_(_q19_branch(f, "Brand#35", ["LG CASE", "LG BOX", "LG PACK", "LG PKG"], 20.0, 15)),
            ),
            expected_columns=["revenue"],
            expected_result=_rows([_O3L1 + _O4L1 + _O4L2]),
        ),
        # --- R2: the time a line item was pending, from shipping to receipt ---
        TestExpectation(
            name="supplemental_lineitem_shipping_to_receipt_durations",
            query=lambda f: f.find_all(
                *_P1, [f.order().order_key(), f.line_number(), f.ship_date().diff_days(f.receipt_date()).alias("duration")],
            ).order_by(f.order().order_key().ascending(), f.line_number().ascending()),
            expected_columns=["Order Order Key", "Line Number", "duration"],
            expected_result=_rows([1, 1, 15], [1, 2, 16], [2, 1, 18], [2, 2, 28]),
        ),
        _unsupported(
            "R3a_temporal_aggregation_greatest_number_of_unshipped_items",
            "Temporal aggregation groups by the points where the data changes (an interval join on the period "
            "boundaries), then takes the maximum over them",
            ["Max Count"],
        ),
        # --- The queries the finders cannot express as TPC-H defines them ---
        _unsupported(
            "Q2_minimum_cost_supplier",
            "A correlated scalar subquery: ps_supplycost = (select min(ps_supplycost) ... for the same part)",
            ["s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment"],
        ),
        _unsupported(
            "Q11_important_stock_identification",
            "A scalar subquery in HAVING: sum(ps_supplycost * ps_availqty) > (select sum(...) * FRACTION ...)",
            ["ps_partkey", "value"],
        ),
        _unsupported(
            "Q13_customer_distribution",
            "A derived table with a LEFT OUTER JOIN whose ON clause has a condition (o_comment not like "
            "'%[WORD1]%[WORD2]%'), grouped again by the count it produced",
            ["c_count", "custdist"],
        ),
        _unsupported(
            "Q15_top_supplier",
            "A view of revenue by supplier, and total_revenue = (select max(total_revenue) from the view)",
            ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"],
        ),
        _unsupported(
            "Q16_parts_supplier_relationship",
            "ps_suppkey not in (select s_suppkey ... where s_comment like '%Customer%Complaints%'), and p_type not "
            "like '[TYPE]%': a NOT IN subquery and NOT LIKE",
            ["p_brand", "p_type", "p_size", "supplier_cnt"],
        ),
        _unsupported(
            "Q17_small_quantity_order_revenue",
            "A correlated scalar subquery: l_quantity < (select 0.2 * avg(l_quantity) ... for the same part)",
            ["avg_yearly"],
        ),
        _unsupported(
            "Q20_potential_part_promotion",
            "Nested IN subqueries and a correlated scalar subquery: ps_availqty > (select 0.5 * sum(l_quantity) ...)",
            ["s_name", "s_address"],
        ),
        _unsupported(
            "Q21_suppliers_who_kept_orders_waiting",
            "EXISTS and NOT EXISTS with conditions on the other lines of the order (l2.l_suppkey <> l1.l_suppkey, "
            "l3.l_receiptdate > l3.l_commitdate): exists() takes no condition",
            ["s_name", "numwait"],
        ),
        _unsupported(
            "Q22_global_sales_opportunity",
            "substring(c_phone from 1 for 2) in a list, a scalar subquery average, and NOT EXISTS",
            ["cntrycode", "numcust", "totacctbal"],
        ),
    ],
)


def _q19_branch(f, brand: str, containers: list[str], quantity: float, max_size: int):
    return (f.part().brand().eq(brand).and_(f.part().container().is_in(containers))
            .and_(f.quantity() >= quantity).and_(f.quantity() <= quantity + 10)
            .and_(f.part().size() >= 1).and_(f.part().size() <= max_size)
            .and_(f.ship_mode().is_in(["AIR", "AIR REG"])).and_(f.ship_instruct().eq("DELIVER IN PERSON")))


ORDER_PRIORITY_FINDER_SPECS = FinderSpec(
    finder_name="OrderFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        _unsupported(
            "Q4_order_priority_checking",
            "exists (select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate): exists() takes "
            "no condition, and a filter through the line items joins, so it repeats an order for each line",
            ["o_orderpriority", "order_count"],
        ),
        _unsupported(
            "Q18_large_volume_customer",
            "o_orderkey in (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > [QUANTITY]): "
            "an IN subquery with GROUP BY and HAVING",
            ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice", "sum(l_quantity)"],
        ),
    ],
)

CUSTOMER_REVENUE_FINDER_SPECS = FinderSpec(
    finder_name="CustomerFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        TestExpectation(
            # Q10, DATE 1993-10-01: the first 20 rows
            name="Q10_returned_item_reporting",
            query=lambda f: f.find_all(
                *_P1,
                [f.customer_key().alias("c_custkey"), f.name().alias("c_name"),
                 (f.orders().line_items().extended_price() * (constant(1) - f.orders().line_items().discount()))
                 .sum().alias("revenue"),
                 f.acct_bal().alias("c_acctbal"), f.nation().name().alias("n_name"), f.address().alias("c_address"),
                 f.phone().alias("c_phone"), f.comment().alias("c_comment")],
                (f.orders().order_date() >= datetime.date(1993, 10, 1))
                .and_(f.orders().order_date() < datetime.date(1994, 1, 1))
                .and_(f.orders().line_items().return_flag().eq("R")),
            ).group_by(f.customer_key(), f.name(), f.acct_bal(), f.phone(), f.nation().name(), f.address(), f.comment())
            .order_by((f.orders().line_items().extended_price() * (constant(1) - f.orders().line_items().discount()))
                      .sum().descending()).limit(20),
            expected_columns=["c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment"],
            expected_result=_rows(
                [1, "Customer#1", _O1L1, 100.0, "UNITED STATES", "1 Customer Avenue", "10-101-201-3001",
                 "customer 1 is a regular customer"],
                [2, "Customer#2", _O2L2, 3000.0, "GERMANY", "2 Customer Avenue", "11-102-202-3002",
                 "customer 2 is a regular customer"],
            ),
        ),
    ],
)

CUSTOMER_JOIN_STRINGS_FINDER_SPECS = FinderSpec(
    finder_name="CustomerFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        TestExpectation(
            # The orders are reached only through the aggregate's ORDER BY, and are joined at the query's dates:
            # order 2 has two versions and order 5 was cancelled later, so an unmilestoned join repeats customers.
            name="join_strings_ordered_by_a_navigation_is_milestoned",
            query=lambda f: f.find_all(
                "1996-06-01", "2022-06-01",
                [f.nation().name(), f.name().join(", ", order_by=[f.orders().total_price().ascending()])],
            ).order_by(f.nation().name().ascending()),
            expected_columns=["Nation Name", "Join Name"],
            expected_result=_rows(["FRANCE", "Customer#3"], ["UNITED STATES", "Customer#1, Customer#4, Customer#2"]),
        ),
    ],
)

# ---------------------------------------------------------------------------------------------------------------------
# The other tables: overwriting application time, a degenerate table and a table without history
# ---------------------------------------------------------------------------------------------------------------------

PART_FINDER_SPECS = FinderSpec(
    finder_name="PartFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        # Part 4's availability is delayed at S2: its application period is overwritten.
        TestExpectation(
            name="availability_delayed_not_available_before_the_new_date",
            query=lambda f: f.find_all("1996-03-01", _NOW, [f.part_key()], f.part_key().eq(4)),
            expected_columns=["Part Key"],
            expected_result=np.empty((0, 1), dtype=object),
        ),
        TestExpectation(
            name="availability_delayed_available_from_the_new_date",
            query=lambda f: f.find_all("1996-07-01", _NOW, [f.part_key()], f.part_key().eq(4)),
            expected_columns=["Part Key"],
            expected_result=_rows([4]),
        ),
        TestExpectation(
            name="availability_delayed_as_recorded_before_the_delay",
            query=lambda f: f.find_all("1996-03-01", "2021-06-01", [f.part_key()], f.part_key().eq(4)),
            expected_columns=["Part Key"],
            expected_result=_rows([4]),
        ),
        # Part 2's price changes at S1, effective 2021-03-01.
        TestExpectation(
            name="price_before_the_change",
            query=lambda f: f.find_all("1995-02-01", _NOW, [f.retail_price()], f.part_key().eq(2)),
            expected_columns=["Retail Price"],
            expected_result=_rows([1000.0]),
        ),
        TestExpectation(
            name="price_after_the_change",
            query=lambda f: f.find_all("1995-04-01", _NOW, [f.retail_price()], f.part_key().eq(2)),
            expected_columns=["Retail Price"],
            expected_result=_rows([1050.0]),
        ),
        TestExpectation(
            name="price_after_the_change_as_recorded_before_it",
            query=lambda f: f.find_all("1995-04-01", "2020-06-01", [f.retail_price()], f.part_key().eq(2)),
            expected_columns=["Retail Price"],
            expected_result=_rows([1000.0]),
        ),
    ],
)

SUPPLIER_FINDER_SPECS = FinderSpec(
    finder_name="SupplierFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        # SUPPLIER is degenerate: only system time, so the application time is ignored.
        TestExpectation(
            name="degenerate_before_any_update",
            query=lambda f: f.find_all(None, "2021-06-01", [f.supplier_key(), f.acct_bal()])
            .order_by(f.supplier_key().ascending()),
            expected_columns=["Supplier Key", "Acct Bal"],
            expected_result=_rows([1, 1000.0], [2, 500.0], [3, 800.0]),
        ),
        TestExpectation(
            name="degenerate_after_the_first_update",
            query=lambda f: f.find_all(None, "2022-06-01", [f.supplier_key(), f.acct_bal()])
            .order_by(f.supplier_key().ascending()),
            expected_columns=["Supplier Key", "Acct Bal"],
            expected_result=_rows([1, 1200.0], [2, 500.0], [3, 800.0]),
        ),
        TestExpectation(
            name="degenerate_current",
            query=lambda f: f.find_all(None, _NOW, [f.supplier_key(), f.acct_bal()])
            .order_by(f.supplier_key().ascending()),
            expected_columns=["Supplier Key", "Acct Bal"],
            expected_result=_rows([1, 1200.0], [2, 450.0], [3, 800.0]),
        ),
        TestExpectation(
            name="degenerate_all_versions_of_a_key",
            query=lambda f: f.find_all(None, None, [f.acct_bal()], f.supplier_key().eq(1))
            .order_by(f.sys_time_begin().ascending()),
            expected_columns=["Acct Bal"],
            expected_result=_rows([1000.0], [1200.0]),
        ),
    ],
)

NATION_FINDER_SPECS = FinderSpec(
    finder_name="NationFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        # NATION and REGION have no temporal columns: every query sees the same rows.
        TestExpectation(
            name="not_versioned",
            query=lambda f: f.find_all(None, None, [f.name(), f.region().name()]).order_by(f.name().ascending()),
            expected_columns=["Name", "Region Name"],
            expected_result=_rows(["FRANCE", "EUROPE"], ["GERMANY", "EUROPE"], ["UNITED STATES", "AMERICA"]),
        ),
        TestExpectation(
            name="not_versioned_dates_have_no_effect",
            query=lambda f: f.find_all("1993-01-01", "2019-01-01", [f.name()], f.region().name().eq("AMERICA")),
            expected_columns=["Name"],
            expected_result=_rows(["UNITED STATES"]),
            unexpected_sql={"duckdb": "sys_time"},
        ),
    ],
)

ALL_TPCH_BIH_SPECS = [
    ORDER_FINDER_SPECS,
    CUSTOMER_FINDER_SPECS,
    PARTSUPP_FINDER_SPECS,
    LINEITEM_FINDER_SPECS,
    ORDER_PRIORITY_FINDER_SPECS,
    CUSTOMER_REVENUE_FINDER_SPECS,
    CUSTOMER_JOIN_STRINGS_FINDER_SPECS,
    PART_FINDER_SPECS,
    SUPPLIER_FINDER_SPECS,
    NATION_FINDER_SPECS,
]
