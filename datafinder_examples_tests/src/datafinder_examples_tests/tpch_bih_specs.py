"""
Test specifications for the TPC-BiH benchmark (Kaufmann et al., "TPC-BiH: A Benchmark for Bitemporal Databases",
TPCTC 2013), using tpch_bih_mapping.md on a small data set.

Appendix B benchmark queries are either implemented as specified or represented by an ``_unsupported`` expectation
with the reason the current finder API cannot express them. The TPC-H queries below use fixture-compatible parameters;
H.1 follows Appendix B.2's definition and current-version time parameters.

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
import re

import numpy as np

from datafinder.conditional import when
from datafinder.expressions import constant, count_all
from datafinder_examples_tests.spec import TestExpectation

TPCH_BIH_MAPPING = "tpch_bih_mapping.md"

_SHIFT = 26


def _D(year: int, month: int, day: int) -> datetime.date:
    """An application date. The history was designed on 2019 to 2024 and the data has it moved back 26 years, into TPC-H's
    range (see generate_tpch_bih_data.py), so the years here are as designed. The end of time is not moved."""
    return datetime.date(year if year > 3000 else year - _SHIFT, month, day)


_S0 = datetime.datetime(2020, 1, 1)
_S1 = datetime.datetime(2021, 1, 1)
_S2 = datetime.datetime(2022, 1, 1)
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



# ---------------------------------------------------------------------------------------------------------------------
# TPC-H queries with time travel, as the TPC-H specification (3.0.1, clause 2.4) defines them: its columns, filters, grouping,
# ordering and row limit, and values for its substitution parameters. A query the finders cannot express as defined is
# written as _unsupported, which is skipped with the reason.
# ---------------------------------------------------------------------------------------------------------------------

# The TPC-H queries are asked at points that have the orders they need:
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




def _q19_branch(f, brand: str, containers: list[str], quantity: float, max_size: int):
    return (f.part().brand().eq(brand).and_(f.part().container().is_in(containers))
            .and_(f.quantity() >= quantity).and_(f.quantity() <= quantity + 10)
            .and_(f.part().size() >= 1).and_(f.part().size() <= max_size)
            .and_(f.ship_mode().is_in(["AIR", "AIR REG"])).and_(f.ship_instruct().eq("DELIVER IN PERSON")))

# Appendix B.1: time-travel queries
T_CASES = [
    # T.1: one application-time and system-time point on the stable PARTSUPP relation.
    TestExpectation(
        name="T1_average_supply_cost_and_count_at_two_time_points",
        query=lambda q: q.partsupp.find_all(
            "1994-06-01", "2022-06-01",
            [q.partsupp.supply_cost().average().alias("avg_supplycost"), count_all().alias("count")],
        ),
        expected_columns=["avg_supplycost", "count"],
        expected_result=_rows([215.0, 6]),
    ),

    # T.2: point in both time dimensions; benchmark query and aggregate output.
    TestExpectation(
        name="T2_open_order_revenue_at_two_time_points",
        query=lambda q: q.order.find_all(
            "1996-06-01", "2022-06-01",
            [q.order.total_price().sum().alias("revenue"), count_all().alias("count")],
            q.order.order_status().eq("O"),
        ),
        expected_columns=["revenue", "count"],
        expected_result=_rows([4300.0, 3]),
    ),

    _unsupported(
        "T3_sys_compare_open_order_revenue_between_system_times",
        "Requires comparing two independently aggregated time-travel subqueries, which finder specs cannot express",
        ["old_revenue", "new_revenue"],
    ),

    _unsupported(
        "T3_app_compare_open_order_revenue_between_application_times",
        "Requires comparing two independently aggregated time-travel subqueries, which finder specs cannot express",
        ["old_revenue", "new_revenue"],
    ),

    _unsupported(
        "T4_sys_top_ten_customers_by_open_to_filled_revenue_change",
        "Requires joining two grouped time-travel subqueries and ordering by their aggregate difference",
        ["inc_revenue", "o_custkey"],
    ),

    _unsupported(
        "T4_app_top_ten_customers_by_open_order_revenue_change",
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
        name="T6_sys_average_open_order_price_per_customer_at_system_time",
        query=lambda q: q.order.find_all(
            None, "2022-06-01", [q.order.customer().customer_key(), q.order.total_price().average().alias("supp_revenue")],
            q.order.order_status().eq("O"),
        ).group_by(q.order.customer().customer_key()).order_by(q.order.customer().customer_key().ascending()),
        expected_columns=["Customer Customer Key", "supp_revenue"],
        expected_result=_rows([1, 800.0], [3, 500.0], [4, 3000.0]),
    ),

    TestExpectation(
        name="T6_app_average_open_order_price_per_customer_at_application_time",
        query=lambda q: q.order.find_all(
            "1996-06-01", None,
            [q.order.customer().customer_key(), q.order.total_price().average().alias("supp_revenue")],
            q.order.order_status().eq("O"),
        ).group_by(q.order.customer().customer_key()).order_by(q.order.customer().customer_key().ascending()),
        expected_columns=["Customer Customer Key", "supp_revenue"],
        expected_result=_rows([1, 800.0], [2, 2000.0], [3, 600.0], [4, 3000.0]),
    ),

    TestExpectation(
        name="T7_current_average_availability_by_part",
        query=lambda q: q.partsupp.find_all(
            "1998-06-01", _NOW, [q.partsupp.part().part_key(), q.partsupp.avail_qty().average().alias("avail")],
        ).group_by(q.partsupp.part().part_key()).order_by(q.partsupp.part().part_key().ascending()),
        expected_columns=["Part Part Key", "avail"],
        expected_result=_rows([1, 130.0 / 3], [2, 60.0], [3, 60.0]),
    ),

    TestExpectation(
        name="T8_count_orders_payable_at_application_time_as_recorded_at_system_time",
        query=lambda q: q.order.find_all(
            "1996-06-01", "2022-06-01", [count_all().alias("count")],
            (q.order.receivable_time_begin() <= datetime.date(1996, 6, 1))
            .and_(q.order.receivable_time_end() > datetime.date(1996, 6, 1)),
        ),
        expected_columns=["count"],
        expected_result=_rows([3]),
    ),

    TestExpectation(
        name="T9_count_orders_in_transit_at_two_independent_times",
        query=lambda q: q.order.find_all(
            None, "2022-06-01", [count_all().alias("count")],
            (q.order.receivable_time_begin() <= datetime.date(1996, 6, 1))
            .and_(q.order.receivable_time_end() > datetime.date(1996, 6, 1)),
        ),
        expected_columns=["count"],
        expected_result=_rows([5]),
    ),

]

# Appendix B.2: TPC-H query
H_CASES = [
    TestExpectation(
        # Appendix B.2 H.1: current version in each temporal dimension; exact 1998-11-01 cutoff.
        name="H1_pricing_summary_report",
        query=lambda q: q.lineitem.find_all(
            datetime.date(9999, 12, 30), _NOW,
            [q.lineitem.return_flag().alias("l_returnflag"), q.lineitem.line_status().alias("l_linestatus"),
             q.lineitem.quantity().sum().alias("sum_qty"), q.lineitem.extended_price().sum().alias("sum_base_price"),
             _revenue(q.lineitem).sum().alias("sum_disc_price"),
             (_revenue(q.lineitem) * (constant(1) + q.lineitem.tax())).sum().alias("sum_charge"),
             q.lineitem.quantity().average().alias("avg_qty"), q.lineitem.extended_price().average().alias("avg_price"),
             q.lineitem.discount().average().alias("avg_disc"), count_all().alias("count_order")],
            q.lineitem.ship_date() <= datetime.date(1998, 11, 1),
        ).group_by(q.lineitem.return_flag(), q.lineitem.line_status())
        .order_by(q.lineitem.return_flag().ascending(), q.lineitem.line_status().ascending()),
        expected_columns=["l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price",
                          "sum_charge", "avg_qty", "avg_price", "avg_disc", "count_order"],
        expected_result=_rows(
            ["N", "O", 59.0, 63950.0, 61898.5,
             _O3L1 * (1 + 0.01) + _O4L1 * (1 + 0.08) + _O4L2 * (1 + 0.08)
             + _O6L1 * (1 + 0.04) + _O6L2 * (1 + 0.04), 11.8, 63950.0 / 5,
             (0.02 + 0.04 + 0.06 + 0.03 + 0.05) / 5, 5],
        ),
    ),

]

# Appendix B.3: key-history queries
K_CASES = [
    # K.1: all system versions at one application-time point; all application versions as recorded now.
    TestExpectation(
        name="K1_sys_customer_system_history_at_application_point",
        query=lambda q: q.customer.find_all(
            "1995-06-01", None, [q.customer.customer_key(), q.customer.acct_bal(), q.customer.sys_time_begin()], q.customer.customer_key().eq(1),
        ).order_by(q.customer.sys_time_begin().ascending()),
        expected_columns=["Customer Key", "Acct Bal", "Sys Time Begin"],
        expected_result=_rows([1, 100.0, _S0], [1, 6000.0, _S1]),
    ),

    TestExpectation(
        name="K1_app_customer_application_history_as_recorded",
        query=lambda q: q.customer.find_all(
            None, _NOW,
            [q.customer.customer_key(), q.customer.acct_bal(), q.customer.visible_time_begin(), q.customer.visible_time_end()], q.customer.customer_key().eq(1),
        ).order_by(q.customer.visible_time_begin().ascending()),
        expected_columns=["Customer Key", "Acct Bal", "Visible Time Begin", "Visible Time End"],
        expected_result=_rows([1, 100.0, _D(2019, 1, 1), _D(2021, 1, 1)],
                              [1, 6000.0, _D(2021, 1, 1), _END_OF_TIME]),
    ),

    # K.2: temporal ranges are expressible by filtering version bounds or using the business-range finder.
    TestExpectation(
        name="K2_sys_customer_system_history_range_at_application_point",
        query=lambda q: q.customer.find_all(
            "1995-06-01", None, [q.customer.customer_key(), q.customer.acct_bal(), q.customer.sys_time_begin()],
            q.customer.customer_key().eq(1).and_(q.customer.sys_time_begin() < _S2).and_(q.customer.sys_time_end() > _S0),
        ).order_by(q.customer.sys_time_begin().ascending()),
        expected_columns=["Customer Key", "Acct Bal", "Sys Time Begin"],
        expected_result=_rows([1, 100.0, _S0], [1, 6000.0, _S1]),
    ),

    TestExpectation(
        name="K2_app_customer_application_history_range_as_recorded",
        query=lambda q: q.customer.find_for_date_range(
            "1993-01-01", "2022-01-01", _NOW,
            [q.customer.customer_key(), q.customer.acct_bal(), q.customer.visible_time_begin()], q.customer.customer_key().eq(1),
        ).order_by(q.customer.visible_time_begin().ascending()),
        expected_columns=["Customer Key", "Acct Bal", "Visible Time Begin"],
        expected_result=_rows([1, 100.0, _D(2019, 1, 1)], [1, 6000.0, _D(2021, 1, 1)]),
    ),

    # K.3: MIN over either kind of temporal range.
    TestExpectation(
        name="K3_sys_minimum_balance_over_system_history",
        query=lambda q: q.customer.find_all(
            "1995-06-01", None, [q.customer.acct_bal().min().alias("min_balance")],
            q.customer.customer_key().eq(1).and_(q.customer.sys_time_begin() < _S2).and_(q.customer.sys_time_end() > _S0),
        ),
        expected_columns=["min_balance"],
        expected_result=_rows([100.0]),
    ),

    TestExpectation(
        name="K3_app_minimum_balance_over_application_history",
        query=lambda q: q.customer.find_for_date_range(
            "1993-01-01", "2022-01-01", _NOW, [q.customer.acct_bal().min().alias("min_balance")],
            q.customer.customer_key().eq(1),
        ),
        expected_columns=["min_balance"],
        expected_result=_rows([100.0]),
    ),

    # K.4: latest-version retrieval. The chosen key has only two versions, below HSIZE=10.
    TestExpectation(
        name="K4_sys_latest_ten_customer_system_versions",
        query=lambda q: q.customer.find_all(
            "1995-06-01", None,
            [q.customer.customer_key(), q.customer.acct_bal(), q.customer.sys_time_begin(), q.customer.sys_time_end()], q.customer.customer_key().eq(1),
        ).order_by(q.customer.sys_time_begin().ascending()).limit(10),
        expected_columns=["Customer Key", "Acct Bal", "Sys Time Begin", "Sys Time End"],
        expected_result=_rows([1, 100.0, _S0, _S1], [1, 6000.0, _S1, _FOREVER]),
    ),

    TestExpectation(
        name="K4_app_latest_ten_customer_application_versions",
        query=lambda q: q.customer.find_for_date_range(
            "0001-01-01", "9999-12-30", _NOW,
            [q.customer.customer_key(), q.customer.acct_bal(), q.customer.visible_time_begin(), q.customer.visible_time_end()], q.customer.customer_key().eq(1),
        ).order_by(q.customer.visible_time_begin().ascending()).limit(10),
        expected_columns=["Customer Key", "Acct Bal", "Visible Time Begin", "Visible Time End"],
        expected_result=_rows([1, 100.0, _D(2019, 1, 1), _D(2021, 1, 1)],
                              [1, 6000.0, _D(2021, 1, 1), _END_OF_TIME]),
    ),

    # K.5 uses adjacent versions here. The generated history is contiguous; arbitrary gaps would need correlated MAX.
    TestExpectation(
        name="K5_sys_preceding_customer_system_version",
        query=lambda q: q.customer.find_all(
            "1995-06-01", None, [q.customer.other_version().acct_bal(), q.customer.other_version().sys_time_begin()],
            q.customer.customer_key().eq(1).and_(q.customer.sys_time_begin() <= _S2)
            .and_(q.customer.other_version().sys_time_end() == q.customer.sys_time_begin()),
        ),
        expected_columns=["Other Version Acct Bal", "Other Version Sys Time Begin"],
        expected_result=_rows([100.0, _S0]),
    ),

    TestExpectation(
        name="K5_app_preceding_customer_application_version",
        query=lambda q: q.customer.find_all(
            None, _NOW, [q.customer.other_version().acct_bal(), q.customer.other_version().visible_time_begin()],
            q.customer.customer_key().eq(1).and_(q.customer.visible_time_begin() <= _D(2022, 1, 1))
            .and_(q.customer.other_version().visible_time_end() == q.customer.visible_time_begin()),
        ),
        expected_columns=["Other Version Acct Bal", "Other Version Visible Time Begin"],
        expected_result=_rows([100.0, _D(2019, 1, 1)]),
    ),

    # K.6: a value predicate over the selected history is an ordinary finder filter.
    TestExpectation(
        name="K6_sys_customer_history_filtered_by_balance_at_application_point",
        query=lambda q: q.customer.find_all(
            "1995-06-01", None, [q.customer.customer_key(), q.customer.acct_bal(), q.customer.sys_time_begin()], q.customer.acct_bal() > 5000.0,
        ).order_by(q.customer.customer_key().ascending(), q.customer.sys_time_begin().ascending()),
        expected_columns=["Customer Key", "Acct Bal", "Sys Time Begin"],
        expected_result=_rows([1, 6000.0, _S1], [4, 9000.0, _S0]),
    ),

    TestExpectation(
        name="K6_app_customer_application_history_filtered_by_balance",
        query=lambda q: q.customer.find_for_date_range(
            "0001-01-01", "9999-12-30", _NOW,
            [q.customer.customer_key(), q.customer.acct_bal(), q.customer.visible_time_begin()], q.customer.acct_bal() > 5000.0,
        ).order_by(q.customer.customer_key().ascending(), q.customer.acct_bal().ascending(), q.customer.visible_time_begin().ascending()),
        expected_columns=["Customer Key", "Acct Bal", "Visible Time Begin"],
        expected_result=_rows([1, 6000.0, _D(2021, 1, 1)], [4, 9000.0, _D(2019, 1, 1)]),
    ),

]

# Appendix B.4: range queries
R_CASES = [
    _unsupported("R1_sys_customers_moving_to_us_in_system_time_interval",
                 "Requires grouped temporal subqueries and a range join over system-time transitions",
                 ["Name"]),

    _unsupported("R1_app_customers_moving_to_us_in_application_time_interval",
                 "Requires grouped temporal subqueries and a range join over application-time transitions",
                 ["Name"]),

    _unsupported("R2_sys_mean_order_to_ship_time_over_system_history",
                 "Requires grouped earliest system-time transitions and a join",
                 ["AVG"]),

    _unsupported("R2_app_mean_order_to_ship_time_as_currently_recorded",
                 "Requires grouped earliest application-time transitions and a join",
                 ["AVG"]),

    # --- R2: the time a line item was pending, from shipping to receipt ---
    _unsupported(
        "R3a_temporal_aggregation_greatest_number_of_unshipped_items",
        "Temporal aggregation groups by the points where the data changes (an interval join on the period "
        "boundaries), then takes the maximum over them",
        ["Max Count"],
    ),

    # --- R.3b, R.5, R.6 ---
    _unsupported(
        "R3b_temporal_aggregation_maximum_value_of_unshipped_orders",
        "Temporal aggregation groups by the points where the data changes (an interval join on the period "
        "boundaries). The finders aggregate over the versions a date selects, not over change points",
        ["Max Total Price"],
    ),

    # --- R.4, R.7 ---
    _unsupported(
        "R4_products_with_the_smallest_difference_in_stock_over_the_history",
        "Needs every version of a key compared with the others (max minus min of the stock, then the smallest "
        "difference over all keys) as a nested aggregate over versions of the same table",
        ["Part Part Key"],
    ),

    _unsupported(
        "R5_count_distinct_open_orders_overlapping_low_balance_customer_system_history",
        "COUNT DISTINCT is supported, but the finder joins a related temporal row at the root version's time; it "
        "cannot enumerate every customer/order version pair whose system intervals overlap",
        ["count(distinct(o_orderkey))"],
    ),

    _unsupported(
        # Appendix B.4.6 of the local TPCTC 2013 report gives the SQL. This implementation gap is in the finder,
        # not missing query text or benchmark output.
        "R6_temporal_aggregation_with_a_join_of_two_temporal_tables",
        "Combines a temporal aggregation (a result row for each timestamp where the data changed, from an interval "
        "join on the period boundaries) with a join of two temporal tables, which the finders cannot do",
        [],
    ),

    TestExpectation(
        # The fixture has contiguous validity intervals, so the immediate predecessor is the row ending when this
        # version begins. For non-contiguous histories the benchmark's correlated MAX would be needed.
        name="R7_business_time_price_increase_from_latest_preceding_version",
        query=lambda q: q.partsupp.find_all(
            None, _NOW,
            [q.partsupp.part().part_key(), q.partsupp.supplier().supplier_key(), q.partsupp.supply_cost(),
             q.partsupp.validity_time_begin(), q.partsupp.validity_time_end(),
             q.partsupp.same_part().validity_time_begin(), q.partsupp.same_part().validity_time_end(), q.partsupp.same_part().supply_cost()],
            q.partsupp.supplier().supplier_key().eq(q.partsupp.same_part().supplier().supplier_key())
            .and_(q.partsupp.same_part().validity_time_end() == q.partsupp.validity_time_begin())
            .and_(q.partsupp.supply_cost() > q.partsupp.same_part().supply_cost() * 1.075),
        ).order_by(q.partsupp.validity_time_end().ascending(), q.partsupp.same_part().validity_time_end().descending()),
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

]

# Appendix B.5: bitemporal self-join queries
B5_CASES = [
    # --- B.5.1: current application and system times ---
    TestExpectation(
        # Supplier 3's rows for part 4 ended in 2021 (recorded at S3), so part 4 is not found at 2023-06-01.
        name="B5_1_application_point_system_point_current",
        query=lambda q: _other_parts(q.partsupp, "1997-06-01", _NOW),
        expected_columns=_B5_COLUMNS,
        expected_result=_rows([2], [3]),
    ),

    TestExpectation(
        name="B5_2_app_and_system_points_with_distinct_business_times",
        query=lambda q: _other_parts_at_distinct_business_times(
            q.partsupp, "1995-06-01", "1997-06-01", "2022-06-01",
        ),
        expected_columns=_B5_COLUMNS,
        expected_result=_rows([2], [3]),
    ),

    # --- B.5.3, B.5.4, B.5.5, B.5.8, B.5.10: row periods correlate in a dimension ---
    # Only B.5.3 loses part 4: the periods of supplier 3's rows for part 4 (ended in 2021) and for part 1 (from
    # 2022) are disjoint in application time. In system time they overlapped until S3, when the first was corrected.
    TestExpectation(
        name="B5_3_application_correlation_system_point_current",
        query=lambda q: _other_parts(q.partsupp, None, _NOW, correlate=("application",)),
        expected_columns=_B5_COLUMNS,
        expected_result=_rows([2], [3]),
    ),

    TestExpectation(
        name="B5_4_application_point_system_correlation",
        query=lambda q: _other_parts(q.partsupp, "1997-06-01", None, correlate=("system",)),
        expected_columns=_B5_COLUMNS,
        expected_result=_rows([2], [3], [4]),
    ),

    TestExpectation(
        name="B5_5_application_correlation_system_correlation",
        query=lambda q: _other_parts(q.partsupp, None, None, correlate=("application", "system")),
        expected_columns=_B5_COLUMNS,
        expected_result=_rows([2], [3], [4]),
    ),

    TestExpectation(
        name="B5_6_current_system_as_known_all_application_time",
        query=lambda q: _other_parts(q.partsupp, None, _NOW),
        expected_columns=_B5_COLUMNS,
        expected_result=_rows([2], [3], [4]),
    ),

    # --- B.5.7: application time agnostic / past system point ---
    TestExpectation(
        name="B5_7_application_agnostic_system_point_past",
        query=lambda q: _other_parts(q.partsupp, None, "2020-06-01"),
        expected_columns=_B5_COLUMNS,
        expected_result=_rows([2], [3]),
    ),

    TestExpectation(
        name="B5_8_application_agnostic_system_correlation",
        query=lambda q: _other_parts(q.partsupp, None, None, correlate=("system",)),
        expected_columns=_B5_COLUMNS,
        expected_result=_rows([2], [3], [4]),
    ),

    # --- B.5.9, B.5.11: system time agnostic ---
    TestExpectation(
        name="B5_9_application_point_system_agnostic_at_earlier_application_date",
        query=lambda q: _other_parts(q.partsupp, "1997-06-01", None),
        expected_columns=_B5_COLUMNS,
        expected_result=_rows([2], [3], [4]),
    ),

    TestExpectation(
        name="B5_9_application_point_system_agnostic_before_new_supplier",
        query=lambda q: _other_parts(q.partsupp, "1995-06-01", None),
        expected_columns=_B5_COLUMNS,
        expected_result=_rows([2], [3]),
    ),

    TestExpectation(
        name="B5_10_application_correlation_system_agnostic",
        query=lambda q: _other_parts(q.partsupp, None, None, correlate=("application",)),
        expected_columns=_B5_COLUMNS,
        expected_result=_rows([2], [3], [4]),
    ),

    TestExpectation(
        name="B5_11_application_agnostic_system_agnostic",
        query=lambda q: _other_parts(q.partsupp, None, None, correlate=("system",)),
        expected_columns=_B5_COLUMNS,
        expected_result=_rows([2], [3], [4]),
    ),

]

# TPC-H queries
Q_CASES = [
    # --- The queries the finders cannot express as TPC-H defines them ---
    _unsupported(
        "Q2_minimum_cost_supplier",
        "A correlated scalar subquery: ps_supplycost = (select min(ps_supplycost) ... for the same part)",
        ["s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment"],
    ),

    TestExpectation(
        # Q3, SEGMENT MACHINERY, DATE 1995-03-15: the first 10 rows
        name="Q3_shipping_priority",
        query=lambda q: q.lineitem.find_all(
            *_PC,
            [q.lineitem.order().order_key().alias("l_orderkey"), _revenue(q.lineitem).sum().alias("revenue"),
             q.lineitem.order().order_date().alias("o_orderdate"), q.lineitem.order().ship_priority().alias("o_shippriority")],
            q.lineitem.order().customer().mkt_segment().eq("MACHINERY")
            .and_(q.lineitem.order().order_date() < datetime.date(1995, 3, 15))
            .and_(q.lineitem.ship_date() > datetime.date(1995, 3, 15)),
        ).group_by(q.lineitem.order().order_key(), q.lineitem.order().order_date(), q.lineitem.order().ship_priority())
        .order_by(_revenue(q.lineitem).sum().descending(), q.lineitem.order().order_date().ascending()).limit(10),
        expected_columns=["l_orderkey", "revenue", "o_orderdate", "o_shippriority"],
        expected_result=_rows([4, _O4L1 + _O4L2, datetime.date(1994, 12, 15), 0]),
    ),

    _unsupported(
        "Q4_order_priority_checking",
        "exists (select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate): exists() takes "
        "no condition, and a filter through the line items joins, so it repeats an order for each line",
        ["o_orderpriority", "order_count"],
    ),

    TestExpectation(
        # Q5 with REGION AMERICA and order dates 1995-01-01 through 1996-12-31. Comparing nation names through
        # each navigation is equivalent to comparing the keys because the nation key/name mapping is unique.
        name="Q5_local_supplier_volume",
        query=lambda q: q.lineitem.find_all(
            *_PD,
            [q.lineitem.order().customer().nation().name().alias("n_name"), _revenue(q.lineitem).sum().alias("revenue")],
            q.lineitem.order().customer().nation().region().name().eq("AMERICA")
            .and_(q.lineitem.order().order_date() >= datetime.date(1995, 1, 1))
            .and_(q.lineitem.order().order_date() < datetime.date(1997, 1, 1))
            .and_(q.lineitem.order().customer().nation().name().eq(q.lineitem.supplier().nation().name())),
        ).group_by(q.lineitem.order().customer().nation().name())
        .order_by(_revenue(q.lineitem).sum().descending()),
        expected_columns=["n_name", "revenue"],
        expected_result=_rows(["UNITED STATES", _O6L2]),
    ),

    TestExpectation(
        # Q6, DATE 1993-01-01, DISCOUNT 0.06, QUANTITY 24
        name="Q6_forecasting_revenue_change",
        query=lambda q: q.lineitem.find_all(
            *_P1, [(q.lineitem.extended_price() * q.lineitem.discount()).sum().alias("revenue")],
            (q.lineitem.ship_date() >= datetime.date(1993, 1, 1)).and_(q.lineitem.ship_date() < datetime.date(1994, 1, 1))
            .and_(q.lineitem.discount() >= float(decimal.Decimal("0.06") - decimal.Decimal("0.01")))
            .and_(q.lineitem.discount() <= float(decimal.Decimal("0.06") + decimal.Decimal("0.01")))
            .and_(q.lineitem.quantity() < 24.0),
        ),
        expected_columns=["revenue"],
        expected_result=_rows([9000.0 * 0.05 + 8800.0 * 0.07]),
    ),

    TestExpectation(
        # Q7, NATION1 GERMANY, NATION2 UNITED STATES
        name="Q7_volume_shipping",
        query=lambda q: q.lineitem.find_all(
            *_PD,
            [q.lineitem.supplier().nation().name().alias("supp_nation"),
             q.lineitem.order().customer().nation().name().alias("cust_nation"),
             q.lineitem.ship_date().year().alias("l_year"), _revenue(q.lineitem).sum().alias("revenue")],
            q.lineitem.supplier().nation().name().eq("GERMANY").and_(q.lineitem.order().customer().nation().name().eq("UNITED STATES"))
            .or_(q.lineitem.supplier().nation().name().eq("UNITED STATES")
                 .and_(q.lineitem.order().customer().nation().name().eq("GERMANY")))
            .and_(q.lineitem.ship_date() >= datetime.date(1995, 1, 1)).and_(q.lineitem.ship_date() <= datetime.date(1996, 12, 31)),
        ).order_by(q.lineitem.supplier().nation().name().ascending(), q.lineitem.order().customer().nation().name().ascending(),
                   q.lineitem.ship_date().year().ascending()),
        expected_columns=["supp_nation", "cust_nation", "l_year", "revenue"],
        expected_result=_rows(["GERMANY", "UNITED STATES", 1995, _O4L2], ["GERMANY", "UNITED STATES", 1996, _O6L1]),
    ),

    TestExpectation(
        # Q8, NATION UNITED STATES, REGION AMERICA, TYPE STANDARD POLISHED BRASS
        name="Q8_national_market_share",
        query=lambda q: q.lineitem.find_all(
            *_PD,
            [q.lineitem.order().order_date().year().alias("o_year"),
             (when(q.lineitem.supplier().nation().name().eq("UNITED STATES")).then(_revenue(q.lineitem)).otherwise(0).sum()
              / _revenue(q.lineitem).sum()).alias("mkt_share")],
            q.lineitem.order().customer().nation().region().name().eq("AMERICA")
            .and_(q.lineitem.order().order_date() >= datetime.date(1995, 1, 1))
            .and_(q.lineitem.order().order_date() <= datetime.date(1996, 12, 31))
            .and_(q.lineitem.part().type_().eq("STANDARD POLISHED BRASS")),
        ).group_by(q.lineitem.order().order_date().year()).order_by(q.lineitem.order().order_date().year().ascending()),
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
        # Q10, DATE 1993-10-01: the first 20 rows
        name="Q10_returned_item_reporting",
        query=lambda q: q.customer.find_all(
            *_P1,
            [q.customer.customer_key().alias("c_custkey"), q.customer.name().alias("c_name"),
             (q.customer.orders().line_items().extended_price() * (constant(1) - q.customer.orders().line_items().discount()))
             .sum().alias("revenue"),
             q.customer.acct_bal().alias("c_acctbal"), q.customer.nation().name().alias("n_name"), q.customer.address().alias("c_address"),
             q.customer.phone().alias("c_phone"), q.customer.comment().alias("c_comment")],
            (q.customer.orders().order_date() >= datetime.date(1993, 10, 1))
            .and_(q.customer.orders().order_date() < datetime.date(1994, 1, 1))
            .and_(q.customer.orders().line_items().return_flag().eq("R")),
        ).group_by(q.customer.customer_key(), q.customer.name(), q.customer.acct_bal(), q.customer.phone(), q.customer.nation().name(), q.customer.address(), q.customer.comment())
        .order_by((q.customer.orders().line_items().extended_price() * (constant(1) - q.customer.orders().line_items().discount()))
                  .sum().descending()).limit(20),
        expected_columns=["c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment"],
        expected_result=_rows(
            [1, "Customer#1", _O1L1, 100.0, "UNITED STATES", "1 Customer Avenue", "10-101-201-3001",
             "customer 1 is a regular customer"],
            [2, "Customer#2", _O2L2, 3000.0, "GERMANY", "2 Customer Avenue", "11-102-202-3002",
             "customer 2 is a regular customer"],
        ),
    ),

    _unsupported(
        "Q11_important_stock_identification",
        "A scalar subquery in HAVING: sum(ps_supplycost * ps_availqty) > (select sum(...) * FRACTION ...)",
        ["ps_partkey", "value"],
    ),

    TestExpectation(
        # Q12, SHIPMODE1 MAIL, SHIPMODE2 SHIP, DATE 1993-01-01
        name="Q12_shipping_modes_and_order_priority",
        query=lambda q: q.lineitem.find_all(
            *_P1,
            [q.lineitem.ship_mode().alias("l_shipmode"),
             when(q.lineitem.order().order_priority().eq("1-URGENT").or_(q.lineitem.order().order_priority().eq("2-HIGH")))
             .then(1).otherwise(0).sum().alias("high_line_count"),
             when(q.lineitem.order().order_priority().ne("1-URGENT").and_(q.lineitem.order().order_priority().ne("2-HIGH")))
             .then(1).otherwise(0).sum().alias("low_line_count")],
            q.lineitem.ship_mode().is_in(["MAIL", "SHIP"]).and_(q.lineitem.commit_date() < q.lineitem.receipt_date())
            .and_(q.lineitem.ship_date() < q.lineitem.commit_date())
            .and_(q.lineitem.receipt_date() >= datetime.date(1993, 1, 1)).and_(q.lineitem.receipt_date() < datetime.date(1994, 1, 1)),
        ).order_by(q.lineitem.ship_mode().ascending()),
        expected_columns=["l_shipmode", "high_line_count", "low_line_count"],
        expected_result=_rows(["MAIL", 1, 1], ["SHIP", 1, 0]),
    ),

    _unsupported(
        "Q13_customer_distribution",
        "A derived table with a LEFT OUTER JOIN whose ON clause has a condition (o_comment not like "
        "'%[WORD1]%[WORD2]%'), grouped again by the count it produced",
        ["c_count", "custdist"],
    ),

    TestExpectation(
        # Q14, DATE 1993-11-01: 100.00 * sum(promo) / sum(all)
        name="Q14_promotion_effect",
        query=lambda q: q.lineitem.find_all(
            *_P1,
            [((constant(100.0) * when(q.lineitem.part().type_().starts_with("PROMO")).then(_revenue(q.lineitem)).otherwise(0).sum())
              / _revenue(q.lineitem).sum()).alias("promo_revenue")],
            (q.lineitem.ship_date() >= datetime.date(1993, 11, 1)).and_(q.lineitem.ship_date() < datetime.date(1993, 12, 1)),
        ),
        expected_columns=["promo_revenue"],
        expected_result=_rows([(100.0 * _O1L1) / (_O1L1 + _O1L2)]),
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
        "Q18_large_volume_customer",
        "o_orderkey in (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > [QUANTITY]): "
        "an IN subquery with GROUP BY and HAVING",
        ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice", "sum(l_quantity)"],
    ),

    TestExpectation(
        # Q19, QUANTITY1 1, QUANTITY2 10, QUANTITY3 20, BRAND1 Brand#13, BRAND2 Brand#24, BRAND3 Brand#35
        name="Q19_discounted_revenue",
        query=lambda q: q.lineitem.find_all(
            *_PD, [_revenue(q.lineitem).sum().alias("revenue")],
            _q19_branch(q.lineitem, "Brand#13", ["SM CASE", "SM BOX", "SM PACK", "SM PKG"], 1.0, 5)
            .or_(_q19_branch(q.lineitem, "Brand#24", ["MED BAG", "MED BOX", "MED PKG", "MED PACK"], 10.0, 10))
            .or_(_q19_branch(q.lineitem, "Brand#35", ["LG CASE", "LG BOX", "LG PACK", "LG PKG"], 20.0, 15)),
        ),
        expected_columns=["revenue"],
        expected_result=_rows([_O3L1 + _O4L1 + _O4L2]),
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

]

ALL_TPCH_BIH_CASES = [
    *T_CASES,
    *H_CASES,
    *K_CASES,
    *R_CASES,
    *B5_CASES,
    *Q_CASES,
]
