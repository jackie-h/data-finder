"""
Test specifications for the TPC-BiH benchmark (Kaufmann et al., "TPC-BiH: A Benchmark for Bitemporal Databases",
TPCTC 2013), using tpch_bih_mapping.md on a small data set.

Every query class in the paper has an entry. Those the finders cannot express are skipped, each with the reason.

Schema (the TPC-H tables; comment columns and a few descriptive ones are left out):

    region, nation   not versioned
    supplier         degenerate: system time only
    customer         bitemporal, application time visible_time
    part             bitemporal, application time availability_time
    partsupp         bitemporal, application time validity_time
    orders           bitemporal, active_time as application time, receivable_time as ordinary dates
    lineitem         bitemporal, application time active_time

The CSVs are written by datafinder_examples/tools/generate_tpch_bih_data.py, which applies the history below. System
times are S0 = 2020-01-01 (the initial load), S1 = 2021-01-01, S2 = 2022-01-01 and S3 = 2023-01-01.

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

import numpy as np

from datafinder.conditional import when
from datafinder.expressions import constant, count_all
from datafinder_examples_tests.spec import FinderSpec, TestExpectation

TPCH_BIH_MAPPING = "tpch_bih_mapping.md"

_D = datetime.date
_S0 = datetime.datetime(2020, 1, 1)
_S1 = datetime.datetime(2021, 1, 1)
_S2 = datetime.datetime(2022, 1, 1)
_S3 = datetime.datetime(2023, 1, 1)
_NOW = datetime.datetime(2024, 1, 1)
_FOREVER = datetime.datetime(9999, 12, 31, 23, 59, 59)
_END_OF_TIME = _D(9999, 12, 31)

_BACKENDS = ("duckdb", "graphql")


def _skip(reason: str) -> dict[str, str]:
    return {backend: reason for backend in _BACKENDS}


def _not_supported(reason: str):
    def query(_finder):
        raise NotImplementedError(reason)
    return query


def _unsupported(name: str, reason: str, columns: list[str] | None = None) -> TestExpectation:
    """An expectation for a query the finders cannot express, kept so the benchmark is complete."""
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
        "2022-06-01", _NOW, [f.order_key(), f.total_price(), at_s0.total_price()],
        f.total_price() != at_s0.total_price(),
    )

ORDER_FINDER_SPECS = FinderSpec(
    finder_name="OrderFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        # --- T2: point in both dimensions on ORDERS (insert-heavy history) ---
        TestExpectation(
            name="T2_point_point_before_cancel",
            query=lambda f: f.find_all("2022-06-01", "2022-06-01", [f.order_key(), f.total_price()])
            .order_by(f.order_key().ascending()),
            expected_columns=_ORDER_COLUMNS,
            expected_result=_rows([3, 500.0], [4, 3000.0], [5, 800.0]),
            expected_sql={"duckdb": "t0.active_time_begin <= '2022-06-01'"},
        ),
        TestExpectation(
            name="T2_point_point_after_cancel_and_price_manipulation",
            query=lambda f: f.find_all("2022-06-01", _NOW, [f.order_key(), f.total_price()])
            .order_by(f.order_key().ascending()),
            expected_columns=_ORDER_COLUMNS,
            expected_result=_rows([3, 700.0], [4, 3000.0]),
        ),
        TestExpectation(
            name="T2_point_point_early",
            query=lambda f: f.find_all("2019-12-12", "2020-06-01", [f.order_key(), f.total_price()])
            .order_by(f.order_key().ascending()),
            expected_columns=_ORDER_COLUMNS,
            expected_result=_rows([1, 1000.0], [2, 2000.0]),
        ),
        # --- T3, T4: two time travels on the same table, correlated (a navigation to the order at another time) ---
        TestExpectation(
            name="T3_price_changed_between_two_system_times",
            query=lambda f: _price_changed(f),
            expected_columns=["Order Key", "Total Price", "Other Version Total Price"],
            expected_result=_rows([3, 700.0, 500.0]),
        ),
        TestExpectation(
            name="T4_largest_price_change_between_two_system_times_top_n",
            query=lambda f: _price_changed(f).order_by(
                (f.total_price() - f.other_version(processing_time=_S0).total_price()).descending()
            ).limit(1),
            expected_columns=["Order Key", "Total Price", "Other Version Total Price"],
            expected_result=_rows([3, 700.0, 500.0]),
        ),
        # --- T5: the whole history ---
        TestExpectation(
            name="T5_all_versions",
            query=lambda f: f.find_all(None, None, [f.order_key(), f.order_status(), f.total_price()])
            .order_by(f.order_key().ascending(), f.sys_time_begin().ascending()),
            expected_columns=["Order Key", "Order Status", "Total Price"],
            expected_result=_rows(
                [1, "F", 1000.0], [2, "O", 2000.0], [2, "F", 2000.0], [3, "O", 500.0], [3, "O", 700.0],
                [4, "O", 3000.0], [5, "O", 800.0], [6, "O", 1500.0],
            ),
            unexpected_sql={"duckdb": "WHERE"},
        ),
        # --- T6: one dimension a point, the other the whole history ---
        TestExpectation(
            name="T6_application_time_point_all_system_time",
            query=lambda f: f.find_all("2020-06-01", None, [f.order_key(), f.order_status(), f.total_price()])
            .order_by(f.order_key().ascending(), f.sys_time_begin().ascending()),
            expected_columns=["Order Key", "Order Status", "Total Price"],
            expected_result=_rows([2, "O", 2000.0], [2, "F", 2000.0], [3, "O", 500.0], [3, "O", 700.0]),
        ),
        TestExpectation(
            name="T6_system_time_point_all_application_time",
            query=lambda f: f.find_all(None, "2022-06-01", [f.order_key(), f.order_status()])
            .order_by(f.order_key().ascending()),
            expected_columns=["Order Key", "Order Status"],
            expected_result=_rows([1, "F"], [2, "F"], [3, "O"], [4, "O"], [5, "O"]),
        ),
        # --- T7: current system time given implicitly (here, the time now) ---
        TestExpectation(
            name="T7_current_system_time",
            query=lambda f: f.find_all("2022-06-01", datetime.datetime.now(), [f.order_key(), f.total_price()])
            .order_by(f.order_key().ascending()),
            expected_columns=_ORDER_COLUMNS,
            expected_result=_rows([3, 700.0], [4, 3000.0]),
        ),
        # --- T8, T9: the second application time, receivable_time, is an ordinary pair of dates ---
        TestExpectation(
            name="T8_second_application_time_point",
            query=lambda f: f.find_all(
                None, "2020-06-01", [f.order_key()],
                (f.receivable_time_begin() <= _D(2020, 1, 10)).and_(f.receivable_time_end() > _D(2020, 1, 10)),
            ).order_by(f.order_key().ascending()),
            expected_columns=["Order Key"],
            expected_result=_rows([1], [2], [3]),
        ),
        TestExpectation(
            name="T9_second_application_time_slice",
            query=lambda f: f.find_all(
                None, None, [f.order_key()],
                (f.receivable_time_begin() <= _D(2021, 12, 31)).and_(f.receivable_time_end() > _D(2021, 1, 1)),
            ).order_by(f.order_key().ascending(), f.sys_time_begin().ascending()),
            expected_columns=["Order Key"],
            expected_result=_rows([1], [2], [2], [3], [3], [4]),
        ),
        # --- K1 to K6: the history of one key (audit) ---
        TestExpectation(
            name="K1_key_history_all_versions",
            query=lambda f: f.find_all(
                None, None,
                [f.order_key(), f.order_status(), f.total_price(), f.sys_time_begin(), f.sys_time_end()],
                f.order_key().eq(3),
            ).order_by(f.sys_time_begin().ascending()),
            expected_columns=["Order Key", "Order Status", "Total Price", "Sys Time Begin", "Sys Time End"],
            expected_result=_rows([3, "O", 500.0, _S0, _S3], [3, "O", 700.0, _S3, _FOREVER]),
        ),
        TestExpectation(
            name="K2_key_history_in_a_system_time_range",
            query=lambda f: f.find_all(
                None, None,
                [f.order_key(), f.order_status(), f.total_price(), f.sys_time_begin(), f.sys_time_end()],
                f.order_key().eq(3).and_(f.sys_time_begin() <= _NOW).and_(f.sys_time_end() > datetime.datetime(2023, 6, 1)),
            ).order_by(f.sys_time_begin().ascending()),
            expected_columns=["Order Key", "Order Status", "Total Price", "Sys Time Begin", "Sys Time End"],
            expected_result=_rows([3, "O", 700.0, _S3, _FOREVER]),
        ),
        TestExpectation(
            name="K3_key_history_in_a_range_one_column",
            query=lambda f: f.find_all(
                None, None, [f.total_price()],
                f.order_key().eq(3).and_(f.sys_time_begin() <= datetime.datetime(2023, 6, 1))
                .and_(f.sys_time_end() > datetime.datetime(2022, 6, 1)),
            ).order_by(f.sys_time_begin().ascending()),
            expected_columns=["Total Price"],
            expected_result=_rows([500.0], [700.0]),
        ),
        TestExpectation(
            name="K4_latest_versions_top_n",
            query=lambda f: f.find_all(None, None, [f.total_price()], f.order_key().eq(3))
            .order_by(f.sys_time_begin().descending()).limit(2),
            expected_columns=["Total Price"],
            expected_result=_rows([700.0], [500.0]),
        ),
        TestExpectation(
            # The paper correlates the timestamps of a version with those of the one before it. Skipping the latest
            # version of a key gives the same row, and needs no self-join.
            name="K5_previous_version",
            query=lambda f: f.find_all(None, None, [f.total_price()], f.order_key().eq(3))
            .order_by(f.sys_time_begin().descending()).offset(1).limit(1),
            expected_columns=["Total Price"],
            expected_result=_rows([500.0]),
        ),
        TestExpectation(
            name="K6_history_selected_by_a_value",
            query=lambda f: f.find_all(None, None, [f.order_key(), f.total_price()], f.total_price() > 1500.0)
            .order_by(f.order_key().ascending(), f.sys_time_begin().ascending()),
            expected_columns=_ORDER_COLUMNS,
            expected_result=_rows([2, 2000.0], [2, 2000.0], [4, 3000.0]),
        ),
        TestExpectation(
            name="K6_history_selected_by_a_value_high_selectivity",
            query=lambda f: f.find_all(None, None, [f.order_key(), f.total_price()], f.total_price() > 2500.0),
            expected_columns=_ORDER_COLUMNS,
            expected_result=_rows([4, 3000.0]),
        ),
        # --- R3b, R5 ---
        _unsupported(
            "R3b_temporal_aggregation_maximum_value_of_unshipped_orders",
            "Temporal aggregation groups by the points where the data changes (an interval join on the period "
            "boundaries). The finders aggregate over the versions a date selects, not over change points",
            ["Max Total Price"],
        ),
        TestExpectation(
            # How often a customer had a low balance while an order of theirs was active, in both dimensions: every
            # pair of an order version and a customer version whose application and system periods overlap.
            name="R5_temporal_join_customers_with_low_balance_and_orders",
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
            # R5 at one point in time: not the benchmark's temporal join, but the same filter on both tables.
            name="R5_at_a_point_customers_with_low_balance_and_orders",
            query=lambda f: f.find_all(
                "2022-06-01", "2022-06-01", [f.customer().customer_key()],
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
        "2022-06-01", _NOW, [f.customer_key(), f.name()],
        f.nation().name().eq("UNITED STATES").and_(before.nation().name() != "UNITED STATES"),
    )


def _customers_at(f, business_date, processing_time):
    return f.find_all(business_date, processing_time, [f.customer_key(), f.acct_bal(), f.nation().name()]) \
        .order_by(f.customer_key().ascending())


CUSTOMER_FINDER_SPECS = FinderSpec(
    finder_name="CustomerFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        # --- T1: point in both dimensions on CUSTOMER (update-heavy history) ---
        TestExpectation(
            name="T1_point_point_current",
            query=lambda f: _customers_at(f, "2022-06-01", "2022-06-01"),
            expected_columns=_CUSTOMER_COLUMNS,
            expected_result=_rows(
                [1, 6000.0, "UNITED STATES"], [2, 3000.0, "UNITED STATES"], [3, 4000.0, "FRANCE"],
                [4, 9000.0, "UNITED STATES"],
            ),
        ),
        TestExpectation(
            name="T1_point_point_past",
            query=lambda f: _customers_at(f, "2020-06-01", "2020-06-01"),
            expected_columns=_CUSTOMER_COLUMNS,
            expected_result=_rows(
                [1, 100.0, "UNITED STATES"], [2, 3000.0, "GERMANY"], [3, 4000.0, "FRANCE"],
                [4, 9000.0, "UNITED STATES"],
            ),
        ),
        TestExpectation(
            # Tomorrow's state in application time, as recorded yesterday: the 2021 payment was not known in 2020.
            name="T1_future_application_time_as_recorded_in_the_past",
            query=lambda f: _customers_at(f, "2021-06-01", "2020-06-01"),
            expected_columns=_CUSTOMER_COLUMNS,
            expected_result=_rows(
                [1, 100.0, "UNITED STATES"], [2, 3000.0, "GERMANY"], [3, 4000.0, "FRANCE"],
                [4, 9000.0, "UNITED STATES"],
            ),
        ),
        TestExpectation(
            name="T1_same_application_time_as_recorded_later",
            query=lambda f: _customers_at(f, "2021-06-01", "2021-06-01"),
            expected_columns=_CUSTOMER_COLUMNS,
            expected_result=_rows(
                [1, 6000.0, "UNITED STATES"], [2, 3000.0, "GERMANY"], [3, 4000.0, "FRANCE"],
                [4, 9000.0, "UNITED STATES"],
            ),
        ),
        # --- K1 along application time: the versions of one customer at the current system time ---
        TestExpectation(
            name="K1_key_history_in_application_time",
            query=lambda f: f.find_all(
                None, _NOW, [f.customer_key(), f.acct_bal(), f.visible_time_begin(), f.visible_time_end()],
                f.customer_key().eq(1),
            ).order_by(f.visible_time_begin().ascending()),
            expected_columns=["Customer Key", "Acct Bal", "Visible Time Begin", "Visible Time End"],
            expected_result=_rows(
                [1, 100.0, _D(2019, 1, 1), _D(2021, 1, 1)], [1, 6000.0, _D(2021, 1, 1), _END_OF_TIME],
            ),
        ),
        # --- R1: customers who moved to the US between two dates and still live there ---
        TestExpectation(
            name="R1_customers_who_moved_to_the_us_and_still_live_there",
            query=lambda f: _moved_to_us(f),
            expected_columns=["Customer Key", "Name"],
            expected_result=_rows([2, "Customer#2"]),
        ),
    ],
)

# ---------------------------------------------------------------------------------------------------------------------
# Bitemporal self-join (B3): the other parts supplied by the suppliers who supply part 1
# ---------------------------------------------------------------------------------------------------------------------

_B3_COLUMNS = ["Part Part Key"]


def _other_parts(f, business_date, processing_time, correlate=()):
    """PARTSUPP rows of other parts, for suppliers that also have a PARTSUPP row for part 1: a self-join of PARTSUPP
    through SUPPLIER. A navigation takes the dates of the root, so a dimension that is a point (or agnostic) for both
    rows is a date of the query, and one that is correlated is left open (None) and the periods of the two rows
    are compared: ``correlate`` names "application" and "system"."""
    other = f.supplier().part_supps()
    condition = other.part().name().eq("Part#1").and_(f.part().name().ne("Part#1"))
    if "application" in correlate:
        condition = condition.and_(_overlap(f.validity_time_begin(), f.validity_time_end(),
                                            other.validity_time_begin(), other.validity_time_end()))
    if "system" in correlate:
        condition = condition.and_(_overlap(f.sys_time_begin(), f.sys_time_end(),
                                            other.sys_time_begin(), other.sys_time_end()))
    return f.find_all(business_date, processing_time, [f.part().part_key()], condition) \
        .distinct().order_by(f.part().part_key().ascending())


PARTSUPP_FINDER_SPECS = FinderSpec(
    finder_name="PartSuppFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        # --- B3.1, B3.2: point in both dimensions ---
        TestExpectation(
            # Supplier 3's rows for part 4 ended in 2021 (recorded at S3), so part 4 is not found at 2023-06-01.
            name="B3_1_application_point_system_point_current",
            query=lambda f: _other_parts(f, "2023-06-01", _NOW),
            expected_columns=_B3_COLUMNS,
            expected_result=_rows([2], [3]),
        ),
        TestExpectation(
            name="B3_2_application_point_system_point_past",
            query=lambda f: _other_parts(f, "2023-06-01", "2020-06-01"),
            expected_columns=_B3_COLUMNS,
            expected_result=_rows([2], [3]),
        ),
        # --- B3.6, B3.7: application time agnostic ---
        TestExpectation(
            name="B3_6_application_agnostic_system_point_current",
            query=lambda f: _other_parts(f, None, _NOW),
            expected_columns=_B3_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        TestExpectation(
            name="B3_7_application_agnostic_system_point_past",
            query=lambda f: _other_parts(f, None, "2020-06-01"),
            expected_columns=_B3_COLUMNS,
            expected_result=_rows([2], [3]),
        ),
        # --- B3.9, B3.11: system time agnostic ---
        TestExpectation(
            name="B3_9_application_point_system_agnostic",
            query=lambda f: _other_parts(f, "2023-06-01", None),
            expected_columns=_B3_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        TestExpectation(
            name="B3_9_application_point_before_the_new_supplier_system_agnostic",
            query=lambda f: _other_parts(f, "2021-06-01", None),
            expected_columns=_B3_COLUMNS,
            expected_result=_rows([2], [3]),
        ),
        TestExpectation(
            name="B3_11_application_agnostic_system_agnostic",
            query=lambda f: _other_parts(f, None, None),
            expected_columns=_B3_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        # --- B3.3, B3.4, B3.5, B3.8, B3.10: the periods of the two rows must overlap in a dimension ---
        # Only B3.3 loses part 4: the periods of supplier 3's rows for part 4 (ended in 2021) and for part 1 (from
        # 2022) are disjoint in application time. In system time they overlapped until S3, when the first was corrected.
        TestExpectation(
            name="B3_3_application_correlation_system_point_current",
            query=lambda f: _other_parts(f, None, _NOW, correlate=("application",)),
            expected_columns=_B3_COLUMNS,
            expected_result=_rows([2], [3]),
        ),
        TestExpectation(
            name="B3_4_application_point_system_correlation",
            query=lambda f: _other_parts(f, "2023-06-01", None, correlate=("system",)),
            expected_columns=_B3_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        TestExpectation(
            name="B3_5_application_correlation_system_correlation",
            query=lambda f: _other_parts(f, None, None, correlate=("application", "system")),
            expected_columns=_B3_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        TestExpectation(
            name="B3_8_application_agnostic_system_correlation",
            query=lambda f: _other_parts(f, None, None, correlate=("system",)),
            expected_columns=_B3_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        TestExpectation(
            name="B3_10_application_correlation_system_agnostic",
            query=lambda f: _other_parts(f, None, None, correlate=("application",)),
            expected_columns=_B3_COLUMNS,
            expected_result=_rows([2], [3], [4]),
        ),
        # --- The supply cost history (price change scenario) ---
        TestExpectation(
            name="supply_cost_change_effective_in_the_future_as_recorded",
            query=lambda f: f.find_all(
                "2021-07-01", "2021-06-01", [f.supply_cost()],
                f.part().name().eq("Part#1").and_(f.supplier().name().eq("Supplier#1")),
            ),
            expected_columns=["Supply Cost"],
            expected_result=_rows([110.0]),
        ),
        TestExpectation(
            name="supply_cost_before_the_change_was_recorded",
            query=lambda f: f.find_all(
                "2021-07-01", "2020-06-01", [f.supply_cost()],
                f.part().name().eq("Part#1").and_(f.supplier().name().eq("Supplier#1")),
            ),
            expected_columns=["Supply Cost"],
            expected_result=_rows([100.0]),
        ),
        TestExpectation(
            name="supply_cost_current",
            query=lambda f: f.find_all(
                "2023-06-01", _NOW, [f.supply_cost()],
                f.part().name().eq("Part#1").and_(f.supplier().name().eq("Supplier#1")),
            ),
            expected_columns=["Supply Cost"],
            expected_result=_rows([125.0]),
        ),
        TestExpectation(
            name="supply_cost_as_it_was_before_the_last_change_was_recorded",
            query=lambda f: f.find_all(
                "2023-06-01", "2022-06-01", [f.supply_cost()],
                f.part().name().eq("Part#1").and_(f.supplier().name().eq("Supplier#1")),
            ),
            expected_columns=["Supply Cost"],
            expected_result=_rows([110.0]),
        ),
        # --- R4, R7 ---
        _unsupported(
            "R4_products_with_the_smallest_difference_in_stock_over_the_history",
            "Needs every version of a key compared with the others (max minus min of the stock, then the smallest "
            "difference over all keys) as a nested aggregate over versions of the same table",
            ["Part Part Key"],
        ),
        TestExpectation(
            # A version and the one before it: it ended when this one began, for the same supplier and part (rows of the
            # same part are joined, and the supplier is compared), with application periods that overlap.
            name="R7_suppliers_who_raised_a_price_by_more_than_7_5_percent_in_one_update",
            query=lambda f: f.find_all(
                None, None, [f.supplier().supplier_key()],
                (f.supplier().supplier_key() == f.same_part().supplier().supplier_key())
                .and_(f.same_part().sys_time_end() == f.sys_time_begin())
                .and_(_overlap(f.validity_time_begin(), f.validity_time_end(),
                               f.same_part().validity_time_begin(), f.same_part().validity_time_end()))
                .and_(f.supply_cost() > f.same_part().supply_cost() * 1.075),
            ).distinct(),
            expected_columns=["Supplier Supplier Key"],
            expected_result=_rows([1]),
        ),
    ],
)

# ---------------------------------------------------------------------------------------------------------------------
# TPC-H queries with time travel, on LINEITEM and the tables around it
# ---------------------------------------------------------------------------------------------------------------------

# One point in both dimensions for H1, H3, H5, H6, H7, H10, H12 and H19: it holds orders 1 and 2 with their four lines.
_H_APPLICATION = "2019-12-12"
_H_SYSTEM = "2020-06-01"

# 1 - 0.05 and the like are inexact doubles, so the expected values use the same operations the SQL does.
_O1L1 = _discounted(9000.0, 0.05)   # part 1, supplier 1 (GERMANY), customer 1, R F, MAIL
_O1L2 = _discounted(5000.0, 0.10)   # part 2, supplier 1 (GERMANY), customer 1, A F, SHIP
_O2L1 = _discounted(18000.0, 0.00)  # part 1, supplier 2 (FRANCE), customer 2 (GERMANY), N O, MAIL
_O2L2 = _discounted(8800.0, 0.07)   # part 3, supplier 2 (FRANCE), customer 2 (GERMANY), N O, AIR


def _revenue(l):
    return l.extended_price() * (constant(1) - l.discount())


_SKIP_SUBQUERY = "The finders have no subqueries (a correlated or nested SELECT in the WHERE or HAVING clause)"

LINEITEM_FINDER_SPECS = FinderSpec(
    finder_name="LineItemFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        TestExpectation(
            name="H1_pricing_summary",
            query=lambda f: f.find_all(
                _H_APPLICATION, _H_SYSTEM,
                [
                    f.return_flag(), f.line_status(),
                    f.quantity().sum().alias("sum_qty"),
                    f.extended_price().sum().alias("sum_base_price"),
                    _revenue(f).sum().alias("sum_disc_price"),
                    (_revenue(f) * (constant(1) + f.tax())).sum().alias("sum_charge"),
                    f.quantity().average().alias("avg_qty"),
                    count_all().alias("count_order"),
                ],
                f.ship_date() <= _D(2020, 1, 1),
            ).order_by(f.return_flag().ascending(), f.line_status().ascending()),
            expected_columns=[
                "Return Flag", "Line Status", "sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "avg_qty",
                "count_order",
            ],
            expected_result=_rows(
                ["A", "F", 5.0, 5000.0, _O1L2, _O1L2 * (1 + 0.02), 5.0, 1],
                ["N", "O", 28.0, 26800.0, _O2L1 + _O2L2, _O2L1 * (1 + 0.05) + _O2L2 * (1 + 0.05), 14.0, 2],
                ["R", "F", 10.0, 9000.0, _O1L1, _O1L1 * (1 + 0.02), 10.0, 1],
            ),
        ),
        TestExpectation(
            name="H3_shipping_priority",
            query=lambda f: f.find_all(
                _H_APPLICATION, _H_SYSTEM,
                [f.order().order_key(), _revenue(f).sum().alias("revenue"), f.order().order_date(),
                 f.order().ship_priority()],
                f.order().customer().mkt_segment().eq("BUILDING")
                .and_(f.order().order_date() < _D(2019, 12, 1)).and_(f.ship_date() > _D(2019, 11, 5)),
            ).group_by(f.order().order_key(), f.order().order_date(), f.order().ship_priority())
            .order_by(_revenue(f).sum().descending()).limit(10),
            expected_columns=["Order Order Key", "revenue", "Order Order Date", "Order Ship Priority"],
            expected_result=_rows([1, _O1L1 + _O1L2, _D(2019, 11, 1), 0]),
        ),
        TestExpectation(
            # TPC-H Q5 also requires the customer and the supplier to be in the same nation, which compares two columns.
            name="H5_revenue_by_supplier_nation_in_a_region",
            query=lambda f: f.find_all(
                _H_APPLICATION, _H_SYSTEM, [f.supplier().nation().name(), _revenue(f).sum().alias("revenue")],
                f.supplier().nation().region().name().eq("EUROPE"),
            ).order_by(_revenue(f).sum().descending()),
            expected_columns=["Supplier Nation Name", "revenue"],
            expected_result=_rows(["FRANCE", _O2L1 + _O2L2], ["GERMANY", _O1L1 + _O1L2]),
        ),
        TestExpectation(
            name="H6_forecasting_revenue_change",
            query=lambda f: f.find_all(
                _H_APPLICATION, _H_SYSTEM, [(f.extended_price() * f.discount()).sum().alias("revenue")],
                (f.ship_date() >= _D(2019, 1, 1)).and_(f.ship_date() < _D(2020, 1, 1))
                .and_(f.discount() >= 0.05).and_(f.discount() <= 0.07).and_(f.quantity() < 24.0),
            ),
            expected_columns=["revenue"],
            expected_result=_rows([9000.0 * 0.05 + 8800.0 * 0.07]),
        ),
        TestExpectation(
            name="H7_volume_shipping",
            query=lambda f: f.find_all(
                _H_APPLICATION, _H_SYSTEM,
                [f.supplier().nation().name(), f.order().customer().nation().name(), f.ship_date().year(),
                 _revenue(f).sum().alias("revenue")],
                f.supplier().nation().name().eq("FRANCE").and_(f.order().customer().nation().name().eq("GERMANY"))
                .or_(f.supplier().nation().name().eq("GERMANY").and_(f.order().customer().nation().name().eq("FRANCE"))),
            ),
            expected_columns=["Supplier Nation Name", "Order Customer Nation Name", "Year Ship Date", "revenue"],
            expected_result=_rows(["FRANCE", "GERMANY", 2019, _O2L1 + _O2L2]),
        ),
        TestExpectation(
            name="H19_discounted_revenue",
            query=lambda f: f.find_all(
                _H_APPLICATION, _H_SYSTEM, [_revenue(f).sum().alias("revenue")],
                f.part().brand().eq("Brand#13").and_(f.part().container().is_in(["SM BOX"]))
                .and_(f.quantity() >= 1.0).and_(f.quantity() <= 11.0).and_(f.ship_mode().is_in(["AIR", "MAIL"]))
                .or_(f.part().brand().eq("Brand#24").and_(f.quantity() >= 5.0).and_(f.quantity() <= 20.0)),
            ),
            expected_columns=["revenue"],
            expected_result=_rows([_O1L1 + _O1L2 + _O2L2]),
        ),
        # --- A milestoned navigation used only inside a CASE is joined at the query's dates ---
        TestExpectation(
            name="H12_shipping_modes_and_order_priority",
            query=lambda f: f.find_all(
                _H_APPLICATION, _H_SYSTEM,
                [f.ship_mode(),
                 when(f.order().order_priority().eq("2-HIGH")).then(1).otherwise(0).sum().alias("high_line_count")],
                f.ship_mode().is_in(["MAIL", "SHIP"]),
            ).order_by(f.ship_mode().ascending()),
            expected_columns=["Ship Mode", "high_line_count"],
            expected_result=_rows(["MAIL", 1], ["SHIP", 0]),
        ),
        TestExpectation(
            name="H14_promotion_effect",
            query=lambda f: f.find_all(
                "2022-01-15", "2022-06-01",
                [when(f.part().type_().starts_with("PROMO")).then(_revenue(f)).otherwise(0).sum().alias("promo_revenue"),
                 _revenue(f).sum().alias("total_revenue")],
            ),
            expected_columns=["promo_revenue", "total_revenue"],
            expected_result=_rows([
                _discounted(3300.0, 0.06) + _discounted(3600.0, 0.00),
                _discounted(2400.0, 0.02) + _discounted(6300.0, 0.04) + _discounted(3300.0, 0.06)
                + _discounted(3600.0, 0.00),
            ]),
        ),
        _unsupported(
            "H8_national_market_share",
            "Divides one CASE sum by another next to a date extract. That expression is not recognised as an aggregate, so "
            "the extract is not grouped, and group_by does not accept an extract",
            ["Year Ship Date", "mkt_share"],
        ),
        # --- The rest of the 22 ---
        _unsupported("H2_minimum_cost_supplier", _SKIP_SUBQUERY + " (the minimum cost per part)", ["Supplier Acct Bal"]),
        _unsupported(
            "H9_product_type_profit_measure",
            "Joins LINEITEM to PARTSUPP on a composite key (partkey and suppkey); an association joins on one column",
            ["Nation Name", "Year Ship Date", "sum_profit"],
        ),
        _unsupported("H11_important_stock_identification", _SKIP_SUBQUERY + " (HAVING against a total)", ["Part Key"]),
        _unsupported(
            "H13_customer_distribution",
            "Groups the result of a grouped query (the number of orders per customer, then customers per count)",
            ["c_count", "custdist"],
        ),
        _unsupported(
            "H15_top_supplier",
            "Selects the supplier whose revenue equals the maximum revenue over all suppliers (a view and a subquery)",
            ["Supplier Key", "total_revenue"],
        ),
        _unsupported(
            "H16_parts_supplier_relationship",
            "Needs COUNT(DISTINCT) of suppliers and NOT IN a subquery",
            ["Brand", "Type", "Size", "supplier_cnt"],
        ),
        _unsupported("H17_small_quantity_order_revenue", _SKIP_SUBQUERY + " (the average quantity per part)", ["avg_yearly"]),
        _unsupported("H18_large_volume_customer", _SKIP_SUBQUERY + " (IN a grouped subquery)", ["Customer Name"]),
        _unsupported("H20_potential_part_promotion", _SKIP_SUBQUERY + " (nested IN and a correlated sum)", ["Supplier Name"]),
        _unsupported("H21_suppliers_who_kept_orders_waiting", _SKIP_SUBQUERY + " (EXISTS and NOT EXISTS on other suppliers)", ["Supplier Name", "numwait"]),
        _unsupported("H22_global_sales_opportunity", _SKIP_SUBQUERY + " (an average and NOT EXISTS), and the model has no phone", ["cntrycode", "numcust"]),
        TestExpectation(
            name="H5_local_supplier_volume",
            query=lambda f: f.find_all(
                "2022-01-15", "2022-06-01", [f.supplier().nation().name(), _revenue(f).sum().alias("revenue")],
                f.supplier().nation().region().name().eq("AMERICA")
                .and_(f.supplier().nation().nation_key() == f.order().customer().nation().nation_key())
                .and_(f.order().order_date() >= _D(2020, 1, 1)),
            ),
            expected_columns=["Supplier Nation Name", "revenue"],
            expected_result=_rows(["UNITED STATES", _discounted(6300.0, 0.04)]),
        ),
        # --- R2, R3 ---
        TestExpectation(
            name="R2_state_durations_of_line_items",
            query=lambda f: f.find_all(
                _H_APPLICATION, _H_SYSTEM,
                [f.order().order_key(), f.line_number(), f.ship_date().diff_days(f.receipt_date()).alias("duration")],
            ).order_by(f.order().order_key().ascending(), f.line_number().ascending()),
            expected_columns=["Order Order Key", "Line Number", "duration"],
            expected_result=_rows([1, 1, 15], [1, 2, 16], [2, 1, 29], [2, 2, 28]),
        ),
        _unsupported(
            "R3a_temporal_aggregation_greatest_number_of_unshipped_items",
            "Temporal aggregation groups by the points where the data changes (an interval join on the period "
            "boundaries), then takes the maximum over them",
            ["Max Count"],
        ),
    ],
)

ORDER_PRIORITY_FINDER_SPECS = FinderSpec(
    finder_name="OrderFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        TestExpectation(
            # TPC-H Q4 counts orders that have a line committed before it was received. The condition is on the line, so
            # it is a filter through the navigation (exists() takes no condition), which repeats an order once per
            # such line; counting the distinct orders counts it once.
            name="H4_order_priority_checking",
            query=lambda f: f.find_all(
                _H_APPLICATION, _H_SYSTEM,
                [f.order_priority(), f.order_key().count_distinct().alias("order_count")],
                f.line_items().commit_date() < f.line_items().receipt_date(),
            ).order_by(f.order_priority().ascending()),
            expected_columns=["Order Priority", "order_count"],
            expected_result=_rows(["1-URGENT", 1], ["2-HIGH", 1]),
        ),
        # --- exists() on a to-many association is one row per order, not one per line ---
        TestExpectation(
            name="exists_to_many_returns_each_order_once",
            query=lambda f: f.find_all(_H_APPLICATION, _H_SYSTEM, [f.order_key()], f.line_items().exists())
            .order_by(f.order_key().ascending()),
            expected_columns=["Order Key"],
            expected_result=_rows([1], [2]),
        ),
        TestExpectation(
            # Order 5's lines were cancelled at S3: a navigation given a later processing time no longer finds them,
            # whatever the query's own dates are.
            name="exists_to_many_uses_the_navigations_own_processing_time",
            query=lambda f: f.find_all(
                "2022-06-01", "2022-06-01", [f.order_key()], f.line_items(processing_time=_NOW).exists()
            ).order_by(f.order_key().ascending()),
            expected_columns=["Order Key"],
            expected_result=_rows([3], [4]),
        ),
        TestExpectation(
            name="exists_to_many_over_a_business_date_range",
            skip_backends={"graphql": "A sort across a date range cannot be pushed: each date is a separate query"},
            query=lambda f: f.find_for_date_range(
                "2019-12-01", "2019-12-31", _H_SYSTEM, [f.order_key()], f.line_items().exists()
            ).order_by(f.order_key().ascending()),
            expected_columns=["Order Key"],
            expected_result=_rows([1], [2], [3]),
        ),
        TestExpectation(
            name="exists_to_many_counts_each_order_once",
            query=lambda f: f.find_all(
                _H_APPLICATION, _H_SYSTEM, [f.order_priority(), count_all().alias("order_count")],
                f.line_items().exists(),
            ).order_by(f.order_priority().ascending()),
            expected_columns=["Order Priority", "order_count"],
            expected_result=_rows(["1-URGENT", 1], ["2-HIGH", 1]),
        ),
    ],
)

CUSTOMER_REVENUE_FINDER_SPECS = FinderSpec(
    finder_name="CustomerFinder",
    mapping_file=TPCH_BIH_MAPPING,
    expectations=[
        TestExpectation(
            name="H10_returned_item_reporting",
            query=lambda f: f.find_all(
                _H_APPLICATION, _H_SYSTEM,
                [f.customer_key(), f.name(),
                 (f.orders().line_items().extended_price() * (constant(1) - f.orders().line_items().discount()))
                 .sum().alias("revenue"),
                 f.acct_bal(), f.nation().name()],
                f.orders().line_items().return_flag().eq("R").and_(f.orders().order_date() >= _D(2019, 10, 1)),
            ).group_by(f.customer_key(), f.name(), f.acct_bal(), f.nation().name()),
            expected_columns=["Customer Key", "Name", "revenue", "Acct Bal", "Nation Name"],
            expected_result=_rows([1, "Customer#1", _O1L1, 100.0, "UNITED STATES"]),
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
                "2022-06-01", "2022-06-01",
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
            query=lambda f: f.find_all("2022-03-01", _NOW, [f.name()], f.name().eq("Part#4")),
            expected_columns=["Name"],
            expected_result=np.empty((0, 1), dtype=object),
        ),
        TestExpectation(
            name="availability_delayed_available_from_the_new_date",
            query=lambda f: f.find_all("2022-07-01", _NOW, [f.name()], f.name().eq("Part#4")),
            expected_columns=["Name"],
            expected_result=_rows(["Part#4"]),
        ),
        TestExpectation(
            name="availability_delayed_as_recorded_before_the_delay",
            query=lambda f: f.find_all("2022-03-01", "2021-06-01", [f.name()], f.name().eq("Part#4")),
            expected_columns=["Name"],
            expected_result=_rows(["Part#4"]),
        ),
        # Part 2's price changes at S1, effective 2021-03-01.
        TestExpectation(
            name="price_before_the_change",
            query=lambda f: f.find_all("2021-02-01", _NOW, [f.retail_price()], f.name().eq("Part#2")),
            expected_columns=["Retail Price"],
            expected_result=_rows([1000.0]),
        ),
        TestExpectation(
            name="price_after_the_change",
            query=lambda f: f.find_all("2021-04-01", _NOW, [f.retail_price()], f.name().eq("Part#2")),
            expected_columns=["Retail Price"],
            expected_result=_rows([1050.0]),
        ),
        TestExpectation(
            name="price_after_the_change_as_recorded_before_it",
            query=lambda f: f.find_all("2021-04-01", "2020-06-01", [f.retail_price()], f.name().eq("Part#2")),
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
            query=lambda f: f.find_all("2019-01-01", "2019-01-01", [f.name()], f.region().name().eq("AMERICA")),
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
