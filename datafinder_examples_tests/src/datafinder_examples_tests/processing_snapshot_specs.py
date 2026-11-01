"""
Test specifications for a processing snapshot (processing_snapshot_mapping.md).

A quote is one row for each processing time it was taken at, and a query's processing time must equal that time exactly.

    mkt.quotes    — id INT, symbol VARCHAR, price DOUBLE, taken_at TIMESTAMP
    mkt.positions — id INT, symbol VARCHAR (no milestoning)
"""
import datetime

import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

PROCESSING_SNAPSHOT_MAPPING = "processing_snapshot_mapping.md"

_JAN_1 = datetime.datetime(2024, 1, 1, 9, 0, 0)
_JAN_2 = datetime.datetime(2024, 1, 2, 9, 0, 0)

PROCESSING_SNAPSHOT_QUOTE_FINDER_SPECS = FinderSpec(
    finder_name="QuoteFinder",
    mapping_file=PROCESSING_SNAPSHOT_MAPPING,
    expectations=[
        TestExpectation(
            name="the_rows_taken_at_a_time",
            query=lambda f: f.find_all(None, _JAN_1, [f.symbol(), f.price()]).order_by(f.symbol().ascending()),
            expected_columns=["Symbol", "Price"],
            expected_result=np.array([["AAPL", 150.0], ["MSFT", 300.0]], dtype=object),
        ),
        TestExpectation(
            name="a_later_time_gets_the_rows_taken_then",
            query=lambda f: f.find_all(None, _JAN_2, [f.symbol(), f.price()]),
            expected_columns=["Symbol", "Price"],
            expected_result=np.array([["AAPL", 152.0]], dtype=object),
        ),
        TestExpectation(
            name="a_time_between_snapshots_finds_nothing",
            query=lambda f: f.find_all(None, datetime.datetime(2024, 1, 1, 10, 0, 0), [f.symbol()]),
            expected_columns=["Symbol"],
            expected_result=np.empty((0, 1), dtype=object),
        ),
        TestExpectation(
            name="the_time_is_compared_whole_so_a_date_alone_is_midnight",
            query=lambda f: f.find_all(None, "2024-01-01", [f.symbol()]),
            expected_columns=["Symbol"],
            expected_result=np.empty((0, 1), dtype=object),
        ),
        TestExpectation(
            name="every_row_taken_in_a_range_of_times",
            query=lambda f: f.find_for_processing_range(
                datetime.datetime(2024, 1, 1, 9, 0, 0), datetime.datetime(2024, 1, 2, 9, 0, 0), None,
                [f.symbol(), f.price()]).order_by(f.symbol().ascending(), f.price().ascending()),
            expected_columns=["Symbol", "Price"],
            expected_result=np.array([["AAPL", 150.0], ["AAPL", 152.0], ["MSFT", 300.0]], dtype=object),
        ),
        TestExpectation(
            name="a_range_leaves_out_rows_taken_after_it",
            query=lambda f: f.find_for_processing_range(
                datetime.datetime(2024, 1, 1, 0, 0, 0), datetime.datetime(2024, 1, 1, 23, 59, 59), None,
                [f.symbol()]).order_by(f.symbol().ascending()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"], ["MSFT"]], dtype=object),
        ),
    ],
)

PROCESSING_SNAPSHOT_POSITION_FINDER_SPECS = FinderSpec(
    finder_name="PositionFinder",
    mapping_file=PROCESSING_SNAPSHOT_MAPPING,
    expectations=[
        TestExpectation(
            name="the_query_time_reaches_the_quotes_a_position_joins",
            query=lambda f: f.find_all(None, _JAN_1, [f.id_(), f.quotes().price()]).order_by(f.id_().ascending()),
            expected_columns=["Id", "Quotes Price"],
            expected_result=np.array([[1, 150.0], [2, 300.0]], dtype=object),
        ),
        TestExpectation(
            name="a_navigation_can_be_given_its_own_time",
            query=lambda f: f.find_all(None, None, [f.id_(), f.quotes(processing_time=_JAN_2).price()])
                             .order_by(f.id_().ascending()),
            expected_columns=["Id", "Quotes Price"],
            expected_result=np.array([[1, 152.0], [2, None]], dtype=object),
        ),
    ],
)
