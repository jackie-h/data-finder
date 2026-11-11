"""
Test specifications for a bitemporal table whose axes end a row that has not ended in different forms
(bitemporal_dates_mapping.md): a datetime, 9999-12-31 00:00:00, on the processing axis and a date with no time,
9999-12-31, on the business axis. Both axes have an infinity, so a query accepts no NULL end on either.

    dated.quotes — code VARCHAR, amount DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP, from_z DATE, thru_z DATE
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

BITEMPORAL_DATES_MAPPING = "bitemporal_dates_mapping.md"

BITEMPORAL_DATES_QUOTE_FINDER_SPECS = FinderSpec(
    finder_name="QuoteFinder",
    mapping_file=BITEMPORAL_DATES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_date_infinity_on_one_axis_and_a_datetime_on_the_other_accept_no_null_end",
            query=lambda f: f.find_all("2021-06-01", "2021-06-01 00:00:00", [f.code(), f.amount()])
                             .order_by(f.code().ascending()),
            expected_columns=["Code", "Amount"],
            expected_result=np.array([["AAA", 1.1], ["BBB", 2.0], ["CCC", 3.0]], dtype=object),
            expected_sql={"duckdb": "t0.out_z > '2021-06-01 00:00:00' AND t0.from_z <= '2021-06-01' AND "
                                    "t0.thru_z > '2021-06-01' ORDER BY"},
        ),
        TestExpectation(
            name="a_row_that_ended_on_the_business_axis_is_not_found_later",
            query=lambda f: f.find_all("2023-01-01", "2021-06-01 00:00:00", [f.code(), f.amount()])
                             .order_by(f.code().ascending()),
            expected_columns=["Code", "Amount"],
            # BBB's business range ended in 2022; the others end at the date infinity
            expected_result=np.array([["AAA", 1.1], ["CCC", 3.0]], dtype=object),
        ),
        TestExpectation(
            name="a_processing_time_before_a_row_was_replaced_gets_the_earlier_version",
            query=lambda f: f.find_all("2021-06-01", "2020-06-01 00:00:00", [f.code(), f.amount()])
                             .order_by(f.code().ascending()),
            expected_columns=["Code", "Amount"],
            expected_result=np.array([["AAA", 1.0], ["BBB", 2.0], ["CCC", 3.0]], dtype=object),
        ),
    ],
)
