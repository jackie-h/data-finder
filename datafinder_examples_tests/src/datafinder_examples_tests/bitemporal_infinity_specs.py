"""
Test specifications for a bitemporal table whose two axes end a row that has not ended differently
(bitemporal_infinity_mapping.md): the processing end holds the infinity 9999-12-31 00:00:00, and the business end is NULL.

A query accepts a NULL end only on the axis that has no infinity.

    mkt.quotes — code VARCHAR, amount DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP, from_z DATE, thru_z DATE (NULL when open)
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

BITEMPORAL_INFINITY_MAPPING = "bitemporal_infinity_mapping.md"

BITEMPORAL_INFINITY_QUOTE_FINDER_SPECS = FinderSpec(
    finder_name="QuoteFinder",
    mapping_file=BITEMPORAL_INFINITY_MAPPING,
    expectations=[
        TestExpectation(
            name="only_the_axis_with_no_infinity_accepts_a_null_end",
            query=lambda f: f.find_all("2021-06-01", "2021-06-01 00:00:00", [f.code(), f.amount()])
                             .order_by(f.code().ascending()),
            expected_columns=["Code", "Amount"],
            expected_result=np.array([["AAA", 1.1], ["BBB", 2.0], ["CCC", 3.0]], dtype=object),
            expected_sql={"duckdb": "t0.out_z > '2021-06-01 00:00:00' AND t0.from_z <= '2021-06-01' AND "
                                    "(t0.thru_z > '2021-06-01' OR t0.thru_z IS NULL)"},
        ),
        TestExpectation(
            name="a_row_with_a_null_business_end_applies_to_any_later_date",
            query=lambda f: f.find_all("2023-01-01", "2021-06-01 00:00:00", [f.code(), f.amount()])
                             .order_by(f.code().ascending()),
            expected_columns=["Code", "Amount"],
            # BBB's business range ended in 2022
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
