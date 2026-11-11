"""
Test specifications for a business date range that ends at a date with no time (business_range_infinity_mapping.md).

A row that has not ended holds 9999-12-31 in ``thru_z``, which the scheme gives as ``business_infinite_datetime`` alone, so
a query accepts no NULL end.

    ranged.rates — code VARCHAR, amount DOUBLE, from_z DATE, thru_z DATE
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

BUSINESS_RANGE_INFINITY_MAPPING = "business_range_infinity_mapping.md"

BUSINESS_RANGE_INFINITY_RATE_FINDER_SPECS = FinderSpec(
    finder_name="RateFinder",
    mapping_file=BUSINESS_RANGE_INFINITY_MAPPING,
    expectations=[
        TestExpectation(
            name="a_date_infinity_accepts_no_null_end",
            query=lambda f: f.find_all("2021-06-01", None, [f.code(), f.amount()]).order_by(f.code().ascending()),
            expected_columns=["Code", "Amount"],
            expected_result=np.array([["AAA", 1.7], ["BBB", 2.0]], dtype=object),
            expected_sql={"duckdb": "t0.from_z <= '2021-06-01' AND t0.thru_z > '2021-06-01' ORDER BY"},
        ),
        TestExpectation(
            name="a_row_that_ends_at_the_date_infinity_applies_to_a_far_later_date",
            query=lambda f: f.find_all("2999-01-01", None, [f.code(), f.amount()]).order_by(f.code().ascending()),
            expected_columns=["Code", "Amount"],
            expected_result=np.array([["AAA", 1.7]], dtype=object),
        ),
    ],
)
