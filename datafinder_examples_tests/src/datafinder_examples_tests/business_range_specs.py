"""
Test specifications for a business date range with no processing time (business_range_mapping.md).

A row applies from ``from_z`` up to, but not including, ``thru_z``. A NULL ``thru_z`` is open-ended.

    mkt.rates — code VARCHAR, amount DOUBLE, from_z DATE, thru_z DATE
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

BUSINESS_RANGE_MAPPING = "business_range_mapping.md"

BUSINESS_RANGE_RATE_FINDER_SPECS = FinderSpec(
    finder_name="RateFinder",
    mapping_file=BUSINESS_RANGE_MAPPING,
    expectations=[
        TestExpectation(
            name="the_version_in_range_on_a_date",
            query=lambda f: f.find_all("2020-06-01", None, [f.code(), f.amount()]).order_by(f.code().ascending()),
            expected_columns=["Code", "Amount"],
            expected_result=np.array([["AAA", 1.5], ["BBB", 2.0]], dtype=object),
        ),
        TestExpectation(
            name="a_later_date_gets_the_later_version",
            query=lambda f: f.find_all("2021-06-01", None, [f.code(), f.amount()]).order_by(f.code().ascending()),
            expected_columns=["Code", "Amount"],
            expected_result=np.array([["AAA", 1.7], ["BBB", 2.0]], dtype=object),
        ),
        TestExpectation(
            name="the_end_date_is_not_in_the_range",
            query=lambda f: f.find_all("2021-01-01", None, [f.code(), f.amount()], f.code().eq("AAA")),
            expected_columns=["Code", "Amount"],
            expected_result=np.array([["AAA", 1.7]], dtype=object),
        ),
        TestExpectation(
            name="a_date_before_every_row_finds_nothing",
            query=lambda f: f.find_all("2019-01-01", None, [f.code()]),
            expected_columns=["Code"],
            expected_result=np.empty((0, 1), dtype=object),
        ),
        TestExpectation(
            name="a_null_end_is_open_ended",
            query=lambda f: f.find_all("2099-01-01", None, [f.code(), f.amount()]),
            expected_columns=["Code", "Amount"],
            expected_result=np.array([["AAA", 1.7]], dtype=object),
        ),
        TestExpectation(
            name="every_version_overlapping_a_range",
            query=lambda f: f.find_for_date_range("2020-12-01", "2021-02-01", None, [f.code(), f.amount()])
                             .order_by(f.code().ascending(), f.amount().ascending()),
            expected_columns=["Code", "Amount"],
            expected_result=np.array([["AAA", 1.5], ["AAA", 1.7], ["BBB", 2.0]], dtype=object),
        ),
    ],
)
