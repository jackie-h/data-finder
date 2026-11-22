"""
Test specifications for a business date range with no processing time (business_range_mapping.md).

A row applies from ``from_z`` up to, but not including, ``thru_z``. A NULL ``thru_z`` is open-ended.

    mkt.rates — code VARCHAR, amount DOUBLE, from_z DATE, thru_z DATE
"""
import datetime


from datafinder_examples_tests.spec import FinderSpec, TestExpectation

BUSINESS_RANGE_MAPPING = "business_range_mapping.md"

BUSINESS_RANGE_RATE_FINDER_SPECS = FinderSpec(
    finder_name="RateFinder",
    mapping_file=BUSINESS_RANGE_MAPPING,
    expectations=[
        TestExpectation(
            name="the_version_in_range_on_a_date",
            query=lambda f: f.find_all("2020-06-01", None, [f.code(), f.amount()]).order_by(f.code().ascending()),
        ),
        TestExpectation(
            name="a_later_date_gets_the_later_version",
            query=lambda f: f.find_all("2021-06-01", None, [f.code(), f.amount()]).order_by(f.code().ascending()),
        ),
        TestExpectation(
            name="the_end_date_is_not_in_the_range",
            query=lambda f: f.find_all("2021-01-01", None, [f.code(), f.amount()], f.code().eq("AAA")),
        ),
        TestExpectation(
            name="a_date_before_every_row_finds_nothing",
            query=lambda f: f.find_all("2019-01-01", None, [f.code()]),
        ),
        TestExpectation(
            name="a_null_end_is_open_ended",
            query=lambda f: f.find_all("2099-01-01", None, [f.code(), f.amount()]),
        ),
        TestExpectation(
            name="every_version_overlapping_a_range",
            query=lambda f: f.find_for_date_range("2020-12-01", "2021-02-01", None, [f.code(), f.amount()])
                             .order_by(f.code().ascending(), f.amount().ascending()),
        ),
        # the milestoning columns are properties the mapping gives (the model does not declare them), and they read the column
        TestExpectation(
            name="the_range_of_a_version_is_read_from_its_columns",
            query=lambda f: f.find_for_date_range("2019-01-01", "2030-01-01", None, [f.code(), f.amount(), f.business_from(), f.business_thru()])
                             .order_by(f.code().ascending(), f.amount().ascending()),
        ),
        TestExpectation(
            name="the_range_can_be_filtered_on",
            query=lambda f: f.find_for_date_range("2019-01-01", "2030-01-01", None, [f.code(), f.amount()], f.business_thru() > datetime.date(2021, 6, 1)),
        ),
    ],
)
