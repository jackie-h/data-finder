"""Runs POSITION_DATE_FINDER_SPECS (date extract, arithmetic, diff) against Spark."""
import pytest

from datafinder_examples_tests.positions_specs import POSITION_DATE_FINDER_SPECS
from datafinder_examples_tests.tables import POSITIONS
from spark_harness import DATE_MINUS_DATE, DATE_PLUS_INTERVAL, ISODOW, expecting, finder_of

_GAPS = {
    **{name: ISODOW for name in (
        "day_of_week", "day_of_week_name", "a_filter_on_the_name_of_the_day", "the_number_of_each_day_of_the_week_is_the_iso_number",
        "the_name_of_each_day_of_the_week", "most_recent_monday", "the_most_recent_friday_of_a_friday_is_that_day",
        "the_previous_friday_of_a_friday_is_a_week_before", "the_previous_sunday", "rows_after_the_previous_monday")},
    **{name: DATE_PLUS_INTERVAL for name in (
        "add_days_ten", "subtract_days_five", "add_months_one", "subtract_months_one", "add_years_one", "subtract_years_one",
        "add_weeks", "subtract_weeks", "add_timedelta_days", "sub_timedelta_days")},
    "diff_days": DATE_MINUS_DATE,
}


@pytest.fixture(scope="module")
def position_finder(spark):
    with finder_of(spark, POSITION_DATE_FINDER_SPECS.mapping_file, POSITIONS, "position_finder", "PositionFinder") as finder:
        yield finder


@pytest.mark.parametrize("expectation", expecting(POSITION_DATE_FINDER_SPECS.expectations, _GAPS))
def test_position_date(expectation, position_finder):
    expectation.run(position_finder, backend="spark")
