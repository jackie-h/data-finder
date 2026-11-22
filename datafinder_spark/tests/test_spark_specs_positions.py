"""Runs POSITION_DATE_FINDER_SPECS (date extract, arithmetic, diff) against Spark."""
import pytest

from datafinder_examples_tests.positions_specs import POSITION_DATE_FINDER_SPECS
from datafinder_examples_tests.suite_data import tables
from spark_harness import finder_of


@pytest.fixture(scope="module")
def position_finder(spark):
    with finder_of(spark, POSITION_DATE_FINDER_SPECS.mapping_file, tables("positions"), "position_finder", "PositionFinder") as finder:
        yield finder


@pytest.mark.parametrize("expectation", [pytest.param(each, id=each.name) for each in POSITION_DATE_FINDER_SPECS.expectations])
def test_position_date(expectation, position_finder):
    expectation.run(position_finder, backend="spark")
