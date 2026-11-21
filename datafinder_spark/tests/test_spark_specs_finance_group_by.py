"""Runs TRADE_GROUP_BY_FINDER_SPECS (group_by, aggregates, percentiles, joins of results) against Spark."""
import pytest

from datafinder_examples_tests.finance_group_by_specs import TRADE_GROUP_BY_FINDER_SPECS
from datafinder_examples_tests.tables import FINANCE_GROUP_BY
from spark_harness import expecting, finder_of


@pytest.fixture(scope="module")
def trade_group_by_finder(spark):
    with finder_of(spark, TRADE_GROUP_BY_FINDER_SPECS.mapping_file, FINANCE_GROUP_BY, "finance.trade.trade_finder", "TradeFinder") as finder:
        yield finder


@pytest.mark.parametrize("expectation", expecting(TRADE_GROUP_BY_FINDER_SPECS.expectations, {}))
def test_trade_group_by(expectation, trade_group_by_finder):
    expectation.run(trade_group_by_finder, backend="spark")
