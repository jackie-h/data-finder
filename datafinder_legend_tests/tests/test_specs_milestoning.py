"""
Runs the specs ported from legend-engine's ``milestoning/tests`` against the generated finders: the SQL they
generate is compared with Legend's, and the rows they return are compared with the rows Legend's SQL returns on
Legend's test data.
"""
import pytest

from datafinder_legend_tests import legend_path
from datafinder_legend_tests.data import seeded_database
from datafinder_legend_tests.finders import generated_finders
from datafinder_legend_tests.legend import discover_specs
from datafinder_legend_tests.results import check_ported

_SPECS = discover_specs("milestoning/tests/milestoning_mapping.md")
_CASES = [pytest.param(spec, e, id=f"{spec.finder_name}-{e.name}") for spec in _SPECS for e in spec.expectations]


@pytest.fixture(scope="module")
def finders():
    with generated_finders(legend_path("milestoning/tests/milestoning_mapping.md")):
        from bitemporalproduct_finder import BiTemporalProductFinder  # pyright: ignore[reportMissingImports]
        from order_finder import OrderFinder  # pyright: ignore[reportMissingImports]
        from product_finder import ProductFinder  # pyright: ignore[reportMissingImports]
        from trader_finder import TraderFinder  # pyright: ignore[reportMissingImports]
        yield {"BiTemporalProductFinder": BiTemporalProductFinder(), "OrderFinder": OrderFinder(),
               "ProductFinder": ProductFinder(), "TraderFinder": TraderFinder()}


@pytest.fixture(scope="module")
def database():
    with seeded_database(legend_path("milestoning/tests/milestoning_data.sql")) as path:
        yield path


@pytest.mark.parametrize("spec,expectation", _CASES)
def test_milestoning(spec, expectation, finders, database):
    check_ported(database, finders[spec.finder_name], expectation)
