"""
Runs the specs ported from legend-engine's milestoning tests that read a property of an object embedded in a product's
row (its classification's system) against the generated finder: the SQL it generates is compared with Legend's, and
the rows it returns with the rows Legend's SQL returns on Legend's test data.
"""
import pytest

from datafinder_legend_tests import legend_path
from datafinder_legend_tests.data import seeded_database
from datafinder_legend_tests.finders import generated_finders
from datafinder_legend_tests.legend import discover_specs
from datafinder_legend_tests.results import check_ported

_MAPPING = "milestoning/tests/embedded_system_mapping.md"
_SPECS = discover_specs(_MAPPING)
_CASES = [pytest.param(spec, e, id=f"{spec.finder_name}-{e.name}") for spec in _SPECS for e in spec.expectations]


@pytest.fixture(scope="module")
def finders():
    with generated_finders(legend_path(_MAPPING)):
        from product_finder import ProductFinder  # pyright: ignore[reportMissingImports]
        yield {"ProductFinder": ProductFinder()}


@pytest.fixture(scope="module")
def database():
    with seeded_database(legend_path("milestoning/tests/embedded_system_data.sql")) as path:
        yield path


@pytest.mark.parametrize("spec,expectation", _CASES)
def test_embedded_system(spec, expectation, finders, database):
    check_ported(database, finders[spec.finder_name], expectation)
