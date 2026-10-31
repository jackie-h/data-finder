"""
Runs the specs ported from legend-engine's ``testBusinessSnapshotMilestoning.pure`` against the generated finders: the
SQL they generate is compared with Legend's, and the rows they return are compared with the rows Legend's SQL returns
on Legend's test data.
"""
import pytest

from datafinder_legend_tests import legend_path
from datafinder_legend_tests.data import seeded_database
from datafinder_legend_tests.finders import generated_finders
from datafinder_legend_tests.gaps import checkable, gap_is_only
from datafinder_legend_tests.legend import BACKEND, discover_specs
from datafinder_legend_tests.results import assert_same_rows

_MAPPING = "milestoning/tests/business_snapshot_mapping.md"
_SPECS = discover_specs(_MAPPING)
_CASES = [pytest.param(spec, e, id=f"{spec.finder_name}-{e.name}") for spec in _SPECS for e in spec.expectations]


@pytest.fixture(scope="module")
def finders():
    with generated_finders(legend_path(_MAPPING)):
        from order_finder import OrderFinder  # type: ignore[import]
        from product_finder import ProductFinder  # type: ignore[import]
        yield {"OrderFinder": OrderFinder(), "ProductFinder": ProductFinder()}


@pytest.mark.parametrize("spec,expectation", _CASES)
def test_business_snapshot(spec, expectation, finders):
    expectation.run(finders[spec.finder_name], backend=BACKEND)


@pytest.fixture(scope="module")
def database():
    with seeded_database(legend_path("milestoning/tests/business_snapshot_data.sql")) as path:
        yield path


@pytest.mark.parametrize("spec,expectation", _CASES)
def test_business_snapshot_rows(spec, expectation, finders, database):
    assert_same_rows(database, finders[spec.finder_name], expectation)


_GAP_CASES = [pytest.param(spec, e, id=f"{spec.finder_name}-{e.name}") for spec in _SPECS for e in spec.expectations
              if checkable(e.known_gaps.get(BACKEND, ""))]


@pytest.mark.parametrize("spec,expectation", _GAP_CASES)
def test_known_sql_gap_is_only_what_it_says(spec, expectation, finders):
    actual = expectation.query(finders[spec.finder_name]).to_sql()
    assert gap_is_only(expectation.known_gaps[BACKEND], actual, expectation.equivalent_sql[BACKEND])
