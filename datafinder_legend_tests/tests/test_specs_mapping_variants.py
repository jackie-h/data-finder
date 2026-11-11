"""
Runs the specs of the mappings that each have a few tests (bitemporal types on tables milestoned less than the types
are, joins with conditions and chains) against the generated finders: the SQL they generate is compared with Legend's, and the rows they return with the rows Legend's SQL returns on
Legend's test data.
"""
import importlib

import pytest

from datafinder_legend_tests import legend_path
from datafinder_legend_tests.data import seeded_database
from datafinder_legend_tests.finders import generated_finders
from datafinder_legend_tests.gaps import checkable, gap_is_only
from datafinder_legend_tests.legend import BACKEND, discover_specs
from datafinder_legend_tests.results import assert_same_rows

# mapping -> the data it runs on
_VARIANTS = {
    "milestoning/tests/bitemporal_none_mapping.md": "milestoning/tests/bitemporal_none_data.sql",
    "milestoning/tests/bitemporal_single_mapping.md": "milestoning/tests/business_date_data.sql",
    "tests/mapping/join/advanced_join_mapping.md": "tests/mapping/join/advanced_join_data.sql",
    "tests/mapping/join/chained_joins_mapping.md": "tests/mapping/join/advanced_join_data.sql",
}
_ENTRIES = [(mapping, spec, e) for mapping in _VARIANTS for spec in discover_specs(mapping) for e in spec.expectations]
_CASES = [pytest.param(mapping, spec, e, id=f"{spec.finder_name}-{e.name}") for mapping, spec, e in _ENTRIES]


def _finder(mapping: str, finder_name: str):
    """``BiTemporalProductFinder`` from ``bitemporalproduct_finder``, which the generator wrote."""
    module = importlib.import_module(finder_name.removesuffix("Finder").lower() + "_finder")
    return getattr(module, finder_name)()


@pytest.mark.parametrize("mapping,spec,expectation", _CASES)
def test_mapping_variant(mapping, spec, expectation):
    with generated_finders(legend_path(mapping)):
        expectation.run(_finder(mapping, spec.finder_name), backend=BACKEND)


@pytest.mark.parametrize("mapping,spec,expectation", _CASES)
def test_mapping_variant_rows(mapping, spec, expectation):
    with generated_finders(legend_path(mapping)), seeded_database(legend_path(_VARIANTS[mapping])) as database:
        assert_same_rows(database, _finder(mapping, spec.finder_name), expectation)


_GAP_CASES = [pytest.param(mapping, spec, e, id=f"{spec.finder_name}-{e.name}") for mapping, spec, e in _ENTRIES
              if checkable(e.known_gaps.get(BACKEND, ""))]


@pytest.mark.parametrize("mapping,spec,expectation", _GAP_CASES)
def test_known_sql_gap_is_only_what_it_says(mapping, spec, expectation):
    with generated_finders(legend_path(mapping)):
        actual = expectation.query(_finder(mapping, spec.finder_name)).to_sql()
    assert gap_is_only(expectation.known_gaps[BACKEND], actual, expectation.equivalent_sql[BACKEND])
