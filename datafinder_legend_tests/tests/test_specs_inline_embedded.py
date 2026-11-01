"""
Runs the specs ported from legend-engine's inline embedded mapping tests (``testInlineEmbedded*.pure``) against the generated finders: the SQL they
generate is compared with Legend's, and the rows they return are compared with the rows Legend's SQL returns on
Legend's test data.
"""
import importlib
from contextlib import ExitStack

import pytest

from datafinder_legend_tests import legend_path
from datafinder_legend_tests.data import seeded_database
from datafinder_legend_tests.finders import generated_finders
from datafinder_legend_tests.gaps import checkable, gap_is_only
from datafinder_legend_tests.legend import BACKEND, discover_specs
from datafinder_legend_tests.results import assert_same_rows

# each mapping with the data its specs run on: both read the one denormalised table
_DATA = "tests/mapping/embedded/inline_data.sql"
_MAPPINGS = {
    "tests/mapping/embedded/inline_product_mapping.md": _DATA,
    "tests/mapping/embedded/inline_bond_detail_mapping.md": _DATA,
}
_SPECS = [spec for mapping in _MAPPINGS for spec in discover_specs(mapping)]
_CASES = [pytest.param(spec, e, id=f"{spec.finder_name}-{e.name}") for spec in _SPECS for e in spec.expectations]


@pytest.fixture(scope="module")
def finders():
    with ExitStack() as stack:
        found = {}
        for mapping in _MAPPINGS:
            stack.enter_context(generated_finders(legend_path(mapping)))
            for name in {spec.finder_name for spec in discover_specs(mapping)}:
                module = importlib.import_module(name.removesuffix("Finder").lower() + "_finder")
                found[name] = getattr(module, name)()
        yield found


@pytest.fixture(scope="module")
def database():
    with seeded_database(legend_path(_DATA)) as path:
        yield path


@pytest.mark.parametrize("spec,expectation", _CASES)
def test_sql(spec, expectation, finders):
    expectation.run(finders[spec.finder_name], backend=BACKEND)


@pytest.mark.parametrize("spec,expectation", _CASES)
def test_rows(spec, expectation, finders, database):
    assert_same_rows(database, finders[spec.finder_name], expectation)


_GAP_CASES = [pytest.param(spec, e, id=f"{spec.finder_name}-{e.name}") for spec in _SPECS for e in spec.expectations
              if checkable(e.known_gaps.get(BACKEND, ""))]


@pytest.mark.parametrize("spec,expectation", _GAP_CASES)
def test_known_sql_gap_is_only_what_it_says(spec, expectation, finders):
    actual = expectation.query(finders[spec.finder_name]).to_sql()
    assert gap_is_only(expectation.known_gaps[BACKEND], actual, expectation.equivalent_sql[BACKEND])
