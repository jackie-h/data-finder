"""
Runs the specs ported from legend-engine against the simple Person/Firm/Address/Location model
(tests/simple_mapping.md): the SQL the finders generate is compared with Legend's, and the rows they return are
compared with the rows Legend's SQL returns on Legend's test data.
"""
import importlib

import pytest

from datafinder_legend_tests import legend_path
from datafinder_legend_tests.data import seeded_database
from datafinder_legend_tests.finders import generated_finders
from datafinder_legend_tests.legend import discover_specs
from datafinder_legend_tests.results import check_ported

_SPECS = discover_specs("tests/simple_mapping.md")
_CASES = [pytest.param(spec, e, id=f"{spec.finder_name}-{e.source.split('/')[-1]}") for spec in _SPECS
          for e in spec.expectations]


@pytest.fixture(scope="module")
def finders():
    with generated_finders(legend_path("tests/simple_mapping.md")):
        found = {}
        for name in {spec.finder_name for spec in _SPECS}:
            module = importlib.import_module(name.removesuffix("Finder").lower() + "_finder")
            found[name] = getattr(module, name)()
        yield found


@pytest.fixture(scope="module")
def database():
    with seeded_database(legend_path("tests/simple_data.sql")) as path:
        yield path


@pytest.mark.parametrize("spec,expectation", _CASES)
def test_simple_model(spec, expectation, finders, database):
    check_ported(database, finders[spec.finder_name], expectation)
