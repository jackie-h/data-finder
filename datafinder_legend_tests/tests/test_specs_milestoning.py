"""
Runs the SQL specs ported from legend-engine's ``milestoning/tests`` against the generated finders.
No database is involved: only the SQL the finders generate is checked.
"""
import pytest

from datafinder_legend_tests import legend_path
from datafinder_legend_tests.finders import generated_finders
from datafinder_legend_tests.legend import BACKEND

from datafinder_legend_tests.milestoning.tests.bitemporal_specs import BITEMPORAL_PRODUCT_SPECS

_SPECS = [BITEMPORAL_PRODUCT_SPECS]
_CASES = [pytest.param(spec, e, id=f"{spec.finder_name}-{e.name}") for spec in _SPECS for e in spec.expectations]


@pytest.fixture(scope="module")
def finders():
    with generated_finders(legend_path("milestoning/tests/milestoning_mapping.md")):
        from bitemporalproduct_finder import BiTemporalProductFinder  # type: ignore[import]
        yield {"BiTemporalProductFinder": BiTemporalProductFinder()}


@pytest.mark.parametrize("spec,expectation", _CASES)
def test_milestoning(spec, expectation, finders):
    expectation.run(finders[spec.finder_name], backend=BACKEND)
