"""
Runs the SQL specs ported from legend-engine's ``functions/tests/projection`` against the generated finders.
No database is involved: only the SQL the finders generate is checked.
"""
import pytest

from datafinder_legend_tests import legend_path
from datafinder_legend_tests.finders import generated_finders
from datafinder_legend_tests.legend import BACKEND

from datafinder_legend_tests.functions.tests.projection.simple_specs import SIMPLE_PROJECTION_SPECS
from datafinder_legend_tests.functions.tests.projection.association_to_one_specs import ASSOCIATION_TO_ONE_SPECS
from datafinder_legend_tests.functions.tests.projection.association_to_many_specs import (
    ASSOCIATION_TO_MANY_PERSON_SPECS, ASSOCIATION_TO_MANY_FIRM_SPECS,
)
from datafinder_legend_tests.functions.tests.projection.filters_specs import FILTERS_PERSON_SPECS
from datafinder_legend_tests.functions.tests.projection.in_specs import IN_PERSON_SPECS

_SPECS = [SIMPLE_PROJECTION_SPECS, ASSOCIATION_TO_ONE_SPECS, ASSOCIATION_TO_MANY_PERSON_SPECS,
          ASSOCIATION_TO_MANY_FIRM_SPECS, FILTERS_PERSON_SPECS, IN_PERSON_SPECS]
_CASES = [pytest.param(spec, e, id=f"{spec.finder_name}-{e.name}") for spec in _SPECS for e in spec.expectations]


@pytest.fixture(scope="module")
def finders():
    with generated_finders(legend_path("tests/simple_mapping.md")):
        from person_finder import PersonFinder  # type: ignore[import]
        from firm_finder import FirmFinder  # type: ignore[import]
        yield {"PersonFinder": PersonFinder(), "FirmFinder": FirmFinder()}


@pytest.mark.parametrize("spec,expectation", _CASES)
def test_functions_projection(spec, expectation, finders):
    expectation.run(finders[spec.finder_name], backend=BACKEND)
