"""
Runs the specs ported from legend-engine against the simple Person/Firm/Address/Location model
(tests/simple_mapping.md): the SQL the finders generate is compared with Legend's, and the rows they return are
compared with the rows Legend's SQL returns on Legend's test data.
"""
import pytest

from datafinder_legend_tests import legend_path
from datafinder_legend_tests.data import seeded_database
from datafinder_legend_tests.finders import generated_finders
from datafinder_legend_tests.gaps import checkable, gap_is_only
from datafinder_legend_tests.legend import BACKEND, discover_specs
from datafinder_legend_tests.results import assert_same_rows

_SPECS = discover_specs("tests/simple_mapping.md")
_CASES = [pytest.param(spec, e, id=f"{spec.finder_name}-{e.source.split('/')[-1]}") for spec in _SPECS
          for e in spec.expectations]


@pytest.fixture(scope="module")
def finders():
    with generated_finders(legend_path("tests/simple_mapping.md")):
        from person_finder import PersonFinder  # type: ignore[import]
        from firm_finder import FirmFinder  # type: ignore[import]
        from trade_finder import TradeFinder  # type: ignore[import]
        from account_finder import AccountFinder  # type: ignore[import]
        yield {"PersonFinder": PersonFinder(), "FirmFinder": FirmFinder(), "TradeFinder": TradeFinder(),
               "AccountFinder": AccountFinder()}


@pytest.fixture(scope="module")
def database():
    with seeded_database(legend_path("tests/simple_data.sql")) as path:
        yield path


@pytest.mark.parametrize("spec,expectation", _CASES)
def test_sql(spec, expectation, finders):
    expectation.run(finders[spec.finder_name], backend=BACKEND)


@pytest.mark.parametrize("spec,expectation", _CASES)
def test_rows(spec, expectation, finders, database):
    assert_same_rows(database, finders[spec.finder_name], expectation)


_GAP_CASES = [pytest.param(spec, e, id=f"{spec.finder_name}-{e.source.split('/')[-1]}") for spec in _SPECS
              for e in spec.expectations if checkable(e.known_gaps.get(BACKEND, ""))]


@pytest.mark.parametrize("spec,expectation", _GAP_CASES)
def test_known_sql_gap_is_only_what_it_says(spec, expectation, finders):
    actual = expectation.query(finders[spec.finder_name]).to_sql()
    assert gap_is_only(expectation.known_gaps[BACKEND], actual, expectation.equivalent_sql[BACKEND]), (
        f"[{expectation.name}] differs from Legend by more than: {expectation.known_gaps[BACKEND]}\n"
        f"Actual: {actual}\nExpected: {expectation.equivalent_sql[BACKEND]}")
