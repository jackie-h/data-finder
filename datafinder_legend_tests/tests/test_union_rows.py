"""
The union tests of legend-engine ``tests/mapping/union/testUnion.pure`` that assert the rows and not the SQL, on the mappings in
tests/mapping/union where each union is a query: the rows Legend asserts are the rows the finder returns.
"""
import pytest

from datafinder_legend_tests import legend_path
from datafinder_legend_tests.data import seeded_database
from datafinder_legend_tests.finders import generated_finders

_DATA = "tests/mapping/union/union_data.sql"


def _names(result, column: int = 0) -> list:
    return sorted(row[column] for row in result.to_pandas().itertuples(index=False, name=None))


# the mappings that have a person and a firm: the finders of each, and the tests that run on every one of them
_MAPPINGS = ["union_mapping.md", "union_to_union_mapping.md", "union_join_in_property_mapping.md"]


@pytest.fixture(scope="module", params=_MAPPINGS)
def union_finders(request):
    with generated_finders(legend_path(f"tests/mapping/union/{request.param}")), seeded_database(legend_path(_DATA)):
        from firm_finder import FirmFinder  # type: ignore[import-not-found]  # written by the generator
        from person_finder import PersonFinder  # type: ignore[import-not-found]
        yield PersonFinder(), FirmFinder()


def test_simple_project(union_finders):
    people, _ = union_finders
    rows = _names(people.find_all([people.last_name()]))
    assert rows == sorted(['Anand', 'Scott', 'Taylor', 'Wright', 'Roberts'])


def test_simple_query_from(union_finders):
    people, _ = union_finders
    assert _names(people.find_all([people.last_name()], people.firm().legal_name().eq("Firm A"))) == ['Roberts']
    assert _names(people.find_all([people.last_name()], people.firm().legal_name().eq("Firm X"))) == \
        sorted(['Anand', 'Scott', 'Taylor', 'Wright'])


def test_simple_query_to(union_finders):
    _, firms = union_finders
    assert _names(firms.find_all([firms.legal_name()], firms.employees().last_name().eq("Wright"))) == ['Firm X']
    assert _names(firms.find_all([firms.legal_name()], firms.employees().last_name().eq("Roberts"))) == ['Firm A']


def test_project_through_association(union_finders):
    _, firms = union_finders
    rows = firms.find_all([firms.legal_name(), firms.employees().last_name()]).to_pandas().itertuples(index=False, name=None)
    assert sorted(f"{name} {firm}" for firm, name in rows) == sorted(
        ['Anand Firm X', 'Scott Firm X', 'Taylor Firm X', 'Wright Firm X', 'Roberts Firm A'])
