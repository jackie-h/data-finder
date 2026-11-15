"""
The union tests of legend-engine ``tests/mapping/union/testUnion.pure`` whose sets embed an object in their rows (the firm of a person), on the mappings in
tests/mapping/union where each set is a query: the rows Legend asserts are the rows the finder returns. A module of its own, so the finders of another
mapping that are still imported are not the ones read.
"""
import pytest

from datafinder_legend_tests import legend_path
from datafinder_legend_tests.data import seeded_database
from datafinder_legend_tests.finders import generated_finders

_DATA = "tests/mapping/union/union_data.sql"


def _names(result, column: int = 0) -> list:
    return sorted(row[column] for row in result.to_pandas().itertuples(index=False, name=None))


def test_simple_query_from_with_embedded_in_mapping():
    """``unionMappingWithEmbeddedProperty``: the firm of a person is embedded in the row of each set, and a filter on it reads the rows of both."""
    with generated_finders(legend_path("tests/mapping/union/union_embedded_mapping.md")), seeded_database(legend_path(_DATA)):
        from person_finder import PersonFinder  # pyright: ignore[reportMissingImports]  # written by the generator
        people = PersonFinder()
        assert _names(people.find_all([people.last_name()], people.firm().legal_name().eq("Firm A"))) == ["Roberts"]
        assert _names(people.find_all([people.last_name()], people.firm().legal_name().eq("Firm X"))) == sorted(["Anand", "Scott", "Taylor", "Wright"])


@pytest.mark.parametrize("mapping,expected", [
    ("union_embedded_constant_mapping.md", ["Anand Wendys", "Scott Wendys", "Taylor Vogue", "Wright Vogue", "Roberts Wendys"]),
    ("union_embedded_join_mapping.md", ["Scott Firm X", "Anand Firm X", "Roberts Firm A", "Taylor Firm X", "Wright Firm X"]),
])
def test_simple_projection_from_with_embedded_in_mapping(mapping, expected):
    """``unionMappingWithEmbeddedPropertyWithConstant`` and ``...OneThroughJoin``: a constant, or a value read through a join, is a column of the query of its set."""
    with generated_finders(legend_path(f"tests/mapping/union/{mapping}")), seeded_database(legend_path(_DATA)):
        from person_finder import PersonFinder  # pyright: ignore[reportMissingImports]  # written by the generator
        people = PersonFinder()
        rows = people.find_all([people.last_name(), people.firm().legal_name()]).to_pandas().itertuples(index=False, name=None)
        assert sorted(f"{last} {firm}" for last, firm in rows) == sorted(expected)
