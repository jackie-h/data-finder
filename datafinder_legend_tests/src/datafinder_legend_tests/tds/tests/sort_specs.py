"""
Ported from legend-engine core_relational ``tds/tests/testSort.pure``.

Sorting.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tds/tests/testSort.pure"

SORT_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testSimpleSortAscInputToProject", f"{_SRC}::testSimpleSortAscInputToProject",
               'select "root".LASTNAME as "LASTNAME" from personTable as "root" order by "root".LASTNAME asc',
               lambda f: f.find_all(None, None, [f.last_name()]).order_by(f.last_name().ascending())),
    ],
)
