"""
Ported from legend-engine core_relational ``tds/tests/testTDSUnion.pure``.

Union of two projections.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported, BACKEND

_SRC = "tds/tests/testTDSUnion.pure"

UNION_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[

        ported("testSimpleUnion", f"{_SRC}::testSimpleUnion",
               'select "unionalias_0"."lastName" as "lastName" from (select "root".LASTNAME as "lastName" from personTable as "root" union select "root".LASTNAME as "lastName" from personTable as "root") as "unionalias_0"',
               lambda f: f.find_all(None, None, [f.last_name()]).union(f.find_all(None, None, [f.last_name()]))),    ],
)
