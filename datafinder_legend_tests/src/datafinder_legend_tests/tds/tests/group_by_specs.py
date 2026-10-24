"""
Ported from legend-engine core_relational ``tds/tests/testGroupBy.pure``.

Grouping a projection.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tds/tests/testGroupBy.pure"

TDS_GROUP_BY_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("simpleGroupByWithJoinStrings", f"{_SRC}::simpleGroupByWithJoinStrings",
               'select "root".FIRSTNAME as "firstName", listagg("root".LASTNAME, \'*\') as "lastNames" from personTable as "root" group by "firstName"',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name().join("*")]).group_by(f.first_name())),
    ],
)
