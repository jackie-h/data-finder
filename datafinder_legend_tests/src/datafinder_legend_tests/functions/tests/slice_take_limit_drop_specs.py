"""
Ported from legend-engine core_relational ``functions/tests/testSliceTakeLimitDrop.pure``.

Limits, in the order the operations are applied.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported, BACKEND, TDS_FILTER_OPTIONAL_PROPERTY

_SRC = "functions/tests/testSliceTakeLimitDrop.pure"

SLICE_TAKE_LIMIT_DROP_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testFilterLimitInSequence", f"{_SRC}::testFilterLimitInSequence",
               'select top 2 "root".FIRSTNAME as "firstName", "root".LASTNAME as "lastName", "root".AGE as "age" from personTable as "root" where "root".AGE > 30',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name(), f.age()], f.age() > 30).limit(2),
               known_gaps={BACKEND: TDS_FILTER_OPTIONAL_PROPERTY}),
        unsupported("testLimitFilterInSequence", f"{_SRC}::testLimitFilterInSequence",
                    'select "subselect"."firstName" as "firstName", "subselect"."lastName" as "lastName", "subselect"."age" as "age" from (select top 1 "root".FIRSTNAME as "firstName", "root".LASTNAME as "lastName", "root".AGE as "age" from personTable as "root") as "subselect" where "subselect"."age" > 25',
                    'a filter cannot be applied after a limit'),
    ],
)
