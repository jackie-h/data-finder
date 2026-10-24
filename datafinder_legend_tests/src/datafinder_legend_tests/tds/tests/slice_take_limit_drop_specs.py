"""
Ported from legend-engine core_relational ``tds/tests/testSliceTakeLimitDrop.pure``.

Limits, offsets and slices on a projection.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tds/tests/testSliceTakeLimitDrop.pure"

TDS_SLICE_TAKE_LIMIT_DROP_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testSimpleLimit", f"{_SRC}::testSimpleLimit",
               'select top 0 "root".FIRSTNAME as "name" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name()]).limit(0)),
        ported("testSimpleTake", f"{_SRC}::testSimpleTake",
               'select top 0 "root".FIRSTNAME as "name" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name()]).limit(0)),
        ported("testSimpleSlice", f"{_SRC}::testSimpleSlice",
               'select "root".FIRSTNAME as "name" from personTable as "root" offset 1 rows fetch next 2 rows only',
               lambda f: f.find_all(None, None, [f.first_name()]).slice(1, 3)),
        ported("testSimpleDrop", f"{_SRC}::testSimpleDrop",
               'select "root".FIRSTNAME as "name" from personTable as "root" offset 0 rows',
               lambda f: f.find_all(None, None, [f.first_name()]).offset(0)),
        unsupported("testSimpleNestedDrop", f"{_SRC}::testSimpleNestedDrop",
                    'select "subselect"."name" as "name" from (select "root".FIRSTNAME as "name" from personTable as "root" offset 5 rows) as "subselect" order by "name" asc',
                    'an offset before more operations, or after a limit, is nested in a subselect; the finder applies them together'),
        unsupported("testSimpleNestedDropAfterConcatenate", f"{_SRC}::testSimpleNestedDropAfterConcatenate",
                    'select "subselect"."name" as "name" from (select "unionalias_1"."name" as "name" from (select "root".FIRSTNAME as "name" from personTable as "root" union all select "root".FIRSTNAME as "name" from personTable as "root") as "unionalias_1" offset 5 rows) as "subselect" order by "name" asc',
                    'an offset before more operations, or after a limit, is nested in a subselect; the finder applies them together'),
        unsupported("testSimpleNestedTake", f"{_SRC}::testSimpleNestedTake",
                    'select "subselect"."name" as "name" from (select top 5 "root".FIRSTNAME as "name" from personTable as "root") as "subselect" order by "name" asc',
                    'operations cannot be applied after a limit'),
        unsupported("testSimpleNestedTakeAfterConcatenate", f"{_SRC}::testSimpleNestedTakeAfterConcatenate",
                    'select "subselect"."name" as "name" from (select top 5 "unionalias_1"."name" as "name" from (select "root".FIRSTNAME as "name" from personTable as "root" union all select "root".FIRSTNAME as "name" from personTable as "root") as "unionalias_1") as "subselect" order by "name" asc',
                    'no concatenate / union'),
        unsupported("testSimpleNestedSlice", f"{_SRC}::testSimpleNestedSlice",
                    'select "subselect"."name" as "name" from (select "root".FIRSTNAME as "name" from personTable as "root" offset 1 rows fetch next 4 rows only) as "subselect" order by "name" asc',
                    'an offset or slice before more operations, or after a limit, is nested in a subselect; the finder applies them together'),
        unsupported("testSimpleNestedSliceAfterConcatenate", f"{_SRC}::testSimpleNestedSliceAfterConcatenate",
                    'select "subselect"."name" as "name" from (select "unionalias_1"."name" as "name" from (select "root".FIRSTNAME as "name" from personTable as "root" union all select "root".FIRSTNAME as "name" from personTable as "root") as "unionalias_1" offset 1 rows fetch next 4 rows only) as "subselect" order by "name" asc',
                    'an offset or slice before more operations, or after a limit, is nested in a subselect; the finder applies them together'),
    ],
)
