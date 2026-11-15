"""
Ported from legend-engine core_relational ``tds/tests/testSliceTakeLimitDrop.pure``.

Limits, offsets and slices on a projection.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported

_SRC = "tds/tests/testSliceTakeLimitDrop.pure"

TDS_SLICE_TAKE_LIMIT_DROP_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testSimpleLimit", f"{_SRC}::testSimpleLimit",
               'select top 0 "root".FIRSTNAME as "name" from personTable as "root"',
               lambda f: f.find_all([f.first_name()]).limit(0)),
        ported("testSimpleTake", f"{_SRC}::testSimpleTake",
               'select top 0 "root".FIRSTNAME as "name" from personTable as "root"',
               lambda f: f.find_all([f.first_name()]).limit(0)),
        ported("testSimpleSlice", f"{_SRC}::testSimpleSlice",
               'select "root".FIRSTNAME as "name" from personTable as "root" offset 1 rows fetch next 2 rows only',
               lambda f: f.find_all([f.first_name()]).slice(1, 3)),
        ported("testSimpleDrop", f"{_SRC}::testSimpleDrop",
               'select "root".FIRSTNAME as "name" from personTable as "root" offset 0 rows',
               lambda f: f.find_all([f.first_name()]).offset(0)),
        ported("testSimpleNestedDrop", f"{_SRC}::testSimpleNestedDrop",
                    'select "subselect"."name" as "name" from (select "root".FIRSTNAME as "name" from personTable as "root" offset 5 rows) as "subselect" order by "name" asc',
                    lambda f: f.find_all([f.first_name().alias("name")]).offset(5).order_by(f.first_name().ascending())),
        ported("testSimpleNestedDropAfterConcatenate", f"{_SRC}::testSimpleNestedDropAfterConcatenate",
                    'select "subselect"."name" as "name" from (select "unionalias_1"."name" as "name" from (select "root".FIRSTNAME as "name" from personTable as "root" union all select "root".FIRSTNAME as "name" from personTable as "root") as "unionalias_1" offset 5 rows) as "subselect" order by "name" asc',
                    lambda f: (lambda r: r.order_by(r["name"].ascending()))(f.find_all([f.first_name().alias("name")]).concatenate(f.find_all([f.first_name().alias("name")]), all=True).offset(5))),
        ported("testSimpleNestedTake", f"{_SRC}::testSimpleNestedTake",
                    'select "subselect"."name" as "name" from (select top 5 "root".FIRSTNAME as "name" from personTable as "root") as "subselect" order by "name" asc',
                    lambda f: f.find_all([f.first_name().alias("name")]).limit(5).order_by(f.first_name().ascending())),
        ported("testSimpleNestedTakeAfterConcatenate", f"{_SRC}::testSimpleNestedTakeAfterConcatenate",
                    'select "subselect"."name" as "name" from (select top 5 "unionalias_1"."name" as "name" from (select "root".FIRSTNAME as "name" from personTable as "root" union all select "root".FIRSTNAME as "name" from personTable as "root") as "unionalias_1") as "subselect" order by "name" asc',
                    lambda f: (lambda r: r.order_by(r["name"].ascending()))(f.find_all([f.first_name().alias("name")]).concatenate(f.find_all([f.first_name().alias("name")]), all=True).limit(5))),
        ported("testSimpleNestedSlice", f"{_SRC}::testSimpleNestedSlice",
                    'select "subselect"."name" as "name" from (select "root".FIRSTNAME as "name" from personTable as "root" offset 1 rows fetch next 4 rows only) as "subselect" order by "name" asc',
                    lambda f: f.find_all([f.first_name().alias("name")]).slice(1, 5).order_by(f.first_name().ascending())),
        ported("testSimpleNestedSliceAfterConcatenate", f"{_SRC}::testSimpleNestedSliceAfterConcatenate",
                    'select "subselect"."name" as "name" from (select "unionalias_1"."name" as "name" from (select "root".FIRSTNAME as "name" from personTable as "root" union all select "root".FIRSTNAME as "name" from personTable as "root") as "unionalias_1" offset 1 rows fetch next 4 rows only) as "subselect" order by "name" asc',
                    lambda f: (lambda r: r.order_by(r["name"].ascending()))(f.find_all([f.first_name().alias("name")]).concatenate(f.find_all([f.first_name().alias("name")]), all=True).slice(1, 5))),
    ],
)
