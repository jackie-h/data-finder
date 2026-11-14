"""
Ported from legend-engine core_relational ``tds/tests/testTableToTDS.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder import constant
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tds/tests/testTableToTDS.pure"


TABLE_TO_T_D_S_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testProjectStringColumn", f"{_SRC}::testProjectStringColumn",
               'select "root".FIRSTNAME as "first_name", "root".FIRSTNAME as "name_again" from personTable as "root"',
               lambda f: f.find_all([f.first_name().alias("first_name"), f.first_name().alias("name_again")])),
        unsupported("testView", f"{_SRC}::testView",
                    'select "root".firstName as "first_name", "root".firstName as "name_again" from (select distinct "root".FIRSTNAME as firstName, "addresstable_0".NAME as address from personTable as "root" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "root".ADDRESSID)) as "root"',
                    'a table as a result (tableToTDS), without a class'),
        ported("testSimpleFilter", f"{_SRC}::testSimpleFilter",
               'select "root".FIRSTNAME as "name" from personTable as "root" where "root".FIRSTNAME = \'John\'',
               lambda f: f.find_all([f.first_name().alias("name")], f.first_name().eq("John"))),
        ported("testExtendStringLiteral", f"{_SRC}::testExtendStringLiteral",
               'select "root".FIRSTNAME as "firstName", \'Hello\' as "hello" from personTable as "root"',
               lambda f: f.find_all([f.first_name().alias("firstName"), constant("Hello").alias("hello")])),
        ported("testSimpleConcatenate", f"{_SRC}::testSimpleConcatenate",
               'select "unionalias_0"."lastName" as "lastName" from (select "root".LASTNAME as "lastName" from personTable as "root" union all select "root".LASTNAME as "lastName" from personTable as "root") as "unionalias_0"',
               lambda f: f.find_all([f.last_name().alias("lastName")]).concatenate(f.find_all([f.last_name().alias("lastName")]), all=True)),
        ported("testSimpleSortAsc", f"{_SRC}::testSimpleSortAsc",
               'select "root".FIRSTNAME as "name" from personTable as "root" order by "name" asc',
               lambda f: f.find_all([f.first_name().alias("name")]).order_by(f.first_name().ascending())),
    ],
)
