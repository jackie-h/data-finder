"""
Ported from legend-engine core_relational ``tds/tests/testTableToTDS.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tds/tests/testTableToTDS.pure"


TABLE_TO_T_D_S_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testProjectStringColumn", f"{_SRC}::testProjectStringColumn",
                    'select "root".FIRSTNAME as "first_name", "root".FIRSTNAME as "name_again" from personTable as "root"',
                    'a table as a result (tableToTDS), without a class'),
        unsupported("testView", f"{_SRC}::testView",
                    'select "root".firstName as "first_name", "root".firstName as "name_again" from (select distinct "root".FIRSTNAME as firstName, "addresstable_0".NAME as address from personTable as "root" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "root".ADDRESSID)) as "root"',
                    'a table as a result (tableToTDS), without a class'),
        unsupported("testSimpleFilter", f"{_SRC}::testSimpleFilter",
                    'select "root".FIRSTNAME as "name" from personTable as "root" where "root".FIRSTNAME = \'John\'',
                    'a table as a result (tableToTDS), without a class'),
        unsupported("testExtendStringLiteral", f"{_SRC}::testExtendStringLiteral",
                    'select "root".FIRSTNAME as "firstName", \'Hello\' as "hello" from personTable as "root"',
                    'a table as a result (tableToTDS), without a class'),
        unsupported("testSimpleConcatenate", f"{_SRC}::testSimpleConcatenate",
                    'select "unionalias_0"."lastName" as "lastName" from (select "root".LASTNAME as "lastName" from personTable as "root" union all select "root".LASTNAME as "lastName" from personTable as "root") as "unionalias_0"',
                    'a table as a result (tableToTDS), without a class'),
        unsupported("testSimpleSortAsc", f"{_SRC}::testSimpleSortAsc",
                    'select "root".FIRSTNAME as "name" from personTable as "root" order by "name" asc',
                    'a table as a result (tableToTDS), without a class'),
    ],
)
