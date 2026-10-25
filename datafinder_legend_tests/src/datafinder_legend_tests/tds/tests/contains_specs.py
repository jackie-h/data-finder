"""
Ported from legend-engine core_relational ``tds/tests/testTDSContains.pure``.

Filtering on whether a TDS contains the row.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tds/tests/testTDSContains.pure"

TDS_CONTAINS_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testTDSContainsType1NegationFurtherOps", f"{_SRC}::testTDSContainsType1NegationFurtherOps",
                    'select "firmtable_0".LEGALNAME as "FN", count("root".FIRSTNAME) as "Count" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "root".ADDRESSID) where exists (select 1 from (select top 1 "root".LEGALNAME as "Name" from firmTable as "root") as "firmtable_1" where "firmtable_0".LEGALNAME = "firmtable_1"."Name" or ("firmtable_0".LEGALNAME is null and "firmtable_1"."Name" is null)) and "addresstable_0".NAME = \'New York\' group by "FN"',
                    'no filter on membership of another query (tdsContains / exists subquery)'),
        unsupported("testTDSContainsType2NegationFurtherOps", f"{_SRC}::testTDSContainsType2NegationFurtherOps",
                    'select "firmtable_0".LEGALNAME as "FN", count("root".FIRSTNAME) as "Count" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "root".ADDRESSID) where exists (select 1 from (select top 1 concat("root".LEGALNAME, \'_Append\') as "Name" from firmTable as "root") as "firmtable_1" where char_length("firmtable_0".LEGALNAME) = (char_length("firmtable_1"."Name") - 7)) and "addresstable_0".NAME = \'New York\' group by "FN"',
                    'no filter on membership of another query (tdsContains / exists subquery)'),
    ],
)
