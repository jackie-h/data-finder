"""
Ported from legend-engine core_relational ``functions/tests/testFrom.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/testFrom.pure"


FROM_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testMappingFromQueriesViaMappingWithAssociationsOnly", f"{_SRC}::testMappingFromQueriesViaMappingWithAssociationsOnly",
                    'select "unionalias_0"."Name" as "Name" from (select "firmtable_0".LEGALNAME as "Name" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) where "root".FIRSTNAME = \'Peter\' union all select "persontable_1".FIRSTNAME as "Name" from firmTable as "root" left outer join personTable as "persontable_1" on ("root".ID = "persontable_1".FIRMID) where "root".LEGALNAME = \'Firm X\') as "unionalias_0"',
                    'from() with an explicit mapping and runtime'),
        unsupported("testClassMappingsAreCorrectlyIsolated", f"{_SRC}::testClassMappingsAreCorrectlyIsolated",
                    'select "unionalias_0"."FirstName" as "FirstName" from (select "root".FIRSTNAME as "FirstName" from personTable as "root" where "root".FIRSTNAME = \'Peter\' union all select "root".FIRSTNAME as "FirstName" from personTable as "root" where "root".FIRSTNAME = \'Don\') as "unionalias_0"',
                    'from() with an explicit mapping and runtime'),
        unsupported("testClassMappingsWithUnions", f"{_SRC}::testClassMappingsWithUnions",
                    'select "unionBase"."personTableFIRSTNAME_personTableFIRSTNAME" as "FirstName" from (select "root".ID as "pk_0_0", null as "pk_0_1", "root".FIRSTNAME as "personTableFIRSTNAME_personTableFIRSTNAME" from personTable as "root" union all select null as "pk_0_0", "root".ID as "pk_0_1", "root".FIRSTNAME as "personTableFIRSTNAME_personTableFIRSTNAME" from personTable as "root") as "unionBase"',
                    'from() with an explicit mapping and runtime'),
        unsupported("testFromWithMapping", f"{_SRC}::testFromWithMapping",
                    'select "firmtable_0".LEGALNAME as "Name" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) where "root".FIRSTNAME = \'Peter\'',
                    'from() with an explicit mapping and runtime'),
        unsupported("testFromWithMappingAndIntermediateFuncCall", f"{_SRC}::testFromWithMappingAndIntermediateFuncCall",
                    'select "firmtable_0".LEGALNAME as "Name" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) where "root".FIRSTNAME = \'Peter\'',
                    'from() with an explicit mapping and runtime'),
        unsupported("testMultipleFromWithMapping", f"{_SRC}::testMultipleFromWithMapping",
                    'select "unionalias_0"."FirstName" as "FirstName" from (select "root".FIRSTNAME as "FirstName" from personTable as "root" where "root".FIRSTNAME = \'Peter\' union all select "root".FIRSTNAME as "FirstName" from personTable as "root" where "root".FIRSTNAME = \'Don\') as "unionalias_0"',
                    'from() with an explicit mapping and runtime'),
    ],
)
