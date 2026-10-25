"""
Ported from legend-engine core_relational ``functions/tests/projection/testExists.pure``.

``exists`` over an association. Only the test on the simple mapping is here; the others use their own mappings.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import unsupported

_SRC = "functions/tests/projection/testExists.pure"

EXISTS_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[

        unsupported("testExistsAsQualifier", f"{_SRC}::testExistsAsQualifier",
                    'select "dep_0".mainId is not null as "prop" from main as "root" left outer join (select distinct "dep_1".mainId from dep as "dep_1" where "dep_1".name = \'name2\') as "dep_0" on ("root".id = "dep_0".mainId)',
                    'exists over an association, as a column or a filter'),
        unsupported("testExistsEmbeddedOnSameTable", f"{_SRC}::testExistsEmbeddedOnSameTable",
                    'select "root".depName = \'name2\' as "prop" from mainDenorm as "root"',
                    'exists over an association, as a column or a filter'),
        unsupported("testExistsAsQualifierWithFilter", f"{_SRC}::testExistsAsQualifierWithFilter",
                    'select "dep_0".mainId is not null as "prop" from main as "root" left outer join (select distinct "dep_1".mainId from dep as "dep_1" where "dep_1".otherFilter = 2 and "dep_1".name = \'name2\') as "dep_0" on ("root".id = "dep_0".mainId) where "root".name = \'name3\'',
                    'exists over an association, as a column or a filter'),
        unsupported("testExistsAsQualifierWithFilterAndWrongJoin", f"{_SRC}::testExistsAsQualifierWithFilterAndWrongJoin",
                    'select "dep_0".otherFilter is not null and "dep_0".mainId is not null as "prop" from main as "root" left outer join (select distinct "dep_1".mainId, "dep_1".otherFilter from dep as "dep_1" where "dep_1".otherFilter = 2 and "dep_1".name = \'name2\') as "dep_0" on ("root".id = "dep_0".mainId and "root".name = \'name3\' and "dep_0".otherFilter = 2) where "root".name = \'name3\'',
                    'exists over an association, as a column or a filter'),
        unsupported("testExistsAsNullWithSubType", f"{_SRC}::testExistsAsNullWithSubType",
                    'select "fn_0".classId is not null as "hasPrivateFn", "fn_2".classId is not null as "hasPublicFn" from main as "root" left outer join (select distinct "fn_1".classId from fn as "fn_1" left outer join publicFn as "publicfn_0" on ("publicfn_0".fnId = "fn_1".id) where "publicfn_0".id is not null) as "fn_0" on ("root".id = "fn_0".classId) left outer join (select distinct "fn_1".classId from fn as "fn_1" left outer join privateFn as "privatefn_0" on ("privatefn_0".fnId = "fn_1".id) where "privatefn_0".id is not null) as "fn_2" on ("root".id = "fn_2".classId)',
                    'exists over an association, as a column or a filter'),    ],
)


PERSON_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testSimpleExists", f"{_SRC}::testSimpleExists",
                    'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "a", exists (select 1 from (select "addresstable_0".ID as ID, null as ADDRESSID, "addresstable_0".NAME as "addressTableNAME" from addressTable as "addresstable_0" union all select "firmtable_0".ID as ID, "firmtable_0".ADDRESSID as ADDRESSID, "addresstable_1".NAME as "addressTableNAME" from firmTable as "firmtable_0" left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "firmtable_0".ADDRESSID)) as "unionalias_0" where ("unionalias_0".ID = "root".FIRMID or "unionalias_0".ID = "root".ADDRESSID) and "unionalias_0"."addressTableNAME" = \'Hoboken\') as "prop" from personTable as "root"',
                    'exists over an association that is mapped as a union'),
    ],
)
