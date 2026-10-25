"""
Ported from legend-engine core_relational ``tests/mapping/join/testMappingAssociationToAdvancedJoin.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/join/testMappingAssociationToAdvancedJoin.pure"


MAPPING_ASSOCIATION_TO_ADVANCED_JOIN_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testProjectOfTypeMadeOfMultipleJoinsOnlyIncludesJoinsRequiredForProjectedColumns", f"{_SRC}::testProjectOfTypeMadeOfMultipleJoinsOnlyIncludesJoinsRequiredForProjectedColumns",
                    'select "root".FIRSTNAME as "firstName" from personTable as "root"',
                    'an association mapped to a join with a function, or or, condition'),
        unsupported("testProjectOfTypeMadeOfMultipleJoinsWithFilterOnlyIncludesJoinsRequiredForProjectedColumns", f"{_SRC}::testProjectOfTypeMadeOfMultipleJoinsWithFilterOnlyIncludesJoinsRequiredForProjectedColumns",
                    'select "root".FIRSTNAME as "firstName" from personTable as "root" where "root".FIRSTNAME = \'John\'',
                    'an association mapped to a join with a function, or or, condition'),
        unsupported("testChainedOuterJoinsMerge", f"{_SRC}::testChainedOuterJoinsMerge",
                    'select "persontable_0".FIRSTNAME as "firstname", "root".LEGALNAME as "firm", "persontable_0".LASTNAME as "lastname" from firmTable as "root" left outer join firmPersonBridgeTable as "firmpersonbridgetable_0" on ("root".ID = "firmpersonbridgetable_0".FIRM_ID) left outer join personTable as "persontable_0" on ("persontable_0".ID = "firmpersonbridgetable_0".PERSON_ID)',
                    'an association mapped to a join with a function, or or, condition'),
        unsupported("testChainedInnerJoinsMerge", f"{_SRC}::testChainedInnerJoinsMerge",
                    'select "firmpersonbridgetable_0".FIRSTNAME as "firstname", "root".LEGALNAME as "firm", "firmpersonbridgetable_0".LASTNAME as "lastname" from firmTable as "root" left outer join (select "firmpersonbridgetable_1".FIRM_ID as FIRM_ID, "persontable_0".FIRSTNAME as FIRSTNAME, "persontable_0".LASTNAME as LASTNAME from firmPersonBridgeTable as "firmpersonbridgetable_1" inner join personTable as "persontable_0" on ("persontable_0".ID = "firmpersonbridgetable_1".PERSON_ID)) as "firmpersonbridgetable_0" on ("root".ID = "firmpersonbridgetable_0".FIRM_ID)',
                    'an association mapped to a join with a function, or or, condition'),
        unsupported("testChainedInnerJoinsWithFilterMerge", f"{_SRC}::testChainedInnerJoinsWithFilterMerge",
                    'select "firmpersonbridgetable_0".FIRSTNAME as "firstname", "root".LEGALNAME as "firm", "firmpersonbridgetable_0".LASTNAME as "lastname" from firmTable as "root" left outer join (select "firmpersonbridgetable_1".FIRM_ID as FIRM_ID, "persontable_0".FIRSTNAME as FIRSTNAME, "persontable_0".LASTNAME as LASTNAME from firmPersonBridgeTable as "firmpersonbridgetable_1" inner join personTable as "persontable_0" on ("persontable_0".ID = "firmpersonbridgetable_1".PERSON_ID)) as "firmpersonbridgetable_0" on ("root".ID = "firmpersonbridgetable_0".FIRM_ID) where "root".LEGALNAME = \'Firm C\'',
                    'an association mapped to a join with a function, or or, condition'),
        unsupported("testConvertToStringH2", f"{_SRC}::testConvertToStringH2",
                    'select "accounttable_0".name as "name" from tradeTable as "root" left outer join accountTable as "accounttable_0" on (cast("root".accountID as varchar(128)) = "accounttable_0".ID)',
                    'an association mapped to a join with a function, or or, condition'),
        unsupported("testChainedOuterJoinsWithFilterInproject", f"{_SRC}::testChainedOuterJoinsWithFilterInproject",
                    'select "root".LEGALNAME as "firm", "persontable_0".EXTRAINFO as "extrainfo" from firmTable as "root" left outer join (select "persontable_1".FIRMID as FIRMID, "persontable_1".FLAG as FLAG, "personextensiontable_1".EXTRAINFO as EXTRAINFO from personTable as "persontable_1" left outer join MiddleTable as "middletable_0" on ("persontable_1".ID = "middletable_0".ID) left outer join personExtensionTable as "personextensiontable_0" on ("middletable_0".LINKID = "personextensiontable_0".ID and "middletable_0".LINKID <> 999999999 and "personextensiontable_0".ID <> -99999999) left outer join MiddleTable as "middletable_1" on ("persontable_1".ID = "middletable_1".ID) left outer join personExtensionTable as "personextensiontable_1" on ("middletable_1".LINKID = "personextensiontable_1".ID and "middletable_1".LINKID <> 999999999 and "personextensiontable_1".ID <> -99999999) where "personextensiontable_0".AGE is not null and "personextensiontable_0".AGE < 10 and "persontable_1".FLAG is not null) as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "root".FLAG is null and "persontable_0".FLAG is not null) where "root".LEGALNAME = \'Firm C\'',
                    'an association mapped to a join with a function, or or, condition'),
        unsupported("testChainedOuterJoinsWithQualifierInproject", f"{_SRC}::testChainedOuterJoinsWithQualifierInproject",
                    'select "root".LEGALNAME as "firm", "persontable_0".LASTNAME as "lastname", "persontable_2".AGE as "age" from firmTable as "root" left outer join (select "persontable_1".FIRMID as FIRMID, "persontable_1".FLAG as FLAG, "persontable_1".LASTNAME as LASTNAME from personTable as "persontable_1" left outer join MiddleTable as "middletable_0" on ("persontable_1".ID = "middletable_0".ID) left outer join personExtensionTable as "personextensiontable_0" on ("middletable_0".LINKID = "personextensiontable_0".ID and "middletable_0".LINKID <> 999999999 and "personextensiontable_0".ID <> -99999999) where "personextensiontable_0".AGE < 10 and "persontable_1".FLAG is not null) as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "root".FLAG is null and "persontable_0".FLAG is not null) left outer join (select "persontable_1".FIRMID as FIRMID, "persontable_1".FLAG as FLAG, "personextensiontable_1".AGE as AGE from personTable as "persontable_1" left outer join MiddleTable as "middletable_0" on ("persontable_1".ID = "middletable_0".ID) left outer join personExtensionTable as "personextensiontable_0" on ("middletable_0".LINKID = "personextensiontable_0".ID and "middletable_0".LINKID <> 999999999 and "personextensiontable_0".ID <> -99999999) left outer join MiddleTable as "middletable_1" on ("persontable_1".ID = "middletable_1".ID) left outer join personExtensionTable as "personextensiontable_1" on ("middletable_1".LINKID = "personextensiontable_1".ID and "middletable_1".LINKID <> 999999999 and "personextensiontable_1".ID <> -99999999) where "personextensiontable_0".AGE < 10 and "persontable_1".FLAG is not null) as "persontable_2" on ("root".ID = "persontable_2".FIRMID and "root".FLAG is null and "persontable_2".FLAG is not null) where "root".LEGALNAME = \'Firm C\'',
                    'an association mapped to a join with a function, or or, condition'),
        unsupported("testChainedInnerJoinsWithQualifierInGroupBy", f"{_SRC}::testChainedInnerJoinsWithQualifierInGroupBy",
                    'select count("firmpersonbridgetable_0".EXTRAINFO) as "count" from firmTable as "root" left outer join (select "firmpersonbridgetable_0".FIRM_ID as FIRM_ID, "persontable_0".LASTNAME as LASTNAME, "persontable_0".ID as ID, "personextensiontable_0".EXTRAINFO as EXTRAINFO from firmPersonBridgeTable as "firmpersonbridgetable_0" inner join personTable as "persontable_0" on ("persontable_0".ID = "firmpersonbridgetable_0".PERSON_ID) left outer join MiddleTable as "middletable_0" on ("persontable_0".ID = "middletable_0".ID) left outer join personExtensionTable as "personextensiontable_0" on ("middletable_0".LINKID = "personextensiontable_0".ID and "middletable_0".LINKID <> 999999999 and "personextensiontable_0".ID <> -99999999) where "persontable_0".LASTNAME = \'Firm X\') as "firmpersonbridgetable_0" on ("firmpersonbridgetable_0".LASTNAME = "root".LEGALNAME and "firmpersonbridgetable_0".LASTNAME = \'Firm X\' and "root".ID = "firmpersonbridgetable_0".FIRM_ID) where "root".LEGALNAME = \'Firm X\'',
                    'an association mapped to a join with a function, or or, condition'),
    ],
)
