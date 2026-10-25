"""
Ported from legend-engine core_relational ``tests/mapping/union/testUnionWithExtends.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/union/testUnionWithExtends.pure"


UNION_WITH_EXTENDS_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testSimpleQueryFromWithFilterInMapping", f"{_SRC}::testSimpleQueryFromWithFilterInMapping",
                    'select "unionBase"."PersonSet1lastName_s1_PersonSet2lastName_s2" as "Lastname" from (select "root".ID as "pk_0_0", null as "pk_0_1", "root".lastName_s1 as "PersonSet1lastName_s1_PersonSet2lastName_s2" from PersonSet1 as "root" where "root".FirmID = 1 union all select null as "pk_0_0", "root".ID as "pk_0_1", "root".lastName_s2 as "PersonSet1lastName_s1_PersonSet2lastName_s2" from PersonSet2 as "root" where "root".FirmID = 1) as "unionBase"',
                    'union class mappings (Operation_SetImplementation)'),
        unsupported("testProjectEmbeddedMappingUnionWithSameColumnsNames", f"{_SRC}::testProjectEmbeddedMappingUnionWithSameColumnsNames",
                    'select "unionBase"."ApplicationsSet1date_ApplicationsSet2date" as "date", "unionBase"."ApplicationsSet1nickName_s1_ApplicationsSet2nickName" as "applicant.name", "unionBase"."ApplicationsSet1firm_nickName_s1_ApplicationsSet2firm_nickName" as "firm.name" from (select "root".date as "pk_0_0", "root".personID as "pk_1_0", "root".firmID as "pk_2_0", null as "pk_0_1", null as "pk_1_1", null as "pk_2_1", null as "pk_3_1", "root".date as "ApplicationsSet1date_ApplicationsSet2date", "root".nickName_s1 as "ApplicationsSet1nickName_s1_ApplicationsSet2nickName", "root".firm_nickName_s1 as "ApplicationsSet1firm_nickName_s1_ApplicationsSet2firm_nickName" from ApplicationsSet1 as "root" union all select null as "pk_0_0", null as "pk_1_0", null as "pk_2_0", "root".date as "pk_0_1", "root".firstName as "pk_1_1", "root".lastName as "pk_2_1", "root".firm_legalName as "pk_3_1", "root".date as "ApplicationsSet1date_ApplicationsSet2date", "root".nickName as "ApplicationsSet1nickName_s1_ApplicationsSet2nickName", "root".firm_nickName as "ApplicationsSet1firm_nickName_s1_ApplicationsSet2firm_nickName" from ApplicationsSet2 as "root") as "unionBase"',
                    'union class mappings (Operation_SetImplementation)'),
        unsupported("testProjectEmbeddedMappingUnionWithSameColumnsNamesDeep", f"{_SRC}::testProjectEmbeddedMappingUnionWithSameColumnsNamesDeep",
                    'select "unionBase"."ApplicationsSet1date_ApplicationsSet2date" as "date", "unionBase"."ApplicationsSet1person_address_s1_ApplicationsSet2person_address_s2" as "applicant.adress", "unionBase"."ApplicationsSet1firm_address_s1_ApplicationsSet2firm_address_s2" as "firm.address" from (select "root".date as "pk_0_0", "root".personID as "pk_1_0", "root".firmID as "pk_2_0", null as "pk_0_1", null as "pk_1_1", null as "pk_2_1", null as "pk_3_1", "root".date as "ApplicationsSet1date_ApplicationsSet2date", "root".person_address_s1 as "ApplicationsSet1person_address_s1_ApplicationsSet2person_address_s2", "root".firm_address_s1 as "ApplicationsSet1firm_address_s1_ApplicationsSet2firm_address_s2" from ApplicationsSet1 as "root" union all select null as "pk_0_0", null as "pk_1_0", null as "pk_2_0", "root".date as "pk_0_1", "root".firstName as "pk_1_1", "root".lastName as "pk_2_1", "root".firm_legalName as "pk_3_1", "root".date as "ApplicationsSet1date_ApplicationsSet2date", "root".person_address_s2 as "ApplicationsSet1person_address_s1_ApplicationsSet2person_address_s2", "root".firm_address_s2 as "ApplicationsSet1firm_address_s1_ApplicationsSet2firm_address_s2" from ApplicationsSet2 as "root") as "unionBase"',
                    'union class mappings (Operation_SetImplementation)'),
        unsupported("testProjectMappingWithSameColumnsNames", f"{_SRC}::testProjectMappingWithSameColumnsNames",
                    'select "root".date as "date", "personset1_0".nickName as "applicant.name", "firmset1_0".nickName as "firm.name" from ApplicationsSet1 as "root" left outer join PersonSet1 as "personset1_0" on ("root".personID = "personset1_0".ID) left outer join FirmSet1 as "firmset1_0" on ("root".firmID = "firmset1_0".ID)',
                    'union class mappings (Operation_SetImplementation)'),
    ],
)
