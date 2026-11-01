"""
Ported from legend-engine core_relational ``functions/tests/testExists.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/testExists.pure"


EXISTS_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testExistsWithEmbedded", f"{_SRC}::testExistsWithEmbedded",
                    'select "root".FIRM_LEGALNAME as "name" from (select distinct "person_firm_denorm_1".FIRM_LEGALNAME as FIRM_LEGALNAME, "person_firm_denorm_1".FIRM_ID as FIRM_ID from PERSON_FIRM_DENORM as "person_firm_denorm_1" left outer join (select distinct "person_firm_denorm_3".FIRM_ID from PERSON_FIRM_DENORM as "person_firm_denorm_3" where "person_firm_denorm_3".PERSON_LASTNAME = \'Smith\') as "person_firm_denorm_2" on ("person_firm_denorm_1".FIRM_ID = "person_firm_denorm_2".FIRM_ID) where not "person_firm_denorm_2".FIRM_ID is not null) as "root"',
                    'a class mapping with ~distinct (Firm over the denormalised table), with exists on a to-many step inside it'),
        unsupported("testExistsWithEmbeddedWithPostProcessor", f"{_SRC}::testExistsWithEmbeddedWithPostProcessor",
                    'select "root".FIRM_LEGALNAME as "Name" from (select distinct "person_firm_denorm_1".FIRM_LEGALNAME as FIRM_LEGALNAME, "person_firm_denorm_1".FIRM_ID as FIRM_ID from PERSON_FIRM_DENORM as "person_firm_denorm_1" left outer join (select distinct "person_firm_denorm_3".FIRM_ID from PERSON_FIRM_DENORM as "person_firm_denorm_3" where "person_firm_denorm_3".PERSON_FIRSTNAME = \'John\') as "person_firm_denorm_2" on ("person_firm_denorm_1".FIRM_ID = "person_firm_denorm_2".FIRM_ID) where not "person_firm_denorm_2".FIRM_ID is not null) as "root"',
                    'a class mapping with ~distinct (Firm over the denormalised table), with exists on a to-many step inside it'),
        unsupported("testAssociationWithProjectionHandlingDups", f"{_SRC}::testAssociationWithProjectionHandlingDups",
                    'select "firmtable_0".LEGALNAME as "firm" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) left outer join (select distinct "persontable_2".FIRMID from personTable as "persontable_2" left outer join locationTable as "locationtable_0" on ("persontable_2".ID = "locationtable_0".PERSONID) left outer join placeOfInterestTable as "placeofinteresttable_0" on ("locationtable_0".ID = "placeofinteresttable_0".locationID) where "placeofinteresttable_0".NAME = \'Broadway\') as "persontable_1" on ("firmtable_0".ID = "persontable_1".FIRMID) where "persontable_1".FIRMID is not null',
                    'a condition inside exists that reaches through two navigations (locations.placeOfInterest.name), and PlaceOfInterest is not in the model'),
        unsupported("testExistsForDataType", f"{_SRC}::testExistsForDataType",
                    'select "root".FIRSTNAME as "firstName" from personTable as "root" left outer join (select distinct "othernamestable_1".PERSON_ID from otherNamesTable as "othernamestable_1" where "othernamestable_1".OTHER_NAME = \'abc\') as "othernamestable_0" on ("root".ID = "othernamestable_0".PERSON_ID) where "othernamestable_0".PERSON_ID is not null',
                    'a to-many property of primitive values (otherNames), which the model does not have'),
    ],
)
