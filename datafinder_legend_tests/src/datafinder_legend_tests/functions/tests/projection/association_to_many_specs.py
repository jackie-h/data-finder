"""
Ported from legend-engine core_relational ``functions/tests/projection/testAssociationToMany.pure``.

Only the plain to-many projections are ported. The exists() filters and the qualified property are not (see the
coverage report). Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, BACKEND, unsupported

_SRC = "functions/tests/projection/testAssociationToMany.pure"

_PERSON_LOCATIONS = (
    'select "root".FIRSTNAME as "firstName", "locationtable_0".PLACE as "place" from personTable as "root" '
    'left outer join locationTable as "locationtable_0" on ("root".ID = "locationtable_0".PERSONID)'
)

ASSOCIATION_TO_MANY_PERSON_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testAssociationToManyColumnProtocolOneColumn", f"{_SRC}::testAssociationToManyColumnProtocolOneColumn",
               'select "root".FIRSTNAME as "firstName" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name()])),
        ported("testAssociationToManyColumnProtocol", f"{_SRC}::testAssociationToManyColumnProtocol",
               _PERSON_LOCATIONS,
               lambda f: f.find_all(None, None, [f.first_name(), f.locations().place()])),
        ported("testAssociationToManyColumnProtocolWithDoc", f"{_SRC}::testAssociationToManyColumnProtocolWithDoc",
               _PERSON_LOCATIONS,
               lambda f: f.find_all(None, None, [f.first_name(), f.locations().place()])),
        ported("testAssociationToMany", f"{_SRC}::testAssociationToMany", _PERSON_LOCATIONS,
               lambda f: f.find_all(None, None, [f.first_name(), f.locations().place()])),
        unsupported("testQualifiedPropertyUsingColumnProtocol", f"{_SRC}::testQualifiedPropertyUsingColumnProtocol",
                    'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "personName", "persontable_1".aggCol as "#30YrOldCoWorkers" from personTable as "root" left outer join (select "persontable_2".ID as ID, count(*) as aggCol from personTable as "persontable_2" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "persontable_2".FIRMID) left outer join personTable as "persontable_3" on ("firmtable_0".ID = "persontable_3".FIRMID) where "persontable_3".AGE < 30 group by "persontable_2".ID) as "persontable_1" on ("root".ID = "persontable_1".ID) where concat("root".FIRSTNAME, \' \', "root".LASTNAME) = \'Peter Smith\'',
                    'a mapping that the port does not load'),
        unsupported("testAssociationToManyWithQuery", f"{_SRC}::testAssociationToManyWithQuery",
                    'select "root".FIRSTNAME as "firstName", "locationtable_2".PLACE as "place" from personTable as "root" left outer join (select distinct "locationtable_1".PERSONID from locationTable as "locationtable_1" where "locationtable_1".PLACE = \'Hoboken\') as "locationtable_0" on ("root".ID = "locationtable_0".PERSONID) left outer join locationTable as "locationtable_2" on ("root".ID = "locationtable_2".PERSONID) where "locationtable_0".PERSONID is not null',
                    'a mapping that the port does not load'),
    ],
)

ASSOCIATION_TO_MANY_FIRM_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testAssociationToManyDeepTwo", f"{_SRC}::testAssociationToManyDeepTwo",
               'select "root".LEGALNAME as "legalName", "persontable_0".FIRSTNAME as "firstName", '
               '"persontable_0".LASTNAME as "lastName", "addresstable_0".NAME as "name", "addresstable_0".NAME as "n2" '
               'from firmTable as "root" '
               'left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID) '
               'left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "persontable_0".ADDRESSID)',
               lambda f: f.find_all(None, None, [f.legal_name(), f.employees().first_name(),
                                                 f.employees().last_name(), f.employees().address().name(),
                                                 f.employees().address().name().alias("n2")])),
    ],
)
