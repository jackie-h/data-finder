"""
Ported from legend-engine core_relational ``functions/tests/projection/testAssociationToMany.pure``.

Only the plain to-many projections are ported. The exists() filters and the qualified property are not (see the
coverage report). Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, BACKEND, JOIN_OPERAND_ORDER

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
                                                 f.employees().address().name()])),
    ],
)
