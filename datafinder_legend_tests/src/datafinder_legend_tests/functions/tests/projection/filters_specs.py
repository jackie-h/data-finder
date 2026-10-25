"""
Ported from legend-engine core_relational ``functions/tests/projection/testFilters.pure`` and ``testFilter.pure``.

Only the Person filters are ported; the Trade, Interaction and mapping-specific tests need model and mapping
features the simple subset does not have. Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_FILTERS = "functions/tests/projection/testFilters.pure"
_FILTER = "functions/tests/projection/testFilter.pure"
_SRC = _FILTER

_OPTIONAL_PROPERTY_GAP = (
    "Legend adds `AGE is not null` before a comparison on an optional property; the model has no multiplicity, "
    "and the extra predicate does not change the result"
)

FILTERS_PERSON_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testGreaterThanWithOptionalProperty", f"{_FILTERS}::testGreaterThanWithOptionalProperty",
               'select "root".FIRSTNAME as "firstName", "root".AGE as "age" from personTable as "root" '
               'where "root".AGE is not null and "root".AGE > 30',
               lambda f: f.find_all(None, None, [f.first_name(), f.age()], f.age() > 30),
               known_gaps={"duckdb": _OPTIONAL_PROPERTY_GAP}),
        ported("testGreaterThanWithOptionalPropertyIsEmpty", f"{_FILTERS}::testGreaterThanWithOptionalPropertyIsEmpty",
               'select "root".FIRSTNAME as "firstName", "root".AGE as "age" from personTable as "root" '
               'where "root".AGE is null',
               lambda f: f.find_all(None, None, [f.first_name(), f.age()], f.age().is_none())),
        ported("testIsNullColumn", f"{_FILTER}::testIsNullColumn",
               'select "root".FIRSTNAME as "firstName", "root".AGE as "age" from personTable as "root" '
               'where "root".AGE > 30 and "root".AGE is null',
               lambda f: f.find_all(None, None, [f.first_name(), f.age()],
                                    (f.age() > 30).and_(f.age().is_none()))),
        ported("testIsNotNullColumn", f"{_FILTER}::testIsNotNullColumn",
               'select "root".FIRSTNAME as "firstName", "root".AGE as "age" from personTable as "root" '
               'where "root".AGE > 30 and "root".AGE is not null',
               lambda f: f.find_all(None, None, [f.first_name(), f.age()],
                                    (f.age() > 30).and_(f.age().is_not_none()))),
        unsupported("testCompressSQLforINFilter", f"{_SRC}::testCompressSQLforINFilter",
                    'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "Name", "locationtable_0".PLACE as "place", "locationtable_0".date as "CD" from personTable as "root" left outer join locationTable as "locationtable_0" on ("root".ID = "locationtable_0".PERSONID and "locationtable_0".PLACE in (\'Hoboken\', \'New York\'))',
                    'no filter inside a navigation'),
        unsupported("testCompressSQLforINFilter2", f"{_SRC}::testCompressSQLforINFilter2",
                    'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "Name", "locationtable_0".PLACE as "place", "locationtable_1".date as "CD" from personTable as "root" left outer join locationTable as "locationtable_0" on ("root".ID = "locationtable_0".PERSONID and "locationtable_0".PLACE = \'New York\') left outer join locationTable as "locationtable_1" on ("root".ID = "locationtable_1".PERSONID and "locationtable_1".PLACE in (\'New York\', \'Hoboken\'))',
                    'no filter inside a navigation'),
        unsupported("testSavedRootPropogatedCorrectlyThroughFilter", f"{_SRC}::testSavedRootPropogatedCorrectlyThroughFilter",
                    'select "root".LEGALNAME as "firm", "persontable_0".FIRSTNAME as "targetEmployee" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "persontable_0".FIRSTNAME = \'Peter\' and "persontable_0".LASTNAME = \'Smith\')',
                    'a mapping that the port does not load'),
        unsupported("testChainedFiltersProject", f"{_SRC}::testChainedFiltersProject",
                    'select "root".LEGALNAME as "firm", "locationtable_0".PLACE as "targetEmployee" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "persontable_0".LASTNAME = \'Smith\') left outer join (select "locationtable_1".PERSONID as PERSONID, "locationtable_1".PLACE as PLACE from locationTable as "locationtable_1" where "locationtable_1".PLACE = \'Hoboken\') as "locationtable_0" on ("persontable_0".ID = "locationtable_0".PERSONID)',
                    'a mapping that the port does not load'),
    ],
)
