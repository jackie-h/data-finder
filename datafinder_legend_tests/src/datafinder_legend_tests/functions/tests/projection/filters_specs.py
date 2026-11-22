"""
Ported from legend-engine core_relational ``functions/tests/projection/testFilters.pure`` and ``testFilter.pure``.

Only the Person filters are ported; the Trade, Interaction and mapping-specific tests need model and mapping
features the simple subset does not have. Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import CONCAT_FUNCTION, FILTERED_STEP_BELOW_A_FILTERED_STEP, QUALIFIED_PROPERTIES, not_supported, ported, unsupported, BACKEND, TDS_FILTER_OPTIONAL_PROPERTY

_FILTERS = "functions/tests/projection/testFilters.pure"
_FILTER = "functions/tests/projection/testFilter.pure"
_SRC = _FILTER

FILTERS_PERSON_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testGreaterThanWithOptionalProperty", f"{_FILTERS}::testGreaterThanWithOptionalProperty",
               'select "root".FIRSTNAME as "firstName", "root".AGE as "age" from personTable as "root" '
               'where "root".AGE is not null and "root".AGE > 30',
               lambda f: f.find_all([f.first_name(), f.age()], f.age() > 30)),
        ported("testGreaterThanWithOptionalPropertyIsEmpty", f"{_FILTERS}::testGreaterThanWithOptionalPropertyIsEmpty",
               'select "root".FIRSTNAME as "firstName", "root".AGE as "age" from personTable as "root" '
               'where "root".AGE is null',
               lambda f: f.find_all([f.first_name(), f.age()], f.age().is_none())),
        ported("testIsNullColumn", f"{_FILTER}::testIsNullColumn",
               'select "root".FIRSTNAME as "firstName", "root".AGE as "age" from personTable as "root" '
               'where "root".AGE > 30 and "root".AGE is null',
               lambda f: f.find_all([f.first_name(), f.age()],
                                    (f.age() > 30).and_(f.age().is_none())),
               known_gaps={BACKEND: TDS_FILTER_OPTIONAL_PROPERTY}),
        ported("testIsNotNullColumn", f"{_FILTER}::testIsNotNullColumn",
               'select "root".FIRSTNAME as "firstName", "root".AGE as "age" from personTable as "root" '
               'where "root".AGE > 30 and "root".AGE is not null',
               lambda f: f.find_all([f.first_name(), f.age()],
                                    (f.age() > 30).and_(f.age().is_not_none())),
               known_gaps={BACKEND: TDS_FILTER_OPTIONAL_PROPERTY}),
        ported("testCompressSQLforINFilter", f"{_SRC}::testCompressSQLforINFilter",
               'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "Name", "locationtable_0".PLACE as "place", "locationtable_0".date as "CD" from personTable as "root" left outer join locationTable as "locationtable_0" on ("root".ID = "locationtable_0".PERSONID and "locationtable_0".PLACE in (\'Hoboken\', \'New York\'))',
               lambda f: f.find_all([
                   (f.first_name() + " " + f.last_name()).alias("Name"),
                   f.locations().filter(lambda each: each.place().is_in(["Hoboken", "New York"])).place().alias("place"),
                   f.locations().filter(lambda each: each.place().is_in(["Hoboken", "New York"])).censusdate().alias("CD")]),
               known_gaps={BACKEND: CONCAT_FUNCTION}),
        ported("testCompressSQLforINFilter2", f"{_SRC}::testCompressSQLforINFilter2",
               'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "Name", "locationtable_0".PLACE as "place", "locationtable_1".date as "CD" from personTable as "root" left outer join locationTable as "locationtable_0" on ("root".ID = "locationtable_0".PERSONID and "locationtable_0".PLACE = \'New York\') left outer join locationTable as "locationtable_1" on ("root".ID = "locationtable_1".PERSONID and "locationtable_1".PLACE in (\'New York\', \'Hoboken\'))',
               lambda f: f.find_all([
                   (f.first_name() + " " + f.last_name()).alias("Name"),
                   f.locations().filter(lambda each: each.place().is_in(["New York"])).place().alias("place"),
                   f.locations().filter(lambda each: each.place().is_in(["New York", "Hoboken"])).censusdate().alias("CD")]),
               known_gaps={BACKEND: CONCAT_FUNCTION}),
        not_supported("testSavedRootPropogatedCorrectlyThroughFilter", f"{_SRC}::testSavedRootPropogatedCorrectlyThroughFilter",
                    'select "root".LEGALNAME as "firm", "persontable_0".FIRSTNAME as "targetEmployee" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "persontable_0".FIRSTNAME = \'Peter\' and "persontable_0".LASTNAME = \'Smith\')',
                    QUALIFIED_PROPERTIES),
    ],
)


FILTERS_FIRM_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testChainedFiltersProject", f"{_SRC}::testChainedFiltersProject",
               'select "root".LEGALNAME as "firm", "locationtable_0".PLACE as "targetEmployee" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "persontable_0".LASTNAME = \'Smith\') left outer join (select "locationtable_1".PERSONID as PERSONID, "locationtable_1".PLACE as PLACE from locationTable as "locationtable_1" where "locationtable_1".PLACE = \'Hoboken\') as "locationtable_0" on ("persontable_0".ID = "locationtable_0".PERSONID)',
               lambda f: (lambda hoboken: f.find_all([f.legal_name().alias("firm"), hoboken.place().alias("targetEmployee")]))(
                   f.employees().filter(lambda e: e.last_name().eq("Smith")).locations().filter(lambda o: o.place().eq("Hoboken"))),
               known_gaps={BACKEND: FILTERED_STEP_BELOW_A_FILTERED_STEP}),
    ],
)
