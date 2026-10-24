"""
Ported from legend-engine core_relational ``functions/tests/projection/testFunction.pure``.

Projecting several properties and navigating to-many associations.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/projection/testFunction.pure"

FUNCTION_PERSON_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testSimpleFunction", f"{_SRC}::testSimpleFunction",
               'select "root".FIRSTNAME as "firstName" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name()])),
        ported("testFunctionAndPathOneSide", f"{_SRC}::testFunctionAndPathOneSide",
               'select "root".LASTNAME as "lastName", "root".FIRSTNAME as "firstName" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.last_name(), f.first_name()])),
        ported("testFunctionAndPathOtherSide", f"{_SRC}::testFunctionAndPathOtherSide",
               'select "root".FIRSTNAME as "firstName", "root".LASTNAME as "lastName" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()])),
        unsupported("testSimpleRealFunction", f"{_SRC}::testSimpleRealFunction",
                    'select "root".LASTNAME as "lastName", concat("root".FIRSTNAME, \'|\', "root".LASTNAME) as "fullName" from personTable as "root"',
                    'no string concatenation of attributes'),
    ],
)

FUNCTION_FIRM_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testSimpleFunctionWithToManyMap", f"{_SRC}::testSimpleFunctionWithToManyMap",
               'select "root".LEGALNAME as "legalName", "persontable_0".LASTNAME as "lastName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID)',
               lambda f: f.find_all(None, None, [f.legal_name(), f.employees().last_name()])),
        ported("testSimpleFunctionWithTwoToManyMap", f"{_SRC}::testSimpleFunctionWithTwoToManyMap",
               'select "root".LEGALNAME as "legalName", "persontable_0".FIRSTNAME as "firstName", "persontable_0".LASTNAME as "lastName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID)',
               lambda f: f.find_all(None, None, [f.legal_name(), f.employees().first_name(), f.employees().last_name()])),
    ],
)
