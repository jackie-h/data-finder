"""
Ported from legend-engine core_relational ``functions/tests/projection/testFunctionVariables.pure``.

Variables in nested expressions.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/projection/testFunctionVariables.pure"

FUNCTION_VARIABLES_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testVariableReferenceInMapWithSameNameAsThatInParentProject", f"{_SRC}::testVariableReferenceInMapWithSameNameAsThatInParentProject",
                    'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "personName", "firmtable_0".LEGALNAME as "firmName" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID)',
                    'no qualified (derived) properties'),
        unsupported("testVariableReferenceInMapWithNestedFilter", f"{_SRC}::testVariableReferenceInMapWithNestedFilter",
                    'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "personName", "persontable_1".NAME as "addressName" from personTable as "root" left outer join (select "persontable_2".ID as ID, "addresstable_0".NAME as NAME from personTable as "persontable_2" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "persontable_2".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_0".ADDRESSID) left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "persontable_2".ADDRESSID) where "addresstable_0".NAME = "addresstable_1".NAME or ("addresstable_0".NAME is null and "addresstable_1".NAME is null)) as "persontable_1" on ("root".ID = "persontable_1".ID)',
                    'no qualified (derived) properties'),
        unsupported("testVariableReferenceWithNestedFilterMultiple", f"{_SRC}::testVariableReferenceWithNestedFilterMultiple",
                    'select "root".LASTNAME as "personName", "persontable_1".NAME as "addressName" from personTable as "root" left outer join (select "persontable_2".ID as ID, "addresstable_2".NAME as NAME from personTable as "persontable_2" left outer join (select "firmtable_0".ADDRESSID as ADDRESSID, "persontable_3".ID as ID from personTable as "persontable_3" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "persontable_3".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_0".ADDRESSID) left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "persontable_3".ADDRESSID) where "addresstable_0".NAME = "addresstable_1".NAME or ("addresstable_0".NAME is null and "addresstable_1".NAME is null)) as "persontable_3" on ("persontable_2".ID = "persontable_3".ID) left outer join addressTable as "addresstable_2" on ("addresstable_2".ID = "persontable_3".ADDRESSID) left outer join addressTable as "addresstable_3" on ("addresstable_3".ID = "persontable_2".ADDRESSID) where "addresstable_2".STREET = "addresstable_3".STREET or ("addresstable_2".STREET is null and "addresstable_3".STREET is null)) as "persontable_1" on ("root".ID = "persontable_1".ID)',
                    'no filter inside a navigation'),
    ],
)

FUNCTION_VARIABLES_FIRM_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testProjectReferenceInRhsFilterWithDistinctVarNamesViaNonPropertyFunctionExpression", f"{_SRC}::testProjectReferenceInRhsFilterWithDistinctVarNamesViaNonPropertyFunctionExpression",
                    'select "root".LEGALNAME as "legalName", "firmtable_1".FIRSTNAME as "employeeWithFirmAddress", "addresstable_2".NAME as "address" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, "persontable_0".FIRSTNAME as FIRSTNAME from firmTable as "firmtable_2" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_2".ADDRESSID) left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "persontable_0".ADDRESSID) left outer join personTable as "persontable_1" on ("persontable_0".MANAGERID = "persontable_1".ID) where "addresstable_1".NAME = "addresstable_0".NAME or ("addresstable_1".NAME is null and "addresstable_0".NAME is null) or concat("persontable_1".FIRSTNAME, \' \', "persontable_1".LASTNAME) = \'\') as "firmtable_1" on ("root".ID = "firmtable_1".ID) left outer join addressTable as "addresstable_2" on ("addresstable_2".ID = "root".ADDRESSID)',
                    'no filter inside a navigation'),
    ],
)
