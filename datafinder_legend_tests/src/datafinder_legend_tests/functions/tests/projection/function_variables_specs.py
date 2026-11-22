"""
Ported from legend-engine core_relational ``functions/tests/projection/testFunctionVariables.pure``.

Variables in nested expressions.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported, QUALIFIED_PROPERTIES, not_supported

_SRC = "functions/tests/projection/testFunctionVariables.pure"

FUNCTION_VARIABLES_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        not_supported("testVariableReferenceInMapWithSameNameAsThatInParentProject", f"{_SRC}::testVariableReferenceInMapWithSameNameAsThatInParentProject",
                    'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "personName", "firmtable_0".LEGALNAME as "firmName" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID)',
                    QUALIFIED_PROPERTIES),
        not_supported("testVariableReferenceInMapWithNestedFilter", f"{_SRC}::testVariableReferenceInMapWithNestedFilter",
                    'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "personName", "persontable_1".NAME as "addressName" from personTable as "root" left outer join (select "persontable_2".ID as ID, "addresstable_0".NAME as NAME from personTable as "persontable_2" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "persontable_2".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_0".ADDRESSID) left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "persontable_2".ADDRESSID) where "addresstable_0".NAME is not distinct from "addresstable_1".NAME) as "persontable_1" on ("root".ID = "persontable_1".ID)',
                    QUALIFIED_PROPERTIES),
        unsupported("testVariableReferenceWithNestedFilterMultiple", f"{_SRC}::testVariableReferenceWithNestedFilterMultiple",
                    'select "root".LASTNAME as "personName", "persontable_1".NAME as "addressName" from personTable as "root" left outer join (select "persontable_2".ID as ID, "addresstable_2".NAME as NAME from personTable as "persontable_2" left outer join (select "firmtable_0".ADDRESSID as ADDRESSID, "persontable_3".ID as ID from personTable as "persontable_3" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "persontable_3".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_0".ADDRESSID) left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "persontable_3".ADDRESSID) where "addresstable_0".NAME is not distinct from "addresstable_1".NAME) as "persontable_3" on ("persontable_2".ID = "persontable_3".ID) left outer join addressTable as "addresstable_2" on ("addresstable_2".ID = "persontable_3".ADDRESSID) left outer join addressTable as "addresstable_3" on ("addresstable_3".ID = "persontable_2".ADDRESSID) where "addresstable_2".STREET is not distinct from "addresstable_3".STREET) as "persontable_1" on ("root".ID = "persontable_1".ID)',
                    'a filter inside a navigation that reads a column of the root of the query (the address of the person, in the filter of the firm and of its address): a filter of a navigation reads that navigation and the ones below it only'),
        not_supported("testProjectOnQualifiedPropertyReturningAConstant", f"{_SRC}::testProjectOnQualifiedPropertyReturningAConstant",
                    'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "name", \'constant\' as "constant" from personTable as "root"',
                    QUALIFIED_PROPERTIES),
    ],
)

FUNCTION_VARIABLES_FIRM_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testProjectReferenceInRhsFilterWithDistinctVarNamesViaNonPropertyFunctionExpression", f"{_SRC}::testProjectReferenceInRhsFilterWithDistinctVarNamesViaNonPropertyFunctionExpression",
                    'select "root".LEGALNAME as "legalName", "firmtable_1".FIRSTNAME as "employeeWithFirmAddress", "addresstable_2".NAME as "address" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, "persontable_0".FIRSTNAME as FIRSTNAME from firmTable as "firmtable_2" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_2".ADDRESSID) left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "persontable_0".ADDRESSID) left outer join personTable as "persontable_1" on ("persontable_0".MANAGERID = "persontable_1".ID) where "addresstable_1".NAME is not distinct from "addresstable_0".NAME or concat("persontable_1".FIRSTNAME, \' \', "persontable_1".LASTNAME) = \'\') as "firmtable_1" on ("root".ID = "firmtable_1".ID) left outer join addressTable as "addresstable_2" on ("addresstable_2".ID = "root".ADDRESSID)',
                    'the filter reads manager.name, a qualified property (the first and last name concatenated), and reads through another navigation (address.name)'),
        not_supported("testProjectReferenceInRhsFilterWithDistinctVarNamesViaQualifiedProperty", f"{_SRC}::testProjectReferenceInRhsFilterWithDistinctVarNamesViaQualifiedProperty",
                    'select "root".LEGALNAME as "legalName", "firmtable_1".aggCol as "employeeWithFirmAddress", "addresstable_2".NAME as "address" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, listagg(concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME), \'\') as aggCol from firmTable as "firmtable_2" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_2".ADDRESSID) left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "persontable_0".ADDRESSID) left outer join personTable as "persontable_1" on ("persontable_0".MANAGERID = "persontable_1".ID) where "addresstable_1".NAME is not distinct from "addresstable_0".NAME or concat("persontable_1".FIRSTNAME, \' \', "persontable_1".LASTNAME) = \'\' group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID) left outer join addressTable as "addresstable_2" on ("addresstable_2".ID = "root".ADDRESSID)',
                    QUALIFIED_PROPERTIES),
        not_supported("testProjectReferenceInRhsFilterWithConflictingVarNamesViaQualifiedProperty", f"{_SRC}::testProjectReferenceInRhsFilterWithConflictingVarNamesViaQualifiedProperty",
                    'select "root".LEGALNAME as "legalName", "firmtable_1".aggCol as "employeeWithFirmAddress", "addresstable_2".NAME as "address" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, listagg(concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME), \'\') as aggCol from firmTable as "firmtable_2" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_2".ADDRESSID) left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "persontable_0".ADDRESSID) left outer join personTable as "persontable_1" on ("persontable_0".MANAGERID = "persontable_1".ID) where "addresstable_1".NAME is not distinct from "addresstable_0".NAME or concat("persontable_1".FIRSTNAME, \' \', "persontable_1".LASTNAME) = \'\' group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID) left outer join addressTable as "addresstable_2" on ("addresstable_2".ID = "root".ADDRESSID)',
                    QUALIFIED_PROPERTIES),
        not_supported("testProjectReferenceInRhsFilterWithPotentiallyConflictingVarNamesViaQualifiedProperty", f"{_SRC}::testProjectReferenceInRhsFilterWithPotentiallyConflictingVarNamesViaQualifiedProperty",
                    'select "root".LEGALNAME as "legalName", "firmtable_1".aggCol as "employeeWithFirmAddress", "addresstable_2".NAME as "address" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, listagg(concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME), \'\') as aggCol from firmTable as "firmtable_2" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_2".ADDRESSID) left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "persontable_0".ADDRESSID) left outer join personTable as "persontable_1" on ("persontable_0".MANAGERID = "persontable_1".ID) where "addresstable_1".NAME is not distinct from "addresstable_0".NAME or concat("persontable_1".FIRSTNAME, \' \', "persontable_1".LASTNAME) = \'\' group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID) left outer join addressTable as "addresstable_2" on ("addresstable_2".ID = "root".ADDRESSID)',
                    QUALIFIED_PROPERTIES),
    ],
)
