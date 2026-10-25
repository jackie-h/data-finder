"""
Ported from legend-engine core_relational ``functions/tests/projection/testQualifier.pure``.

Qualified (derived) properties.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/projection/testQualifier.pure"

QUALIFIER_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testSimpleDerived", f"{_SRC}::testSimpleDerived",
                    'select "root".FIRSTNAME as "firstName", concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "name" from personTable as "root"',
                    'no qualified (derived) properties'),
        unsupported("testDerivedWithFiltering", f"{_SRC}::testDerivedWithFiltering",
                    'select "root".NAME as "name", "synonymtable_0".NAME as "cusip" from productSchema.productTable as "root" left outer join productSchema.synonymTable as "synonymtable_0" on ("synonymtable_0".PRODID = "root".ID and "synonymtable_0".TYPE = \'CUSIP\')',
                    'no qualified (derived) properties'),
        unsupported("testDerivedWithFilteringTwoProperties", f"{_SRC}::testDerivedWithFilteringTwoProperties",
                    'select "root".NAME as "name", "synonymtable_0".NAME as "cusip", "synonymtable_1".NAME as "isin" from productSchema.productTable as "root" left outer join productSchema.synonymTable as "synonymtable_0" on ("synonymtable_0".PRODID = "root".ID and "synonymtable_0".TYPE = \'CUSIP\') left outer join productSchema.synonymTable as "synonymtable_1" on ("synonymtable_1".PRODID = "root".ID and "synonymtable_1".TYPE = \'ISIN\')',
                    'no qualified (derived) properties'),
        unsupported("testQualifierWithFilteringAndParameters", f"{_SRC}::testQualifierWithFilteringAndParameters",
                    'select "root".NAME as "name", "synonymtable_0".NAME as "cusip" from productSchema.productTable as "root" left outer join productSchema.synonymTable as "synonymtable_0" on ("synonymtable_0".PRODID = "root".ID and "synonymtable_0".TYPE = \'CUSIP\')',
                    'no qualified (derived) properties'),
        unsupported("testQualifierWithClassAsParameter", f"{_SRC}::testQualifierWithClassAsParameter",
                    'select case when true then concat(\'Prefix_\', \' \', "root".LASTNAME, \', \', "root".FIRSTNAME) else concat("root".FIRSTNAME, \' \', "root".LASTNAME) end as "a" from personTable as "root"',
                    'no qualified (derived) properties'),
        unsupported("testTwoQualifiersUsingSameJoinWithNoUserParams", f"{_SRC}::testTwoQualifiersUsingSameJoinWithNoUserParams",
                    'select "synonymtable_0".NAME as "isin", "synonymtable_1".NAME as "cusip" from productSchema.productTable as "root" left outer join productSchema.synonymTable as "synonymtable_0" on ("synonymtable_0".PRODID = "root".ID and "synonymtable_0".TYPE = \'ISIN\') left outer join productSchema.synonymTable as "synonymtable_1" on ("synonymtable_1".PRODID = "root".ID and "synonymtable_1".TYPE = \'CUSIP\')',
                    'no qualified (derived) properties'),
        unsupported("testQualifierInLambdaDeep", f"{_SRC}::testQualifierInLambdaDeep",
                    'select "synonymtable_0".NAME as "name" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) left outer join (select "synonymtable_1".PRODID as PRODID, "synonymtable_1".NAME as NAME from productSchema.synonymTable as "synonymtable_1" where "synonymtable_1".TYPE = \'CUSIP\') as "synonymtable_0" on ("synonymtable_0".PRODID = "producttable_0".ID)',
                    'no qualified (derived) properties'),
        unsupported("testQualifierBeforeProject", f"{_SRC}::testQualifierBeforeProject",
                    'select "synonymtable_0".NAME as "name" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) left outer join productSchema.synonymTable as "synonymtable_0" on ("synonymtable_0".PRODID = "producttable_0".ID) where "synonymtable_0".TYPE = \'CUSIP\'',
                    'no qualified (derived) properties'),
        unsupported("testQualifierWithFilterWithDataTypeResult", f"{_SRC}::testQualifierWithFilterWithDataTypeResult",
                    'select "tradeeventtable_0".eventType as "tradeDateEventTypeInlined" from tradeTable as "root" left outer join tradeEventTable as "tradeeventtable_0" on ("root".ID = "tradeeventtable_0".trade_id and "tradeeventtable_0".eventDate = "root".tradeDate)',
                    'no qualified (derived) properties'),
        unsupported("testQualifierFunctionConsistencyWithDataTypeProperty", f"{_SRC}::testQualifierFunctionConsistencyWithDataTypeProperty",
                    'select "tradeeventtable_0".eventType as "tradeDateEventType" from tradeTable as "root" left outer join tradeEventTable as "tradeeventtable_0" on ("root".ID = "tradeeventtable_0".trade_id and "tradeeventtable_0".eventDate = "root".tradeDate)',
                    'no qualified (derived) properties'),
        unsupported("testQualifierFunctionConsistencyWithComplexTypeProperty", f"{_SRC}::testQualifierFunctionConsistencyWithComplexTypeProperty",
                    'select "persontable_0".FIRSTNAME as "initiator_firstname" from tradeTable as "root" left outer join tradeEventTable as "tradeeventtable_0" on ("root".ID = "tradeeventtable_0".trade_id and "tradeeventtable_0".eventDate = "root".tradeDate) left outer join personTable as "persontable_0" on ("tradeeventtable_0".person_id = "persontable_0".ID)',
                    'no qualified (derived) properties'),
        unsupported("testFilterInQualifierWithFilterInMappingInAggGroupBy", f"{_SRC}::testFilterInQualifierWithFilterInMappingInAggGroupBy",
                    'select "root".NAME as "name", count("synonymtable_0".NAME) as "cusip" from productSchema.productTable as "root" left outer join productSchema.synonymTable as "synonymtable_0" on ("synonymtable_0".PRODID = "root".ID and "synonymtable_0".TYPE = \'CUSIP\' and "synonymtable_0".ID <> 1) group by "name" order by "name" asc',
                    'no qualified (derived) properties'),
        unsupported("testFilterInQualifierWithFilterInMappingInProjectGroupBy", f"{_SRC}::testFilterInQualifierWithFilterInMappingInProjectGroupBy",
                    'select "synonymtable_0".NAME as "cusip", count("root".NAME) as "count" from productSchema.productTable as "root" left outer join productSchema.synonymTable as "synonymtable_0" on ("synonymtable_0".PRODID = "root".ID and "synonymtable_0".TYPE = \'CUSIP\' and "synonymtable_0".ID <> 1) group by "cusip" order by "cusip" asc',
                    'no qualified (derived) properties'),
        unsupported("testQualifierWithVariableArg", f"{_SRC}::testQualifierWithVariableArg",
                    'select "root".LEGALNAME as "firmId", concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME) as "employeeWithFirmLastName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "persontable_0".LASTNAME = "root".LEGALNAME)',
                    'no qualified (derived) properties'),
        unsupported("testQualifierWithVariableArgReferencedFirstInFilterEqualCriteria", f"{_SRC}::testQualifierWithVariableArgReferencedFirstInFilterEqualCriteria",
                    'select "root".LEGALNAME as "firmId", concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME) as "employeeWithFirmLastName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "root".LEGALNAME = "persontable_0".LASTNAME)',
                    'no qualified (derived) properties'),
        unsupported("testQualifierWithVariableArgWithComplexTypeProperty", f"{_SRC}::testQualifierWithVariableArgWithComplexTypeProperty",
                    'select "root".LEGALNAME as "legalName", "firmtable_1".aggCol as "employeeWithFirmAddress", "addresstable_2".NAME as "address" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, listagg(concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME), \',\') as aggCol from firmTable as "firmtable_2" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_2".ADDRESSID) left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "persontable_0".ADDRESSID) left outer join personTable as "persontable_1" on ("persontable_0".MANAGERID = "persontable_1".ID) where "addresstable_1".NAME = "addresstable_0".NAME or ("addresstable_1".NAME is null and "addresstable_0".NAME is null) or concat("persontable_1".FIRSTNAME, \' \', "persontable_1".LASTNAME) = \'\' group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID) left outer join addressTable as "addresstable_2" on ("addresstable_2".ID = "root".ADDRESSID)',
                    'no qualified (derived) properties'),
        unsupported("testQualifierWithVariableArgWithComplexTypePropertyAndSubsequentComplexTypePropertyCall", f"{_SRC}::testQualifierWithVariableArgWithComplexTypePropertyAndSubsequentComplexTypePropertyCall",
                    'select "root".LEGALNAME as "legalName", "addresstable_2".NAME as "addressFor-employeeWithFirmAddressCity" from firmTable as "root" left outer join (select "persontable_0".ADDRESSID as ADDRESSID, "firmtable_2".ID as ID from firmTable as "firmtable_2" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_2".ADDRESSID) left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "persontable_0".ADDRESSID) left outer join personTable as "persontable_1" on ("persontable_0".MANAGERID = "persontable_1".ID) where "addresstable_1".NAME = "addresstable_0".NAME or ("addresstable_1".NAME is null and "addresstable_0".NAME is null) or concat("persontable_1".FIRSTNAME, \' \', "persontable_1".LASTNAME) = \'\') as "firmtable_1" on ("root".ID = "firmtable_1".ID) left outer join addressTable as "addresstable_2" on ("addresstable_2".ID = "firmtable_1".ADDRESSID)',
                    'no qualified (derived) properties'),
        unsupported("testFilterOnRootTypeWithOrWithFilterInQualifier", f"{_SRC}::testFilterOnRootTypeWithOrWithFilterInQualifier",
                    'select "root".ID as "id", "accounttable_0".name as "acc" from tradeTable as "root" left outer join tradeEventTable as "tradeeventtable_0" on ("root".ID = "tradeeventtable_0".trade_id and "tradeeventtable_0".trade_id = \'1\' and "tradeeventtable_0".eventDate = "root".tradeDate) left outer join accountTable as "accounttable_0" on ("root".accountID = "accounttable_0".ID) where ("tradeeventtable_0".eventType is not null or "accounttable_0".name = \'Account 1\') and "root".ID = \'1\'',
                    'no qualified (derived) properties'),
    ],
)

QUALIFIER_FIRM_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testSimpleDerivedThroughAssociation", f"{_SRC}::testSimpleDerivedThroughAssociation",
                    'select "root".LEGALNAME as "legalName", concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME) as "name" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID)',
                    'no qualified (derived) properties'),
    ],
)
