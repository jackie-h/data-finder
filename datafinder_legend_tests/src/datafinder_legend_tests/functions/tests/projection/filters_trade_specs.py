"""
Ported from legend-engine core_relational ``functions/tests/projection/testFilters.pure``.

Comparison filters on a numeric property.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/projection/testFilters.pure"

FILTERS_TRADE_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testGreaterThan", f"{_SRC}::testGreaterThan",
               'select "root".quantity as "quantity" from tradeTable as "root" where "root".quantity > 45',
               lambda f: f.find_all(None, None, [f.quantity()], f.quantity() > 45)),
        ported("testGreaterThanEqual", f"{_SRC}::testGreaterThanEqual",
               'select "root".quantity as "quantity" from tradeTable as "root" where "root".quantity >= 45',
               lambda f: f.find_all(None, None, [f.quantity()], f.quantity() >= 45)),
        ported("testLessThan", f"{_SRC}::testLessThan",
               'select "root".quantity as "quantity" from tradeTable as "root" where "root".quantity < 45',
               lambda f: f.find_all(None, None, [f.quantity()], f.quantity() < 45)),
        ported("testLessThanFilterAsVariable", f"{_SRC}::testLessThanFilterAsVariable",
               'select "root".quantity as "quantity" from tradeTable as "root" where "root".quantity < 45',
               lambda f: f.find_all(None, None, [f.quantity()], f.quantity() < 45)),
        ported("testLessThanEqual", f"{_SRC}::testLessThanEqual",
               'select "root".quantity as "quantity" from tradeTable as "root" where "root".quantity <= 45',
               lambda f: f.find_all(None, None, [f.quantity()], f.quantity() <= 45)),
        unsupported("testIsolationOfVariableNamesAccrossDifferentPaths", f"{_SRC}::testIsolationOfVariableNamesAccrossDifferentPaths",
                    'select case when false then concat("firmtable_1".FIRSTNAME, \' \', "firmtable_1".LASTNAME) else "root".LEGALNAME end as "name" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, "persontable_0".FIRSTNAME as FIRSTNAME, "persontable_0".LASTNAME as LASTNAME from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_2".ADDRESSID) where concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME) = "addresstable_0".NAME) as "firmtable_1" on ("root".ID = "firmtable_1".ID)',
                    'no qualified (derived) properties'),
        unsupported("testIsolatioWhereNoConstaintsAndInnerJoin", f"{_SRC}::testIsolatioWhereNoConstaintsAndInnerJoin",
                    'select "root".LEGALNAME as "legalName", case when "firmpersonbridgetable_0".ADDRESSID = 1 or "addresstable_0".ID = 1 then \'UK\' else \'Europe\' end as "addressName" from firmTable as "root" left outer join (select "firmpersonbridgetable_1".FIRM_ID as FIRM_ID, "persontable_0".ADDRESSID as ADDRESSID from firmPersonBridgeTable as "firmpersonbridgetable_1" inner join personTable as "persontable_0" on ("persontable_0".ID = "firmpersonbridgetable_1".PERSON_ID)) as "firmpersonbridgetable_0" on ("root".ID = "firmpersonbridgetable_0".FIRM_ID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "root".ADDRESSID)',
                    'no qualified (derived) properties'),
        unsupported("testIsolationOfFiltersWithoutAlias", f"{_SRC}::testIsolationOfFiltersWithoutAlias",
                    'select "addresstable_0".NAME as "address", "persontable_0".FIRSTNAME as "employeeFirstName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and \'Smith\' = \'Smith\') left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "persontable_0".ADDRESSID)',
                    'no qualified (derived) properties'),
        unsupported("testIsolationOfFiltersWithoutAliasWithChainedJoins", f"{_SRC}::testIsolationOfFiltersWithoutAliasWithChainedJoins",
                    'select "root".ID as "tradeId", "synonymtable_0".NAME as "SynonyName1", "synonymtable_2".NAME as "SynonymName2" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) left outer join (select "synonymtable_1".PRODID as PRODID, "synonymtable_1".NAME as NAME from productSchema.synonymTable as "synonymtable_1" where \'CUSIP\' = \'CUSIP\') as "synonymtable_0" on ("synonymtable_0".PRODID = "producttable_0".ID) left outer join (select "synonymtable_1".PRODID as PRODID, "synonymtable_1".NAME as NAME from productSchema.synonymTable as "synonymtable_1" where \'CUSIP\' = \'ISIN\') as "synonymtable_2" on ("synonymtable_2".PRODID = "producttable_0".ID)',
                    'no qualified (derived) properties'),
    ],
)
