"""
Ported from legend-engine core_relational ``functions/tests/testModelGroupBy.pure``.

Grouping on the model.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported
import datetime

_SRC = "functions/tests/testModelGroupBy.pure"

MODEL_GROUP_BY_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testGroupByOnRootLevelPrimitiveAttributeWithNoJoinAndFilter", f"{_SRC}::testGroupByOnRootLevelPrimitiveAttributeWithNoJoinAndFilter",
               'select "root".tradeDate as "Date", sum("root".quantity) as "Total Quantity" from tradeTable as "root" where "root".tradeDate = \'2014-12-01\' group by "Date"',
               lambda f: f.find_all(None, None, [f.date(), f.quantity().sum()], f.date().eq(datetime.date(2014, 12, 1))).group_by(f.date())),
        unsupported("testUsingAFunctionInTheMapLambdaAndAMultiplyInTheAggregation", f"{_SRC}::testUsingAFunctionInTheMapLambdaAndAMultiplyInTheAggregation",
                    'select "producttable_0".NAME as "prodName", (sum(case when (("producttable_0".NAME is not null and "producttable_0".NAME like \'%X\') or ("producttable_0".NAME is not null and "producttable_0".NAME like \'%C\')) then "root".quantity else 0.0 end) * 2) as "cnt" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
                    'no arithmetic on the result of an aggregate whose argument is a function of the row'),
        unsupported("testUsingAnAssociationInTheMapFunction", f"{_SRC}::testUsingAnAssociationInTheMapFunction",
                    'select "producttable_0".NAME as "prodName", (sum(case when ("accounttable_0".name is not null and "accounttable_0".name like \'Account 1%\') then "root".quantity else 0.0 end) * 2) as "cnt" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) left outer join accountTable as "accounttable_0" on ("root".accountID = "accounttable_0".ID) group by "prodName" order by "prodName" desc',
                    'no conditional inside the argument of an aggregate'),
        unsupported("testUniqueValueOnly1", f"{_SRC}::testUniqueValueOnly1",
                    'select "producttable_0".NAME as "prodName", count(distinct("root".quantity)) as "count", case when count(distinct("root".quantity)) = 1 then max("root".quantity) else null end as "uniqueValue" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) where ("producttable_0".NAME = \'Firm C\' and "root".quantity = 45.0) group by "prodName"',
                    'no uniqueValueOnly aggregation'),
        unsupported("testUniqueValueOnly2", f"{_SRC}::testUniqueValueOnly2",
                    'select "producttable_0".NAME as "prodName", count(distinct("root".quantity)) as "count", case when count(distinct("root".quantity)) = 1 then max("root".quantity) else -1 end as "uniqueValue" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) where ("producttable_0".NAME = \'Firm C\' and "root".quantity = 45.0) group by "prodName"',
                    'no uniqueValueOnly aggregation'),
        unsupported("testObjectLevelGroupByWTDSLevelGroupByWTDSExtend", f"{_SRC}::testObjectLevelGroupByWTDSLevelGroupByWTDSExtend",
                    'select "persontable_0"."FirstName" as "FirstName", "persontable_0"."Case Column" as "Case Column", group_concat("LastName" separator \',\') as "Joined Last Name" from (select "root".FIRSTNAME as "FirstName", "root".LASTNAME as "LastName", "firmtable_0".LEGALNAME as "FirmName", avg(1.0 * "root".AGE) as "Average Age 1", case when avg(1.0 * "root".AGE) < 10 then 10 else (avg(1.0 * "root".AGE) * 0.9) end as "Case Column" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) group by "FirstName","LastName","FirmName" having "firmtable_0".LEGALNAME = \'Firm X\') as "persontable_0" group by "FirstName","Case Column"',
                    'a group by over a group by and extend is nested in subselects'),
        unsupported("testReprocessGroupByAlias", f"{_SRC}::testReprocessGroupByAlias",
                    'select "salespersontable_0".lastName as "last name", "salespersontable_0".firm_name as "legal name", sum("root".quantity) as "quantity" from orderTable as "root" left outer join (select "salespersontable_0".ACCOUNT_ID as ACCOUNT_ID, "personfirmview_0".lastName as lastName, "personfirmview_0".firm_name as firm_name from salesPersonTable as "salespersontable_0" inner join (select "root".ID as PERSON_ID, "root".LASTNAME as lastName, "firmtable_0".LEGALNAME as firm_name from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID)) as "personfirmview_0" on ("salespersontable_0".PERSON_ID = "personfirmview_0".PERSON_ID)) as "salespersontable_0" on ("root".accountID = "salespersontable_0".ACCOUNT_ID) group by "last name", "legal name"',
                    'Order is mapped through a view, which is not supported'),
    ],
)
