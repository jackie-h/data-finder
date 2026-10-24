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
    ],
)
