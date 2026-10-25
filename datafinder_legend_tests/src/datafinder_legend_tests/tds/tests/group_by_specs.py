"""
Ported from legend-engine core_relational ``tds/tests/testGroupBy.pure``.

Grouping a projection.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported, BACKEND, JOIN_OPERAND_ORDER, JOIN_OPERAND_ORDER_AND_FLOAT_AVERAGE

_SRC = "tds/tests/testGroupBy.pure"

TDS_GROUP_BY_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("simpleGroupByWithJoinStrings", f"{_SRC}::simpleGroupByWithJoinStrings",
               'select "root".FIRSTNAME as "firstName", listagg("root".LASTNAME, \'*\') as "lastNames" from personTable as "root" group by "firstName"',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name().join("*")]).group_by(f.first_name())),
        unsupported("simpleGroupDistinctCount", f"{_SRC}::simpleGroupDistinctCount",
                    'select concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME) as "sourceName", count(distinct("root".ID)) as "count" from interactionTable as "root" left outer join personTable as "persontable_0" on ("root".sourceId = "persontable_0".ID) group by "sourceName" order by "count" desc, "sourceName" asc',
                    'Interaction is not in the model, and its source name is a derived property'),
        unsupported("simpleGroupDistinctCountWithExtendedPrimitives", f"{_SRC}::simpleGroupDistinctCountWithExtendedPrimitives",
                    'select concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME) as "sourceName", count(distinct("root".ID)) as "count" from interactionTable as "root" left outer join personTable as "persontable_0" on ("root".sourceId = "persontable_0".ID) group by "sourceName" order by "count" desc, "sourceName" asc',
                    'Interaction is not in the model, and its source name is a derived property'),
    ],
)


GROUP_BY_TRADE_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("simpleGroupCount", f"{_SRC}::simpleGroupCount",
                    'select "producttable_0".NAME as "prodName", count(*) as "cnt" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
                    'no count(*) in the finder API'),
        unsupported("simpleGroupCountUsingVariables", f"{_SRC}::simpleGroupCountUsingVariables",
                    'select "producttable_0".NAME as "prodName", count(*) as "cnt" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
                    'no count(*) in the finder API'),
        unsupported("simpleFilterWithGroupByWithDistinct", f"{_SRC}::simpleFilterWithGroupByWithDistinct",
                    'select "tradetable_0"."quantity" as "quantity", count("quantity") as "qty" from (select distinct "root".quantity as "quantity" from tradeTable as "root" where "root".ID < 4 and "root".quantity > 11) as "tradetable_0" group by "quantity"',
                    'a group by over a distinct is nested in a subselect'),
        ported("simpleGroupBySum", f"{_SRC}::simpleGroupBySum",
               'select "producttable_0".NAME as "prodName", sum("root".quantity) as "sum" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all(None, None, [f.product().name(), f.quantity().sum()]).group_by(f.product().name()).order_by(f.product().name().descending()),
               known_gaps={BACKEND: JOIN_OPERAND_ORDER}),
        unsupported("simpleGroupConstantAggregates", f"{_SRC}::simpleGroupConstantAggregates",
                    'select "producttable_0".NAME as "prodName", sum(1) as "sum", count(*) as "count" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
                    'no aggregate of a constant'),
        ported("simpleGroupByAggFuncAsVariable", f"{_SRC}::simpleGroupByAggFuncAsVariable",
               'select "producttable_0".NAME as "prodName", sum("root".quantity) as "sum" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all(None, None, [f.product().name(), f.quantity().sum()]).group_by(f.product().name()).order_by(f.product().name().descending()),
               known_gaps={BACKEND: JOIN_OPERAND_ORDER}),
        ported("simpleGroupByAverage", f"{_SRC}::simpleGroupByAverage",
               'select "producttable_0".NAME as "prodName", avg(1.0 * "root".quantity) as "avg" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all(None, None, [f.product().name(), f.quantity().average()]).group_by(f.product().name()).order_by(f.product().name().descending()),
               known_gaps={BACKEND: JOIN_OPERAND_ORDER_AND_FLOAT_AVERAGE}),
        unsupported("simpleGroupByTwo", f"{_SRC}::simpleGroupByTwo",
                    'select "producttable_0".NAME as "prodName", count(*) as "count", sum("root".quantity) as "sum" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
                    'no count(*) in the finder API'),
        ported("simpleGroupByMax", f"{_SRC}::simpleGroupByMax",
               'select "producttable_0".NAME as "prodName", max("root".quantity) as "max" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all(None, None, [f.product().name(), f.quantity().max()]).group_by(f.product().name()).order_by(f.product().name().descending()),
               known_gaps={BACKEND: JOIN_OPERAND_ORDER}),
        ported("simpleGroupByMin", f"{_SRC}::simpleGroupByMin",
               'select "producttable_0".NAME as "prodName", min("root".quantity) as "min" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all(None, None, [f.product().name(), f.quantity().min()]).group_by(f.product().name()).order_by(f.product().name().descending()),
               known_gaps={BACKEND: JOIN_OPERAND_ORDER}),
        unsupported("simpleGroupByAnd", f"{_SRC}::simpleGroupByAnd",
                    'select "producttable_0".NAME as "prodName", bool_and("root".quantity > 20) as "and" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
                    'no boolean and/or aggregation'),
        unsupported("simpleGroupByOr", f"{_SRC}::simpleGroupByOr",
                    'select "producttable_0".NAME as "prodName", bool_or("root".quantity > 50) as "or" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
                    'no boolean and/or aggregation'),
        unsupported("GroupByWithMapFnConstant", f"{_SRC}::GroupByWithMapFnConstant",
                    'select "producttable_0".NAME as "prodName", sum("root".quantity * 3) as "sum" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
                    'no aggregate of an expression'),
        unsupported("GroupByWithMapFnColumns", f"{_SRC}::GroupByWithMapFnColumns",
                    'select "producttable_0".NAME as "prodName", sum("root".quantity * "root".quantity) as "sum" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
                    'no aggregate of an expression'),
        unsupported("GroupByWithIfInMap", f"{_SRC}::GroupByWithIfInMap",
                    'select "producttable_0".NAME as "prodName", sum(case when "root".quantity > 2.0 then "root".quantity else ((1.0 * "root".quantity) / 2) end) as "sum" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
                    'no aggregate of an expression'),
        unsupported("simpleGroupByAggFuncNotStopFunc", f"{_SRC}::simpleGroupByAggFuncNotStopFunc",
                    'select "producttable_0".NAME as "prodName", case when count(distinct("producttable_0".NAME)) = 1 then max("producttable_0".NAME) else null end as "unique" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
                    'no uniqueValueOnly aggregation'),
    ],
)


GROUP_BY_FIRM_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("simpleGroupByWithAssociationWithJoinStrings", f"{_SRC}::simpleGroupByWithAssociationWithJoinStrings",
               'select "root".LEGALNAME as "legalName", listagg("persontable_0".LASTNAME, \'*\') as "lastNames" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID) group by "legalName"',
               lambda f: f.find_all(None, None, [f.legal_name(), f.employees().last_name().join("*")]).group_by(f.legal_name())),
        ported("groupByAfterASortOnColumnInGroupBy", f"{_SRC}::groupByAfterASortOnColumnInGroupBy",
               'select "root".LEGALNAME as "legalName", listagg("persontable_0".LASTNAME, \'*\') as "lastNames" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID) group by "legalName" order by "legalName" asc',
               lambda f: f.find_all(None, None, [f.legal_name(), f.employees().last_name().join("*")]).group_by(f.legal_name()).order_by(f.legal_name().ascending())),
        unsupported("groupByAfterASortOnColumnNotInGroupBy", f"{_SRC}::groupByAfterASortOnColumnNotInGroupBy",
                    'select "firmtable_0"."legalName" as "legalName", listagg("lastName", \'*\') as "lastNames" from (select "root".LEGALNAME as "legalName", "persontable_0".LASTNAME as "lastName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID) order by "lastName" asc) as "firmtable_0" group by "legalName"',
                    'a sort before a group by on a column that is not grouped is nested in a subselect'),
        unsupported("groupByAfterConcatenate", f"{_SRC}::groupByAfterConcatenate",
                    'select "unionalias_0"."legalName" as "legalName", count(distinct("lastName")) as "lastNamesCount" from (select distinct "unionalias_1"."legalName" as "legalName", "unionalias_1"."lastName" as "lastName" from (select "root".LEGALNAME as "legalName", "persontable_0".LASTNAME as "lastName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID) union all select "root".LEGALNAME as "legalName", "persontable_0".LASTNAME as "lastName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID)) as "unionalias_1") as "unionalias_0" group by "legalName"',
                    'no concatenate / union'),
    ],
)
