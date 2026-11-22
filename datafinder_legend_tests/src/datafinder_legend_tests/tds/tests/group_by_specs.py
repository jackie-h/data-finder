"""
Ported from legend-engine core_relational ``tds/tests/testGroupBy.pure``.

Grouping a projection.
Mapping: tests/simple_mapping.md.
"""
from datafinder import constant, count_all, when
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import PERCENTILE_AS_DOUBLE, ROWS, ported, unsupported, DISTINCT_COUNT_QUALIFIED, FLOAT_DIVISION, BACKEND, FLOAT_AVERAGE, AGGREGATE_COLUMN_QUALIFIED, QUALIFIED_PROPERTIES, not_supported
from datafinder_legend_tests.legend import FLOAT_LITERAL_CAST

_SRC = "tds/tests/testGroupBy.pure"

TDS_GROUP_BY_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("simpleGroupByWithJoinStrings", f"{_SRC}::simpleGroupByWithJoinStrings",
               'select "root".FIRSTNAME as "firstName", listagg("root".LASTNAME, \'*\') as "lastNames" from personTable as "root" group by "firstName"',
               lambda f: f.find_all([f.first_name(), f.last_name().join("*")]).group_by(f.first_name())),
        not_supported("simpleGroupDistinctCount", f"{_SRC}::simpleGroupDistinctCount",
                    'select concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME) as "sourceName", count(distinct("root".ID)) as "count" from interactionTable as "root" left outer join personTable as "persontable_0" on ("root".sourceId = "persontable_0".ID) group by "sourceName" order by "count" desc, "sourceName" asc',
                    QUALIFIED_PROPERTIES + ' Also: Interaction is not in the model, and its source name is a derived property'),
        not_supported("simpleGroupDistinctCountWithExtendedPrimitives", f"{_SRC}::simpleGroupDistinctCountWithExtendedPrimitives",
                    'select concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME) as "sourceName", count(distinct("root".ID)) as "count" from interactionTable as "root" left outer join personTable as "persontable_0" on ("root".sourceId = "persontable_0".ID) group by "sourceName" order by "count" desc, "sourceName" asc',
                    QUALIFIED_PROPERTIES + ' Also: Interaction is not in the model, and its source name is a derived property'),
        unsupported("testGroupByWithWavgAggregation", f"{_SRC}::testGroupByWithWavgAggregation",
                    'select dateadd(day, 1, "root".tradeDate) as "tradeDate", count("root".ID) as "ID count", (1.0 * sum("root".quantity * "root".weight)) / sum("root".weight) as "Trade Weighted Average" from weightedTradeTable as "root" group by "tradeDate" order by "tradeDate" asc',
                    'no weighted average aggregate'),
        unsupported("testGroupByWithMultipleWavgAggregation", f"{_SRC}::testGroupByWithMultipleWavgAggregation",
                    'select dateadd(day, 1, "root".tradeDate) as "tradeDate", count("root".ID) as "ID count", (1.0 * sum("root".quantity * "root".weight)) / sum("root".weight) as "Trade Weighted Average", (1.0 * sum("root".quantity * "root".weight2)) / sum("root".weight2) as "Trade Weighted Average1" from weightedTradeTable as "root" group by "tradeDate" order by "tradeDate" asc',
                    'no weighted average aggregate'),
    ],
)


GROUP_BY_TRADE_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testTDSGroupByPercentile", f"{_SRC}::testTDSGroupByPercentile",
               'select "producttable_0".NAME as "prodName", percentile_cont(cast(0.9 as float)) within group (order by "root".quantity + 2 asc) as "p1", percentile_disc(cast(0.5 as float)) within group (order by "root".quantity + 2 desc) as "p2", percentile_cont(cast(0.75 as float)) within group (order by "root".quantity + 2 asc) as "p3" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(),
                                     (f.quantity() + 2).percentile(0.9).alias("p1"),
                                     (f.quantity() + 2).percentile(0.5, ascending=False, continuous=False).alias("p2"),
                                     (f.quantity() + 2).percentile(0.75).alias("p3")])
                         .group_by(f.product().name()).order_by(f.product().name().descending()),
               known_gaps={BACKEND: PERCENTILE_AS_DOUBLE}),
        ported("simpleFilterWithGroupByWithDistinct", f"{_SRC}::simpleFilterWithGroupByWithDistinct",
                    'select "tradetable_0"."quantity" as "quantity", count("quantity") as "qty" from (select distinct "root".quantity as "quantity" from tradeTable as "root" where "root".ID < 4 and "root".quantity > 11) as "tradetable_0" group by "quantity"',
                    lambda f: f.find_all([f.quantity()], (f.id_() < 4).and_(f.quantity() > 11)).distinct().group_by(lambda r: r.quantity).aggregate(qty=lambda r: r.quantity.count()),
               known_gaps={BACKEND: AGGREGATE_COLUMN_QUALIFIED}),
        ported("simpleGroupBySum", f"{_SRC}::simpleGroupBySum",
               'select "producttable_0".NAME as "prodName", sum("root".quantity) as "sum" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), f.quantity().sum()]).group_by(f.product().name()).order_by(f.product().name().descending())),
        ported("simpleGroupByAggFuncAsVariable", f"{_SRC}::simpleGroupByAggFuncAsVariable",
               'select "producttable_0".NAME as "prodName", sum("root".quantity) as "sum" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), f.quantity().sum()]).group_by(f.product().name()).order_by(f.product().name().descending())),
        ported("simpleGroupByAverage", f"{_SRC}::simpleGroupByAverage",
               'select "producttable_0".NAME as "prodName", avg(1.0 * "root".quantity) as "avg" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), f.quantity().average()]).group_by(f.product().name()).order_by(f.product().name().descending()),
               known_gaps={BACKEND: FLOAT_AVERAGE}),
        ported("simpleGroupByMax", f"{_SRC}::simpleGroupByMax",
               'select "producttable_0".NAME as "prodName", max("root".quantity) as "max" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), f.quantity().max()]).group_by(f.product().name()).order_by(f.product().name().descending())),
        ported("simpleGroupByMin", f"{_SRC}::simpleGroupByMin",
               'select "producttable_0".NAME as "prodName", min("root".quantity) as "min" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), f.quantity().min()]).group_by(f.product().name()).order_by(f.product().name().descending())),
        unsupported("simpleGroupByAggFuncNotStopFunc", f"{_SRC}::simpleGroupByAggFuncNotStopFunc",
                    'select "producttable_0".NAME as "prodName", case when count(distinct("producttable_0".NAME)) = 1 then max("producttable_0".NAME) else null end as "unique" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
                    'no uniqueValueOnly aggregation'),
        ported("simpleGroupCount", f"{_SRC}::simpleGroupCount",
               'select "producttable_0".NAME as "prodName", count(*) as "cnt" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), count_all()]).group_by(f.product().name()).order_by(f.product().name().descending())),
        ported("simpleGroupCountUsingVariables", f"{_SRC}::simpleGroupCountUsingVariables",
               'select "producttable_0".NAME as "prodName", count(*) as "cnt" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), count_all()]).group_by(f.product().name()).order_by(f.product().name().descending())),
        ported("simpleGroupByTwo", f"{_SRC}::simpleGroupByTwo",
               'select "producttable_0".NAME as "prodName", count(*) as "count", sum("root".quantity) as "sum" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), count_all(), f.quantity().sum()]).group_by(f.product().name()).order_by(f.product().name().descending())),
        ported("simpleGroupConstantAggregates", f"{_SRC}::simpleGroupConstantAggregates",
               'select "producttable_0".NAME as "prodName", sum(1) as "sum", count(*) as "count" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), constant(1).sum(), count_all()]).group_by(f.product().name()).order_by(f.product().name().descending())),
        ported("simpleGroupByAnd", f"{_SRC}::simpleGroupByAnd",
               'select "producttable_0".NAME as "prodName", bool_and("root".quantity > 20) as "and" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), (f.quantity() > 20).all()]).group_by(f.product().name()).order_by(f.product().name().descending())),
        ported("simpleGroupByOr", f"{_SRC}::simpleGroupByOr",
               'select "producttable_0".NAME as "prodName", bool_or("root".quantity > 50) as "or" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), (f.quantity() > 50).any()]).group_by(f.product().name()).order_by(f.product().name().descending())),
        ported("GroupByWithMapFnConstant", f"{_SRC}::GroupByWithMapFnConstant",
               'select "producttable_0".NAME as "prodName", sum("root".quantity * 3) as "sum" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), (f.quantity() * 3).sum()]).group_by(f.product().name()).order_by(f.product().name().descending())),
        ported("GroupByWithMapFnColumns", f"{_SRC}::GroupByWithMapFnColumns",
               'select "producttable_0".NAME as "prodName", sum("root".quantity * "root".quantity) as "sum" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), (f.quantity() * f.quantity()).sum()]).group_by(f.product().name()).order_by(f.product().name().descending())),
        ported("GroupByWithIfInMap", f"{_SRC}::GroupByWithIfInMap",
               'select "producttable_0".NAME as "prodName", sum(case when "root".quantity > cast(2.0 as float) then "root".quantity else (1.0 * "root".quantity) / 2 end) as "sum" from tradeTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".prodId = "producttable_0".ID) group by "prodName" order by "prodName" desc',
               lambda f: f.find_all([f.product().name(), when(f.quantity() > 2.0).then(f.quantity()).otherwise(f.quantity() / 2).sum()]).group_by(f.product().name()).order_by(f.product().name().descending()),
               known_gaps={BACKEND: FLOAT_DIVISION + '. ' + FLOAT_LITERAL_CAST}),
    ],
)


GROUP_BY_FIRM_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testTDSGroupByEmptyColsNonTerminalOperation", f"{_SRC}::testTDSGroupByEmptyColsNonTerminalOperation",
                    'select "subselect"."Firm Count" as "Firm Count" from (select count(*) as "Firm Count" from firmTable as "root") as "subselect" where "subselect"."Firm Count" > 10',
                    lambda f: f.find_all([f.legal_name().alias("Legal Name")]).aggregate(**{"Firm Count": lambda r: count_all()}).filter(lambda r: getattr(r, "Firm Count") > 10)),
        ported("simpleGroupByWithAssociationWithJoinStrings", f"{_SRC}::simpleGroupByWithAssociationWithJoinStrings",
               'select "root".LEGALNAME as "legalName", listagg("persontable_0".LASTNAME, \'*\') as "lastNames" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID) group by "legalName"',
               lambda f: f.find_all([f.legal_name(), f.employees().last_name().join("*")]).group_by(f.legal_name())),
        ported("groupByAfterASortOnColumnInGroupBy", f"{_SRC}::groupByAfterASortOnColumnInGroupBy",
               'select "root".LEGALNAME as "legalName", listagg("persontable_0".LASTNAME, \'*\') as "lastNames" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID) group by "legalName" order by "legalName" asc',
               lambda f: f.find_all([f.legal_name(), f.employees().last_name().join("*")]).group_by(f.legal_name()).order_by(f.legal_name().ascending())),
        ported("groupByAfterASortOnColumnNotInGroupBy", f"{_SRC}::groupByAfterASortOnColumnNotInGroupBy",
                    'select "firmtable_0"."legalName" as "legalName", listagg("lastName", \'*\') as "lastNames" from (select "root".LEGALNAME as "legalName", "persontable_0".LASTNAME as "lastName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID) order by "lastName" asc) as "firmtable_0" group by "legalName"',
                    lambda f: f.find_all([f.legal_name().alias("legalName"), f.employees().last_name().alias("lastName")]).order_by(f.employees().last_name().ascending()).group_by(lambda r: r.legalName).aggregate(lastNames=lambda r: r.lastName.join("*")),
               known_gaps={BACKEND: AGGREGATE_COLUMN_QUALIFIED}),
        ported("groupByAfterConcatenate", f"{_SRC}::groupByAfterConcatenate",
               'select "unionalias_0"."legalName" as "legalName", count(distinct("lastName")) as "lastNamesCount" from (select distinct "unionalias_1"."legalName" as "legalName", "unionalias_1"."lastName" as "lastName" from (select "root".LEGALNAME as "legalName", "persontable_0".LASTNAME as "lastName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID) union all select "root".LEGALNAME as "legalName", "persontable_0".LASTNAME as "lastName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID)) as "unionalias_1") as "unionalias_0" group by "legalName"',
               lambda f: f.find_all([f.legal_name().alias("legalName"), f.employees().last_name().alias("lastName")]).concatenate(f.find_all([f.legal_name().alias("legalName"), f.employees().last_name().alias("lastName")]), all=True).distinct().group_by(lambda r: r.legalName).aggregate(lastNamesCount=lambda r: r.lastName.count_distinct()),
               known_gaps={BACKEND: DISTINCT_COUNT_QUALIFIED}),
        ported("testTDSGroupByEmptyColsTerminalOperation", f"{_SRC}::testTDSGroupByEmptyColsTerminalOperation",
               'select count(*) as "Firm Count" from firmTable as "root"',
               lambda f: f.find_all([count_all()])),
        ported("testTDSGroupByIsDistinct", f"{_SRC}::testTDSGroupByIsDistinct",
               'select "root".LEGALNAME as "LegalName", count(distinct("persontable_0".FIRSTNAME)) = count("persontable_0".FIRSTNAME) as "FirstNameIsDistinct", count(distinct("persontable_0".LASTNAME)) = count("persontable_0".LASTNAME) as "LastNameIsDistinct" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID) group by "LegalName" order by "LegalName" desc',
               lambda f: f.find_all([f.legal_name(), (f.employees().first_name().count_distinct() == f.employees().first_name().count()).alias("FirstNameIsDistinct"), (f.employees().last_name().count_distinct() == f.employees().last_name().count()).alias("LastNameIsDistinct")]).group_by(f.legal_name()).order_by(f.legal_name().descending())),
    ],
)
