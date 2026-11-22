"""
Ported from legend-engine core_relational ``tests/query/datePeriods.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/query/datePeriods.pure"


DATE_PERIODS_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testProjectWithSeparateGroupBy", f"{_SRC}::testProjectWithSeparateGroupBy",
                    'select "org_chart_entity_0".name as "Sales Division", "income_function_0".code as "Income Function", sum(case when "calendar_0"."fiscal day" <= 37 then cast(1.0 as float) else cast(0.0 as float) end * "root".credits) as "YTD Gross Credits", sum(case when extract(week from "calendar_0"."date") = extract(week from DATE\'2015-02-25\') and extract(dow from "calendar_0"."date") <= extract(dow from DATE\'2015-02-25\') then cast(1.0 as float) else cast(0.0 as float) end * "root".credits) as "WTD Gross Credits" from SALES_GCS as "root" left outer join ORG_CHART_ENTITY as "org_chart_entity_0" on ("root".division_id = "org_chart_entity_0".oe_id) left outer join INCOME_FUNCTION as "income_function_0" on ("root".if_code = "income_function_0".code) left outer join calendar as "calendar_0" on ("root".tradeDate = "calendar_0"."date") where "calendar_0"."date" > DATE\'2015-02-01\' and "calendar_0"."date" <= DATE\'2015-02-25\' group by "Sales Division", "Income Function" order by "Sales Division" desc, "Income Function" desc',
                    'date period functions'),
        unsupported("testDayOfWeek", f"{_SRC}::testDayOfWeek",
                    'select "org_chart_entity_0".name as "name" from SALES_GCS as "root" left outer join calendar as "calendar_0" on ("root".tradeDate = "calendar_0"."date") left outer join ORG_CHART_ENTITY as "org_chart_entity_0" on ("root".division_id = "org_chart_entity_0".oe_id) where "calendar_0"."date" > DATE\'2022-02-21\' and "calendar_0"."date" <= DATE\'2022-03-25\'',
                    'date period functions'),
    ],
)
