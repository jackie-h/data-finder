"""
Ported from legend-engine core_relational ``tests/query/testWithFunction.pure``.

Date part functions.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/query/testWithFunction.pure"

WITH_FUNCTION_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testDayOfWeekFunction", f"{_SRC}::testDayOfWeekFunction",
                    'select formatdatetime("root".tradeDate, \'EEEE\') as "Day Of Week Name" from tradeTable as "root" where formatdatetime("root".tradeDate, \'EEEE\') <> \'Monday\' or formatdatetime("root".tradeDate, \'EEEE\') is null',
                    'no adjust, day-of-week names, or dates relative to now'),
        unsupported("testDayOfWeekNumberFunction", f"{_SRC}::testDayOfWeekNumberFunction",
                    'select extract(dow from "root".tradeDate) as "Day Of Week Number" from tradeTable as "root" where extract(isodow from "root".tradeDate) <> \'2\' or extract(isodow from "root".tradeDate) is null',
                    'no adjust, day-of-week names, or dates relative to now'),
    ],
)


WITH_FUNCTION_SPECS_2 = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testFilterUsingRoundFunctionWithScale", f"{_SRC}::testFilterUsingRoundFunctionWithScale",
                    'select round((1.0 * "root".quantity) / 3, 2) as "roundedQuantity" from tradeTable as "root" where round((1.0 * "root".quantity) / 3, 2) > 10',
                    'functions of an arithmetic expression, or qualified properties'),
        unsupported("testFilterTimesWithManyOperands", f"{_SRC}::testFilterTimesWithManyOperands",
                    'select "root".LASTNAME as "name", "root".AGE * 2 * "firmtable_1".aggCol as "ages" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) left outer join (select "firmtable_2".ID as ID, sum("persontable_1".AGE) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_1" on ("firmtable_2".ID = "persontable_1".FIRMID) group by "firmtable_2".ID) as "firmtable_1" on ("firmtable_0".ID = "firmtable_1".ID)',
                    'functions of an arithmetic expression, or qualified properties'),
    ],
)
