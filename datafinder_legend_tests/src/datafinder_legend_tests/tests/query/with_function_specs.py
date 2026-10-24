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
                    'no date functions such as adjust or firstDayOf...'),
        unsupported("testDayOfWeekNumberFunction", f"{_SRC}::testDayOfWeekNumberFunction",
                    'select extract(dow from "root".tradeDate) as "Day Of Week Number" from tradeTable as "root" where extract(isodow from "root".tradeDate) <> \'2\' or extract(isodow from "root".tradeDate) is null',
                    'no date functions such as adjust or firstDayOf...'),
    ],
)
