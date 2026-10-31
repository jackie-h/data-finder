"""
Ported from legend-engine core_relational ``functions/tests/projection/testDateFilters.pure``.

Filtering on dates and timestamps.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported
import datetime

_SRC = "functions/tests/projection/testDateFilters.pure"

DATE_FILTERS_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testDateWithSeconds", f"{_SRC}::testDateWithSeconds",
               'select "root".settlementDateTime as "date" from tradeTable as "root" where ("root".settlementDateTime is not null and "root".settlementDateTime > \'2014-12-04 15:22:24\')',
               lambda f: f.find_all(None, None, [f.settlement_date_time()],
                                    f.settlement_date_time() > datetime.datetime(2014, 12, 4, 15, 22, 24))),
        unsupported("testAdjust", f"{_SRC}::testAdjust",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = dateadd(DAY, 1, \'2014-11-30\')',
                    'no adjust, day-of-week names, or dates relative to now'),
        unsupported("testAdjustWithMicroseconds", f"{_SRC}::testAdjustWithMicroseconds",
                    'select "root".settlementDateTime as "settlementDateTime" from tradeTable as "root" where (("root".settlementDateTime is not null and "root".settlementDateTime > dateadd(MICROSECOND, 123456, \'2014-12-04 15:22:23\')) and ("root".settlementDateTime is not null and "root".settlementDateTime < dateadd(MICROSECOND, 123457, \'2014-12-04 15:22:23\')))',
                    'no adjust, day-of-week names, or dates relative to now'),
        unsupported("testFirstDayOfWeek", f"{_SRC}::testFirstDayOfWeek",
                    'select "root".tradeDate as "date" from tradeTable as "root" where dateadd(DAY, -(mod(dayofweek("root".tradeDate)+5, 7)), "root".tradeDate) = \'2017-09-18\'',
                    'Legend computes the first day with dateadd arithmetic, which DuckDB cannot run; the finder has first_day_of_* (DATE_TRUNC)'),
        unsupported("testFirstDayOfThisMonth", f"{_SRC}::testFirstDayOfThisMonth",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = cast(cast(date_trunc(\'month\', now()) as timestamp) as date)',
                    'no adjust, day-of-week names, or dates relative to now'),
        unsupported("testFirstDayOfMonth", f"{_SRC}::testFirstDayOfMonth",
                    'select "root".tradeDate as "date" from tradeTable as "root" where dateadd(DAY, -(dayofmonth("root".tradeDate) - 1), "root".tradeDate) = \'2017-02-01\'',
                    'Legend computes the first day with dateadd arithmetic, which DuckDB cannot run; the finder has first_day_of_* (DATE_TRUNC)'),
        unsupported("testFirstDayOfThisQuarter", f"{_SRC}::testFirstDayOfThisQuarter",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = cast(cast(date_trunc(\'quarter\', now()) as timestamp) as date)',
                    'no adjust, day-of-week names, or dates relative to now'),
        unsupported("testFirstDayOfQuarter", f"{_SRC}::testFirstDayOfQuarter",
                    'select "root".tradeDate as "date" from tradeTable as "root" where dateadd(MONTH, 3 * quarter("root".tradeDate) - 3, dateadd(DAY, -(dayofyear("root".tradeDate) - 1), "root".tradeDate)) = \'2017-04-01\'',
                    'Legend computes the first day with dateadd arithmetic, which DuckDB cannot run; the finder has first_day_of_* (DATE_TRUNC)'),
        unsupported("testFirstDayOfThisYear", f"{_SRC}::testFirstDayOfThisYear",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = cast(cast(date_trunc(\'year\', now()) as timestamp) as date)',
                    'no adjust, day-of-week names, or dates relative to now'),
        unsupported("testFirstDayOfYear", f"{_SRC}::testFirstDayOfYear",
                    'select "root".tradeDate as "date" from tradeTable as "root" where dateadd(DAY, -(dayofyear("root".tradeDate) - 1), "root".tradeDate) = \'2017-01-01\'',
                    'Legend computes the first day with dateadd arithmetic, which DuckDB cannot run; the finder has first_day_of_* (DATE_TRUNC)'),
        unsupported("testPreviousDayOfWeek", f"{_SRC}::testPreviousDayOfWeek",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = dateadd(day, case when (2 - extract(dow from cast(now() as date))) >= 0 then (2 - extract(dow from cast(now() as date))) - 7 else 2 - extract(dow from cast(now() as date)) end, cast(now() as date))',
                    'no adjust, day-of-week names, or dates relative to now'),
        unsupported("testMostRecentDayOfWeekWithDate", f"{_SRC}::testMostRecentDayOfWeekWithDate",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = dateadd(DAY, case when 2 - DAY_OF_WEEK(\'2014-12-04\') > 0 then 2 - DAY_OF_WEEK(\'2014-12-04\') - 7 else 2 - DAY_OF_WEEK(\'2014-12-04\') end, \'2014-12-04\')',
                    'no adjust, day-of-week names, or dates relative to now'),
        unsupported("testPreviousDayOfWeekWithDate", f"{_SRC}::testPreviousDayOfWeekWithDate",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = dateadd(DAY, case when 2 - DAY_OF_WEEK(\'2014-12-08\') >= 0 then 2 - DAY_OF_WEEK(\'2014-12-08\') - 7 else 2 - DAY_OF_WEEK(\'2014-12-08\') end, \'2014-12-08\')',
                    'no adjust, day-of-week names, or dates relative to now'),
    ],
)
