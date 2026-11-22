"""
Ported from legend-engine core_relational ``functions/tests/projection/testDateFilters.pure``.

Filtering on dates and timestamps.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder import DayOfWeek, constant, first_day_of_this_month, first_day_of_this_quarter, first_day_of_this_year, previous_day_of_week
from datafinder_legend_tests.legend import BACKEND, DATE_ARITHMETIC_SHAPE, EXTRACT_DOW_FROM_SUNDAY_AS_ONE, ROWS, ported, unsupported
from datafinder_legend_tests.legend import TYPED_DATE_LITERAL
import datetime

_SRC = "functions/tests/projection/testDateFilters.pure"

DATE_FILTERS_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testDateWithSeconds", f"{_SRC}::testDateWithSeconds",
               'select "root".settlementDateTime as "date" from tradeTable as "root" where "root".settlementDateTime is not null and "root".settlementDateTime > TIMESTAMP\'2014-12-04 15:22:24\'',
               lambda f: f.find_all([f.settlement_date_time()],
                                    f.settlement_date_time() > datetime.datetime(2014, 12, 4, 15, 22, 24)),
               known_gaps={BACKEND: TYPED_DATE_LITERAL}),
        ported("testAdjust", f"{_SRC}::testAdjust",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = dateadd(day, 1, DATE\'2014-11-30\')',
                    lambda f: f.find_all([f.date().alias("date")], f.date().eq(constant(datetime.date(2014, 11, 30)).add_days(1))),
                    known_gaps={BACKEND: DATE_ARITHMETIC_SHAPE}),
        ported("testAdjustWithMicroseconds", f"{_SRC}::testAdjustWithMicroseconds",
                    'select "root".settlementDateTime as "settlementDateTime" from tradeTable as "root" where "root".settlementDateTime is not null and "root".settlementDateTime > dateadd(microsecond, 123456, TIMESTAMP\'2014-12-04 15:22:23\') and "root".settlementDateTime is not null and "root".settlementDateTime < dateadd(microsecond, 123457, TIMESTAMP\'2014-12-04 15:22:23\')',
                    lambda f: f.find_all([f.settlement_date_time().alias("settlementDateTime")], (f.settlement_date_time() > constant(datetime.datetime(2014, 12, 4, 15, 22, 23)).add_microseconds(123456)).and_(f.settlement_date_time() < constant(datetime.datetime(2014, 12, 4, 15, 22, 23)).add_microseconds(123457))),
                    known_gaps={BACKEND: DATE_ARITHMETIC_SHAPE}),
        ported("testFirstDayOfWeek", f"{_SRC}::testFirstDayOfWeek",
                    'select "root".tradeDate as "date" from tradeTable as "root" where cast(cast(date_trunc(\'week\', "root".tradeDate) as timestamp) as date) = DATE\'2017-09-18\'',
                    lambda f: f.find_all([f.date().alias("date")], f.date().first_day_of_week().eq(datetime.date(2017, 9, 18))),
                    known_gaps={BACKEND: DATE_ARITHMETIC_SHAPE}),
        ported("testFirstDayOfThisMonth", f"{_SRC}::testFirstDayOfThisMonth",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = cast(cast(date_trunc(\'month\', now()) as timestamp) as date)',
                    lambda f: f.find_all([f.date().alias("date")], f.date().eq(first_day_of_this_month())),
                    known_gaps={BACKEND: DATE_ARITHMETIC_SHAPE}),
        ported("testFirstDayOfMonth", f"{_SRC}::testFirstDayOfMonth",
                    'select "root".tradeDate as "date" from tradeTable as "root" where cast(cast(date_trunc(\'month\', "root".tradeDate) as timestamp) as date) = DATE\'2017-02-01\'',
                    lambda f: f.find_all([f.date().alias("date")], f.date().first_day_of_month().eq(datetime.date(2017, 2, 1))),
                    known_gaps={BACKEND: DATE_ARITHMETIC_SHAPE}),
        ported("testFirstDayOfThisQuarter", f"{_SRC}::testFirstDayOfThisQuarter",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = cast(cast(date_trunc(\'quarter\', now()) as timestamp) as date)',
                    lambda f: f.find_all([f.date().alias("date")], f.date().eq(first_day_of_this_quarter())),
                    known_gaps={BACKEND: DATE_ARITHMETIC_SHAPE}),
        ported("testFirstDayOfQuarter", f"{_SRC}::testFirstDayOfQuarter",
                    'select "root".tradeDate as "date" from tradeTable as "root" where cast(cast(date_trunc(\'quarter\', "root".tradeDate) as timestamp) as date) = DATE\'2017-04-01\'',
                    lambda f: f.find_all([f.date().alias("date")], f.date().first_day_of_quarter().eq(datetime.date(2017, 4, 1))),
                    known_gaps={BACKEND: DATE_ARITHMETIC_SHAPE}),
        ported("testFirstDayOfThisYear", f"{_SRC}::testFirstDayOfThisYear",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = cast(cast(date_trunc(\'year\', now()) as timestamp) as date)',
                    lambda f: f.find_all([f.date().alias("date")], f.date().eq(first_day_of_this_year())),
                    known_gaps={BACKEND: DATE_ARITHMETIC_SHAPE}),
        ported("testFirstDayOfYear", f"{_SRC}::testFirstDayOfYear",
                    'select "root".tradeDate as "date" from tradeTable as "root" where cast(cast(date_trunc(\'year\', "root".tradeDate) as timestamp) as date) = DATE\'2017-01-01\'',
                    lambda f: f.find_all([f.date().alias("date")], f.date().first_day_of_year().eq(datetime.date(2017, 1, 1))),
                    known_gaps={BACKEND: DATE_ARITHMETIC_SHAPE}),
        ported("testPreviousDayOfWeek", f"{_SRC}::testPreviousDayOfWeek",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = dateadd(day, case when (2 - extract(dow from cast(now() as date))) >= 0 then (2 - extract(dow from cast(now() as date))) - 7 else 2 - extract(dow from cast(now() as date)) end, cast(now() as date))',
                    lambda f: f.find_all([f.date().alias("date")], f.date().eq(previous_day_of_week(DayOfWeek.MONDAY))),
                    known_gaps={BACKEND: DATE_ARITHMETIC_SHAPE}),
        ported("testMostRecentDayOfWeekWithDate", f"{_SRC}::testMostRecentDayOfWeekWithDate",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = dateadd(day, case when (2 - extract(dow from DATE\'2014-12-04\')) > 0 then (2 - extract(dow from DATE\'2014-12-04\')) - 7 else 2 - extract(dow from DATE\'2014-12-04\') end, DATE\'2014-12-04\')',
                    lambda f: f.find_all([f.date().alias("date")], f.date().eq(constant(datetime.date(2014, 12, 4)).most_recent_day_of_week(DayOfWeek.MONDAY))),
                    known_gaps={BACKEND: DATE_ARITHMETIC_SHAPE, ROWS: EXTRACT_DOW_FROM_SUNDAY_AS_ONE}),
        ported("testPreviousDayOfWeekWithDate", f"{_SRC}::testPreviousDayOfWeekWithDate",
                    'select "root".tradeDate as "date" from tradeTable as "root" where "root".tradeDate = dateadd(day, case when (2 - extract(dow from DATE\'2014-12-08\')) >= 0 then (2 - extract(dow from DATE\'2014-12-08\')) - 7 else 2 - extract(dow from DATE\'2014-12-08\') end, DATE\'2014-12-08\')',
                    lambda f: f.find_all([f.date().alias("date")], f.date().eq(constant(datetime.date(2014, 12, 8)).previous_day_of_week(DayOfWeek.MONDAY))),
                    known_gaps={BACKEND: DATE_ARITHMETIC_SHAPE, ROWS: EXTRACT_DOW_FROM_SUNDAY_AS_ONE}),
    ],
)
