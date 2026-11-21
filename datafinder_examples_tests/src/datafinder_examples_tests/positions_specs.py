"""
Test specifications for date functions using positions_mapping.md.

    positions — id INT, trade_date DATE, npv DOUBLE

Data rows:
    id=1, trade_date=2024-03-15, npv=500.0
    id=2, trade_date=2024-07-20, npv=800.0
    id=3, trade_date=2024-11-05, npv=1200.0
"""
import datetime

import numpy as np

from datafinder import DayOfWeek, constant, first_day_of_this_month, first_day_of_this_quarter, first_day_of_this_year, now, previous_day_of_week, today
from datafinder_examples_tests.spec import FinderSpec, TestExpectation

POSITIONS_MAPPING = "positions_mapping.md"
_NO_DATE_FUNCTIONS = {"graphql": "The GraphQL finder has no date functions beyond the parts of a date"}

_D1 = datetime.date(2024, 3, 15)
_D2 = datetime.date(2024, 7, 20)
_D3 = datetime.date(2024, 11, 5)
_WEEK = [datetime.date(2024, 3, 4) + datetime.timedelta(days=n) for n in range(7)]  # Monday to Sunday

POSITION_DATE_FINDER_SPECS = FinderSpec(
    finder_name="PositionFinder",
    mapping_file=POSITIONS_MAPPING,
    expectations=[
        # --- Date extract ---
        TestExpectation(
            name="year_extract",
            query=lambda f: f.find_all([f.trade_date().year()], f.id_().eq(1)),
            expected_columns=["Year Trade Date"],
            expected_result=np.array([[_D1.year]], dtype=object),
        ),
        TestExpectation(
            name="month_extract",
            query=lambda f: f.find_all([f.trade_date().month()], f.id_().eq(1)),
            expected_columns=["Month Trade Date"],
            expected_result=np.array([[_D1.month]], dtype=object),
        ),
        TestExpectation(
            name="day_extract",
            query=lambda f: f.find_all([f.trade_date().day()], f.id_().eq(1)),
            expected_columns=["Day Trade Date"],
            expected_result=np.array([[_D1.day]], dtype=object),
        ),
        TestExpectation(
            name="quarter_extract",
            query=lambda f: f.find_all([f.trade_date().quarter()], f.id_().eq(2)),
            expected_columns=["Quarter Trade Date"],
            expected_result=np.array([[(_D2.month - 1) // 3 + 1]], dtype=object),
        ),
        TestExpectation(
            name="week_extract",
            query=lambda f: f.find_all([f.trade_date().week_of_year()], f.id_().eq(1)),
            expected_columns=["Week Of Year Trade Date"],
            expected_result=np.array([[_D1.isocalendar().week]], dtype=object),
        ),
        # --- A condition is a boolean column of a result ---
        TestExpectation(
            name="a_condition_that_is_a_column_of_a_result_is_a_boolean_that_can_be_tested_in_a_list",
            skip_backends={"graphql": "The GraphQL engine does not run steps over a result"},
            query=lambda f: (lambda r: r.order_by(r["id"].ascending()))(
                f.find_all([f.id_().alias("id"), (f.npv() > 600.0).alias("big")])
                 .extend(listed=lambda r: r.big.is_in([True, False]), only_true=lambda r: r.big.is_in([True]))),
            expected_columns=["id", "big", "listed", "only_true"],
            expected_result=np.array([[1, False, True, False], [2, True, True, True], [3, True, True, True]], dtype=object),
        ),
        # --- Day of the year, the name of the day, and a datetime as a date ---
        TestExpectation(
            name="day_of_year_extract",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.trade_date().day_of_year()], f.id_().eq(1)),
            expected_columns=["Day Of Year Trade Date"],
            expected_result=np.array([[75]], dtype=object),  # 31 + 29 (2024 is a leap year) + 15
        ),
        TestExpectation(
            name="day_of_week_name",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.trade_date().day_of_week_name()], f.id_().eq(1)),
            expected_columns=["Day Of Week Name Trade Date"],
            expected_result=np.array([["Friday"]], dtype=object),
        ),
        TestExpectation(
            name="a_filter_on_the_name_of_the_day",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.id_()], f.trade_date().day_of_week_name().eq("Saturday")),
            expected_columns=["Id"],
            expected_result=np.array([[2]], dtype=object),
        ),
        TestExpectation(
            name="the_date_of_a_datetime",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([constant(datetime.datetime(2024, 3, 15, 13, 45, 10)).date()], f.id_().eq(1)),
            expected_columns=["Date 2024-03-15 13:45:10"],
            expected_result=np.array([[datetime.date(2024, 3, 15)]], dtype=object),
        ),
        # every day of the week, from constants: the name of the day is spelled out in SQL, so each of the seven is checked
        TestExpectation(
            name="the_number_of_each_day_of_the_week_is_the_iso_number",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([constant(day).day_of_week().alias(str(day)) for day in _WEEK], f.id_().eq(1)),
            expected_columns=[str(day) for day in _WEEK],
            expected_result=np.array([[day.isoweekday() for day in _WEEK]], dtype=object),  # Monday is 1 and Sunday is 7
        ),
        TestExpectation(
            name="the_name_of_each_day_of_the_week",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([constant(day).day_of_week_name().alias(str(day)) for day in _WEEK], f.id_().eq(1)),
            expected_columns=[str(day) for day in _WEEK],
            expected_result=np.array([[day.strftime("%A") for day in _WEEK]], dtype=object),
        ),
        # --- Weeks and microseconds ---
        TestExpectation(
            name="add_weeks",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.trade_date().add_weeks(1)], f.id_().eq(1)),
            expected_sql={"duckdb": "INTERVAL '7' DAY"},  # a week is 7 days: ANSI has no week unit
            expected_columns=["Add Weeks Trade Date"],
            expected_result=np.array([[np.datetime64("2024-03-22", "us")]], dtype=object),
        ),
        TestExpectation(
            name="subtract_weeks",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.trade_date().subtract_weeks(2)], f.id_().eq(1)),
            expected_columns=["Subtract Weeks Trade Date"],
            expected_result=np.array([[np.datetime64("2024-03-01", "us")]], dtype=object),
        ),
        TestExpectation(
            name="add_microseconds_to_a_datetime",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([constant(datetime.datetime(2014, 12, 4, 15, 22, 23)).add_microseconds(123456)], f.id_().eq(1)),
            expected_sql={"duckdb": "INTERVAL '0.123456' SECOND"},  # a microsecond is a millionth of a second: ANSI has no microsecond unit
            expected_columns=["Add Microseconds 2014-12-04 15:22:23"],
            expected_result=np.array([[np.datetime64("2014-12-04T15:22:23.123456", "us")]], dtype=object),
        ),
        TestExpectation(
            name="a_timedelta_with_microseconds_is_added_to_a_datetime",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([constant(datetime.datetime(2014, 12, 4, 15, 22, 23)) + datetime.timedelta(seconds=1, microseconds=500000)],
                                       f.id_().eq(1)),
            expected_columns=["Add Microseconds 2014-12-04 15:22:23"],
            expected_result=np.array([[np.datetime64("2014-12-04T15:22:24.500000", "us")]], dtype=object),
        ),
        TestExpectation(
            name="a_filter_on_a_date_a_day_later",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.id_()], f.trade_date().eq(constant(datetime.date(2024, 3, 14)).add_days(1))),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        # --- The most recent and the previous day of the week ---
        TestExpectation(
            name="most_recent_monday",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.trade_date().most_recent_day_of_week(DayOfWeek.MONDAY)], f.id_().eq(1)),
            expected_columns=["Most Recent Day Of Week Trade Date"],
            expected_result=np.array([[datetime.date(2024, 3, 11)]], dtype=object),
        ),
        TestExpectation(
            name="the_most_recent_friday_of_a_friday_is_that_day",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.trade_date().most_recent_day_of_week(DayOfWeek.FRIDAY)], f.id_().eq(1)),
            expected_columns=["Most Recent Day Of Week Trade Date"],
            expected_result=np.array([[datetime.date(2024, 3, 15)]], dtype=object),
        ),
        TestExpectation(
            name="the_previous_friday_of_a_friday_is_a_week_before",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.trade_date().previous_day_of_week(DayOfWeek.FRIDAY)], f.id_().eq(1)),
            expected_columns=["Previous Day Of Week Trade Date"],
            expected_result=np.array([[datetime.date(2024, 3, 8)]], dtype=object),
        ),
        TestExpectation(
            name="the_previous_sunday",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.trade_date().previous_day_of_week(DayOfWeek.SUNDAY)], f.id_().eq(2)),
            expected_columns=["Previous Day Of Week Trade Date"],
            expected_result=np.array([[datetime.date(2024, 7, 14)]], dtype=object),
        ),
        # --- Relative to now: today(), now() and the first day of this period ---
        TestExpectation(
            name="rows_before_today",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.id_()], f.trade_date() < today()).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2], [3]], dtype=object),
            expected_sql={"duckdb": "t0.trade_date < CURRENT_DATE"},
        ),
        TestExpectation(
            name="no_row_is_today",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.id_()], f.trade_date().eq(today())),
            expected_columns=["Id"],
            expected_result=np.array([], dtype=object).reshape(0, 1),
        ),
        TestExpectation(
            name="rows_before_now",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.id_()], f.trade_date() < now()).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2], [3]], dtype=object),
            expected_sql={"duckdb": "t0.trade_date < CURRENT_TIMESTAMP"},
        ),
        TestExpectation(
            name="rows_before_the_first_day_of_this_month_quarter_and_year",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.id_()], (f.trade_date() < first_day_of_this_month())
                                       .and_(f.trade_date() < first_day_of_this_quarter())
                                       .and_(f.trade_date() < first_day_of_this_year())).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2], [3]], dtype=object),
            expected_sql={"duckdb": "DATE_TRUNC('quarter', CURRENT_DATE)"},
        ),
        TestExpectation(
            name="rows_after_the_previous_monday",
            skip_backends=_NO_DATE_FUNCTIONS,
            query=lambda f: f.find_all([f.id_()], f.trade_date() > previous_day_of_week(DayOfWeek.MONDAY)),
            expected_columns=["Id"],
            expected_result=np.array([], dtype=object).reshape(0, 1),
        ),
        # --- First day of the week, month, quarter or year ---
        TestExpectation(
            name="first_day_of_week",
            skip_backends={"graphql": "The GraphQL finder cannot truncate dates"},
            query=lambda f: f.find_all([f.trade_date().first_day_of_week()], f.id_().eq(1)),
            expected_columns=["First Day Of Week Trade Date"],
            expected_result=np.array([[datetime.date(2024, 3, 11)]], dtype=object),
            expected_sql={"duckdb": "CAST(DATE_TRUNC('week', t0.trade_date) AS DATE)"},
        ),
        TestExpectation(
            name="first_day_of_month",
            skip_backends={"graphql": "The GraphQL finder cannot truncate dates"},
            query=lambda f: f.find_all([f.trade_date().first_day_of_month()], f.id_().eq(2)),
            expected_columns=["First Day Of Month Trade Date"],
            expected_result=np.array([[datetime.date(2024, 7, 1)]], dtype=object),
        ),
        TestExpectation(
            name="first_day_of_quarter",
            skip_backends={"graphql": "The GraphQL finder cannot truncate dates"},
            query=lambda f: f.find_all([f.trade_date().first_day_of_quarter()], f.id_().eq(3)),
            expected_columns=["First Day Of Quarter Trade Date"],
            expected_result=np.array([[datetime.date(2024, 10, 1)]], dtype=object),
        ),
        TestExpectation(
            name="first_day_of_year",
            skip_backends={"graphql": "The GraphQL finder cannot truncate dates"},
            query=lambda f: f.find_all([f.trade_date().first_day_of_year()], f.id_().eq(3)),
            expected_columns=["First Day Of Year Trade Date"],
            expected_result=np.array([[datetime.date(2024, 1, 1)]], dtype=object),
        ),
        TestExpectation(
            name="filter_on_first_day_of_month",
            skip_backends={"graphql": "A filter on an expression cannot be pushed to the server"},
            query=lambda f: f.find_all(
                [f.id_()], f.trade_date().first_day_of_month() == datetime.date(2024, 7, 1)
            ),
            expected_columns=["Id"],
            expected_result=np.array([[2]], dtype=object),
            expected_sql={"duckdb": "CAST(DATE_TRUNC('month', t0.trade_date) AS DATE) = '2024-07-01'"},
        ),
        # --- Earliest and latest date ---
        TestExpectation(
            name="earliest_and_latest_date",
            skip_backends={"graphql": "The GraphQL finder cannot take the earliest or latest date yet"},
            query=lambda f: f.find_all([f.trade_date().min(), f.trade_date().max()]),
            expected_columns=["Min Trade Date", "Max Trade Date"],
            expected_result=np.array([[_D1, _D3]], dtype=object),
            expected_sql={"duckdb": "MIN(t0.trade_date)"},
        ),
        # --- Date arithmetic ---
        # ibis/DuckDB returns date arithmetic results as datetime64[us], not datetime.date
        TestExpectation(
            name="subtract_months_one",
            query=lambda f: f.find_all([f.trade_date().subtract_months(1)], f.id_().eq(1)),
            expected_columns=["Subtract Months Trade Date"],
            expected_result=np.array([[np.datetime64("2024-02-15", "us")]], dtype=object),
        ),
        TestExpectation(
            name="add_days_ten",
            query=lambda f: f.find_all([f.trade_date().add_days(10)], f.id_().eq(1)),
            expected_columns=["Add Days Trade Date"],
            expected_result=np.array([[np.datetime64("2024-03-25", "us")]], dtype=object),
        ),
        TestExpectation(
            name="subtract_days_five",
            query=lambda f: f.find_all([f.trade_date().subtract_days(5)], f.id_().eq(1)),
            expected_columns=["Subtract Days Trade Date"],
            expected_result=np.array([[np.datetime64("2024-03-10", "us")]], dtype=object),
        ),
        TestExpectation(
            name="add_months_one",
            query=lambda f: f.find_all([f.trade_date().add_months(1)], f.id_().eq(1)),
            expected_columns=["Add Months Trade Date"],
            expected_result=np.array([[np.datetime64("2024-04-15", "us")]], dtype=object),
        ),
        TestExpectation(
            name="add_years_one",
            query=lambda f: f.find_all([f.trade_date().add_years(1)], f.id_().eq(1)),
            expected_columns=["Add Years Trade Date"],
            expected_result=np.array([[np.datetime64("2025-03-15", "us")]], dtype=object),
        ),
        # --- Date diff ---
        TestExpectation(
            name="diff_days",
            query=lambda f: f.find_all(
                [f.trade_date().diff_days(datetime.date(2024, 3, 25))],
                f.id_().eq(1),
            ),
            expected_columns=["Diff Days Trade Date"],
            expected_result=np.array([[10]], dtype=object),
        ),
        TestExpectation(
            name="diff_months",
            query=lambda f: f.find_all(
                [f.trade_date().diff_months(datetime.date(2024, 6, 15))],
                f.id_().eq(1),
            ),
            expected_columns=["Diff Months Trade Date"],
            expected_result=np.array([[3]], dtype=object),
        ),
        TestExpectation(
            name="diff_years",
            query=lambda f: f.find_all(
                [f.trade_date().diff_years(datetime.date(2026, 3, 15))],
                f.id_().eq(1),
            ),
            expected_columns=["Diff Years Trade Date"],
            expected_result=np.array([[2]], dtype=object),
        ),
        # --- DateAttribute extra operations ---
        TestExpectation(
            name="day_of_week",
            query=lambda f: f.find_all([f.trade_date().day_of_week()], f.id_().eq(1)),
            expected_columns=["Day Of Week Trade Date"],
            # 2024-03-15 is a Friday: ISO day 5 (Monday is 1)
            expected_result=np.array([[5]], dtype=object),
        ),
        TestExpectation(
            name="subtract_years_one",
            query=lambda f: f.find_all([f.trade_date().subtract_years(1)], f.id_().eq(1)),
            expected_columns=["Subtract Years Trade Date"],
            expected_result=np.array([[np.datetime64("2023-03-15", "us")]], dtype=object),
        ),
        TestExpectation(
            name="add_timedelta_days",
            query=lambda f: f.find_all([f.trade_date() + datetime.timedelta(days=5)], f.id_().eq(1)),
            expected_columns=["Add Days Trade Date"],
            expected_result=np.array([[np.datetime64("2024-03-20", "us")]], dtype=object),
        ),
        TestExpectation(
            name="sub_timedelta_days",
            query=lambda f: f.find_all([f.trade_date() - datetime.timedelta(days=5)], f.id_().eq(1)),
            expected_columns=["Subtract Days Trade Date"],
            expected_result=np.array([[np.datetime64("2024-03-10", "us")]], dtype=object),
        ),
        # --- DateAttribute comparisons ---
        TestExpectation(
            name="trade_date_eq",
            query=lambda f: f.find_all([f.id_()], f.trade_date().eq(_D1)),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="trade_date_gt",
            query=lambda f: f.find_all([f.id_()], f.trade_date() > _D2).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[3]], dtype=object),
        ),
        TestExpectation(
            name="trade_date_lt",
            query=lambda f: f.find_all([f.id_()], f.trade_date() < _D2),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="trade_date_ge",
            query=lambda f: f.find_all([f.id_()], f.trade_date() >= _D2).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3]], dtype=object),
        ),
        TestExpectation(
            name="trade_date_le",
            query=lambda f: f.find_all([f.id_()], f.trade_date() <= _D2).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2]], dtype=object),
        ),
        # --- DoubleAttribute comparisons ---
        TestExpectation(
            name="npv_eq",
            query=lambda f: f.find_all([f.id_()], f.npv().eq(500.0)),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="npv_gt",
            query=lambda f: f.find_all([f.id_()], f.npv() > 500.0).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3]], dtype=object),
        ),
        TestExpectation(
            name="npv_lt",
            query=lambda f: f.find_all([f.id_()], f.npv() < 800.0),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="npv_ge",
            query=lambda f: f.find_all([f.id_()], f.npv() >= 800.0).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3]], dtype=object),
        ),
        TestExpectation(
            name="npv_le",
            query=lambda f: f.find_all([f.id_()], f.npv() <= 800.0).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2]], dtype=object),
        ),
        # --- NumericAttribute aggregates ---
        TestExpectation(
            name="npv_min",
            query=lambda f: f.find_all([f.npv().min()]),
            expected_columns=["Min Npv"],
            expected_result=np.array([[500.0]], dtype=object),
        ),
        TestExpectation(
            name="npv_max",
            query=lambda f: f.find_all([f.npv().max()]),
            expected_columns=["Max Npv"],
            expected_result=np.array([[1200.0]], dtype=object),
        ),
        TestExpectation(
            name="npv_average",
            query=lambda f: f.find_all([f.npv().average()]),
            expected_columns=["Average Npv"],
            expected_result=np.array([[2500.0 / 3]], dtype=object),
        ),
        # --- DoubleAttribute.__eq__ dunder (vs .eq() method) ---
        TestExpectation(
            name="npv_eq_operator",
            query=lambda f: f.find_all([f.id_()], f.npv() == 500.0),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        # --- IntegerAttribute comparisons ---
        TestExpectation(
            name="id_eq_operator",
            query=lambda f: f.find_all([f.id_()], f.id_() == 1),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="id_gt",
            query=lambda f: f.find_all([f.id_()], f.id_() > 1).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3]], dtype=object),
        ),
        TestExpectation(
            name="id_lt",
            query=lambda f: f.find_all([f.id_()], f.id_() < 3),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2]], dtype=object),
        ),
        TestExpectation(
            name="id_ge",
            query=lambda f: f.find_all([f.id_()], f.id_() >= 2).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3]], dtype=object),
        ),
        TestExpectation(
            name="id_le",
            query=lambda f: f.find_all([f.id_()], f.id_() <= 2).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2]], dtype=object),
        ),
    ],
)
