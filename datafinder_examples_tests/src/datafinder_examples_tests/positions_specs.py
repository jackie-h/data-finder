"""
Test specifications for date functions using positions_mapping.md.

    positions — id INT, trade_date DATE, npv DOUBLE

Data rows:
    id=1, trade_date=2024-03-15, npv=500.0
    id=2, trade_date=2024-07-20, npv=800.0
    id=3, trade_date=2024-11-05, npv=1200.0
"""
import datetime


from datafinder import DayOfWeek, constant, first_day_of_this_month, first_day_of_this_quarter, first_day_of_this_year, now, previous_day_of_week, today
from datafinder_examples_tests.spec import FinderSpec, TestExpectation

POSITIONS_MAPPING = "positions_mapping.md"
_D1 = datetime.date(2024, 3, 15)
_D2 = datetime.date(2024, 7, 20)
_WEEK = [datetime.date(2024, 3, 4) + datetime.timedelta(days=n) for n in range(7)]  # Monday to Sunday

POSITION_DATE_FINDER_SPECS = FinderSpec(
    finder_name="PositionFinder",
    mapping_file=POSITIONS_MAPPING,
    expectations=[
        # --- Date extract ---
        TestExpectation(
            name="year_extract",
            query=lambda f: f.find_all([f.trade_date().year()], f.id_().eq(1)),
        ),
        TestExpectation(
            name="month_extract",
            query=lambda f: f.find_all([f.trade_date().month()], f.id_().eq(1)),
        ),
        TestExpectation(
            name="day_extract",
            query=lambda f: f.find_all([f.trade_date().day()], f.id_().eq(1)),
        ),
        TestExpectation(
            name="quarter_extract",
            query=lambda f: f.find_all([f.trade_date().quarter()], f.id_().eq(2)),
        ),
        TestExpectation(
            name="week_extract",
            query=lambda f: f.find_all([f.trade_date().week_of_year()], f.id_().eq(1)),
        ),
        # --- A condition is a boolean column of a result ---
        TestExpectation(
            name="a_condition_that_is_a_column_of_a_result_is_a_boolean_that_can_be_tested_in_a_list",
            query=lambda f: (lambda r: r.order_by(r["id"].ascending()))(
                f.find_all([f.id_().alias("id"), (f.npv() > 600.0).alias("big")])
                 .extend(listed=lambda r: r.big.is_in([True, False]), only_true=lambda r: r.big.is_in([True]))),
        ),
        # --- Day of the year, the name of the day, and a datetime as a date ---
        TestExpectation(
            name="day_of_year_extract",
            # 31 + 29 (2024 is a leap year) + 15
            query=lambda f: f.find_all([f.trade_date().day_of_year()], f.id_().eq(1)),
        ),
        TestExpectation(
            name="day_of_week_name",
            query=lambda f: f.find_all([f.trade_date().day_of_week_name()], f.id_().eq(1)),
        ),
        TestExpectation(
            name="a_filter_on_the_name_of_the_day",
            query=lambda f: f.find_all([f.id_()], f.trade_date().day_of_week_name().eq("Saturday")),
        ),
        TestExpectation(
            name="the_date_of_a_datetime",
            query=lambda f: f.find_all([constant(datetime.datetime(2024, 3, 15, 13, 45, 10)).date()], f.id_().eq(1)),
        ),
        # every day of the week, from constants: the name of the day is spelled out in SQL, so each of the seven is checked
        TestExpectation(
            name="the_number_of_each_day_of_the_week_is_the_iso_number",
            # Monday is 1 and Sunday is 7
            query=lambda f: f.find_all([constant(day).day_of_week().alias(str(day)) for day in _WEEK], f.id_().eq(1)),
        ),
        TestExpectation(
            name="the_name_of_each_day_of_the_week",
            query=lambda f: f.find_all([constant(day).day_of_week_name().alias(str(day)) for day in _WEEK], f.id_().eq(1)),
        ),
        # --- Weeks and microseconds ---
        TestExpectation(
            name="add_weeks",
            # a week is 7 days: ANSI has no week unit
            query=lambda f: f.find_all([f.trade_date().add_weeks(1)], f.id_().eq(1)),
        ),
        TestExpectation(
            name="subtract_weeks",
            query=lambda f: f.find_all([f.trade_date().subtract_weeks(2)], f.id_().eq(1)),
        ),
        TestExpectation(
            name="add_microseconds_to_a_datetime",
            # a microsecond is a millionth of a second: ANSI has no microsecond unit (the SQL has INTERVAL '0.123456' SECOND)
            query=lambda f: f.find_all([constant(datetime.datetime(2014, 12, 4, 15, 22, 23)).add_microseconds(123456)], f.id_().eq(1)),
        ),
        TestExpectation(
            name="a_timedelta_with_microseconds_is_added_to_a_datetime",
            query=lambda f: f.find_all([constant(datetime.datetime(2014, 12, 4, 15, 22, 23)) + datetime.timedelta(seconds=1, microseconds=500000)],
                                       f.id_().eq(1)),
        ),
        TestExpectation(
            name="a_filter_on_a_date_a_day_later",
            query=lambda f: f.find_all([f.id_()], f.trade_date().eq(constant(datetime.date(2024, 3, 14)).add_days(1))),
        ),
        # --- The most recent and the previous day of the week ---
        TestExpectation(
            name="most_recent_monday",
            query=lambda f: f.find_all([f.trade_date().most_recent_day_of_week(DayOfWeek.MONDAY)], f.id_().eq(1)),
        ),
        TestExpectation(
            name="the_most_recent_friday_of_a_friday_is_that_day",
            query=lambda f: f.find_all([f.trade_date().most_recent_day_of_week(DayOfWeek.FRIDAY)], f.id_().eq(1)),
        ),
        TestExpectation(
            name="the_previous_friday_of_a_friday_is_a_week_before",
            query=lambda f: f.find_all([f.trade_date().previous_day_of_week(DayOfWeek.FRIDAY)], f.id_().eq(1)),
        ),
        TestExpectation(
            name="the_previous_sunday",
            query=lambda f: f.find_all([f.trade_date().previous_day_of_week(DayOfWeek.SUNDAY)], f.id_().eq(2)),
        ),
        # --- Relative to now: today(), now() and the first day of this period ---
        TestExpectation(
            name="rows_before_today",
            query=lambda f: f.find_all([f.id_()], f.trade_date() < today()).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="no_row_is_today",
            query=lambda f: f.find_all([f.id_()], f.trade_date().eq(today())),
        ),
        TestExpectation(
            name="rows_before_now",
            query=lambda f: f.find_all([f.id_()], f.trade_date() < now()).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="rows_before_the_first_day_of_this_month_quarter_and_year",
            query=lambda f: f.find_all([f.id_()], (f.trade_date() < first_day_of_this_month())
                                       .and_(f.trade_date() < first_day_of_this_quarter())
                                       .and_(f.trade_date() < first_day_of_this_year())).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="rows_after_the_previous_monday",
            query=lambda f: f.find_all([f.id_()], f.trade_date() > previous_day_of_week(DayOfWeek.MONDAY)),
        ),
        # --- First day of the week, month, quarter or year ---
        TestExpectation(
            name="first_day_of_week",
            query=lambda f: f.find_all([f.trade_date().first_day_of_week()], f.id_().eq(1)),
        ),
        TestExpectation(
            name="first_day_of_month",
            query=lambda f: f.find_all([f.trade_date().first_day_of_month()], f.id_().eq(2)),
        ),
        TestExpectation(
            name="first_day_of_quarter",
            query=lambda f: f.find_all([f.trade_date().first_day_of_quarter()], f.id_().eq(3)),
        ),
        TestExpectation(
            name="first_day_of_year",
            query=lambda f: f.find_all([f.trade_date().first_day_of_year()], f.id_().eq(3)),
        ),
        TestExpectation(
            name="filter_on_first_day_of_month",
            query=lambda f: f.find_all(
                [f.id_()], f.trade_date().first_day_of_month() == datetime.date(2024, 7, 1)
            ),
        ),
        # --- Earliest and latest date ---
        TestExpectation(
            name="earliest_and_latest_date",
            query=lambda f: f.find_all([f.trade_date().min(), f.trade_date().max()]),
        ),
        # --- Date arithmetic ---
        # ibis/DuckDB returns date arithmetic results as datetime64[us], not datetime.date
        TestExpectation(
            name="subtract_months_one",
            query=lambda f: f.find_all([f.trade_date().subtract_months(1)], f.id_().eq(1)),
        ),
        TestExpectation(
            name="add_days_ten",
            query=lambda f: f.find_all([f.trade_date().add_days(10)], f.id_().eq(1)),
        ),
        TestExpectation(
            name="subtract_days_five",
            query=lambda f: f.find_all([f.trade_date().subtract_days(5)], f.id_().eq(1)),
        ),
        TestExpectation(
            name="add_months_one",
            query=lambda f: f.find_all([f.trade_date().add_months(1)], f.id_().eq(1)),
        ),
        TestExpectation(
            name="add_years_one",
            query=lambda f: f.find_all([f.trade_date().add_years(1)], f.id_().eq(1)),
        ),
        # --- Date diff ---
        TestExpectation(
            name="diff_days",
            query=lambda f: f.find_all(
                [f.trade_date().diff_days(datetime.date(2024, 3, 25))],
                f.id_().eq(1),
            ),
        ),
        TestExpectation(
            name="diff_months",
            query=lambda f: f.find_all(
                [f.trade_date().diff_months(datetime.date(2024, 6, 15))],
                f.id_().eq(1),
            ),
        ),
        TestExpectation(
            name="diff_years",
            query=lambda f: f.find_all(
                [f.trade_date().diff_years(datetime.date(2026, 3, 15))],
                f.id_().eq(1),
            ),
        ),
        # --- DateAttribute extra operations ---
        TestExpectation(
            name="day_of_week",
            # 2024-03-15 is a Friday: ISO day 5 (Monday is 1)
            query=lambda f: f.find_all([f.trade_date().day_of_week()], f.id_().eq(1)),
        ),
        TestExpectation(
            name="subtract_years_one",
            query=lambda f: f.find_all([f.trade_date().subtract_years(1)], f.id_().eq(1)),
        ),
        TestExpectation(
            name="add_timedelta_days",
            query=lambda f: f.find_all([f.trade_date() + datetime.timedelta(days=5)], f.id_().eq(1)),
        ),
        TestExpectation(
            name="sub_timedelta_days",
            query=lambda f: f.find_all([f.trade_date() - datetime.timedelta(days=5)], f.id_().eq(1)),
        ),
        # --- DateAttribute comparisons ---
        TestExpectation(
            name="trade_date_eq",
            query=lambda f: f.find_all([f.id_()], f.trade_date().eq(_D1)),
        ),
        TestExpectation(
            name="trade_date_gt",
            query=lambda f: f.find_all([f.id_()], f.trade_date() > _D2).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="trade_date_lt",
            query=lambda f: f.find_all([f.id_()], f.trade_date() < _D2),
        ),
        TestExpectation(
            name="trade_date_ge",
            query=lambda f: f.find_all([f.id_()], f.trade_date() >= _D2).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="trade_date_le",
            query=lambda f: f.find_all([f.id_()], f.trade_date() <= _D2).order_by(f.id_().ascending()),
        ),
        # --- DoubleAttribute comparisons ---
        TestExpectation(
            name="npv_eq",
            query=lambda f: f.find_all([f.id_()], f.npv().eq(500.0)),
        ),
        TestExpectation(
            name="npv_gt",
            query=lambda f: f.find_all([f.id_()], f.npv() > 500.0).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="npv_lt",
            query=lambda f: f.find_all([f.id_()], f.npv() < 800.0),
        ),
        TestExpectation(
            name="npv_ge",
            query=lambda f: f.find_all([f.id_()], f.npv() >= 800.0).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="npv_le",
            query=lambda f: f.find_all([f.id_()], f.npv() <= 800.0).order_by(f.id_().ascending()),
        ),
        # --- NumericAttribute aggregates ---
        TestExpectation(
            name="npv_min",
            query=lambda f: f.find_all([f.npv().min()]),
        ),
        TestExpectation(
            name="npv_max",
            query=lambda f: f.find_all([f.npv().max()]),
        ),
        TestExpectation(
            name="npv_average",
            query=lambda f: f.find_all([f.npv().average()]),
        ),
        # --- DoubleAttribute.__eq__ dunder (vs .eq() method) ---
        TestExpectation(
            name="npv_eq_operator",
            query=lambda f: f.find_all([f.id_()], f.npv() == 500.0),
        ),
        # --- IntegerAttribute comparisons ---
        TestExpectation(
            name="id_eq_operator",
            query=lambda f: f.find_all([f.id_()], f.id_() == 1),
        ),
        TestExpectation(
            name="id_gt",
            query=lambda f: f.find_all([f.id_()], f.id_() > 1).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="id_lt",
            query=lambda f: f.find_all([f.id_()], f.id_() < 3),
        ),
        TestExpectation(
            name="id_ge",
            query=lambda f: f.find_all([f.id_()], f.id_() >= 2).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="id_le",
            query=lambda f: f.find_all([f.id_()], f.id_() <= 2).order_by(f.id_().ascending()),
        ),
    ],
)
