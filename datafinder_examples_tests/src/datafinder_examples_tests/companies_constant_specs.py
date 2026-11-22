"""
Test specifications for the functions of a constant (companies_mapping.md).

A constant has the functions of its type, as a column of that type has, and a function of a constant is a column of its
own: ``constant("Hello").length()``. Each query reads the first company, so the one row has the values of the constants
and the expected values are those Python gives for the same function.

    companies — id INT, name VARCHAR, category VARCHAR
"""
import datetime
import decimal


from datafinder import constant
from datafinder_examples_tests.spec import FinderSpec, TestExpectation

COMPANIES_MAPPING = "companies_mapping.md"

_D = datetime.date(2024, 1, 31)
_DT = datetime.datetime(2016, 8, 10, 8, 55, 30)
_THE_FIRST = lambda f: f.id_().eq(1)  # noqa: E731  # one row, so each constant is read once


# A difference counts the boundaries crossed: a year, a month and a day are a change of year, month and date, and an hour, a minute and
# a second a change of hour, minute and second, so a day ends at midnight and not 24 hours after it began.
_DATE_PAIRS = [
    (datetime.date(2023, 12, 31), datetime.date(2024, 1, 1)), (datetime.date(2024, 2, 29), datetime.date(2025, 2, 28)),
    (datetime.date(2024, 3, 31), datetime.date(2024, 4, 1)), (datetime.date(2025, 1, 6), datetime.date(2025, 1, 5)),
    (datetime.date(2024, 6, 15), datetime.date(2024, 6, 15)),
    (datetime.date(2024, 3, 4), datetime.date(2024, 3, 10)), (datetime.date(2024, 3, 11), datetime.date(2024, 3, 10)),
    (datetime.date(2024, 3, 4), datetime.date(2024, 3, 31)), (datetime.date(2024, 3, 31), datetime.date(2024, 3, 4)),
]
_DATETIME_PAIRS = [
    (datetime.datetime(2024, 3, 10, 8, 59, 59), datetime.datetime(2024, 3, 10, 9, 0, 0)),
    (datetime.datetime(2024, 3, 10, 8, 0, 0), datetime.datetime(2024, 3, 10, 8, 59, 59)),
    (datetime.datetime(2023, 12, 31, 23, 59, 59), datetime.datetime(2024, 1, 1, 0, 0, 0)),
    (datetime.datetime(2024, 3, 10, 9, 1, 0), datetime.datetime(2024, 3, 10, 8, 59, 0)),
    (datetime.datetime(2024, 3, 10, 23, 59, 59), datetime.datetime(2024, 3, 11, 0, 0, 0)),
]


_DATE_DIFFERENCE_EXPECTATIONS = [
    TestExpectation(
        name=f"date_difference_counts_the_boundaries_crossed_{a}_to_{b}",
        query=lambda f, a=a, b=b: f.find_all([
            constant(a).diff_years(b).alias("years"), constant(a).diff_months(b).alias("months"), constant(a).diff_weeks(b).alias("weeks"),
            constant(a).diff_days(b).alias("days"),
        ], _THE_FIRST(f)),
    )
    for a, b in _DATE_PAIRS
] + [
    TestExpectation(
        name=f"datetime_difference_counts_the_boundaries_crossed_{a:%H%M%S}_to_{b:%d_%H%M%S}",
        query=lambda f, a=a, b=b: f.find_all([
            constant(a).diff_hours(b).alias("hours"), constant(a).diff_minutes(b).alias("minutes"), constant(a).diff_seconds(b).alias("seconds"),
            constant(a).diff_days(b).alias("days"), constant(a).diff_weeks(b).alias("weeks"),
        ], _THE_FIRST(f)),
    )
    for a, b in _DATETIME_PAIRS
]

COMPANIES_CONSTANT_SPECS = FinderSpec(
    finder_name="CompanyFinder",
    mapping_file=COMPANIES_MAPPING,
    expectations=[
        # --- text ---
        TestExpectation(
            name="text_constant_case_and_trimming",
            query=lambda f: f.find_all([
                constant("Hello").upper().alias("upper"), constant("Hello").lower().alias("lower"),
                constant("  hi  ").strip().alias("strip"), constant("  hi  ").lstrip().alias("lstrip"),
                constant("  hi  ").rstrip().alias("rstrip"), constant("abc").reverse().alias("reverse"),
            ], _THE_FIRST(f)),
        ),
        TestExpectation(
            name="text_constant_length_and_pieces",
            query=lambda f: f.find_all([
                constant("Hello").length().alias("length"), constant("Hello").left(2).alias("left"),
                constant("Hello").right(2).alias("right"), constant("ab").repeat(2).alias("repeat"),
                (constant("ab") * 3).alias("times"), constant("Hello").replace("l", "L").alias("replace"),
                constant("Hello").substring(1, 3).alias("substring"), constant("Hello")[1:3].alias("slice"),
                constant("Hello")[::-1].alias("backwards"),
            ], _THE_FIRST(f)),
        ),
        TestExpectation(
            name="text_constant_joins_with_text",
            query=lambda f: f.find_all([
                (constant("a") + "b").alias("after"), ("z" + constant("a")).alias("before"),
                (constant("a") + f.name()).alias("column"),
            ], _THE_FIRST(f)),
        ),
        TestExpectation(
            name="functions_of_a_text_constant_chain",
            query=lambda f: f.find_all([
                constant(" Hello ").strip().upper().alias("two"), constant("Hello").upper().length().alias("then_length"),
                constant("Hello").reverse().left(2).alias("three"),
            ], _THE_FIRST(f)),
        ),
        TestExpectation(
            name="text_constant_default_column_name_is_the_function_and_the_value",
            query=lambda f: f.find_all([constant("Hello").length(), constant("Hello").upper()], _THE_FIRST(f)),
        ),
        TestExpectation(
            name="text_constant_pattern_test_that_holds_keeps_the_rows",
            query=lambda f: f.find_all([f.id_()], constant("Hello").starts_with("He").and_(_THE_FIRST(f))),
        ),
        TestExpectation(
            name="text_constant_pattern_test_that_fails_keeps_none",
            query=lambda f: f.find_all([f.id_()], constant("Hello").ends_with("x")),
        ),
        TestExpectation(
            name="text_constant_contains_and_matches",
            query=lambda f: f.find_all([f.id_()], constant("Hello").contains("ell").and_(
                constant("Hello").matches("^H.*o$")).and_(_THE_FIRST(f))),
        ),
        TestExpectation(
            name="a_text_constant_function_compares_with_a_column_function",
            query=lambda f: f.find_all([f.id_()], constant("Acme Corp").upper().eq(f.name().upper()).and_(_THE_FIRST(f))),
        ),
        # --- whole numbers ---
        TestExpectation(
            name="integer_constant_functions",
            query=lambda f: f.find_all([
                constant(-3).abs().alias("abs"), constant(7).mod(3).alias("mod"), (constant(2) ** 3).alias("power"),
                constant(7).round().alias("round"), constant(16).sqrt().alias("sqrt"),
                constant(0).exp().alias("exp"), constant(1).log().alias("log"),
            ], _THE_FIRST(f)),
        ),
        TestExpectation(
            name="integer_constant_function_is_an_integer_in_arithmetic",
            query=lambda f: f.find_all([(constant(-3).abs() + f.id_()).alias("sum"), (constant(2) ** 3 * 2).alias("scaled")],
                                       _THE_FIRST(f)),
        ),
        # --- floating-point numbers ---
        TestExpectation(
            name="float_constant_functions",
            query=lambda f: f.find_all([
                constant(1.2).ceil().alias("ceil"), constant(1.8).floor().alias("floor"), constant(2.25).sqrt().alias("sqrt"),
                constant(-1.5).abs().alias("abs"),
            ], _THE_FIRST(f)),
        ),
        TestExpectation(
            name="float_constant_trigonometry",
            query=lambda f: f.find_all([
                constant(0.5).sin().alias("sin"), constant(0.5).cos().alias("cos"), constant(0.5).tan().alias("tan"),
                constant(0.5).asin().alias("asin"), constant(0.5).acos().alias("acos"), constant(0.5).atan().alias("atan"),
                constant(1.0).atan2(2).alias("atan2"), constant(0.5).exp().alias("exp"), constant(0.5).log().alias("log"),
            ], _THE_FIRST(f)),
        ),
        # --- decimals ---
        TestExpectation(
            name="decimal_constant_functions",
            query=lambda f: f.find_all([
                constant(decimal.Decimal("-1.5")).abs().alias("abs"),
                constant(decimal.Decimal("1.2345")).round(2).alias("round"),
            ], _THE_FIRST(f)),
        ),
        # --- dates ---
        TestExpectation(
            name="date_constant_parts",
            # SQL numbers the days of the week from Sunday, 0
            query=lambda f: f.find_all([
                constant(_D).year().alias("year"), constant(_D).month().alias("month"), constant(_D).day().alias("day"),
                constant(_D).quarter().alias("quarter"), constant(_D).week_of_year().alias("week"),
                constant(_D).day_of_week().alias("day_of_week"),
            ], _THE_FIRST(f)),
        ),
        TestExpectation(
            name="date_constant_moved",
            # a date moved by a number of days, months or years is read as a timestamp at midnight, as for a date column
            query=lambda f: f.find_all([
                constant(_D).add_days(1).alias("add_days"), constant(_D).add_months(1).alias("add_months"),
                constant(_D).add_years(1).alias("add_years"), constant(_D).subtract_days(31).alias("subtract_days"),
                constant(_D).subtract_months(1).alias("subtract_months"), constant(_D).subtract_years(1).alias("subtract_years"),
                (constant(_D) + datetime.timedelta(days=2)).alias("plus"), (constant(_D) - datetime.timedelta(days=2)).alias("minus"),
            ], _THE_FIRST(f)),
        ),
        TestExpectation(
            name="date_constant_first_day_of_a_period",
            query=lambda f: f.find_all([
                constant(_D).first_day_of_week().alias("week"), constant(_D).first_day_of_month().alias("month"),
                constant(_D).first_day_of_quarter().alias("quarter"), constant(_D).first_day_of_year().alias("year"),
            ], _THE_FIRST(f)),
        ),
        TestExpectation(
            name="date_constant_difference",
            query=lambda f: f.find_all([
                constant(_D).diff_days(datetime.date(2024, 2, 10)).alias("days"),
                constant(_D).diff_months(datetime.date(2024, 4, 30)).alias("months"),
                constant(_D).diff_years(datetime.date(2026, 1, 31)).alias("years"),
            ], _THE_FIRST(f)),
        ),
        TestExpectation(
            name="date_constant_part_compares_with_a_number",
            query=lambda f: f.find_all([f.id_()], (constant(_D).year() == 2024).and_(_THE_FIRST(f))),
        ),
        # --- datetimes ---
        TestExpectation(
            name="datetime_constant_parts",
            query=lambda f: f.find_all([
                constant(_DT).year().alias("year"), constant(_DT).month().alias("month"), constant(_DT).day().alias("day"),
                constant(_DT).hour().alias("hour"), constant(_DT).minute().alias("minute"), constant(_DT).second().alias("second"),
                constant(_DT).quarter().alias("quarter"), constant(_DT).week_of_year().alias("week"),
                constant(_DT).day_of_week().alias("day_of_week"),
            ], _THE_FIRST(f)),
        ),
        TestExpectation(
            name="datetime_constant_moved",
            query=lambda f: f.find_all([
                constant(_DT).add_hours(2).alias("add_hours"), constant(_DT).add_minutes(5).alias("add_minutes"),
                constant(_DT).add_seconds(40).alias("add_seconds"), constant(_DT).add_days(1).alias("add_days"),
                constant(_DT).subtract_hours(1).alias("subtract_hours"), constant(_DT).subtract_minutes(5).alias("subtract_minutes"),
                (constant(_DT) + datetime.timedelta(hours=3)).alias("plus"),
                (constant(_DT) - datetime.timedelta(minutes=30)).alias("minus"),
            ], _THE_FIRST(f)),
        ),
        TestExpectation(
            name="datetime_constant_difference",
            query=lambda f: f.find_all([
                constant(_DT).diff_hours(datetime.datetime(2016, 8, 10, 12, 0, 0)).alias("hours"),
                constant(_DT).diff_minutes(datetime.datetime(2016, 8, 10, 9, 55, 30)).alias("minutes"),
                constant(_DT).diff_seconds(datetime.datetime(2016, 8, 10, 8, 56, 0)).alias("seconds"),
                constant(_DT).diff_days(datetime.datetime(2016, 8, 12, 8, 55, 30)).alias("days"),
            ], _THE_FIRST(f)),
        ),
        *_DATE_DIFFERENCE_EXPECTATIONS,
        TestExpectation(
            name="a_regular_expression_matches_anywhere_in_the_text",
            query=lambda f: f.find_all([f.id_()], constant("Customer X Complaints").matches("X.*Compl").and_(_THE_FIRST(f))),
        ),
        TestExpectation(
            name="a_regular_expression_that_is_anchored_matches_only_there",
            query=lambda f: f.find_all([f.id_()], (~constant("abc").matches("^b")).and_(constant("abc").matches("b")).and_(_THE_FIRST(f))),
        ),
        TestExpectation(
            name="a_function_of_a_datetime_constant_is_ordered_like_a_date",
            query=lambda f: f.find_all([f.id_()], (constant(_DT).add_days(1) > _DT).and_(_THE_FIRST(f))),
        ),
    ],
)
