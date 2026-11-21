"""
Test specifications for the functions of a constant (companies_mapping.md).

A constant has the functions of its type, as a column of that type has, and a function of a constant is a column of its
own: ``constant("Hello").length()``. Each query reads the first company, so the one row has the values of the constants
and the expected values are those Python gives for the same function.

    companies — id INT, name VARCHAR, category VARCHAR
"""
import datetime
import decimal
import math

import numpy as np

from datafinder import constant
from datafinder_examples_tests.spec import FinderSpec, TestExpectation

COMPANIES_MAPPING = "companies_mapping.md"

_D = datetime.date(2024, 1, 31)
_DT = datetime.datetime(2016, 8, 10, 8, 55, 30)
_THE_FIRST = lambda f: f.id_().eq(1)  # noqa: E731  # one row, so each constant is read once


def _one_row(columns: list[str], values: list, **kwargs) -> dict:
    return dict(expected_columns=columns, expected_result=np.array([values], dtype=object), **kwargs)


_SKIP_GRAPHQL = {"graphql": "The GraphQL finder has no scalar functions"}

COMPANIES_CONSTANT_SPECS = FinderSpec(
    finder_name="CompanyFinder",
    mapping_file=COMPANIES_MAPPING,
    skip_backends=_SKIP_GRAPHQL,
    expectations=[
        # --- text ---
        TestExpectation(
            name="text_constant_case_and_trimming",
            query=lambda f: f.find_all([
                constant("Hello").upper().alias("upper"), constant("Hello").lower().alias("lower"),
                constant("  hi  ").strip().alias("strip"), constant("  hi  ").lstrip().alias("lstrip"),
                constant("  hi  ").rstrip().alias("rstrip"), constant("abc").reverse().alias("reverse"),
            ], _THE_FIRST(f)),
            **_one_row(["upper", "lower", "strip", "lstrip", "rstrip", "reverse"],
                       ["Hello".upper(), "Hello".lower(), "  hi  ".strip(), "  hi  ".lstrip(), "  hi  ".rstrip(),
                        "abc"[::-1]]),
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
            **_one_row(["length", "left", "right", "repeat", "times", "replace", "substring", "slice", "backwards"],
                       [len("Hello"), "Hello"[:2], "Hello"[-2:], "ab" * 2, "ab" * 3, "Hello".replace("l", "L"),
                        "Hello"[1:4], "Hello"[1:3], "Hello"[::-1]]),
        ),
        TestExpectation(
            name="text_constant_joins_with_text",
            query=lambda f: f.find_all([
                (constant("a") + "b").alias("after"), ("z" + constant("a")).alias("before"),
                (constant("a") + f.name()).alias("column"),
            ], _THE_FIRST(f)),
            **_one_row(["after", "before", "column"], ["ab", "za", "aAcme Corp"]),
        ),
        TestExpectation(
            name="functions_of_a_text_constant_chain",
            query=lambda f: f.find_all([
                constant(" Hello ").strip().upper().alias("two"), constant("Hello").upper().length().alias("then_length"),
                constant("Hello").reverse().left(2).alias("three"),
            ], _THE_FIRST(f)),
            **_one_row(["two", "then_length", "three"], ["HELLO", 5, "ol"]),
        ),
        TestExpectation(
            name="text_constant_default_column_name_is_the_function_and_the_value",
            query=lambda f: f.find_all([constant("Hello").length(), constant("Hello").upper()], _THE_FIRST(f)),
            **_one_row(["Length Hello", "Upper Hello"], [5, "HELLO"]),
        ),
        TestExpectation(
            name="text_constant_pattern_test_that_holds_keeps_the_rows",
            query=lambda f: f.find_all([f.id_()], constant("Hello").starts_with("He").and_(_THE_FIRST(f))),
            expected_columns=["Id"], expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="text_constant_pattern_test_that_fails_keeps_none",
            query=lambda f: f.find_all([f.id_()], constant("Hello").ends_with("x")),
            expected_columns=["Id"], expected_result=np.empty((0, 1), dtype=object),
        ),
        TestExpectation(
            name="text_constant_contains_and_matches",
            query=lambda f: f.find_all([f.id_()], constant("Hello").contains("ell").and_(
                constant("Hello").matches("^H.*o$")).and_(_THE_FIRST(f))),
            expected_columns=["Id"], expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="a_text_constant_function_compares_with_a_column_function",
            query=lambda f: f.find_all([f.id_()], constant("Acme Corp").upper().eq(f.name().upper()).and_(_THE_FIRST(f))),
            expected_columns=["Id"], expected_result=np.array([[1]], dtype=object),
        ),
        # --- whole numbers ---
        TestExpectation(
            name="integer_constant_functions",
            query=lambda f: f.find_all([
                constant(-3).abs().alias("abs"), constant(7).mod(3).alias("mod"), (constant(2) ** 3).alias("power"),
                constant(7).round().alias("round"), constant(16).sqrt().alias("sqrt"),
                constant(0).exp().alias("exp"), constant(1).log().alias("log"),
            ], _THE_FIRST(f)),
            **_one_row(["abs", "mod", "power", "round", "sqrt", "exp", "log"],
                       [abs(-3), 7 % 3, 2 ** 3, round(7), math.sqrt(16), math.exp(0), math.log(1)]),
        ),
        TestExpectation(
            name="integer_constant_function_is_an_integer_in_arithmetic",
            query=lambda f: f.find_all([(constant(-3).abs() + f.id_()).alias("sum"), (constant(2) ** 3 * 2).alias("scaled")],
                                       _THE_FIRST(f)),
            **_one_row(["sum", "scaled"], [abs(-3) + 1, 2 ** 3 * 2]),
        ),
        # --- floating-point numbers ---
        TestExpectation(
            name="float_constant_functions",
            query=lambda f: f.find_all([
                constant(1.2).ceil().alias("ceil"), constant(1.8).floor().alias("floor"), constant(2.25).sqrt().alias("sqrt"),
                constant(-1.5).abs().alias("abs"),
            ], _THE_FIRST(f)),
            **_one_row(["ceil", "floor", "sqrt", "abs"], [math.ceil(1.2), math.floor(1.8), math.sqrt(2.25), abs(-1.5)]),
            approximate=True,
        ),
        TestExpectation(
            name="float_constant_trigonometry",
            query=lambda f: f.find_all([
                constant(0.5).sin().alias("sin"), constant(0.5).cos().alias("cos"), constant(0.5).tan().alias("tan"),
                constant(0.5).asin().alias("asin"), constant(0.5).acos().alias("acos"), constant(0.5).atan().alias("atan"),
                constant(1.0).atan2(2).alias("atan2"), constant(0.5).exp().alias("exp"), constant(0.5).log().alias("log"),
            ], _THE_FIRST(f)),
            **_one_row(["sin", "cos", "tan", "asin", "acos", "atan", "atan2", "exp", "log"],
                       [math.sin(0.5), math.cos(0.5), math.tan(0.5), math.asin(0.5), math.acos(0.5), math.atan(0.5),
                        math.atan2(1.0, 2), math.exp(0.5), math.log(0.5)]),
            approximate=True,
        ),
        # --- decimals ---
        TestExpectation(
            name="decimal_constant_functions",
            query=lambda f: f.find_all([
                constant(decimal.Decimal("-1.5")).abs().alias("abs"),
                constant(decimal.Decimal("1.2345")).round(2).alias("round"),
            ], _THE_FIRST(f)),
            **_one_row(["abs", "round"], [decimal.Decimal("1.5"), decimal.Decimal("1.23")]),
        ),
        # --- dates ---
        TestExpectation(
            name="date_constant_parts",
            query=lambda f: f.find_all([
                constant(_D).year().alias("year"), constant(_D).month().alias("month"), constant(_D).day().alias("day"),
                constant(_D).quarter().alias("quarter"), constant(_D).week().alias("week"),
                constant(_D).day_of_week().alias("day_of_week"),
            ], _THE_FIRST(f)),
            # SQL numbers the days of the week from Sunday, 0
            **_one_row(["year", "month", "day", "quarter", "week", "day_of_week"],
                       [_D.year, _D.month, _D.day, (_D.month - 1) // 3 + 1, _D.isocalendar().week, (_D.weekday() + 1) % 7]),
        ),
        TestExpectation(
            name="date_constant_moved",
            query=lambda f: f.find_all([
                constant(_D).add_days(1).alias("add_days"), constant(_D).add_months(1).alias("add_months"),
                constant(_D).add_years(1).alias("add_years"), constant(_D).subtract_days(31).alias("subtract_days"),
                constant(_D).subtract_months(1).alias("subtract_months"), constant(_D).subtract_years(1).alias("subtract_years"),
                (constant(_D) + datetime.timedelta(days=2)).alias("plus"), (constant(_D) - datetime.timedelta(days=2)).alias("minus"),
            ], _THE_FIRST(f)),
            # a date moved by a number of days, months or years is read as a timestamp at midnight, as for a date column
            **_one_row(["add_days", "add_months", "add_years", "subtract_days", "subtract_months", "subtract_years", "plus", "minus"],
                       [datetime.datetime(2024, 2, 1), datetime.datetime(2024, 2, 29), datetime.datetime(2025, 1, 31),
                        datetime.datetime(2023, 12, 31), datetime.datetime(2023, 12, 31), datetime.datetime(2023, 1, 31),
                        datetime.datetime(2024, 2, 2), datetime.datetime(2024, 1, 29)]),
        ),
        TestExpectation(
            name="date_constant_first_day_of_a_period",
            query=lambda f: f.find_all([
                constant(_D).first_day_of_week().alias("week"), constant(_D).first_day_of_month().alias("month"),
                constant(_D).first_day_of_quarter().alias("quarter"), constant(_D).first_day_of_year().alias("year"),
            ], _THE_FIRST(f)),
            **_one_row(["week", "month", "quarter", "year"],
                       [datetime.date(2024, 1, 29), datetime.date(2024, 1, 1), datetime.date(2024, 1, 1), datetime.date(2024, 1, 1)]),
        ),
        TestExpectation(
            name="date_constant_difference",
            query=lambda f: f.find_all([
                constant(_D).diff_days(datetime.date(2024, 2, 10)).alias("days"),
                constant(_D).diff_months(datetime.date(2024, 4, 30)).alias("months"),
                constant(_D).diff_years(datetime.date(2026, 1, 31)).alias("years"),
            ], _THE_FIRST(f)),
            **_one_row(["days", "months", "years"], [(datetime.date(2024, 2, 10) - _D).days, 3, 2]),
        ),
        TestExpectation(
            name="date_constant_part_compares_with_a_number",
            query=lambda f: f.find_all([f.id_()], (constant(_D).year() == 2024).and_(_THE_FIRST(f))),
            expected_columns=["Id"], expected_result=np.array([[1]], dtype=object),
        ),
        # --- datetimes ---
        TestExpectation(
            name="datetime_constant_parts",
            query=lambda f: f.find_all([
                constant(_DT).year().alias("year"), constant(_DT).month().alias("month"), constant(_DT).day().alias("day"),
                constant(_DT).hour().alias("hour"), constant(_DT).minute().alias("minute"), constant(_DT).second().alias("second"),
                constant(_DT).quarter().alias("quarter"), constant(_DT).week().alias("week"),
                constant(_DT).day_of_week().alias("day_of_week"),
            ], _THE_FIRST(f)),
            **_one_row(["year", "month", "day", "hour", "minute", "second", "quarter", "week", "day_of_week"],
                       [_DT.year, _DT.month, _DT.day, _DT.hour, _DT.minute, _DT.second, (_DT.month - 1) // 3 + 1,
                        _DT.isocalendar().week, (_DT.weekday() + 1) % 7]),
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
            **_one_row(["add_hours", "add_minutes", "add_seconds", "add_days", "subtract_hours", "subtract_minutes", "plus", "minus"],
                       [_DT + datetime.timedelta(hours=2), _DT + datetime.timedelta(minutes=5), _DT + datetime.timedelta(seconds=40),
                        _DT + datetime.timedelta(days=1), _DT - datetime.timedelta(hours=1), _DT - datetime.timedelta(minutes=5),
                        _DT + datetime.timedelta(hours=3), _DT - datetime.timedelta(minutes=30)]),
        ),
        TestExpectation(
            name="datetime_constant_difference",
            query=lambda f: f.find_all([
                constant(_DT).diff_hours(datetime.datetime(2016, 8, 10, 12, 0, 0)).alias("hours"),
                constant(_DT).diff_minutes(datetime.datetime(2016, 8, 10, 9, 55, 30)).alias("minutes"),
                constant(_DT).diff_seconds(datetime.datetime(2016, 8, 10, 8, 56, 0)).alias("seconds"),
                constant(_DT).diff_days(datetime.datetime(2016, 8, 12, 8, 55, 30)).alias("days"),
            ], _THE_FIRST(f)),
            **_one_row(["hours", "minutes", "seconds", "days"], [4, 60, 30, 2]),
        ),
        TestExpectation(
            name="a_function_of_a_datetime_constant_is_ordered_like_a_date",
            query=lambda f: f.find_all([f.id_()], (constant(_DT).add_days(1) > _DT).and_(_THE_FIRST(f))),
            expected_columns=["Id"], expected_result=np.array([[1]], dtype=object),
        ),
    ],
)
