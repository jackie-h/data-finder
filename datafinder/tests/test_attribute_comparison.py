"""Comparing an attribute with another attribute or an expression, `ne` and `!=` on every type, and a date difference
between two columns."""
import datetime
import decimal

import pytest

from datafinder import DateAttribute, DateTimeAttribute, IntegerAttribute, StringAttribute, to_sql
from datafinder.typed_attributes import BooleanAttribute, DecimalAttribute, DoubleAttribute
from model.relational import NoOperation, Table
from model.relational import Table as _OwnerTable
_OWNER_T = _OwnerTable('t', [])


def _table():
    table = Table("t", [("I", "INT"), ("J", "INT"), ("D", "DOUBLE"), ("E", "DOUBLE")])
    return table


def _attrs():
    return {
        "int": (IntegerAttribute("I", "I", "INT", _OWNER_T), IntegerAttribute("J", "J", "INT", _OWNER_T), 5),
        "double": (DoubleAttribute("D", "D", "DOUBLE", _OWNER_T), DoubleAttribute("E", "E", "DOUBLE", _OWNER_T), 1.5),
        "decimal": (DecimalAttribute("N", "N", "DECIMAL", _OWNER_T), DecimalAttribute("M", "M", "DECIMAL", _OWNER_T),
                    decimal.Decimal("2.5")),
        "string": (StringAttribute("S", "S", "VARCHAR", _OWNER_T), StringAttribute("T", "T", "VARCHAR", _OWNER_T), "x"),
        "date": (DateAttribute("A", "A", "DATE", _OWNER_T), DateAttribute("B", "B", "DATE", _OWNER_T), datetime.date(2024, 1, 2)),
        "datetime": (DateTimeAttribute("U", "U", "TIMESTAMP", _OWNER_T), DateTimeAttribute("V", "V", "TIMESTAMP", _OWNER_T),
                     datetime.datetime(2024, 1, 2, 3, 4, 5)),
        "boolean": (BooleanAttribute("P", "P", "BOOLEAN", _OWNER_T), BooleanAttribute("Q", "Q", "BOOLEAN", _OWNER_T), True),
    }


_ORDERED = ["int", "double", "decimal", "date", "datetime"]


def _sql(select, condition):
    return to_sql(None, None, [select], _table(), condition)


@pytest.mark.parametrize("kind", _ORDERED)
@pytest.mark.parametrize("op, symbol", [
    (lambda a, b: a < b, " < "), (lambda a, b: a <= b, " <= "), (lambda a, b: a > b, " > "),
    (lambda a, b: a >= b, " >= "), (lambda a, b: a == b, " = "), (lambda a, b: a != b, " <> "),
])
def test_an_ordered_attribute_compares_with_another_attribute(kind, op, symbol):
    left, right, _ = _attrs()[kind]
    sql = _sql(left, op(left, right))
    assert f"{left.column().name}{symbol}" in sql.replace("t0.", "")
    assert f"{symbol}t0.{right.column().name}" in sql


@pytest.mark.parametrize("kind", ["string", "boolean"])
def test_equality_of_a_string_or_boolean_attribute_with_another_attribute(kind):
    left, right, _ = _attrs()[kind]
    assert f" = t0.{right.column().name}" in _sql(left, left.eq(right))
    assert f" <> t0.{right.column().name}" in _sql(left, left.ne(right))


@pytest.mark.parametrize("kind", list(_attrs()))
def test_ne_and_not_equal_compare_with_a_value(kind):
    left, _, value = _attrs()[kind]
    assert " <> " in _sql(left, left.ne(value))
    assert " <> " in _sql(left, left != value)


def test_not_equal_is_not_the_negation_of_equal():
    # Python derives != from ==, which here is a filter and so always true: != would be False and the filter lost.
    left, _, _ = _attrs()["int"]
    condition = left != 1
    assert not isinstance(condition, bool)


def test_an_attribute_compares_with_an_expression():
    price, cost, _ = _attrs()["double"]
    sql = _sql(price, price > cost * 1.075)
    assert "t0.D > t0.E * 1.075" in sql


def test_an_expression_compares_with_an_attribute():
    price, cost, _ = _attrs()["double"]
    sql = _sql(price, price * 2 < cost)
    assert "t0.D * 2 < t0.E" in sql


def test_a_value_is_still_a_constant():
    left, _, _ = _attrs()["date"]
    assert "t0.A < '2024-01-02'" in _sql(left, left < datetime.date(2024, 1, 2))


@pytest.mark.parametrize("method, unit", [("diff_days", "day"), ("diff_months", "month"), ("diff_years", "year")])
def test_a_date_difference_with_a_column(method, unit):
    start, end, _ = _attrs()["date"]
    sql = _sql(getattr(start, method)(end), NoOperation())
    assert f"DATE_DIFF('{unit}', t0.A, t0.B)" in sql


@pytest.mark.parametrize("method, unit", [("diff_hours", "hour"), ("diff_minutes", "minute"), ("diff_seconds", "second")])
def test_a_datetime_difference_with_a_column(method, unit):
    start, end, _ = _attrs()["datetime"]
    sql = _sql(getattr(start, method)(end), NoOperation())
    assert f"DATE_DIFF('{unit}', t0.U, t0.V)" in sql


def test_a_date_difference_with_a_value_is_unchanged():
    start, _, _ = _attrs()["date"]
    sql = _sql(start.diff_days(datetime.date(2024, 6, 15)), NoOperation())
    assert "DATE_DIFF('day', t0.A, '2024-06-15')" in sql


def test_a_condition_is_negated_with_a_tilde():
    name = StringAttribute("N", "N", "VARCHAR", _OWNER_T)
    assert "NOT (t0.N LIKE 'MEDIUM POLISHED%')" in _sql(name, ~name.starts_with("MEDIUM POLISHED"))
    assert "NOT (t0.N = 'x' AND t0.N <> 'y')" in _sql(name, ~(name.eq("x").and_(name.ne("y"))))


def test_negating_a_negation_is_the_condition_again():
    name = StringAttribute("N", "N", "VARCHAR", _OWNER_T)
    condition = name.eq("x")
    assert ~~condition is condition


def test_a_negated_ordering_comparison_is_not_guarded_against_a_missing_value():
    price = DoubleAttribute("P", "P", "DOUBLE", _OWNER_T, optional=True)
    assert "IS NOT NULL" in _sql(price, price > 5.0)
    assert "IS NOT NULL" not in _sql(price, ~(price > 5.0))


def test_matches_searches_for_a_regular_expression_anywhere_in_the_text():
    name = StringAttribute("N", "N", "VARCHAR", _OWNER_T)
    assert "REGEXP_MATCHES(t0.N, 'Customer.*Complaints')" in _sql(name, name.matches("Customer.*Complaints"))
    assert "NOT (REGEXP_MATCHES(t0.N, 'a.b'))" in _sql(name, ~name.matches("a.b"))


def test_matches_a_function_of_a_column():
    name = StringAttribute("N", "N", "VARCHAR", _OWNER_T)
    assert "REGEXP_MATCHES(UPPER(t0.N), 'AC.*E')" in _sql(name, name.upper().matches("AC.*E"))


def test_only_a_type_with_an_order_can_be_compared_with_less_than():
    boolean = BooleanAttribute("B", "B", "BOOLEAN", _OWNER_T)
    text = StringAttribute("S", "S", "VARCHAR", _OWNER_T)
    for column in (boolean, text):
        for compare in (lambda c: c < 1, lambda c: c <= 1, lambda c: c > 1, lambda c: c >= 1):
            with pytest.raises(TypeError):
                compare(column)
    assert IntegerAttribute("I", "I", "INT", _OWNER_T) < 1
    assert DateAttribute("D", "D", "DATE", _OWNER_T) >= datetime.date(2020, 1, 1)


def test_in_takes_the_type_of_the_column():
    table = Table("t", [("I", "INT"), ("B", "BOOLEAN"), ("S", "VARCHAR")])
    integer = IntegerAttribute("I", "I", "INT", _OWNER_T)
    assert "IN (1, 2)" in to_sql(None, None, [integer], table, integer.is_in([1, 2]))
    assert "IN ('a')" in to_sql(None, None, [integer], table, StringAttribute("S", "S", "VARCHAR", _OWNER_T).is_in(["a"]))
    assert "NOT IN (TRUE)" in to_sql(None, None, [integer], table, BooleanAttribute("B", "B", "BOOLEAN", _OWNER_T).is_not_in([True]))


def test_a_function_result_has_the_type_the_function_gives():
    from datafinder.scalar_functions import (
        DecimalFunctionOperation, DoubleFunctionOperation, IntegerFunctionOperation, StringFunctionOperation)
    text = StringAttribute("S", "S", "VARCHAR", _OWNER_T)
    integer = IntegerAttribute("I", "I", "INT", _OWNER_T)
    double = DoubleAttribute("D", "D", "DOUBLE", _OWNER_T)
    decimal_ = DecimalAttribute("M", "M", "DECIMAL", _OWNER_T)
    assert isinstance(text.substring(0, 2), StringFunctionOperation)
    assert isinstance(text.length(), IntegerFunctionOperation)
    # abs, ceil, floor, mod and round keep the type of what they are given
    for function in (lambda c: c.abs(), lambda c: c.ceil(), lambda c: c.floor(), lambda c: c.mod(3), lambda c: c.round()):
        assert isinstance(function(integer), IntegerFunctionOperation)
        assert isinstance(function(double), DoubleFunctionOperation)
        assert isinstance(function(decimal_), DecimalFunctionOperation)
    assert isinstance(integer.abs().abs(), IntegerFunctionOperation)
    # the square root, powers, logarithm, exponential and trigonometry give a floating-point number
    for function in (lambda c: c.sqrt(), lambda c: c.log(), lambda c: c.exp(), lambda c: c.sin(),
                     lambda c: c.atan2(1)):
        assert isinstance(function(integer), DoubleFunctionOperation)
    assert isinstance(text.length().sqrt(), DoubleFunctionOperation)
    # a whole, non-negative power of an integer is an integer; any other power is a floating-point number
    assert isinstance(integer.power(2), IntegerFunctionOperation) and integer.power(2).cast == "BIGINT"
    assert isinstance(integer ** 3, IntegerFunctionOperation)
    assert isinstance(integer.power(-1), DoubleFunctionOperation)
    assert isinstance(integer.power(0.5), DoubleFunctionOperation)  # type: ignore[arg-type]
    assert isinstance(double.power(2), DoubleFunctionOperation) and isinstance(decimal_.power(2), DoubleFunctionOperation)
    # fill_null keeps the column's type
    assert isinstance(text.fill_null("x"), StringFunctionOperation)
    assert isinstance(integer.fill_null(0), IntegerFunctionOperation)
    assert isinstance(double.fill_null(0.0), DoubleFunctionOperation)


def test_a_text_function_is_not_ordered_and_a_number_function_is():
    text = StringAttribute("S", "S", "VARCHAR", _OWNER_T)
    with pytest.raises(TypeError):
        text.upper() < "b"  # type: ignore[operator]
    assert text.length() < 3
    assert not hasattr(text.upper(), "sqrt") and not hasattr(text.length(), "upper")


def test_a_date_operation_is_compared_as_what_it_gives():
    from model.relational import DateArithmeticOperation, DateDiffOperation, DateExtractOperation, DateTruncOperation
    table = Table("t", [("D", "DATE"), ("T", "TIMESTAMP")])
    day = DateAttribute("D", "D", "DATE", _OWNER_T)
    moment = DateTimeAttribute("T", "T", "TIMESTAMP", _OWNER_T)
    # a part of a date and a difference are integers
    assert isinstance(day.year(), DateExtractOperation) and isinstance(moment.hour(), DateExtractOperation)
    assert isinstance(day.diff_days(datetime.date(2020, 1, 1)), DateDiffOperation)
    sql = to_sql(None, None, [day], table, day.year().is_in([2019, 2020]))
    assert "EXTRACT(YEAR FROM t0.D) IN (2019, 2020)" in sql
    assert "EXTRACT(HOUR FROM t0.T) >= 9" in to_sql(None, None, [moment], table, moment.hour() >= 9)
    assert "WHERE t0.D + INTERVAL '1' DAY > '2020-01-01'" in to_sql(
        None, None, [day], table, day.add_days(1) > datetime.date(2020, 1, 1))
    assert isinstance(day.first_day_of_month(), DateTruncOperation)
    assert isinstance(day.add_days(1), DateArithmeticOperation)


def test_joined_text_is_compared_for_equality_only():
    first = StringAttribute("F", "F", "VARCHAR", _OWNER_T)
    last = StringAttribute("L", "L", "VARCHAR", _OWNER_T)
    table = Table("t", [("F", "VARCHAR"), ("L", "VARCHAR")])
    name = first + " " + last
    for compare in (lambda n: n < "b", lambda n: n >= "b"):
        with pytest.raises(TypeError):
            compare(name)  # type: ignore[operator]
    assert "IN ('Ann Lee')" in to_sql(None, None, [first], table, name.is_in(["Ann Lee"]))
    assert "NOT IN ('Ann Lee')" in to_sql(None, None, [first], table, name.is_not_in(["Ann Lee"]))
    assert "WHERE t0.F || ' ' || t0.L = 'Ann Lee'" in to_sql(None, None, [first], table, name.eq("Ann Lee"))


def test_an_aggregate_has_the_type_it_gives():
    from datafinder import when
    from model.relational import (
        BooleanAggregateOperation, DateAggregateOperation, DateTimeAggregateOperation, DecimalAggregateOperation,
        DoubleAggregateOperation, IntegerAggregateOperation, StringAggregateOperation, UntypedAggregateOperation)
    text = StringAttribute("S", "S", "VARCHAR", _OWNER_T)
    integer = IntegerAttribute("I", "I", "INT", _OWNER_T)
    double = DoubleAttribute("D", "D", "DOUBLE", _OWNER_T)
    decimal_ = DecimalAttribute("M", "M", "DECIMAL", _OWNER_T)
    day = DateAttribute("A", "A", "DATE", _OWNER_T)
    moment = DateTimeAttribute("T", "T", "TIMESTAMP", _OWNER_T)
    # a count is an integer, whatever it counts
    for column in (text, integer, double, day):
        assert isinstance(column.count(), IntegerAggregateOperation)
        assert isinstance(column.count_distinct(), IntegerAggregateOperation)
    # the sum, minimum and maximum of a number are of its type
    for function in (lambda c: c.sum(), lambda c: c.min(), lambda c: c.max()):
        assert isinstance(function(integer), IntegerAggregateOperation)
        assert isinstance(function(double), DoubleAggregateOperation)
        assert isinstance(function(decimal_), DecimalAggregateOperation)
    # an average, standard deviation and variance are floating point, even of an integer
    for function in (lambda c: c.average(), lambda c: c.std(), lambda c: c.var(ddof=0)):
        for column in (integer, double, decimal_):
            assert isinstance(function(column), DoubleAggregateOperation)
    # the minimum and maximum of text and of dates are of their type
    assert isinstance(text.min(), StringAggregateOperation) and isinstance(text.join(","), StringAggregateOperation)
    assert isinstance(day.min(), DateAggregateOperation) and isinstance(moment.max(), DateTimeAggregateOperation)
    assert isinstance((integer > 1).all(), BooleanAggregateOperation) and isinstance((integer > 1).any(), BooleanAggregateOperation)
    # an expression of an unknown type (a case) gives an untyped sum, but a known count and average
    assert isinstance(when(integer > 1).then(1).otherwise(0).sum(), UntypedAggregateOperation)
    assert isinstance((integer * double).count(), IntegerAggregateOperation)
    assert isinstance((integer * double).average(), DoubleAggregateOperation)
    # over() keeps the type
    assert isinstance(integer.sum().over(partition_by=[text]), IntegerAggregateOperation)


def test_an_aggregate_is_ordered_only_if_what_it_gives_is():
    text = StringAttribute("S", "S", "VARCHAR", _OWNER_T)
    integer = IntegerAttribute("I", "I", "INT", _OWNER_T)
    assert integer.sum() > 3 and integer.count() <= 2 and integer.max() + 1
    assert DateAttribute("A", "A", "DATE", _OWNER_T).max() >= datetime.date(2020, 1, 1)
    for text_aggregate in (text.min(), text.join(",")):
        with pytest.raises(TypeError):
            text_aggregate < "b"  # type: ignore[operator]
        assert text_aggregate.eq("a") and text_aggregate.starts_with("a")
    with pytest.raises(TypeError):
        (integer > 1).all() < True  # type: ignore[operator]
    assert not hasattr(text.min(), "sum")
