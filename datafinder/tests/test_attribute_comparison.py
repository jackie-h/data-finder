"""Comparing an attribute with another attribute or an expression, `ne` and `!=` on every type, and a date difference
between two columns."""
import datetime
import decimal

import pytest

from datafinder import DateAttribute, DateTimeAttribute, IntegerAttribute, StringAttribute, to_sql
from datafinder.typed_attributes import BooleanAttribute, DecimalAttribute, DoubleAttribute
from model.relational import Column, NoOperation, Table


def _table():
    table = Table("t", [Column("I", "INT"), Column("J", "INT"), Column("D", "DOUBLE"), Column("E", "DOUBLE")])
    return table


def _attrs():
    return {
        "int": (IntegerAttribute("I", "I", "INT", "t"), IntegerAttribute("J", "J", "INT", "t"), 5),
        "double": (DoubleAttribute("D", "D", "DOUBLE", "t"), DoubleAttribute("E", "E", "DOUBLE", "t"), 1.5),
        "decimal": (DecimalAttribute("N", "N", "DECIMAL", "t"), DecimalAttribute("M", "M", "DECIMAL", "t"),
                    decimal.Decimal("2.5")),
        "string": (StringAttribute("S", "S", "VARCHAR", "t"), StringAttribute("T", "T", "VARCHAR", "t"), "x"),
        "date": (DateAttribute("A", "A", "DATE", "t"), DateAttribute("B", "B", "DATE", "t"), datetime.date(2024, 1, 2)),
        "datetime": (DateTimeAttribute("U", "U", "TIMESTAMP", "t"), DateTimeAttribute("V", "V", "TIMESTAMP", "t"),
                     datetime.datetime(2024, 1, 2, 3, 4, 5)),
        "boolean": (BooleanAttribute("P", "P", "BOOLEAN", "t"), BooleanAttribute("Q", "Q", "BOOLEAN", "t"), True),
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
    name = StringAttribute("N", "N", "VARCHAR", "t")
    assert "NOT (t0.N LIKE 'MEDIUM POLISHED%')" in _sql(name, ~name.starts_with("MEDIUM POLISHED"))
    assert "NOT (t0.N = 'x' AND t0.N <> 'y')" in _sql(name, ~(name.eq("x").and_(name.ne("y"))))


def test_negating_a_negation_is_the_condition_again():
    name = StringAttribute("N", "N", "VARCHAR", "t")
    condition = name.eq("x")
    assert ~~condition is condition


def test_a_negated_ordering_comparison_is_not_guarded_against_a_missing_value():
    price = DoubleAttribute("P", "P", "DOUBLE", "t", optional=True)
    assert "IS NOT NULL" in _sql(price, price > 5.0)
    assert "IS NOT NULL" not in _sql(price, ~(price > 5.0))


def test_matches_searches_for_a_regular_expression_anywhere_in_the_text():
    name = StringAttribute("N", "N", "VARCHAR", "t")
    assert "REGEXP_MATCHES(t0.N, 'Customer.*Complaints')" in _sql(name, name.matches("Customer.*Complaints"))
    assert "NOT (REGEXP_MATCHES(t0.N, 'a.b'))" in _sql(name, ~name.matches("a.b"))


def test_matches_a_function_of_a_column():
    name = StringAttribute("N", "N", "VARCHAR", "t")
    assert "REGEXP_MATCHES(UPPER(t0.N), 'AC.*E')" in _sql(name, name.upper().matches("AC.*E"))


def test_only_a_type_with_an_order_can_be_compared_with_less_than():
    boolean = BooleanAttribute("B", "B", "BOOLEAN", "t")
    text = StringAttribute("S", "S", "VARCHAR", "t")
    for column in (boolean, text):
        for compare in (lambda c: c < 1, lambda c: c <= 1, lambda c: c > 1, lambda c: c >= 1):
            with pytest.raises(TypeError):
                compare(column)
    assert IntegerAttribute("I", "I", "INT", "t") < 1
    assert DateAttribute("D", "D", "DATE", "t") >= datetime.date(2020, 1, 1)


def test_in_takes_the_type_of_the_column():
    table = Table("t", [Column("I", "INT"), Column("B", "BOOLEAN"), Column("S", "VARCHAR")])
    integer = IntegerAttribute("I", "I", "INT", "t")
    assert "IN (1, 2)" in to_sql(None, None, [integer], table, integer.is_in([1, 2]))
    assert "IN ('a')" in to_sql(None, None, [integer], table, StringAttribute("S", "S", "VARCHAR", "t").is_in(["a"]))
    assert "NOT IN (TRUE)" in to_sql(None, None, [integer], table, BooleanAttribute("B", "B", "BOOLEAN", "t").is_not_in([True]))


def test_a_function_result_has_the_type_the_function_gives():
    from datafinder.scalar_functions import (
        DecimalFunctionOperation, DoubleFunctionOperation, IntegerFunctionOperation, StringFunctionOperation)
    text = StringAttribute("S", "S", "VARCHAR", "t")
    integer = IntegerAttribute("I", "I", "INT", "t")
    double = DoubleAttribute("D", "D", "DOUBLE", "t")
    decimal_ = DecimalAttribute("M", "M", "DECIMAL", "t")
    assert isinstance(text.substring(0, 2), StringFunctionOperation)
    assert isinstance(text.length(), IntegerFunctionOperation)
    # abs, ceil, floor, mod and round keep the type of what they are given
    for function in (lambda c: c.abs(), lambda c: c.ceil(), lambda c: c.floor(), lambda c: c.mod(3), lambda c: c.round()):
        assert isinstance(function(integer), IntegerFunctionOperation)
        assert isinstance(function(double), DoubleFunctionOperation)
        assert isinstance(function(decimal_), DecimalFunctionOperation)
    assert isinstance(integer.abs().abs(), IntegerFunctionOperation)
    # the square root, powers, logarithm, exponential and trigonometry give a floating-point number
    for function in (lambda c: c.sqrt(), lambda c: c.power(2), lambda c: c.log(), lambda c: c.exp(), lambda c: c.sin(),
                     lambda c: c.atan2(1)):
        assert isinstance(function(integer), DoubleFunctionOperation)
    assert isinstance(text.length().sqrt(), DoubleFunctionOperation)
    # fill_null keeps the column's type
    assert isinstance(text.fill_null("x"), StringFunctionOperation)
    assert isinstance(integer.fill_null(0), IntegerFunctionOperation)
    assert isinstance(double.fill_null(0.0), DoubleFunctionOperation)


def test_a_text_function_is_not_ordered_and_a_number_function_is():
    text = StringAttribute("S", "S", "VARCHAR", "t")
    with pytest.raises(TypeError):
        text.upper() < "b"  # type: ignore[operator]
    assert text.length() < 3
    assert not hasattr(text.upper(), "sqrt") and not hasattr(text.length(), "upper")
