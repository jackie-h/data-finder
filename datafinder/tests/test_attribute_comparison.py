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
