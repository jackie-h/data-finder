"""The type of arithmetic, as SQL types it: ``assert_type`` is checked by pyright, the ``isinstance`` by the test run."""
import decimal
from typing import assert_type

import pytest

from datafinder import DecimalAttribute, DoubleAttribute, IntegerAttribute, StringAttribute, to_sql
from model.relational import (
    Column, DecimalArithmeticOperation, DoubleArithmeticOperation, IntegerArithmeticOperation, Table,
    UntypedArithmeticOperation)

integer = IntegerAttribute("I", "I", "INT", "t")
other_integer = IntegerAttribute("J", "J", "INT", "t")
double = DoubleAttribute("D", "D", "DOUBLE", "t")
money = DecimalAttribute("M", "M", "DECIMAL", "t")


def test_integers_stay_integers_for_add_subtract_and_multiply():
    assert isinstance(assert_type(integer + 1, IntegerArithmeticOperation), IntegerArithmeticOperation)
    assert isinstance(assert_type(integer - other_integer, IntegerArithmeticOperation), IntegerArithmeticOperation)
    assert isinstance(assert_type(2 * integer, IntegerArithmeticOperation), IntegerArithmeticOperation)
    assert isinstance(assert_type(1 - integer, IntegerArithmeticOperation), IntegerArithmeticOperation)
    assert isinstance(assert_type(integer * 2 + other_integer * 3, IntegerArithmeticOperation), IntegerArithmeticOperation)
    assert isinstance(assert_type(integer.abs() + 1, IntegerArithmeticOperation), IntegerArithmeticOperation)
    assert isinstance(assert_type(integer.sum() * 2, IntegerArithmeticOperation), IntegerArithmeticOperation)
    assert isinstance(assert_type(integer.count() + 1, IntegerArithmeticOperation), IntegerArithmeticOperation)


def test_a_double_makes_the_result_a_double():
    assert isinstance(assert_type(integer + 1.5, DoubleArithmeticOperation), DoubleArithmeticOperation)
    assert isinstance(assert_type(integer * double, DoubleArithmeticOperation), DoubleArithmeticOperation)
    assert isinstance(assert_type(double + 1, DoubleArithmeticOperation), DoubleArithmeticOperation)
    assert isinstance(assert_type(1.5 * integer, DoubleArithmeticOperation), DoubleArithmeticOperation)
    assert isinstance(assert_type(money * 1.5, DoubleArithmeticOperation), DoubleArithmeticOperation)
    assert isinstance(assert_type(integer.average() + 1, DoubleArithmeticOperation), DoubleArithmeticOperation)


def test_a_decimal_stays_a_decimal_with_an_integer_or_a_decimal():
    assert isinstance(assert_type(money + 1, DecimalArithmeticOperation), DecimalArithmeticOperation)
    assert isinstance(assert_type(integer * money, DecimalArithmeticOperation), DecimalArithmeticOperation)
    assert isinstance(assert_type(money - decimal.Decimal("1.5"), DecimalArithmeticOperation), DecimalArithmeticOperation)
    assert isinstance(assert_type(decimal.Decimal("2") * money, DecimalArithmeticOperation), DecimalArithmeticOperation)


def test_division_is_always_a_double():
    assert isinstance(assert_type(integer / 2, DoubleArithmeticOperation), DoubleArithmeticOperation)
    assert isinstance(assert_type(integer / other_integer, DoubleArithmeticOperation), DoubleArithmeticOperation)
    assert isinstance(assert_type(money / money, DoubleArithmeticOperation), DoubleArithmeticOperation)
    assert isinstance(assert_type(1 / integer, DoubleArithmeticOperation), DoubleArithmeticOperation)


def test_a_result_is_ordered_as_its_type_and_chains():
    table = Table("t", [Column("I", "INT"), Column("J", "INT")])
    assert "t0.I * 2 > 4" in to_sql(None, None, [integer], table, integer * 2 > 4)
    assert "t0.I + t0.J IN (1, 2)" in to_sql(None, None, [integer], table, (integer + other_integer).is_in([1, 2]))
    # a result is not changed by chaining from it
    total = integer + 1
    _ = total * 2
    assert "t0.I + 1" in to_sql(None, None, [total], table, (total > 0))


def test_an_expression_of_unknown_kind_is_untyped_and_takes_any_comparison():
    from datafinder import when
    case = when(integer > 1).then(1).otherwise(0)
    assert isinstance(case * integer, UntypedArithmeticOperation)
    assert isinstance((integer * double).sum() * 2, UntypedArithmeticOperation)


def test_text_is_not_arithmetic():
    text = StringAttribute("S", "S", "VARCHAR", "t")
    with pytest.raises(TypeError):
        text - 1  # type: ignore[operator]
