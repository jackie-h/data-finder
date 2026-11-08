"""The type of arithmetic, as SQL types it: ``assert_type`` is checked by pyright, the ``isinstance`` by the test run."""
import decimal
from typing import assert_type

import pytest

from datafinder import DecimalAttribute, DoubleAttribute, IntegerAttribute, StringAttribute, to_sql
from model.relational import (
    DecimalArithmeticOperation, DoubleArithmeticOperation, IntegerArithmeticOperation, Table,
    UntypedArithmeticOperation)
from model.relational import Table as _OwnerTable
_OWNER_T = _OwnerTable('t', [])

integer = IntegerAttribute("I", "I", "INT", _OWNER_T)
other_integer = IntegerAttribute("J", "J", "INT", _OWNER_T)
double = DoubleAttribute("D", "D", "DOUBLE", _OWNER_T)
money = DecimalAttribute("M", "M", "DECIMAL", _OWNER_T)


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
    table = Table("t", [("I", "INT"), ("J", "INT")])
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
    assert isinstance(case.sum() * integer, UntypedArithmeticOperation)


def test_text_is_not_arithmetic():
    text = StringAttribute("S", "S", "VARCHAR", _OWNER_T)
    with pytest.raises(TypeError):
        text - 1  # type: ignore[operator]


def test_an_aggregate_of_arithmetic_has_the_type_of_the_arithmetic():
    from model.relational import (
        DecimalAggregateOperation, DoubleAggregateOperation, IntegerAggregateOperation, UntypedAggregateOperation)
    # the sum, minimum and maximum are of the expression's kind
    assert isinstance(assert_type((integer * 2).sum(), IntegerAggregateOperation), IntegerAggregateOperation)
    assert isinstance(assert_type((integer + other_integer).max(), IntegerAggregateOperation), IntegerAggregateOperation)
    assert isinstance(assert_type((integer * double).sum(), DoubleAggregateOperation), DoubleAggregateOperation)
    assert isinstance(assert_type((integer / 2).min(), DoubleAggregateOperation), DoubleAggregateOperation)
    assert isinstance(assert_type((money * 2).sum(), DecimalAggregateOperation), DecimalAggregateOperation)
    # a count is an integer and an average is a double, whatever the kind
    assert isinstance(assert_type((money * 2).count(), IntegerAggregateOperation), IntegerAggregateOperation)
    assert isinstance(assert_type((integer * 2).average(), DoubleAggregateOperation), DoubleAggregateOperation)
    assert isinstance(assert_type((integer * 2).std(), DoubleAggregateOperation), DoubleAggregateOperation)
    # so the result can be compared and used in arithmetic as that kind
    assert isinstance(assert_type((integer * 2).sum() * 3, IntegerArithmeticOperation), IntegerArithmeticOperation)
    assert isinstance(assert_type((integer * 2).sum() / (integer * 3).sum(), DoubleArithmeticOperation),
                      DoubleArithmeticOperation)
    assert (integer * 2).sum() > 10
    # a function result has aggregates of its own kind, an expression of unknown kind has untyped ones
    assert isinstance(assert_type(integer.abs().sum(), IntegerAggregateOperation), IntegerAggregateOperation)
    assert isinstance(assert_type(double.sqrt().max(), DoubleAggregateOperation), DoubleAggregateOperation)
    from datafinder import when
    assert isinstance(assert_type(when(integer > 1).then(1).otherwise(0).sum(), UntypedAggregateOperation),
                      UntypedAggregateOperation)


def test_over_keeps_the_kind_of_an_aggregate_of_arithmetic():
    from model.relational import IntegerAggregateOperation
    total = (integer * 2).sum().over(partition_by=[other_integer])
    assert isinstance(total, IntegerAggregateOperation)


def test_a_constant_has_aggregates_of_its_kind():
    import datetime
    from datafinder import constant
    from model.relational import (
        BooleanConstantOperation, DateConstantOperation, DateTimeConstantOperation, DecimalAggregateOperation,
        DecimalConstantOperation, DoubleAggregateOperation, FloatConstantOperation, IntegerAggregateOperation,
        IntegerConstantOperation, StringConstantOperation, UntypedAggregateOperation)
    # constant() is typed by its argument
    assert isinstance(assert_type(constant(1), IntegerConstantOperation), IntegerConstantOperation)
    assert isinstance(assert_type(constant(1.5), FloatConstantOperation), FloatConstantOperation)
    assert isinstance(assert_type(constant(decimal.Decimal("1")), DecimalConstantOperation), DecimalConstantOperation)
    assert isinstance(assert_type(constant("x"), StringConstantOperation), StringConstantOperation)
    assert isinstance(assert_type(constant(True), BooleanConstantOperation), BooleanConstantOperation)
    assert isinstance(assert_type(constant(datetime.date(2020, 1, 1)), DateConstantOperation), DateConstantOperation)
    assert isinstance(assert_type(constant(datetime.datetime(2020, 1, 1)), DateTimeConstantOperation), DateTimeConstantOperation)
    # a number's sum, minimum and maximum are of its kind
    assert isinstance(assert_type(constant(1).sum(), IntegerAggregateOperation), IntegerAggregateOperation)
    assert isinstance(assert_type(constant(1).max(), IntegerAggregateOperation), IntegerAggregateOperation)
    assert isinstance(assert_type(constant(1.5).sum(), DoubleAggregateOperation), DoubleAggregateOperation)
    assert isinstance(assert_type(constant(decimal.Decimal("1.5")).min(), DecimalAggregateOperation), DecimalAggregateOperation)
    # a count is an integer, an average a double
    assert isinstance(assert_type(constant(1).count(), IntegerAggregateOperation), IntegerAggregateOperation)
    assert isinstance(assert_type(constant(1).average(), DoubleAggregateOperation), DoubleAggregateOperation)
    # text, dates and booleans have no typed sum
    assert isinstance(assert_type(constant("x").min(), UntypedAggregateOperation), UntypedAggregateOperation)
    assert isinstance(constant(datetime.date(2020, 1, 1)).max(), UntypedAggregateOperation)
    # a constant's kind is known to arithmetic with it, so the result is typed
    assert isinstance(constant(100.0) * integer.sum(), DoubleArithmeticOperation)
    assert isinstance(constant(2) * integer, IntegerArithmeticOperation)
    assert isinstance(integer * constant(2), IntegerArithmeticOperation)
    assert isinstance(integer * constant(2.5), DoubleArithmeticOperation)


def test_a_constant_has_arithmetic_and_comparisons_of_its_kind():
    import datetime
    from datafinder import constant
    table = Table("t", [("I", "INT"), ("D", "DOUBLE")])
    # arithmetic: the kind of the result follows the same rules as for a column
    assert isinstance(assert_type(constant(2) * 3, IntegerArithmeticOperation), IntegerArithmeticOperation)
    assert isinstance(assert_type(constant(2) + constant(3), IntegerArithmeticOperation), IntegerArithmeticOperation)
    assert isinstance(assert_type(10 - constant(2), IntegerArithmeticOperation), IntegerArithmeticOperation)
    assert isinstance(assert_type(constant(2) * 2.5, DoubleArithmeticOperation), DoubleArithmeticOperation)
    assert isinstance(assert_type(constant(1.5) * integer, DoubleArithmeticOperation), DoubleArithmeticOperation)
    assert isinstance(assert_type(constant(2) / 4, DoubleArithmeticOperation), DoubleArithmeticOperation)
    assert isinstance(assert_type(constant(decimal.Decimal("1.5")) + 1, DecimalArithmeticOperation), DecimalArithmeticOperation)
    assert isinstance(assert_type(integer * constant(2), IntegerArithmeticOperation), IntegerArithmeticOperation)
    # in a query
    assert "5 * t0.I" in to_sql(None, None, [constant(5) * integer], table, constant(1) < integer)
    assert "WHERE 1 < t0.I" in to_sql(None, None, [integer], table, constant(1) < integer)
    assert "WHERE 3 IN (1, 3)" in to_sql(None, None, [integer], table, constant(3).is_in([1, 3]))
    # comparisons: a number and a date are ordered, text and a boolean are only equal or not
    assert constant(1) < 2 and constant(1.5) >= 1.0 and constant(datetime.date(2020, 1, 1)) < datetime.date(2021, 1, 1)
    assert constant("a").eq("a") and constant(True).ne(False)
    for unordered in (constant("a"), constant(True)):
        with pytest.raises(TypeError):
            unordered < 1  # type: ignore[operator]
    # a text constant is not a number
    with pytest.raises(TypeError):
        constant("a") * 2  # type: ignore[operator]
    with pytest.raises(TypeError):
        constant(datetime.date(2020, 1, 1)) + 1  # type: ignore[operator]
