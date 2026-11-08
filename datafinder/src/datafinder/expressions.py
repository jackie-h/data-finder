"""Expressions that are not a column of a finder: values to select next to the columns."""
import datetime
import decimal
from typing import overload

from datafinder.typed_constants import (
    DateConstant, DateTimeConstant, DecimalConstant, FloatConstant, IntegerConstant, StringConstant,
)
from model.relational import BooleanConstantOperation, ConstantOperation, CountAllOperation


@overload
def constant(value: bool) -> BooleanConstantOperation: ...  # pyright: ignore[reportOverlappingOverload]  # a bool is an int
@overload
def constant(value: int) -> IntegerConstant: ...
@overload
def constant(value: float) -> FloatConstant: ...
@overload
def constant(value: decimal.Decimal) -> DecimalConstant: ...
@overload
def constant(value: str) -> StringConstant: ...
@overload
def constant(value: datetime.datetime) -> DateTimeConstant: ...  # pyright: ignore[reportOverlappingOverload]  # a datetime is a date
@overload
def constant(value: datetime.date) -> DateConstant: ...
def constant(value: bool | int | float | decimal.Decimal | str | datetime.date | datetime.datetime) -> ConstantOperation:
    """A fixed value as a column: ``f.find_all([f.name(), constant("EU").alias("Region")])``.

    It is named "Constant" unless ``alias`` gives it a name."""
    if isinstance(value, bool):  # before int: bool is a subclass of int
        return BooleanConstantOperation(value)
    if isinstance(value, int):
        return IntegerConstant(value)
    if isinstance(value, float):
        return FloatConstant(value)
    if isinstance(value, decimal.Decimal):
        return DecimalConstant(value)
    if isinstance(value, str):
        return StringConstant(value)
    if isinstance(value, datetime.datetime):  # before date: datetime is a subclass of date
        return DateTimeConstant(value)
    if isinstance(value, datetime.date):
        return DateConstant(value)
    raise TypeError(f"A constant column holds a bool, number, string, date or datetime, not {type(value).__name__}")


def count_all() -> CountAllOperation:
    """The number of rows in the group, ``COUNT(*)``: ``f.find_all([f.category(), count_all()]).group_by(f.category())``.
    Unlike ``count()`` of a column it also counts rows where that column is missing."""
    return CountAllOperation("")
