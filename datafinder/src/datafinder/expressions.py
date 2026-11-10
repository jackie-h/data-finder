"""Expressions that are not a column of a finder: values to select next to the columns."""
import datetime
import decimal
from typing import overload

from model.relational import (
    CountAllOperation,
    BooleanConstantOperation, ConstantOperation, DateConstantOperation, DateTimeConstantOperation,
    DecimalConstantOperation, FloatConstantOperation, IntegerConstantOperation, IntervalConstantOperation, StringConstantOperation,
)


@overload
def constant(value: bool) -> BooleanConstantOperation: ...  # pyright: ignore[reportOverlappingOverload]  # a bool is an int
@overload
def constant(value: int) -> IntegerConstantOperation: ...
@overload
def constant(value: float) -> FloatConstantOperation: ...
@overload
def constant(value: decimal.Decimal) -> DecimalConstantOperation: ...
@overload
def constant(value: str) -> StringConstantOperation: ...
@overload
def constant(value: datetime.datetime) -> DateTimeConstantOperation: ...  # pyright: ignore[reportOverlappingOverload]  # a datetime is a date
@overload
def constant(value: datetime.date) -> DateConstantOperation: ...
@overload
def constant(value: datetime.timedelta) -> IntervalConstantOperation: ...
def constant(value: bool | int | float | decimal.Decimal | str | datetime.date | datetime.datetime
             | datetime.timedelta) -> ConstantOperation:
    """A fixed value as a column: ``f.find_all([f.name(), constant("EU").alias("Region")])``.

    It is named "Constant" unless ``alias`` gives it a name."""
    if isinstance(value, bool):  # before int: bool is a subclass of int
        return BooleanConstantOperation(value)
    if isinstance(value, int):
        return IntegerConstantOperation(value)
    if isinstance(value, float):
        return FloatConstantOperation(value)
    if isinstance(value, decimal.Decimal):
        return DecimalConstantOperation(value)
    if isinstance(value, str):
        return StringConstantOperation(value)
    if isinstance(value, datetime.datetime):  # before date: datetime is a subclass of date
        return DateTimeConstantOperation(value)
    if isinstance(value, datetime.date):
        return DateConstantOperation(value)
    if isinstance(value, datetime.timedelta):
        return IntervalConstantOperation(value)
    raise TypeError(f"A constant column holds a bool, number, string, date, datetime or timedelta, not {type(value).__name__}")


def count_all() -> CountAllOperation:
    """The number of rows in the group, ``COUNT(*)``: ``f.find_all([f.category(), count_all()]).group_by(f.category())``.
    Unlike ``count()`` of a column it also counts rows where that column is missing."""
    return CountAllOperation("")
