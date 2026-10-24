"""Expressions that are not a column of a finder: values to select next to the columns."""
import datetime
import decimal

from model.relational import (
    BooleanConstantOperation, ConstantOperation, DateConstantOperation, DateTimeConstantOperation,
    DecimalConstantOperation, FloatConstantOperation, IntegerConstantOperation, StringConstantOperation,
)


def constant(value: bool | int | float | decimal.Decimal | str | datetime.date | datetime.datetime) -> ConstantOperation:
    """A fixed value as a column: ``f.find_all(None, None, [f.name(), constant("EU").alias("Region")])``.

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
    raise TypeError(f"A constant column holds a bool, number, string, date or datetime, not {type(value).__name__}")
