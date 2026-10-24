"""A value that depends on a condition: ``if_(f.age() > 30, "senior", "junior")``."""
import datetime
import decimal

from model.relational import (
    BooleanConstantOperation, CaseOperation, ComparisonOperation, ConstantOperation, DateConstantOperation,
    DateTimeConstantOperation, DecimalConstantOperation, ExistsOperation, FloatConstantOperation, InOperation,
    IntegerConstantOperation, IsNotNullOperation, IsNullOperation, LogicalOperation, NotExistsOperation,
    NotInOperation, RelationalOperationElement, StringConstantOperation,
)

_CONDITIONS = (ComparisonOperation, LogicalOperation, IsNullOperation, IsNotNullOperation, InOperation,
               NotInOperation, ExistsOperation, NotExistsOperation)


def _value(value) -> RelationalOperationElement:
    """A branch of the conditional: a column or expression as it is, a Python value as a constant."""
    if isinstance(value, RelationalOperationElement):
        column_with_join = getattr(value, "_column_with_join", None)
        return column_with_join() if column_with_join is not None else value
    constants: list[tuple[type, type[ConstantOperation]]] = [
        (bool, BooleanConstantOperation), (int, IntegerConstantOperation), (float, FloatConstantOperation),
        (decimal.Decimal, DecimalConstantOperation), (str, StringConstantOperation),
        (datetime.datetime, DateTimeConstantOperation), (datetime.date, DateConstantOperation),
    ]  # bool before int and datetime before date: each is a subclass of the next
    for python_type, constant in constants:
        if isinstance(value, python_type):
            return constant(value)  # type: ignore[call-arg]
    raise TypeError(f"A branch is a column, an expression or a value, not {type(value).__name__}")


def if_(condition, then, otherwise=None) -> CaseOperation:
    """``then`` where ``condition`` holds, else ``otherwise``; with no ``otherwise`` the value is missing (NULL).

    A branch is a column, an expression or a Python value, and may itself be a conditional, so several cases nest:
    ``if_(a, 1, if_(b, 2, 3))``. A condition that is NULL for a row (a comparison with a missing value) is not true,
    so that row takes ``otherwise``."""
    if not isinstance(condition, _CONDITIONS):
        raise TypeError("The condition is a comparison, a null check, a membership test, exists, or and_/or_ of those")
    return CaseOperation(condition, _value(then), None if otherwise is None else _value(otherwise))
