"""A value that depends on a condition, written as in polars: ``when(f.age() > 30).then("senior").otherwise("junior")``."""
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


def _condition(condition):
    if not isinstance(condition, _CONDITIONS):
        raise TypeError("The condition is a comparison, a null check, a membership test, exists, or and_/or_ of those")
    return condition


def _chain(branches: list, default) -> CaseOperation:
    """The cases in order as a conditional: each later case is what the one before falls back to."""
    condition, then = branches[0]
    rest = _chain(branches[1:], default) if len(branches) > 1 else default
    return CaseOperation(condition, then, rest)


class Then(CaseOperation):
    """``when(condition).then(value)``: a column on its own (missing where the condition does not hold), or continued
    with another ``when`` or closed with ``otherwise``."""

    def __init__(self, branches: list):
        self._branches = branches
        case = _chain(branches, None)
        super().__init__(case.condition, case.then, case.default)

    def when(self, condition) -> 'When':
        """Another case, tried when the ones before it do not hold."""
        return When(_condition(condition), self._branches)

    def otherwise(self, value) -> CaseOperation:  # type: ignore[override]
        """The value when none of the cases hold."""
        return _chain(self._branches, _value(value))


class When:
    def __init__(self, condition, earlier: list | None = None):
        self._condition = condition
        self._earlier = earlier or []

    def then(self, value) -> Then:
        """The value where the condition holds. A value is a column, an expression or a Python value."""
        return Then([*self._earlier, (self._condition, _value(value))])


def when(condition) -> When:
    """Start a conditional: ``when(f.category().eq("Technology")).then("tech").otherwise("other")``.

    Further cases follow with ``.when(...).then(...)``; with no ``otherwise`` the value is missing (NULL). A condition
    that is NULL for a row (a comparison with a missing value) is not true, so that row takes the next case."""
    return When(_condition(condition))
