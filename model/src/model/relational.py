import datetime
import decimal
from enum import Enum
from typing import Any, Generic, Iterable, Sequence, TypeVar, overload

from model.function_mixins import DateFunctions, DateTimeFunctions, NumericFunctions, StringFunctions

T = TypeVar("T")
K = TypeVar("K")  # the kind of number an expression holds: int, float or Decimal
AG = TypeVar("AG", bound="AggregateOperation")  # the aggregate that gives an expression's own type


# Interface
class RelationalOperationElement:
    def __init__(self):
        pass

    def _arithmetic_operand(self) -> 'RelationalOperationElement':
        """This expression as an operand of arithmetic; an attribute reads its column."""
        return self


class Sortable:
    """Something the result can be ordered by: ``f.id_().count().ascending()``."""

    def ascending(self) -> 'SortOperation':
        """Sort by this expression, smallest first."""
        return SortOperation(self, SortDirection.ASC)  # type: ignore[arg-type]

    def descending(self) -> 'SortOperation':
        """Sort by this expression, largest first."""
        return SortOperation(self, SortDirection.DESC)  # type: ignore[arg-type]


class Aliasable:
    """Something that can be a column of the result, so can be given a name there: ``f.price().sum().alias("Total")``.
    As in Pure, the name is held by an ``Alias`` that wraps the column."""

    def alias(self, name: str) -> 'Alias':
        return Alias(self, name)  # type: ignore[arg-type]


class Aggregates(Generic[AG]):
    """Aggregates of an expression, as a column has them: ``(f.price() * f.quantity()).sum()``, ``constant(1).sum()``,
    ``when(...).then(5.0).otherwise(1.0).sum()``.

    ``AG`` is the aggregate that gives the expression's own type, the type of its ``sum``, ``min`` and ``max``: an
    integer expression has an IntegerAggregateOperation sum. An expression whose type is not known has an
    UntypedAggregateOperation. A count is an integer, and an average, standard deviation and variance are floating
    point, whatever the type."""

    def _aggregate(self, operator: 'AggregateOperator', label: str,
                   kind: 'type[AggregateOperation] | None' = None) -> 'AggregateOperation':
        kind = kind or getattr(self, '_value_aggregate', None) or UntypedAggregateOperation
        return kind(self._arithmetic_operand(), operator, label + ' Expression')  # type: ignore[attr-defined]

    def sum(self) -> AG:
        return self._aggregate(AggregateOperator.SUM, 'Sum')  # type: ignore[return-value]

    def min(self) -> AG:
        return self._aggregate(AggregateOperator.MIN, 'Min')  # type: ignore[return-value]

    def max(self) -> AG:
        return self._aggregate(AggregateOperator.MAX, 'Max')  # type: ignore[return-value]

    def average(self) -> 'DoubleAggregateOperation':
        return self._aggregate(AggregateOperator.AVERAGE, 'Average', DoubleAggregateOperation)  # type: ignore[return-value]

    def count(self) -> 'IntegerAggregateOperation':
        return self._aggregate(AggregateOperator.COUNT, 'Count', IntegerAggregateOperation)  # type: ignore[return-value]

    def std(self, ddof: int = 1) -> 'DoubleAggregateOperation':
        return self._aggregate(AggregateOperator.STDDEV_SAMP if _sample(ddof) else AggregateOperator.STDDEV_POP, 'Std',
                               DoubleAggregateOperation)  # type: ignore[return-value]

    def var(self, ddof: int = 1) -> 'DoubleAggregateOperation':
        return self._aggregate(AggregateOperator.VAR_SAMP if _sample(ddof) else AggregateOperator.VAR_POP, 'Var',
                               DoubleAggregateOperation)  # type: ignore[return-value]


def _sample(ddof: int) -> bool:
    if ddof not in (0, 1):
        raise ValueError(f"ddof is 0 (population) or 1 (sample), not {ddof}")
    return ddof == 1


class BooleanAggregates:
    """What a condition can do: ``~`` negates it (``~f.name().starts_with("Acme")``, as in pandas, polars and SQLAlchemy, since
    Python's ``not`` cannot be overloaded), and ``(f.quantity() > 20).all()`` is true for a group where every row holds,
    ``.any()`` where at least one does."""

    def __invert__(self) -> 'NotOperation':
        return NotOperation(self)  # type: ignore[arg-type]

    def all(self) -> 'BooleanAggregateOperation':
        return BooleanAggregateOperation(self, AggregateOperator.BOOL_AND, 'All Expression')  # type: ignore[arg-type]

    def any(self) -> 'BooleanAggregateOperation':
        return BooleanAggregateOperation(self, AggregateOperator.BOOL_OR, 'Any Expression')  # type: ignore[arg-type]


class Equatable(Generic[T]):
    """Equality for an expression or a column whose values are ``T``: ``eq``, ``ne``, ``==``, ``!=``, ``is_in`` and
    ``is_not_in``. The other side is a ``T``, another column or an expression, and each gives a filter.

    ``==`` and ``!=`` build a comparison, as on every column. A type that has an order takes Orderable instead.
    A boolean or a string is Equatable only."""

    def _to_constant(self, value: T) -> 'ConstantOperation':
        """A Python value as the constant this compares with. A column narrows it to its type."""
        return _value_operand(value)

    def _compared(self, operator: 'ComparisonOperator', other) -> 'ComparisonOperation':
        right = other._arithmetic_operand() if isinstance(other, RelationalOperationElement) else self._to_constant(other)
        return ComparisonOperation(self._arithmetic_operand(), operator, right)  # type: ignore[attr-defined]

    def eq(self, other: 'T | RelationalOperationElement') -> 'ComparisonOperation':
        """The expression equals a value, or another column or expression."""
        return self._compared(ComparisonOperator.EQUAL, other)

    def ne(self, other: 'T | RelationalOperationElement') -> 'ComparisonOperation':
        """The expression does not equal a value, or another column or expression."""
        return self._compared(ComparisonOperator.NOT_EQUAL, other)

    def __eq__(self, other: 'T | RelationalOperationElement') -> 'ComparisonOperation':  # type: ignore[override]
        return self.eq(other)

    def __ne__(self, other: 'T | RelationalOperationElement') -> 'ComparisonOperation':  # type: ignore[override]
        return self.ne(other)

    __hash__ = object.__hash__  # defining __eq__ would otherwise make an expression unhashable

    def _constants(self, values: 'Iterable[T]') -> 'list[ConstantOperation]':
        constants = [self._to_constant(v) for v in values]
        if not constants:
            raise ValueError("is_in / is_not_in need at least one value")
        return constants

    def is_in(self, values: 'Iterable[T]') -> 'InOperation':
        """The expression is one of ``values``: ``f.name().substring(0, 4).is_in(["beta", "Hell"])``."""
        return InOperation(self._arithmetic_operand(), self._constants(values))  # type: ignore[attr-defined]

    def is_not_in(self, values: 'Iterable[T]') -> 'NotInOperation':
        """The expression is none of ``values``."""
        return NotInOperation(self._arithmetic_operand(), self._constants(values))  # type: ignore[attr-defined]


class Orderable(Equatable[T]):
    """Equatable, and ordered: ``<``, ``<=``, ``>`` and ``>=`` against a ``T``, another column or an expression."""

    def __lt__(self, other: 'T | RelationalOperationElement') -> 'ComparisonOperation':
        return self._compared(ComparisonOperator.LESS_THAN, other)

    def __le__(self, other: 'T | RelationalOperationElement') -> 'ComparisonOperation':
        return self._compared(ComparisonOperator.LESS_THAN_OR_EQUAL_TO, other)

    def __gt__(self, other: 'T | RelationalOperationElement') -> 'ComparisonOperation':
        return self._compared(ComparisonOperator.GREATER_THAN, other)

    def __ge__(self, other: 'T | RelationalOperationElement') -> 'ComparisonOperation':
        return self._compared(ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, other)


class Comparable(Orderable[Any]):
    """Comparisons for an expression whose type is not known: ``f.price() * 2 > 10``, ``f.first() + f.last() == "AnnLee"``.
    The other side is any value, another column or an expression. An expression whose type is known takes Equatable or
    Orderable with that type instead."""


class ArithmeticOperator(Enum):
    ADD = '+'
    SUBTRACT = '-'
    MULTIPLY = '*'
    DIVIDE = '/'


def _constant_operand(value) -> 'ConstantOperation':
    if isinstance(value, bool) or not isinstance(value, (int, float, decimal.Decimal)):
        raise TypeError(f"Arithmetic is between numbers and numeric columns, not {type(value).__name__}")
    if isinstance(value, int):
        return IntegerConstantOperation(value)
    if isinstance(value, float):
        return FloatConstantOperation(value)
    return DecimalConstantOperation(value)


def _number_kind_of(value) -> 'type | None':
    """The kind of number a value or an expression is: ``int``, ``float`` or ``decimal.Decimal``, or None if not known."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, decimal.Decimal)):
        return type(value)
    return getattr(value, '_number_kind', None)


def _arithmetic_result_kind(operator: 'ArithmeticOperator', a: 'type | None', b: 'type | None') -> 'type | None':
    """The kind of number ``a operator b`` gives, as SQL gives it: ``/`` is always a double; otherwise a double if either
    is, else a decimal if either is, else an integer. None if either side is not known."""
    if a is None or b is None:
        return None
    if operator is ArithmeticOperator.DIVIDE or float in (a, b):
        return float
    if decimal.Decimal in (a, b):
        return decimal.Decimal
    return int


class Arithmetic(Generic[K]):
    """The operators ``+ - * /`` for an expression that holds a number of kind ``K`` (``int``, ``float`` or
    ``decimal.Decimal``): ``a + b``, ``a * 2``, ``1 - a``. The other operand is a number or another numeric expression.

    The result is typed as SQL types it: ``+ - *`` of two integers is an integer, of an integer and a decimal a
    decimal, with a double a double; ``/`` is always a double. An expression whose kind is not known gives an
    UntypedArithmeticOperation, which takes any comparison."""

    def _numeric(self) -> None:
        """Raise if this expression is not a number."""

    def _arithmetic(self, operator: 'ArithmeticOperator', other, reflected: bool) -> 'ArithmeticOperation':
        self._numeric()
        if isinstance(other, RelationalOperationElement):
            if isinstance(other, Arithmetic):
                other._numeric()
            operand = other._arithmetic_operand()
        else:
            operand = _constant_operand(other)
        me = self._arithmetic_operand()  # type: ignore[attr-defined]
        left, right = (operand, me) if reflected else (me, operand)
        kind = _arithmetic_result_kind(operator, getattr(self, '_number_kind', None), _number_kind_of(other))
        return _ARITHMETIC_RESULTS[kind](left, operator, right)

    @overload
    def __add__(self: 'Arithmetic[int]', other: 'int | Arithmetic[int]') -> 'IntegerArithmeticOperation': ...  # pyright: ignore[reportOverlappingOverload]

    @overload
    def __add__(self: 'Arithmetic[int]', other: 'decimal.Decimal | Arithmetic[decimal.Decimal]') -> 'DecimalArithmeticOperation': ...

    @overload
    def __add__(self: 'Arithmetic[int]', other: 'float | Arithmetic[float]') -> 'DoubleArithmeticOperation': ...

    @overload
    def __add__(self: 'Arithmetic[float]', other: 'int | float | decimal.Decimal | Arithmetic[int] | Arithmetic[float] | Arithmetic[decimal.Decimal]') -> 'DoubleArithmeticOperation': ...

    @overload
    def __add__(self: 'Arithmetic[decimal.Decimal]', other: 'int | decimal.Decimal | Arithmetic[int] | Arithmetic[decimal.Decimal]') -> 'DecimalArithmeticOperation': ...

    @overload
    def __add__(self: 'Arithmetic[decimal.Decimal]', other: 'float | Arithmetic[float]') -> 'DoubleArithmeticOperation': ...

    @overload
    def __add__(self, other: 'RelationalOperationElement') -> 'UntypedArithmeticOperation': ...

    def __add__(self, other):
        return self._arithmetic(ArithmeticOperator.ADD, other, False)

    @overload
    def __radd__(self: 'Arithmetic[int]', other: 'int') -> 'IntegerArithmeticOperation': ...  # pyright: ignore[reportOverlappingOverload]

    @overload
    def __radd__(self: 'Arithmetic[int]', other: 'decimal.Decimal') -> 'DecimalArithmeticOperation': ...

    @overload
    def __radd__(self: 'Arithmetic[int]', other: 'float') -> 'DoubleArithmeticOperation': ...

    @overload
    def __radd__(self: 'Arithmetic[float]', other: 'int | float | decimal.Decimal') -> 'DoubleArithmeticOperation': ...

    @overload
    def __radd__(self: 'Arithmetic[decimal.Decimal]', other: 'int | decimal.Decimal') -> 'DecimalArithmeticOperation': ...

    @overload
    def __radd__(self: 'Arithmetic[decimal.Decimal]', other: 'float') -> 'DoubleArithmeticOperation': ...

    @overload
    def __radd__(self, other: 'RelationalOperationElement') -> 'UntypedArithmeticOperation': ...

    def __radd__(self, other):
        return self._arithmetic(ArithmeticOperator.ADD, other, True)

    @overload
    def __sub__(self: 'Arithmetic[int]', other: 'int | Arithmetic[int]') -> 'IntegerArithmeticOperation': ...  # pyright: ignore[reportOverlappingOverload]

    @overload
    def __sub__(self: 'Arithmetic[int]', other: 'decimal.Decimal | Arithmetic[decimal.Decimal]') -> 'DecimalArithmeticOperation': ...

    @overload
    def __sub__(self: 'Arithmetic[int]', other: 'float | Arithmetic[float]') -> 'DoubleArithmeticOperation': ...

    @overload
    def __sub__(self: 'Arithmetic[float]', other: 'int | float | decimal.Decimal | Arithmetic[int] | Arithmetic[float] | Arithmetic[decimal.Decimal]') -> 'DoubleArithmeticOperation': ...

    @overload
    def __sub__(self: 'Arithmetic[decimal.Decimal]', other: 'int | decimal.Decimal | Arithmetic[int] | Arithmetic[decimal.Decimal]') -> 'DecimalArithmeticOperation': ...

    @overload
    def __sub__(self: 'Arithmetic[decimal.Decimal]', other: 'float | Arithmetic[float]') -> 'DoubleArithmeticOperation': ...

    @overload
    def __sub__(self, other: 'RelationalOperationElement') -> 'UntypedArithmeticOperation': ...

    def __sub__(self, other):
        return self._arithmetic(ArithmeticOperator.SUBTRACT, other, False)

    @overload
    def __rsub__(self: 'Arithmetic[int]', other: 'int') -> 'IntegerArithmeticOperation': ...  # pyright: ignore[reportOverlappingOverload]

    @overload
    def __rsub__(self: 'Arithmetic[int]', other: 'decimal.Decimal') -> 'DecimalArithmeticOperation': ...

    @overload
    def __rsub__(self: 'Arithmetic[int]', other: 'float') -> 'DoubleArithmeticOperation': ...

    @overload
    def __rsub__(self: 'Arithmetic[float]', other: 'int | float | decimal.Decimal') -> 'DoubleArithmeticOperation': ...

    @overload
    def __rsub__(self: 'Arithmetic[decimal.Decimal]', other: 'int | decimal.Decimal') -> 'DecimalArithmeticOperation': ...

    @overload
    def __rsub__(self: 'Arithmetic[decimal.Decimal]', other: 'float') -> 'DoubleArithmeticOperation': ...

    @overload
    def __rsub__(self, other: 'RelationalOperationElement') -> 'UntypedArithmeticOperation': ...

    def __rsub__(self, other):
        return self._arithmetic(ArithmeticOperator.SUBTRACT, other, True)

    @overload
    def __mul__(self: 'Arithmetic[int]', other: 'int | Arithmetic[int]') -> 'IntegerArithmeticOperation': ...  # pyright: ignore[reportOverlappingOverload]

    @overload
    def __mul__(self: 'Arithmetic[int]', other: 'decimal.Decimal | Arithmetic[decimal.Decimal]') -> 'DecimalArithmeticOperation': ...

    @overload
    def __mul__(self: 'Arithmetic[int]', other: 'float | Arithmetic[float]') -> 'DoubleArithmeticOperation': ...

    @overload
    def __mul__(self: 'Arithmetic[float]', other: 'int | float | decimal.Decimal | Arithmetic[int] | Arithmetic[float] | Arithmetic[decimal.Decimal]') -> 'DoubleArithmeticOperation': ...

    @overload
    def __mul__(self: 'Arithmetic[decimal.Decimal]', other: 'int | decimal.Decimal | Arithmetic[int] | Arithmetic[decimal.Decimal]') -> 'DecimalArithmeticOperation': ...

    @overload
    def __mul__(self: 'Arithmetic[decimal.Decimal]', other: 'float | Arithmetic[float]') -> 'DoubleArithmeticOperation': ...

    @overload
    def __mul__(self, other: 'RelationalOperationElement') -> 'UntypedArithmeticOperation': ...

    def __mul__(self, other):
        return self._arithmetic(ArithmeticOperator.MULTIPLY, other, False)

    @overload
    def __rmul__(self: 'Arithmetic[int]', other: 'int') -> 'IntegerArithmeticOperation': ...  # pyright: ignore[reportOverlappingOverload]

    @overload
    def __rmul__(self: 'Arithmetic[int]', other: 'decimal.Decimal') -> 'DecimalArithmeticOperation': ...

    @overload
    def __rmul__(self: 'Arithmetic[int]', other: 'float') -> 'DoubleArithmeticOperation': ...

    @overload
    def __rmul__(self: 'Arithmetic[float]', other: 'int | float | decimal.Decimal') -> 'DoubleArithmeticOperation': ...

    @overload
    def __rmul__(self: 'Arithmetic[decimal.Decimal]', other: 'int | decimal.Decimal') -> 'DecimalArithmeticOperation': ...

    @overload
    def __rmul__(self: 'Arithmetic[decimal.Decimal]', other: 'float') -> 'DoubleArithmeticOperation': ...

    @overload
    def __rmul__(self, other: 'RelationalOperationElement') -> 'UntypedArithmeticOperation': ...

    def __rmul__(self, other):
        return self._arithmetic(ArithmeticOperator.MULTIPLY, other, True)

    @overload
    def __truediv__(self: 'Arithmetic[int]', other: 'int | float | decimal.Decimal | Arithmetic[int] | Arithmetic[float] | Arithmetic[decimal.Decimal]') -> 'DoubleArithmeticOperation': ...

    @overload
    def __truediv__(self: 'Arithmetic[float]', other: 'int | float | decimal.Decimal | Arithmetic[int] | Arithmetic[float] | Arithmetic[decimal.Decimal]') -> 'DoubleArithmeticOperation': ...

    @overload
    def __truediv__(self: 'Arithmetic[decimal.Decimal]', other: 'int | float | decimal.Decimal | Arithmetic[int] | Arithmetic[float] | Arithmetic[decimal.Decimal]') -> 'DoubleArithmeticOperation': ...

    @overload
    def __truediv__(self, other: 'RelationalOperationElement') -> 'UntypedArithmeticOperation': ...

    def __truediv__(self, other):
        return self._arithmetic(ArithmeticOperator.DIVIDE, other, False)

    @overload
    def __rtruediv__(self: 'Arithmetic[int]', other: 'int | float | decimal.Decimal') -> 'DoubleArithmeticOperation': ...

    @overload
    def __rtruediv__(self: 'Arithmetic[float]', other: 'int | float | decimal.Decimal') -> 'DoubleArithmeticOperation': ...

    @overload
    def __rtruediv__(self: 'Arithmetic[decimal.Decimal]', other: 'int | float | decimal.Decimal') -> 'DoubleArithmeticOperation': ...

    @overload
    def __rtruediv__(self, other: 'RelationalOperationElement') -> 'UntypedArithmeticOperation': ...

    def __rtruediv__(self, other):
        return self._arithmetic(ArithmeticOperator.DIVIDE, other, True)


class Operation(RelationalOperationElement, Aliasable):
    def __init__(self):
        super().__init__()

    def and_(self, other) -> 'LogicalOperation':
        return LogicalOperation(self, LogicalOperator.AND, other)

    def or_(self, other) -> 'LogicalOperation':
        return LogicalOperation(self, LogicalOperator.OR, other)

class NoOperation(Operation):
    def __init__(self):
        super().__init__()

class StringPredicates:
    """Pattern tests on an expression that holds text, as on a column: ``f.name().upper().starts_with("AC")``,
    ``f.name().join(",").contains("John")``."""

    def _like(self, pattern: str) -> 'ComparisonOperation':
        return ComparisonOperation(self._arithmetic_operand(), ComparisonOperator.LIKE,  # type: ignore[attr-defined]
                                   StringConstantOperation(pattern))

    def matches(self, pattern: str) -> 'ComparisonOperation':
        """The text has a match for the regular expression anywhere in it, as Python's ``re.search``:
        ``f.comment().matches("Customer.*Complaints")``."""
        return ComparisonOperation(self._arithmetic_operand(), ComparisonOperator.MATCHES,  # type: ignore[attr-defined]
                                   StringConstantOperation(pattern))

    def contains(self, value: str) -> 'ComparisonOperation':
        return self._like(f"%{escape_like(value)}%")

    def starts_with(self, prefix: str) -> 'ComparisonOperation':
        return self._like(f"{escape_like(prefix)}%")

    def ends_with(self, suffix: str) -> 'ComparisonOperation':
        return self._like(f"%{escape_like(suffix)}")


class ConstantOperation(RelationalOperationElement, Aliasable, Sortable):
    """A fixed value. A number has aggregates of its own kind (``constant(1).sum()`` is an integer); a text, date or
    boolean has untyped ones. A constant has the functions a column of its type has: ``constant("Hello").length()``,
    ``constant(16).sqrt()``. A function of a constant is applied to the constant and named for the function and the value."""

    def __init__(self):
        super().__init__()

    def _function_input(self) -> 'ConstantOperation':
        return self

    def _function_label(self) -> str:
        return str(getattr(self, "value"))

class IntegerConstantOperation(ConstantOperation, NumericFunctions['IntegerFunctionOperation'], Arithmetic[int],
                               Orderable[int], Aggregates['IntegerAggregateOperation']):
    _number_kind = int
    value:int

    def __init__(self, value:int):
        super().__init__()
        self.value = value

class FloatConstantOperation(ConstantOperation, NumericFunctions['DoubleFunctionOperation'], Arithmetic[float],
                             Orderable[float], Aggregates['DoubleAggregateOperation']):
    _number_kind = float
    value:float

    def __init__(self, value:float):
        super().__init__()
        self.value = value

class StringConstantOperation(ConstantOperation, StringFunctions, StringPredicates, Equatable[str],
                              Aggregates['UntypedAggregateOperation']):
    """A text constant, which has the string functions and pattern tests of a text column, and joins with ``+``."""
    value:str

    def __init__(self, value:str):
        super().__init__()
        self.value = value

    def __add__(self, other):
        return concat(self, other)

    def __radd__(self, other):
        return concat(other, self)

class DateConstantOperation(ConstantOperation, DateFunctions, Orderable[datetime.date],
                            Aggregates['UntypedAggregateOperation']):
    value:datetime.date

    def __init__(self, value:datetime.date):
        super().__init__()
        self.value = value


class DateTimeConstantOperation(ConstantOperation, DateTimeFunctions, Orderable[datetime.datetime | datetime.date],
                                Aggregates['UntypedAggregateOperation']):
    value:datetime.datetime

    def __init__(self, value:datetime.datetime):
        super().__init__()
        self.value = value

class DecimalConstantOperation(ConstantOperation, NumericFunctions['DecimalFunctionOperation'],
                               Arithmetic[decimal.Decimal], Orderable[decimal.Decimal],
                               Aggregates['DecimalAggregateOperation']):
    _number_kind = decimal.Decimal
    value: decimal.Decimal

    def __init__(self, value: decimal.Decimal):
        super().__init__()
        self.value = value

class BooleanConstantOperation(ConstantOperation, Equatable[bool], Aggregates['UntypedAggregateOperation']):
    value:bool

    def __init__(self, value:bool):
        super().__init__()
        self.value = value


class UnaryOperation(Operation):
    element: RelationalOperationElement

    def __init__(self, element: RelationalOperationElement):
        super().__init__()
        self.element = element


class BinaryOperation(Operation):
    left: RelationalOperationElement
    right: RelationalOperationElement

    def __init__(self, left: RelationalOperationElement, right: RelationalOperationElement):
        super().__init__()
        self.left = left
        self.right = right


class IsNullOperation(UnaryOperation, BooleanAggregates):
    pass


class IsNotNullOperation(UnaryOperation, BooleanAggregates):
    pass


class NotOperation(UnaryOperation, BooleanAggregates):
    """The condition does not hold: ``NOT (element)``. Negating it again gives the condition."""

    def __invert__(self):  # type: ignore[override]
        return self.element


class InOperation(UnaryOperation, BooleanAggregates):
    """The element is one of ``values`` (``element IN (...)``)."""
    values: list[ConstantOperation]

    def __init__(self, element: RelationalOperationElement, values: list[ConstantOperation]):
        super().__init__(element)
        self.values = values


class NotInOperation(UnaryOperation, BooleanAggregates):
    """The element is none of ``values`` (``element NOT IN (...)``)."""
    values: list[ConstantOperation]

    def __init__(self, element: RelationalOperationElement, values: list[ConstantOperation]):
        super().__init__(element)
        self.values = values


class BooleanOperation:
    def __init__(self):
        pass


class ComparisonOperator(Enum):
    EQUAL = 1
    NOT_EQUAL = 2
    LESS_THAN = 3
    GREATER_THAN = 4
    LESS_THAN_OR_EQUAL_TO = 5
    GREATER_THAN_OR_EQUAL_TO = 6
    LIKE = 7
    NOT_LIKE = 8
    MATCHES = 9


# The character that makes the next one literal in a LIKE pattern built from a value (contains, starts_with, ...).
LIKE_ESCAPE = '!'


def escape_like(value: str) -> str:
    """``value`` as part of a LIKE pattern that matches it literally: %, _ and the escape character itself would
    otherwise be wildcards."""
    return value.replace(LIKE_ESCAPE, LIKE_ESCAPE * 2).replace('%', LIKE_ESCAPE + '%').replace('_', LIKE_ESCAPE + '_')


class ComparisonOperation(BinaryOperation, BooleanOperation, BooleanAggregates):
    operator: ComparisonOperator

    def __init__(self, left: RelationalOperationElement, op: ComparisonOperator, right: RelationalOperationElement):
        super().__init__(left, right)
        self.operator = op


class LogicalOperator(Enum):
    AND = 1
    OR = 2


class LogicalOperation(BinaryOperation, BooleanOperation, BooleanAggregates):
    operator: LogicalOperator

    def __init__(self, left: RelationalOperationElement, op: LogicalOperator, right: RelationalOperationElement):
        super().__init__(left, right)
        self.operator = op


def _value_operand(value) -> 'ConstantOperation':
    """A Python value as the constant it compares with."""
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
    raise TypeError(f"A value to compare with is a bool, number, string, date or datetime, not {type(value).__name__}")


class ArithmeticOperation(BinaryOperation, Sortable):
    """``left operator right`` on numbers, e.g. ``price * quantity``. What it can be compared with and have done to it
    depends on the kind of number it gives, so that is a subclass: Integer, Double or Decimal ArithmeticOperation, or
    UntypedArithmeticOperation when the kind of an operand is not known."""
    operator: ArithmeticOperator

    def __init__(self, left: RelationalOperationElement, operator: ArithmeticOperator,
                 right: RelationalOperationElement, display_name: str | None = None):
        super().__init__(left, right)
        self.operator = operator
        self.display_name = display_name


class UntypedArithmeticOperation(ArithmeticOperation, Arithmetic[Any], Comparable, Aggregates['UntypedAggregateOperation']):
    """Arithmetic with an operand of unknown kind, so it takes any comparison."""


class IntegerArithmeticOperation(ArithmeticOperation, Arithmetic[int], Orderable[int], Aggregates['IntegerAggregateOperation']):
    """``+ - *`` of integers: an integer. Its sum, minimum and maximum are integers."""
    _number_kind = int


class DoubleArithmeticOperation(ArithmeticOperation, Arithmetic[float], Orderable[float], Aggregates['DoubleAggregateOperation']):
    """Arithmetic with a double, or a division: a double."""
    _number_kind = float


class DecimalArithmeticOperation(ArithmeticOperation, Arithmetic[decimal.Decimal], Orderable[decimal.Decimal],
                                 Aggregates['DecimalAggregateOperation']):
    """``+ - *`` of decimals, or of a decimal and an integer: a decimal."""
    _number_kind = decimal.Decimal


_ARITHMETIC_RESULTS: 'dict[type | None, type[ArithmeticOperation]]' = {
    None: UntypedArithmeticOperation, int: IntegerArithmeticOperation, float: DoubleArithmeticOperation,
    decimal.Decimal: DecimalArithmeticOperation,
}


class ConcatOperation(Operation, Equatable[str], Sortable, StringPredicates):
    """Text joined end to end: ``first + " " + last``. NULL in any part makes the whole NULL, as with SQL's ``||``.
    It is text, so it is compared for equality with text and has no order, as a string column has none."""
    parts: list[RelationalOperationElement]

    def __init__(self, parts: list[RelationalOperationElement], display_name: str | None = None):
        super().__init__()
        self.parts = parts
        self.display_name = display_name

    def __add__(self, other) -> 'ConcatOperation':
        return concat(self, other)

    def __radd__(self, other) -> 'ConcatOperation':
        return concat(other, self)


def concat(left, right) -> ConcatOperation:
    """``left + right`` for text. A string is a constant; a concatenation on either side is flattened into one."""
    def parts_of(value) -> list[RelationalOperationElement]:
        if isinstance(value, ConcatOperation):
            return value.parts
        if isinstance(value, str):
            return [StringConstantOperation(value)]
        if isinstance(value, RelationalOperationElement):
            return [value._arithmetic_operand()]
        raise TypeError(f"Text is joined with text, not {type(value).__name__}")
    return ConcatOperation([*parts_of(left), *parts_of(right)])



class CaseOperation(Operation, Aggregates['UntypedAggregateOperation']):
    """``CASE WHEN condition THEN then ELSE default END``; without a ``default`` the value is NULL."""

    def __init__(self, condition: RelationalOperationElement, then: RelationalOperationElement,
                 default: RelationalOperationElement | None = None, display_name: str | None = None):
        super().__init__()
        self.condition = condition
        self.then = then
        self.default = default
        self.display_name = display_name


class AggregateOperator(Enum):
    COUNT = 1
    SUM = 2
    MIN = 3
    MAX = 4
    AVERAGE = 5
    STRING_AGG = 6
    COUNT_DISTINCT = 7
    STDDEV_SAMP = 8
    STDDEV_POP = 9
    VAR_SAMP = 10
    VAR_POP = 11
    BOOL_AND = 12
    BOOL_OR = 13


class WindowSpecification:
    def __init__(self, partition_by=None, order_by=None):
        self.partition_by = partition_by or []
        self.order_by = order_by or []


class AggregateOperation(UnaryOperation, Sortable):
    """An aggregate of an expression. What it can then be compared with or have done to it depends on the type it gives,
    so that is a subclass: Integer, Double, Decimal, String, Boolean, Date or DateTime AggregateOperation, or
    UntypedAggregateOperation when the type of what it aggregates is not known."""
    operator: AggregateOperator

    def __init__(self, element: RelationalOperationElement, operator: AggregateOperator, display_name: str | None = None,
                 window=None, separator: str | None = None, order_by: list | None = None):
        super().__init__(element)
        self.operator = operator
        self.display_name = display_name
        self.window = window
        # for STRING_AGG: what goes between the values, and the order they are joined in
        self.separator = separator
        self.order_by = order_by or []

    def over(self, partition_by=None, order_by=None):
        return type(self)(self.element, self.operator, self.display_name,
                          WindowSpecification(partition_by, order_by), self.separator, self.order_by)


class UntypedAggregateOperation(AggregateOperation, Arithmetic[Any], Comparable, StringPredicates):
    """An aggregate of an expression whose type is not known here, so it takes any comparison."""


class IntegerAggregateOperation(AggregateOperation, Arithmetic[int], Orderable[int]):
    """An aggregate that gives a whole number: a count, or the sum, minimum or maximum of an integer."""
    _number_kind = int


class DoubleAggregateOperation(AggregateOperation, Arithmetic[float], Orderable[float]):
    """An aggregate that gives a floating-point number: an average, standard deviation or variance, or the sum, minimum
    or maximum of a double."""
    _number_kind = float


class DecimalAggregateOperation(AggregateOperation, Arithmetic[decimal.Decimal], Orderable[decimal.Decimal]):
    """The sum, minimum or maximum of a decimal."""
    _number_kind = decimal.Decimal


class StringAggregateOperation(AggregateOperation, Equatable[str], StringPredicates):
    """Text: the values of a group joined, or the minimum or maximum of text."""


class BooleanAggregateOperation(AggregateOperation, Equatable[bool]):
    """Whether every row of a group, or any row, holds."""


class DateAggregateOperation(AggregateOperation, Orderable[datetime.date]):
    """The earliest or latest date of a group."""


class DateTimeAggregateOperation(AggregateOperation, Orderable[datetime.datetime | datetime.date]):
    """The earliest or latest datetime of a group."""


class ScalarFunction(Enum):
    ABS = 1
    CEILING = 2
    FLOOR = 3
    MOD = 4
    POWER = 5
    SQRT = 6
    ROUND = 7
    UPPER = 8
    LOWER = 9
    TRIM = 10
    LTRIM = 11
    RTRIM = 12
    LENGTH = 13
    REVERSE = 14
    LEFT = 15
    RIGHT = 16
    REPEAT = 17
    REPLACE = 18
    SUBSTRING = 19
    COALESCE = 20
    SIN = 21
    COS = 22
    TAN = 23
    ASIN = 24
    ACOS = 25
    ATAN = 26
    ATAN2 = 27
    EXP = 28
    LOG = 29


class WindowFunction(Enum):
    ROW_NUMBER = 1
    RANK = 2
    DENSE_RANK = 3
    NTILE = 4
    LAG = 5
    LEAD = 6
    FIRST_VALUE = 7
    LAST_VALUE = 8
    CUME_DIST = 9
    PERCENT_RANK = 10


class WindowFunctionOperation(Operation, Sortable):
    element: RelationalOperationElement | None

    def __init__(self, element: RelationalOperationElement | None, function: WindowFunction,
                 display_name: str | None = None, second_arg: int | float | None = None, extra_args: list | None = None, window=None):
        super().__init__()
        self.element = element
        self.function = function
        self.display_name = display_name
        self.second_arg = second_arg
        self.extra_args = extra_args or []
        self.window = window

    def over(self, partition_by=None, order_by=None):
        return WindowFunctionOperation(self.element, self.function, self.display_name,
                                       second_arg=self.second_arg, extra_args=list(self.extra_args),
                                       window=WindowSpecification(partition_by, order_by))


class ScalarFunctionOperation(UnaryOperation, Sortable):
    """A scalar function applied to an expression. What it can then be compared with, or have done to it, depends on
    the type it gives, so that is a subclass below: StringFunctionOperation,
    IntegerFunctionOperation, DoubleFunctionOperation, DecimalFunctionOperation. One whose type is not known
    (``COALESCE`` of a date) is an UntypedFunctionOperation."""

    def __init__(self, element: RelationalOperationElement, function: ScalarFunction,
                 display_name: str | None = None, second_arg: int | float | None = None, extra_args: list | None = None,
                 cast: str | None = None):
        super().__init__(element)
        self.function = function
        self.display_name = display_name
        self.second_arg = second_arg
        self.extra_args = extra_args or []
        # a SQL type to cast the result to, for a function SQL gives a floating-point number that is a whole number here
        self.cast = cast


class DatePart(Enum):
    YEAR = 'YEAR'
    MONTH = 'MONTH'
    DAY = 'DAY'
    HOUR = 'HOUR'
    MINUTE = 'MINUTE'
    SECOND = 'SECOND'
    QUARTER = 'QUARTER'
    WEEK = 'WEEK'
    DOW = 'DOW'


class DateExtractOperation(UnaryOperation, Sortable, Orderable[int]):
    """A part of a date or time (year, month, hour, ...), as an integer."""

    def __init__(self, element: RelationalOperationElement, part: DatePart, display_name: str | None = None):
        super().__init__(element)
        self.part = part
        self.display_name = display_name


class DateTruncOperation(UnaryOperation, Sortable, Orderable[datetime.date]):
    """The first day of the week, month, quarter or year that the date falls in, as a date."""

    def __init__(self, element: RelationalOperationElement, part: DatePart, display_name: str | None = None):
        super().__init__(element)
        self.part = part
        self.display_name = display_name


class DateArithmeticOperation(UnaryOperation, Sortable, Orderable[T]):
    """A date or datetime moved by an amount of time. It is a ``T``, the type it is moved from: a date stays a date, a
    datetime a datetime."""

    def __init__(self, element: RelationalOperationElement, n: int, unit: DatePart,
                 is_add: bool = True, display_name: str | None = None):
        super().__init__(element)
        self.n = n
        self.unit = unit
        self.is_add = is_add
        self.display_name = display_name


class DateDiffOperation(UnaryOperation, Sortable, Orderable[int]):
    """The whole number of days, months, ... between a date or time and another."""

    def __init__(self, element: RelationalOperationElement, other, unit: DatePart, display_name: str | None = None):
        super().__init__(element)
        self.other = other
        self.unit = unit
        self.display_name = display_name


def string_function(element, function: ScalarFunction, label: str | None = None, second_arg: int | float | None = None,
                    extra_args: list | None = None) -> 'StringFunctionOperation':
    """A function that gives text, applied to ``element``."""
    return StringFunctionOperation(element, function, label, second_arg=second_arg, extra_args=extra_args)


def integer_function(element, function: ScalarFunction, label: str | None = None, second_arg: int | float | None = None,
                     extra_args: list | None = None) -> 'IntegerFunctionOperation':
    """A function that gives a whole number, applied to ``element``."""
    return IntegerFunctionOperation(element, function, label, second_arg=second_arg, extra_args=extra_args)


def double_function(element, function: ScalarFunction, label: str | None = None, second_arg: int | float | None = None,
                    extra_args: list | None = None, cast: str | None = None) -> 'DoubleFunctionOperation':
    """A function that gives a floating-point number, applied to ``element``."""
    return DoubleFunctionOperation(element, function, label, second_arg=second_arg, extra_args=extra_args, cast=cast)


def _label_of(operation: ScalarFunctionOperation) -> str:
    return operation.display_name or operation.function.name.title()


class _Result:
    """What a function result gives its own functions: it is their input, and their label is its own name."""

    def _function_input(self):
        return self

    def _function_label(self) -> str:
        return _label_of(self)  # type: ignore[arg-type]


class StringFunctionOperation(_Result, StringFunctions, StringPredicates, Equatable[str], Aggregates[UntypedAggregateOperation],
                              ScalarFunctionOperation):
    """A function that gives text, which has the string functions too: ``f.name().upper().length()``. It is compared
    for equality with text, as a string column is, and has no order."""

    def __add__(self, other):
        return concat(self, other)

    def __radd__(self, other):
        return concat(other, self)


class IntegerFunctionOperation(_Result, NumericFunctions['IntegerFunctionOperation'], Arithmetic[int], Orderable[int],
                               Aggregates[IntegerAggregateOperation], ScalarFunctionOperation):
    """A function that gives a whole number (``length``, or ``abs`` of an integer): compared with integers."""


class DoubleFunctionOperation(_Result, NumericFunctions['DoubleFunctionOperation'], Arithmetic[float], Orderable[float],
                              Aggregates[DoubleAggregateOperation], ScalarFunctionOperation):
    """A function that gives a floating-point number (``sqrt``, ``power``, or ``abs`` of a double)."""


class DecimalFunctionOperation(_Result, NumericFunctions['DecimalFunctionOperation'], Arithmetic[decimal.Decimal],
                               Orderable[decimal.Decimal], Aggregates[DecimalAggregateOperation], ScalarFunctionOperation):
    """A function that gives a decimal (``abs`` of a decimal column)."""


IntegerConstantOperation._number_type = IntegerFunctionOperation  # type: ignore[attr-defined]
FloatConstantOperation._number_type = DoubleFunctionOperation  # type: ignore[attr-defined]
DecimalConstantOperation._number_type = DecimalFunctionOperation  # type: ignore[attr-defined]
IntegerFunctionOperation._number_type = IntegerFunctionOperation  # type: ignore[attr-defined]
IntegerFunctionOperation._number_kind = int  # type: ignore[attr-defined]
IntegerFunctionOperation._value_aggregate = IntegerAggregateOperation  # type: ignore[attr-defined]
DoubleFunctionOperation._number_type = DoubleFunctionOperation  # type: ignore[attr-defined]
DoubleFunctionOperation._number_kind = float  # type: ignore[attr-defined]
DoubleFunctionOperation._value_aggregate = DoubleAggregateOperation  # type: ignore[attr-defined]
DecimalFunctionOperation._number_type = DecimalFunctionOperation  # type: ignore[attr-defined]
DecimalFunctionOperation._number_kind = decimal.Decimal  # type: ignore[attr-defined]
DecimalFunctionOperation._value_aggregate = DecimalAggregateOperation  # type: ignore[attr-defined]


class Relation:
    """Something rows are read from, as in Legend: it has columns. A table, a view and a query are relations; a table
    and a view also have a name (a NamedRelation). As in Legend a column is a RelationalOperationElement: a table's are
    its Columns, a query's are the expressions it shows."""
    columns: Sequence[RelationalOperationElement]

    def __init__(self, columns: Sequence[RelationalOperationElement]):
        self.columns = columns


class NamedRelation(Relation):
    """A relation that has a name: a table or a view."""

    def __init__(self, name: str, columns: Sequence[RelationalOperationElement]):
        Relation.__init__(self, columns)
        self.name = name

    @property
    def qualified_name(self) -> str:
        """What the relation is called where it is read: a table's name with its schema's."""
        return self.name


class MilestoningScheme:
    def __init__(self, name: str, processing_start: str | None = None, processing_end: str | None = None,
                 business_date: str | None = None, business_date_from: str | None = None,
                 business_date_to: str | None = None, infinite_datetime: str | None = None,
                 processing_datetime: str | None = None):
        self.name = name
        self.processing_start = processing_start
        self.processing_end = processing_end
        self.business_date = business_date
        self.business_date_from = business_date_from
        self.business_date_to = business_date_to
        self.infinite_datetime = infinite_datetime
        # a single datetime column that holds the processing time a row was taken at (a processing snapshot)
        self.processing_datetime = processing_datetime


from abc import ABC, abstractmethod


class DataStore(ABC):
    def __init__(self, name: str):
        self.name = name
        self.schemas: list = []
        self.milestoning_schemes: list = []

    @abstractmethod
    def namespace(self, schema_name: str) -> str:
        pass


class Database(DataStore):
    def __init__(self, name: str, location: str | None = None):
        super().__init__(name)
        self.location = location

    def namespace(self, schema_name: str) -> str:
        return schema_name


class DataCatalog(DataStore):
    def __init__(self, name: str):
        super().__init__(name)

    def namespace(self, schema_name: str) -> str:
        return f"{self.name}.{schema_name}"


class Schema:
    def __init__(self, name: str, datastore: DataStore | None = None):
        self.name = name
        self.datastore = datastore
        self.tables: list = []
        if datastore is not None:
            datastore.schemas.append(self)


class Column(RelationalOperationElement):
    """A column of a relation. As in Legend its ``owner`` is the Relation it belongs to. A Table gives the columns it is
    made of their owner, so a column that is only in a table's list needs none."""
    table: 'Table | None'
    owner: 'Relation | None'

    def __init__(self, name: str, _type: str, owner: 'Relation | None' = None, primary_key: bool = False,
                 nullable: bool = True):
        self.table = None
        super().__init__()
        self.name = name
        self.type = _type
        if isinstance(owner, str):
            raise TypeError(f"A column is owned by a Relation, not by the name {owner!r}: give the table")
        self.owner = owner
        self.primary_key = primary_key
        self.nullable = nullable and not primary_key  # as in Legend, a primary key is never null

    @property
    def owner_relation(self) -> 'Relation':
        """The relation the column belongs to. A column has none until the table it is listed in claims it, and one that
        never is cannot be read from, so asking for it then is an error."""
        if self.owner is None:
            raise ValueError(f"Column '{self.name}' has no owner: it is not a column of any relation, so there is "
                             "nothing to read it from")
        return self.owner

    @property
    def owner_name(self) -> str:
        """The name of the relation the column belongs to: how a query that reads it says which relation that is."""
        owner = self.owner_relation
        if not isinstance(owner, NamedRelation):
            raise TypeError(f"Column '{self.name}' belongs to {type(owner).__name__}, which has no name to read it by")
        return owner.qualified_name

    def qualified_name(self) -> str:
        assert self.table is not None, f"Column {self.name} has no table"
        return self.table.qualified_name


class ForeignKey:
    def __init__(self, column: Column, references: Column):
        self.column = column
        self.references = references


class TableAlias(NamedRelation):
    """A relation under a name where it is used, as in Legend and as ANSI SQL's ``<table reference>`` with its
    ``<correlation name>``: ``(query) AS relation1``. It has the columns of the relation it names, and what reads those
    columns says which alias they belong to by its name. A table is already named, so this is for a relation that is not:
    a query."""
    relation: Relation

    def __init__(self, relation: Relation, name: str):
        super().__init__(name, relation.columns)
        self.relation = relation


class Table(NamedRelation):
    columns: Sequence[Column]  # type: ignore[assignment]  # pyright: ignore[reportIncompatibleVariableOverride]
    # (a table's columns are Columns, as Legend's are: narrower than a relation's elements, never assigned from outside)
    schema: 'Schema | None'
    foreign_keys: 'list[ForeignKey]'

    def __init__(self, name: str, columns: 'Sequence[Column | tuple[str, str]]' = (), schema: 'Schema | None' = None,
                 primary_key: 'Sequence[str | Column]' = ()):
        """A table of its columns, each a Column or a ``(name, type)``, and the columns that are its primary key, by name
        or as the Column: ``Table("accounts", [("ID", "INT"), ("NAME", "VARCHAR")], primary_key=["ID"])``. A column that
        is not nullable is a Column, or is added with ``add_column``."""
        self._columns_by_name: dict[str, Column] = {}
        self._own_columns: list[Column] = []
        super().__init__(name, self._own_columns)  # the list is the table's own: add_column adds to it
        self.schema = schema
        self.foreign_keys: list[ForeignKey] = []
        # for a table no class is mapped to but a join passes through: how its rows are milestoned
        self.milestoning_scheme: 'MilestoningScheme | None' = None
        for column in columns:
            self._add(column if isinstance(column, Column) else Column(column[0], column[1]))
        for key in primary_key:
            name_of_key = key if isinstance(key, str) else key.name
            column = self._columns_by_name.get(name_of_key)
            if column is None or (isinstance(key, Column) and column is not key):
                raise ValueError(f"Primary key column '{name_of_key}' is not a column of table '{name}'")
            column.primary_key = True
            column.nullable = False  # as in Legend, a primary key is never null
        if schema is not None:
            schema.tables.append(self)

    @property
    def primary_key(self) -> list[Column]:
        """The columns that are the table's primary key, as Legend's ``primaryKey``."""
        return [column for column in self._own_columns if column.primary_key]

    def _add(self, column: Column) -> Column:
        if column.name in self._columns_by_name:
            raise ValueError(f"Duplicate column name '{column.name}' in table '{self.name}'")
        self._columns_by_name[column.name] = column
        column.table = self
        column.owner = self  # a table's own columns belong to it
        self._own_columns.append(column)
        return column

    def add_column(self, name: str, type: str, primary_key: bool = False, nullable: bool = True) -> Column:
        """Add a column of this table, which owns it: ``table.add_column("ID", "INT", primary_key=True)``. It is returned."""
        return self._add(Column(name, type, primary_key=primary_key, nullable=nullable))

    @property
    def qualified_name(self) -> str:
        if self.schema is None:
            return self.name
        if self.schema.datastore is None:
            return f"{self.schema.name}.{self.name}"
        return f"{self.schema.datastore.namespace(self.schema.name)}.{self.name}"


class JoinOperation:
    def __init__(self, name: str, target:Table, lhs:Column, rhs:Column, _filter:RelationalOperationElement | None = None):
        self.name = name
        self.target = target
        self.left = lhs
        self.right = rhs
        self.filter = _filter
        self.filter_kwargs: dict = {}
        self.filter_column_map: dict = {}


class JoinTreeNodeOperation:
    """Wraps a JoinOperation and links it to its parent node in the join tree.

    Enables multi-hop traversal (e.g. Employee.manager.reports) by ensuring
    intermediate joins are emitted before the joins that depend on them.
    """
    def __init__(self, join: JoinOperation, parent: 'JoinTreeNodeOperation | None' = None, many: bool = False,
                 optional: bool = False, business_date: 'datetime.date | None' = None,
                 processing_datetime: 'datetime.datetime | datetime.date | None' = None):
        self.join = join
        self.parent = parent
        # The dates this step was given for the milestoned table it reaches, if any. They are the ones it uses, and the
        # ones every step below it uses unless it is given its own; a step with none uses those of the step above it,
        # and at the top the query's.
        self.business_date = business_date
        self.processing_datetime = processing_datetime
        # True when the property this step follows is to-many, so a value read through it repeats the row. As in
        # Pure, that is the multiplicity of the property, not something the join itself says.
        self.many = many
        # True when the property this step follows can be missing (its lower bound is 0)
        self.optional = optional


class ColumnWithJoin(RelationalOperationElement):
    def __init__(self, column: Column, join: 'JoinTreeNodeOperation | None', embedded: 'ColumnWithJoin | None' = None,
                 optional: bool = False):
        super().__init__()
        self.column = column
        self.parent = join
        self.embedded = embedded
        self._optional = optional

    @property
    def optional(self) -> bool:
        """Whether the value can be missing: its property is optional, or a navigation on the way to it is."""
        node = self.parent
        while node is not None:
            if node.optional:
                return True
            node = node.parent
        return self._optional


class SortDirection(Enum):
    ASC = 1
    DESC = 2


class SortOperation(RelationalOperationElement):
    def __init__(self, column: RelationalOperationElement, direction: SortDirection):
        super().__init__()
        self.column = column
        self.direction = direction


class CountAllOperation(RelationalOperationElement, Aliasable, Sortable, Orderable[int]):
    """Represents COUNT(*) — counts all rows without reference to a specific column."""
    def __init__(self, table: str):
        super().__init__()
        self.table = table


class Alias(RelationalOperationElement, Sortable):
    def __init__(self, element: RelationalOperationElement, name: str):
        super().__init__()
        self.element = element
        self.name = name

    def alias(self, name: str) -> 'Alias':
        return Alias(self.element, name)


class ExistsOperation(RelationalOperationElement, Aliasable):
    """Returned by RelatedFinder.exists(). Backend translates this to a join-presence check."""

    def __init__(self, node):
        super().__init__()
        self.node = node

    def and_(self, other) -> 'LogicalOperation':
        return LogicalOperation(self, LogicalOperator.AND, other)

    def or_(self, other) -> 'LogicalOperation':
        return LogicalOperation(self, LogicalOperator.OR, other)


class NotExistsOperation(RelationalOperationElement, Aliasable):
    """Returned by RelatedFinder.not_exists(). Backend translates this to a join-absence check."""

    def __init__(self, node):
        super().__init__()
        self.node = node

    def and_(self, other) -> 'LogicalOperation':
        return LogicalOperation(self, LogicalOperator.AND, other)

    def or_(self, other) -> 'LogicalOperation':
        return LogicalOperation(self, LogicalOperator.OR, other)


class QueryExpression(Relation):
    """What ANSI SQL calls a ``<query expression>``: a query that returns rows and can be used where a table is, as a
    view's definition or a derived table in a FROM clause. It is a QuerySpecification, a single SELECT.

    It is a relation, as Legend's select is: its columns are what it shows. It has no name of its own: a TableAlias
    gives it one where it is used.

    It has no ORDER BY or row limit: a table is a set of rows with no order. An OrderedQuerySpecification adds those."""


class QuerySpecification(QueryExpression):
    """A single SELECT over a root table, as ANSI SQL's ``<query specification>`` with its ``<table expression>``: what to
    show, which rows qualify, how they are grouped, which groups qualify (HAVING) and whether duplicates are removed
    (DISTINCT). It is the part of a SELECT that ANSI SQL also allows in a view definition, so it has no ORDER BY or
    row limit; an OrderedQuerySpecification, which is run rather than stored, sets them.
    """

    def __init__(self, columns: Sequence[RelationalOperationElement], filter: Operation,
                 group_by: list | None = None, table: 'Table | None' = None, distinct: bool = False,
                 having: Operation | None = None):
        Relation.__init__(self, columns)  # not super(): a View is also a NamedRelation
        self.filter = filter
        self.group_by = group_by or []
        self.table = table
        self.distinct = distinct
        self.having = having
        # ON-clause conditions this query adds to its joins (milestoning, kwargs filters), keyed by id(node).
        # Join objects are shared between queries, so per-query conditions live here and not on the join.
        self.join_filters: dict[int, Operation] = {}
        # Steps the query only asks the existence of, each as the query that stands for its table, keyed by id(node):
        # the query joins to that instead of to the table, so a parent is not repeated for each row that matches.
        self.sub_selects: dict[int, 'OrderedQuerySpecification'] = {}

    def expressions(self) -> list[RelationalOperationElement]:
        """Every expression of the query, in the order they are rendered."""
        return [*self.columns, *self.group_by, self.filter]


class OrderedQuerySpecification(QuerySpecification):
    """A query specification that is run: it adds the ordering, row limit and offset applied to its result.

    ANSI SQL calls this a ``<cursor specification>``: a ``<query expression>`` with an ``<order by clause>``, which is not
    part of a ``<query expression>`` and so cannot be in a view definition. (SQL:2008 added the offset and fetch
    first clauses to the same place.)"""

    def __init__(self, columns: Sequence[RelationalOperationElement], filter: Operation,
                 order_by: list[SortOperation] | None = None, group_by: list | None = None,
                 limit: int | None = None, table: 'Table | None' = None, distinct: bool = False,
                 offset: int | None = None, having: Operation | None = None):
        super().__init__(columns, filter, group_by, table, distinct, having)
        self.order_by = order_by or []
        self.limit = limit
        self.offset = offset

    def expressions(self) -> list[RelationalOperationElement]:
        return [*self.columns, *self.group_by, *self.order_by, self.filter]


class UntypedFunctionOperation(ScalarFunctionOperation, Comparable, Aggregates['UntypedAggregateOperation']):
    """A scalar function whose result type is not known here, so it is compared with any value."""


# the aggregate that gives an expression's own type: the type of its sum, minimum and maximum
IntegerArithmeticOperation._value_aggregate = IntegerAggregateOperation  # type: ignore[attr-defined]
DoubleArithmeticOperation._value_aggregate = DoubleAggregateOperation  # type: ignore[attr-defined]
DecimalArithmeticOperation._value_aggregate = DecimalAggregateOperation  # type: ignore[attr-defined]
IntegerConstantOperation._value_aggregate = IntegerAggregateOperation  # type: ignore[attr-defined]
FloatConstantOperation._value_aggregate = DoubleAggregateOperation  # type: ignore[attr-defined]
DecimalConstantOperation._value_aggregate = DecimalAggregateOperation  # type: ignore[attr-defined]
