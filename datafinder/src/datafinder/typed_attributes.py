import datetime
from typing import TYPE_CHECKING, Callable, Generic, TypeVar

from datafinder import Attribute
import decimal

from datafinder.scalar_functions import (
    DecimalFunctionOperation, DoubleFunctionOperation, IntegerFunctionOperation, N, NumericFunctions, StringFunctionOperation,
    StringFunctions,
)

from model.relational import Relation, Equatable, Orderable, RelationalOperationElement, _sample, escape_like, Arithmetic, ConcatOperation, ComparisonOperation, concat, StringConstantOperation, Operation, ComparisonOperator, \
    FloatConstantOperation, IntegerConstantOperation, DateConstantOperation, DateTimeConstantOperation, \
    BooleanConstantOperation, DecimalConstantOperation, AggregateOperator, DateAggregateOperation, DateTimeAggregateOperation, \
    AggregateOperation, DecimalAggregateOperation, DoubleAggregateOperation, IntegerAggregateOperation, StringAggregateOperation, ColumnWithJoin, \
    ScalarFunction, ScalarFunctionOperation, DatePart, DateExtractOperation, DateTruncOperation, DateArithmeticOperation, DateDiffOperation


A = TypeVar("A", bound=AggregateOperation)


class _MinMax(Generic[A]):
    """The smallest and largest value of a group, for a column that can be ordered but not summed. They are of the
    column's type: the minimum of a date is a date."""

    if TYPE_CHECKING:  # provided by the class this is mixed into
        _aggregate_type: Callable[..., A]

    def min(self) -> A:
        """Return the smallest value in the group."""
        return self._aggregate_type(self._column_with_join(), AggregateOperator.MIN, 'Min ' + self.display_name())  # type: ignore[attr-defined]

    def max(self) -> A:
        """Return the largest value in the group."""
        return self._aggregate_type(self._column_with_join(), AggregateOperator.MAX, 'Max ' + self.display_name())  # type: ignore[attr-defined]


def _diff_other(other):
    """The other side of a date difference: a value stays as it is, a column or expression becomes its operand."""
    return other._arithmetic_operand() if isinstance(other, RelationalOperationElement) else other


def _temporal_constant(value: datetime.datetime | datetime.date):
    """A datetime is a timestamp literal; a plain date is a date literal."""
    if isinstance(value, datetime.datetime):  # before date: datetime is a subclass of date
        return DateTimeConstantOperation(value)
    return DateConstantOperation(value)


class StringAttribute(_MinMax[StringAggregateOperation], StringFunctions, Attribute, Equatable[str]):
    """A string attribute supporting scalar functions and filter operations.

    Scalar functions return a ``ScalarFunctionOperation`` that can be passed as
    a column in ``find_all()``.  Filter operations return a ``ComparisonOperation``
    for use as the ``where`` argument.

    Slicing and ``*`` mirror Python ``str`` semantics::

        finder.name()[:5]    # first 5 characters
        finder.name()[-3:]   # last 3 characters
        finder.name()[::-1]  # reversed
        finder.name()[2:7]   # characters at index 2–6 (0-based, exclusive stop)
        finder.name() * 3    # repeated 3 times
    """

    _aggregate_type = StringAggregateOperation

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner: 'Relation | str', parent=None,
                 embedded: 'Attribute | None' = None, nullable: bool = True, optional: bool = False):
        """Create a string attribute."""
        super().__init__(display_name, column_name, column_db_type, owner, parent, embedded, nullable, optional)

    def __add__(self, other) -> ConcatOperation:
        """Join text: ``first_name() + " " + last_name()``. A NULL part makes the whole NULL."""
        return concat(self, other)

    def join(self, separator: str = "", order_by: list | None = None) -> StringAggregateOperation:
        """Join the values of a group into one text with ``separator`` between them. ``order_by`` is the order they
        are joined in (``[f.name().ascending()]``); without it the order is not defined. Missing values are skipped."""
        return StringAggregateOperation(self._column_with_join(), AggregateOperator.STRING_AGG,
                                  'Join ' + self.display_name(), separator=separator, order_by=order_by)

    def __radd__(self, other) -> ConcatOperation:
        return concat(other, self)

    def _to_constant(self, value):
        return StringConstantOperation(value)

    def _coalesce_type(self) -> type[ScalarFunctionOperation]:
        return StringFunctionOperation

    def matches(self, pattern: str) -> ComparisonOperation:
        """Return a regular expression match: the text has a match for ``pattern`` anywhere in it, as Python's ``re.search``.
        ``contains``, ``starts_with`` and ``ends_with`` match their text literally; negate any of them with ``~``."""
        return ComparisonOperation(self._column_with_join(), ComparisonOperator.MATCHES, StringConstantOperation(pattern))

    def contains(self, value: str) -> ComparisonOperation:
        """Return a substring match comparison."""
        return ComparisonOperation(self._column_with_join(), ComparisonOperator.LIKE, StringConstantOperation(f"%{escape_like(value)}%"))

    def starts_with(self, prefix: str) -> ComparisonOperation:
        """Return a prefix match comparison."""
        return ComparisonOperation(self._column_with_join(), ComparisonOperator.LIKE, StringConstantOperation(f"{escape_like(prefix)}%"))

    def ends_with(self, suffix: str) -> Operation:
        """Return a suffix match comparison."""
        return ComparisonOperation(self._column_with_join(), ComparisonOperator.LIKE, StringConstantOperation(f"%{escape_like(suffix)}"))

class NumericAttribute(NumericFunctions[N], Attribute, Generic[N, A]):
    """A numeric attribute supporting aggregate and scalar functions.

    Aggregate functions return an ``AggregateOperation`` for use with
    ``group_by()``; scalar functions return a ``ScalarFunctionOperation``
    that can be passed as a column in ``find_all()``.

    Scalar operations mirror Python's ``math`` module and built-in operators::

        price.abs()        # abs(price)
        price.ceil()       # math.ceil(price)
        price.floor()      # math.floor(price)
        price.sqrt()       # math.sqrt(price)
        price.mod(n)       # price % n  (or price % n)
        price.power(n)     # price ** n (or price ** n)
        price.round()      # round(price)
        price.round(d)     # round(price, d)
    """

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner: 'Relation | str', parent=None,
                 embedded: 'Attribute | None' = None, nullable: bool = True, optional: bool = False):
        """Create a numeric attribute."""
        super().__init__(display_name, column_name, column_db_type, owner, parent, embedded, nullable, optional)

    if TYPE_CHECKING:  # the aggregate of this type, set by each numeric type
        _aggregate_type: Callable[..., A]

    def _coalesce_type(self) -> type[ScalarFunctionOperation]:
        return self._number_type  # type: ignore[return-value]

    def sum(self) -> A:
        """Return the sum of all values in the group."""
        return self._aggregate_type(self._column_with_join(), AggregateOperator.SUM, 'Sum ' + self.display_name())

    def min(self) -> A:
        """Return the minimum value in the group."""
        return self._aggregate_type(self._column_with_join(), AggregateOperator.MIN, 'Min ' + self.display_name())

    def max(self) -> A:
        """Return the maximum value in the group."""
        return self._aggregate_type(self._column_with_join(), AggregateOperator.MAX, 'Max ' + self.display_name())

    def average(self) -> DoubleAggregateOperation:
        """Return the mean of all values in the group."""
        return DoubleAggregateOperation(self._column_with_join(), AggregateOperator.AVERAGE, 'Average ' + self.display_name())

    def std(self, ddof: int = 1) -> DoubleAggregateOperation:
        """Return the standard deviation of the values in the group; ``ddof=1`` (the default) is the sample standard
        deviation and ``ddof=0`` the population one, as in ``Series.std``."""
        operator = AggregateOperator.STDDEV_SAMP if _sample(ddof) else AggregateOperator.STDDEV_POP
        return DoubleAggregateOperation(self._column_with_join(), operator, 'Std ' + self.display_name())

    def var(self, ddof: int = 1) -> DoubleAggregateOperation:
        """Return the variance of the values in the group; ``ddof=1`` (the default) is the sample variance and
        ``ddof=0`` the population one, as in ``Series.var``."""
        operator = AggregateOperator.VAR_SAMP if _sample(ddof) else AggregateOperator.VAR_POP
        return DoubleAggregateOperation(self._column_with_join(), operator, 'Var ' + self.display_name())



class DoubleAttribute(NumericAttribute[DoubleFunctionOperation, DoubleAggregateOperation], Arithmetic[float], Orderable[float]):
    """A floating-point attribute."""

    _number_type = DoubleFunctionOperation
    _aggregate_type = DoubleAggregateOperation
    _number_kind = float

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner: 'Relation | str', parent=None,
                 embedded: 'Attribute | None' = None, nullable: bool = True, optional: bool = False):
        """Create a floating-point attribute."""
        super().__init__(display_name, column_name, column_db_type, owner, parent, embedded, nullable, optional)

    def _to_constant(self, value):
        return FloatConstantOperation(value)


FloatAttribute = DoubleAttribute



class DecimalAttribute(NumericAttribute[DecimalFunctionOperation, DecimalAggregateOperation], Arithmetic[decimal.Decimal],
                       Orderable[decimal.Decimal]):
    """A decimal attribute."""

    _number_type = DecimalFunctionOperation
    _aggregate_type = DecimalAggregateOperation
    _number_kind = decimal.Decimal

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner: 'Relation | str', parent=None,
                 embedded: 'Attribute | None' = None, nullable: bool = True, optional: bool = False):
        """Create a decimal attribute."""
        super().__init__(display_name, column_name, column_db_type, owner, parent, embedded, nullable, optional)

    def _to_constant(self, value):
        return DecimalConstantOperation(value)


class BooleanAttribute(Attribute, Equatable[bool]):
    """A boolean attribute."""

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner: 'Relation | str', parent=None,
                 embedded: 'Attribute | None' = None, nullable: bool = True, optional: bool = False):
        """Create a boolean attribute."""
        super().__init__(display_name, column_name, column_db_type, owner, parent, embedded, nullable, optional)

    def _to_constant(self, value):
        return BooleanConstantOperation(value)

    def is_true(self) -> ComparisonOperation:
        """Return a comparison that matches true values."""
        return ComparisonOperation(self._column_with_join(),ComparisonOperator.EQUAL, BooleanConstantOperation(True))

    def is_false(self) -> ComparisonOperation:
        """Return a comparison that matches false values."""
        return ComparisonOperation(self._column_with_join(),ComparisonOperator.EQUAL, BooleanConstantOperation(False))


class IntegerAttribute(NumericAttribute[IntegerFunctionOperation, IntegerAggregateOperation], Arithmetic[int], Orderable[int]):
    """An integer attribute."""

    _number_type = IntegerFunctionOperation
    _aggregate_type = IntegerAggregateOperation
    _number_kind = int

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner: 'Relation | str', parent=None,
                 embedded: 'Attribute | None' = None, nullable: bool = True, optional: bool = False):
        """Create an integer attribute."""
        super().__init__(display_name, column_name, column_db_type, owner, parent, embedded, nullable, optional)

    def _to_constant(self, value):
        return IntegerConstantOperation(value)


class DateAttribute(_MinMax[DateAggregateOperation], Attribute, Orderable[datetime.date]):
    """A date attribute (YYYY-MM-DD) supporting extract, arithmetic, and diff operations.

    Extract methods mirror ``datetime.date`` attributes::

        trade_date.year()   # d.year
        trade_date.month()  # d.month
        trade_date.day()    # d.day
        trade_date.week()   # d.isocalendar().week
        trade_date.quarter()  # (d.month - 1) // 3 + 1

    Arithmetic methods mirror ``datetime.timedelta`` addition/subtraction::

        trade_date.add_days(n)       # d + timedelta(days=n)
        trade_date.subtract_days(n)  # d - timedelta(days=n)

    Diff methods return the integer difference between the column value and ``other``::

        trade_date.diff_days(other)   # (other - d).days
    """

    _aggregate_type = DateAggregateOperation

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner: 'Relation | str', parent=None,
                 embedded: 'Attribute | None' = None, nullable: bool = True, optional: bool = False):
        """Create a date attribute."""
        super().__init__(display_name, column_name, column_db_type, owner, parent, embedded, nullable, optional)

    def _to_constant(self, value):
        return DateConstantOperation(value)

    def first_day_of_week(self) -> DateTruncOperation:
        """The first day of the week the date falls in (a Monday)."""
        return DateTruncOperation(self._column_with_join(), DatePart.WEEK, 'First Day Of Week ' + self.display_name())

    def first_day_of_month(self) -> DateTruncOperation:
        """The first day of the month the date falls in."""
        return DateTruncOperation(self._column_with_join(), DatePart.MONTH, 'First Day Of Month ' + self.display_name())

    def first_day_of_quarter(self) -> DateTruncOperation:
        """The first day of the quarter the date falls in."""
        return DateTruncOperation(self._column_with_join(), DatePart.QUARTER, 'First Day Of Quarter ' + self.display_name())

    def first_day_of_year(self) -> DateTruncOperation:
        """The first day of the year the date falls in."""
        return DateTruncOperation(self._column_with_join(), DatePart.YEAR, 'First Day Of Year ' + self.display_name())

    def year(self) -> DateExtractOperation:
        """Extract the year. Equivalent to ``d.year``."""
        return DateExtractOperation(self._column_with_join(), DatePart.YEAR, 'Year ' + self.display_name())

    def month(self) -> DateExtractOperation:
        """Extract the month (1–12). Equivalent to ``d.month``."""
        return DateExtractOperation(self._column_with_join(), DatePart.MONTH, 'Month ' + self.display_name())

    def day(self) -> DateExtractOperation:
        """Extract the day of the month (1–31). Equivalent to ``d.day``."""
        return DateExtractOperation(self._column_with_join(), DatePart.DAY, 'Day ' + self.display_name())

    def quarter(self) -> DateExtractOperation:
        """Extract the quarter (1–4). Equivalent to ``(d.month - 1) // 3 + 1``."""
        return DateExtractOperation(self._column_with_join(), DatePart.QUARTER, 'Quarter ' + self.display_name())

    def week(self) -> DateExtractOperation:
        """Extract the ISO week number. Equivalent to ``d.isocalendar().week``."""
        return DateExtractOperation(self._column_with_join(), DatePart.WEEK, 'Week ' + self.display_name())

    def day_of_week(self) -> DateExtractOperation:
        """Extract the day of the week (0=Sunday in SQL). See ``d.weekday()`` for Python's Monday-based equivalent."""
        return DateExtractOperation(self._column_with_join(), DatePart.DOW, 'Day Of Week ' + self.display_name())

    def add_days(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Add ``n`` days. Equivalent to ``d + timedelta(days=n)``."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.DAY, True, 'Add Days ' + self.display_name())

    def add_months(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Add ``n`` calendar months."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.MONTH, True, 'Add Months ' + self.display_name())

    def add_years(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Add ``n`` calendar years."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.YEAR, True, 'Add Years ' + self.display_name())

    def subtract_days(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Subtract ``n`` days. Equivalent to ``d - timedelta(days=n)``."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.DAY, False, 'Subtract Days ' + self.display_name())

    def subtract_months(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Subtract ``n`` calendar months."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.MONTH, False, 'Subtract Months ' + self.display_name())

    def subtract_years(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Subtract ``n`` calendar years."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.YEAR, False, 'Subtract Years ' + self.display_name())

    def __add__(self, td: datetime.timedelta):
        """Add a ``timedelta``. Equivalent to ``d + td``. Only day-precision timedeltas are supported."""
        if not isinstance(td, datetime.timedelta):
            return NotImplemented
        if td.seconds != 0 or td.microseconds != 0:
            raise ValueError("DateAttribute only supports day-precision timedelta; use DateTimeAttribute for sub-day offsets")
        return self.add_days(td.days)

    def __sub__(self, td: datetime.timedelta):
        """Subtract a ``timedelta``. Equivalent to ``d - td``. Only day-precision timedeltas are supported."""
        if not isinstance(td, datetime.timedelta):
            return NotImplemented
        if td.seconds != 0 or td.microseconds != 0:
            raise ValueError("DateAttribute only supports day-precision timedelta; use DateTimeAttribute for sub-day offsets")
        return self.subtract_days(td.days)

    def diff_days(self, other: datetime.date | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of days between the column value and ``other``. Equivalent to ``(other - d).days``."""
        return DateDiffOperation(self._column_with_join(), _diff_other(other), DatePart.DAY, 'Diff Days ' + self.display_name())

    def diff_months(self, other: datetime.date | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole calendar months between the column value and ``other``."""
        return DateDiffOperation(self._column_with_join(), _diff_other(other), DatePart.MONTH, 'Diff Months ' + self.display_name())

    def diff_years(self, other: datetime.date | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole calendar years between the column value and ``other``."""
        return DateDiffOperation(self._column_with_join(), _diff_other(other), DatePart.YEAR, 'Diff Years ' + self.display_name())


class DateTimeAttribute(_MinMax[DateTimeAggregateOperation], Attribute, Orderable[datetime.datetime | datetime.date]):
    """A datetime attribute (YYYY-MM-DD HH:MM:SS) supporting extract, arithmetic, and diff operations.

    Extends the capabilities of ``DateAttribute`` with hour, minute, and second
    granularity. Extract methods mirror ``datetime.datetime`` attributes::

        ts.year()    # dt.year
        ts.month()   # dt.month
        ts.hour()    # dt.hour
        ts.minute()  # dt.minute
        ts.second()  # dt.second

    Arithmetic and diff methods work the same as ``DateAttribute`` but also
    support hours, minutes, and seconds::

        ts.add_hours(n)       # dt + timedelta(hours=n)
        ts.subtract_hours(n)  # dt - timedelta(hours=n)
        ts.diff_seconds(other)
    """

    _aggregate_type = DateTimeAggregateOperation

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner: 'Relation | str', parent=None,
                 embedded: 'Attribute | None' = None, nullable: bool = True, optional: bool = False):
        """Create a datetime attribute."""
        super().__init__(display_name, column_name, column_db_type, owner, parent, embedded, nullable, optional)

    def _to_constant(self, value):
        return _temporal_constant(value)

    def first_day_of_week(self) -> DateTruncOperation:
        """The date of the first day of the week the date falls in (a Monday)."""
        return DateTruncOperation(self._column_with_join(), DatePart.WEEK, 'First Day Of Week ' + self.display_name())

    def first_day_of_month(self) -> DateTruncOperation:
        """The date of the first day of the month the date falls in."""
        return DateTruncOperation(self._column_with_join(), DatePart.MONTH, 'First Day Of Month ' + self.display_name())

    def first_day_of_quarter(self) -> DateTruncOperation:
        """The date of the first day of the quarter the date falls in."""
        return DateTruncOperation(self._column_with_join(), DatePart.QUARTER, 'First Day Of Quarter ' + self.display_name())

    def first_day_of_year(self) -> DateTruncOperation:
        """The date of the first day of the year the date falls in."""
        return DateTruncOperation(self._column_with_join(), DatePart.YEAR, 'First Day Of Year ' + self.display_name())

    def year(self) -> DateExtractOperation:
        """Extract the year. Equivalent to ``dt.year``."""
        return DateExtractOperation(self._column_with_join(), DatePart.YEAR, 'Year ' + self.display_name())

    def month(self) -> DateExtractOperation:
        """Extract the month (1–12). Equivalent to ``dt.month``."""
        return DateExtractOperation(self._column_with_join(), DatePart.MONTH, 'Month ' + self.display_name())

    def day(self) -> DateExtractOperation:
        """Extract the day of the month (1–31). Equivalent to ``dt.day``."""
        return DateExtractOperation(self._column_with_join(), DatePart.DAY, 'Day ' + self.display_name())

    def hour(self) -> DateExtractOperation:
        """Extract the hour (0–23). Equivalent to ``dt.hour``."""
        return DateExtractOperation(self._column_with_join(), DatePart.HOUR, 'Hour ' + self.display_name())

    def minute(self) -> DateExtractOperation:
        """Extract the minute (0–59). Equivalent to ``dt.minute``."""
        return DateExtractOperation(self._column_with_join(), DatePart.MINUTE, 'Minute ' + self.display_name())

    def second(self) -> DateExtractOperation:
        """Extract the second (0–59). Equivalent to ``dt.second``."""
        return DateExtractOperation(self._column_with_join(), DatePart.SECOND, 'Second ' + self.display_name())

    def quarter(self) -> DateExtractOperation:
        """Extract the quarter (1–4). Equivalent to ``(dt.month - 1) // 3 + 1``."""
        return DateExtractOperation(self._column_with_join(), DatePart.QUARTER, 'Quarter ' + self.display_name())

    def week(self) -> DateExtractOperation:
        """Extract the ISO week number. Equivalent to ``dt.isocalendar().week``."""
        return DateExtractOperation(self._column_with_join(), DatePart.WEEK, 'Week ' + self.display_name())

    def day_of_week(self) -> DateExtractOperation:
        """Extract the day of the week (0=Sunday in SQL). See ``dt.weekday()`` for Python's Monday-based equivalent."""
        return DateExtractOperation(self._column_with_join(), DatePart.DOW, 'Day Of Week ' + self.display_name())

    def add_days(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` days. Equivalent to ``dt + timedelta(days=n)``."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.DAY, True, 'Add Days ' + self.display_name())

    def add_months(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` calendar months."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.MONTH, True, 'Add Months ' + self.display_name())

    def add_years(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` calendar years."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.YEAR, True, 'Add Years ' + self.display_name())

    def add_hours(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` hours. Equivalent to ``dt + timedelta(hours=n)``."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.HOUR, True, 'Add Hours ' + self.display_name())

    def add_minutes(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` minutes. Equivalent to ``dt + timedelta(minutes=n)``."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.MINUTE, True, 'Add Minutes ' + self.display_name())

    def add_seconds(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` seconds. Equivalent to ``dt + timedelta(seconds=n)``."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.SECOND, True, 'Add Seconds ' + self.display_name())

    def subtract_days(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` days. Equivalent to ``dt - timedelta(days=n)``."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.DAY, False, 'Subtract Days ' + self.display_name())

    def subtract_months(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` calendar months."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.MONTH, False, 'Subtract Months ' + self.display_name())

    def subtract_years(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` calendar years."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.YEAR, False, 'Subtract Years ' + self.display_name())

    def subtract_hours(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` hours. Equivalent to ``dt - timedelta(hours=n)``."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.HOUR, False, 'Subtract Hours ' + self.display_name())

    def subtract_minutes(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` minutes. Equivalent to ``dt - timedelta(minutes=n)``."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.MINUTE, False, 'Subtract Minutes ' + self.display_name())

    def subtract_seconds(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` seconds. Equivalent to ``dt - timedelta(seconds=n)``."""
        return DateArithmeticOperation(self._column_with_join(), n, DatePart.SECOND, False, 'Subtract Seconds ' + self.display_name())

    def __add__(self, td: datetime.timedelta):
        """Add a ``timedelta``. Equivalent to ``dt + td``.

        Dispatches to the finest unit the timedelta represents:
        days → ``add_days``, hours → ``add_hours``, minutes → ``add_minutes``,
        seconds → ``add_seconds``.
        """
        if not isinstance(td, datetime.timedelta):
            return NotImplemented
        return self._timedelta_op(td, is_add=True)

    def __sub__(self, td: datetime.timedelta):
        """Subtract a ``timedelta``. Equivalent to ``dt - td``."""
        if not isinstance(td, datetime.timedelta):
            return NotImplemented
        return self._timedelta_op(td, is_add=False)

    def _timedelta_op(self, td: datetime.timedelta, is_add: bool):
        total_seconds = int(td.total_seconds())
        if total_seconds % 86400 == 0:
            n = total_seconds // 86400
            return self.add_days(n) if is_add else self.subtract_days(n)
        if total_seconds % 3600 == 0:
            n = total_seconds // 3600
            return self.add_hours(n) if is_add else self.subtract_hours(n)
        if total_seconds % 60 == 0:
            n = total_seconds // 60
            return self.add_minutes(n) if is_add else self.subtract_minutes(n)
        return self.add_seconds(total_seconds) if is_add else self.subtract_seconds(total_seconds)

    def diff_days(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of days between the column value and ``other``. Equivalent to ``(other - dt).days``."""
        return DateDiffOperation(self._column_with_join(), _diff_other(other), DatePart.DAY, 'Diff Days ' + self.display_name())

    def diff_months(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole calendar months between the column value and ``other``."""
        return DateDiffOperation(self._column_with_join(), _diff_other(other), DatePart.MONTH, 'Diff Months ' + self.display_name())

    def diff_years(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole calendar years between the column value and ``other``."""
        return DateDiffOperation(self._column_with_join(), _diff_other(other), DatePart.YEAR, 'Diff Years ' + self.display_name())

    def diff_hours(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole hours between the column value and ``other``. Equivalent to ``int((other - dt).total_seconds() // 3600)``."""
        return DateDiffOperation(self._column_with_join(), _diff_other(other), DatePart.HOUR, 'Diff Hours ' + self.display_name())

    def diff_minutes(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole minutes between the column value and ``other``. Equivalent to ``int((other - dt).total_seconds() // 60)``."""
        return DateDiffOperation(self._column_with_join(), _diff_other(other), DatePart.MINUTE, 'Diff Minutes ' + self.display_name())

    def diff_seconds(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole seconds between the column value and ``other``. Equivalent to ``int((other - dt).total_seconds())``."""
        return DateDiffOperation(self._column_with_join(), _diff_other(other), DatePart.SECOND, 'Diff Seconds ' + self.display_name())
