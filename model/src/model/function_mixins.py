"""Scalar functions as methods, shared by the things that hold a value: columns, the results of other functions, and
constants, so they chain: ``f.name().upper().length()``, ``constant("Hello").upper().length()``.

The class they are mixed into provides ``_function_input()``, what the function is applied to, and ``_function_label()``,
the name its result is labelled with. An attribute reads its column and is labelled by its display name; a function result
or a constant is itself the input and is labelled by its own name or value.

The operations the functions give are in ``model.relational``, which uses these mixins, so they are reached through ``_r``
when a function is called, not when this module is imported."""
from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar

if TYPE_CHECKING:
    from model import relational as _r
    from model.relational import (
        DateArithmeticOperation, DateDiffOperation, DateExtractOperation, DateTruncOperation,
        DecimalFunctionOperation, DoubleFunctionOperation, IntegerFunctionOperation, RelationalOperationElement,
        ScalarFunction, ScalarFunctionOperation, StringFunctionOperation,
    )
else:
    class _Lazy:
        """``model.relational`` as it is when a function is called: it imports this module, so this cannot import it."""

        def __getattr__(self, name: str) -> Any:
            import model.relational
            return getattr(model.relational, name)

    _r = _Lazy()

N = TypeVar("N", bound="ScalarFunctionOperation")


def _diff_other(other):
    """The other side of a date difference: a value stays as it is, a column or expression becomes its operand."""
    return other._arithmetic_operand() if isinstance(other, _r.RelationalOperationElement) else other


class StringFunctions:
    """Functions of text. The user of this class provides ``_function_input()`` and ``_function_label()``."""

    if TYPE_CHECKING:  # provided by the class this is mixed into; declared here only, so it does not hide the real one
        def _function_input(self) -> Any: ...
        def _function_label(self) -> str: ...

    def upper(self):
        """Return the value converted to uppercase. Equivalent to ``str.upper()``."""
        return _r.string_function(self._function_input(), _r.ScalarFunction.UPPER, 'Upper ' + self._function_label())

    def lower(self):
        """Return the value converted to lowercase. Equivalent to ``str.lower()``."""
        return _r.string_function(self._function_input(), _r.ScalarFunction.LOWER, 'Lower ' + self._function_label())

    def strip(self):
        """Remove leading and trailing whitespace. Equivalent to ``str.strip()``."""
        return _r.string_function(self._function_input(), _r.ScalarFunction.TRIM, 'Strip ' + self._function_label())

    def lstrip(self):
        """Remove leading whitespace. Equivalent to ``str.lstrip()``."""
        return _r.string_function(self._function_input(), _r.ScalarFunction.LTRIM, 'Lstrip ' + self._function_label())

    def rstrip(self):
        """Remove trailing whitespace. Equivalent to ``str.rstrip()``."""
        return _r.string_function(self._function_input(), _r.ScalarFunction.RTRIM, 'Rstrip ' + self._function_label())

    def length(self):
        """Return the number of characters. Equivalent to ``len(s)``."""
        return _r.integer_function(self._function_input(), _r.ScalarFunction.LENGTH, 'Length ' + self._function_label())

    def reverse(self):
        """Return the value with characters in reverse order. Equivalent to ``s[::-1]``."""
        return _r.string_function(self._function_input(), _r.ScalarFunction.REVERSE, 'Reverse ' + self._function_label())

    def left(self, n: int):
        """Return the first ``n`` characters. Equivalent to ``s[:n]``."""
        return _r.string_function(self._function_input(), _r.ScalarFunction.LEFT, 'Left ' + self._function_label(), extra_args=[n])

    def right(self, n: int):
        """Return the last ``n`` characters. Equivalent to ``s[-n:]``."""
        return _r.string_function(self._function_input(), _r.ScalarFunction.RIGHT, 'Right ' + self._function_label(), extra_args=[n])

    def repeat(self, n: int):
        """Return the value repeated ``n`` times. Equivalent to ``s * n``."""
        return _r.string_function(self._function_input(), _r.ScalarFunction.REPEAT, 'Repeat ' + self._function_label(), extra_args=[n])

    def __mul__(self, n: int):
        """Repeat the value ``n`` times. Equivalent to ``repeat(n)``."""
        return self.repeat(n)

    def replace(self, from_str: str, to_str: str):
        """Replace all occurrences of ``from_str`` with ``to_str``. Equivalent to ``str.replace()``."""
        return _r.string_function(self._function_input(), _r.ScalarFunction.REPLACE, 'Replace ' + self._function_label(), extra_args=[from_str, to_str])

    def substring(self, start: int, length: int | None = None):
        """Return a substring using 0-based ``start`` index.

        ``substring(start)`` returns from ``start`` to end of string.
        ``substring(start, length)`` returns ``length`` characters from ``start``.
        Equivalent to ``s[start:]`` and ``s[start:start+length]`` respectively.
        """
        args = [start + 1] if length is None else [start + 1, length]
        return _r.string_function(self._function_input(), _r.ScalarFunction.SUBSTRING, 'Substring ' + self._function_label(), extra_args=args)

    def __getitem__(self, key):
        """Support Python slice syntax to produce string operations.

        Supported forms::

            s[:n]     → left(n)
            s[-n:]    → right(n)
            s[::-1]   → reverse()
            s[start:] → substring(start)
            s[a:b]    → substring(a, b - a)
        """
        if not isinstance(key, slice):
            raise TypeError("StringAttribute only supports slice indexing")
        start, stop, step = key.start, key.stop, key.step
        if step == -1 and start is None and stop is None:
            return self.reverse()
        if step is not None and step != 1:
            raise ValueError(f"Unsupported slice step: {step}")
        if start is not None and start < 0 and stop is None:
            return self.right(-start)
        if start is None and stop is not None and stop >= 0:
            return self.left(stop)
        if start is not None and start >= 0:
            if stop is None:
                return self.substring(start)
            if stop > start:
                return self.substring(start, stop - start)
        raise ValueError(f"Unsupported slice: [{start}:{stop}:{step}]")


class NumericFunctions(Generic[N]):
    """Functions of a number. The user of this class provides ``_function_input()``, ``_function_label()`` and
    ``_number_type``, the operation that holds a result of the same type as the input: ``abs``, ``ceil``, ``floor``,
    ``mod`` and ``round`` keep an integer an integer. ``sqrt``, ``power``, the logarithm, exponential and the
    trigonometric functions give a floating-point number, as SQL does."""

    if TYPE_CHECKING:  # provided by the class this is mixed into; declared here only, so it does not hide the real one
        def _function_input(self) -> Any: ...
        def _function_label(self) -> str: ...
        _number_type: Callable[..., N]

    def _same_type(self, function: ScalarFunction, label: str, second_arg: int | float | None = None) -> N:
        return self._number_type(self._function_input(), function, label, second_arg=second_arg)

    def abs(self):
        """Return the absolute value. Equivalent to ``abs(n)``."""
        return self._same_type(_r.ScalarFunction.ABS, 'Abs ' + self._function_label())

    def ceil(self):
        """Return the smallest integer >= the value. Equivalent to ``math.ceil(n)``."""
        return self._same_type(_r.ScalarFunction.CEILING, 'Ceil ' + self._function_label())

    def floor(self):
        """Return the largest integer <= the value. Equivalent to ``math.floor(n)``."""
        return self._same_type(_r.ScalarFunction.FLOOR, 'Floor ' + self._function_label())

    def sqrt(self):
        """Return the square root. Equivalent to ``math.sqrt(n)``."""
        return _r.double_function(self._function_input(), _r.ScalarFunction.SQRT, 'Sqrt ' + self._function_label())

    def sin(self):
        """Return the sine of the value (in radians)."""
        return _r.double_function(self._function_input(), _r.ScalarFunction.SIN, 'Sin ' + self._function_label())

    def cos(self):
        """Return the cosine of the value (in radians)."""
        return _r.double_function(self._function_input(), _r.ScalarFunction.COS, 'Cos ' + self._function_label())

    def tan(self):
        """Return the tangent of the value (in radians)."""
        return _r.double_function(self._function_input(), _r.ScalarFunction.TAN, 'Tan ' + self._function_label())

    def asin(self):
        """Return the inverse sine, in radians."""
        return _r.double_function(self._function_input(), _r.ScalarFunction.ASIN, 'Asin ' + self._function_label())

    def acos(self):
        """Return the inverse cosine, in radians."""
        return _r.double_function(self._function_input(), _r.ScalarFunction.ACOS, 'Acos ' + self._function_label())

    def atan(self):
        """Return the inverse tangent, in radians."""
        return _r.double_function(self._function_input(), _r.ScalarFunction.ATAN, 'Atan ' + self._function_label())

    def atan2(self, x: int | float):
        """Return atan2(value, x), in radians."""
        return _r.double_function(self._function_input(), _r.ScalarFunction.ATAN2, 'Atan2 ' + self._function_label(), second_arg=x)

    def exp(self):
        """Return e raised to the value."""
        return _r.double_function(self._function_input(), _r.ScalarFunction.EXP, 'Exp ' + self._function_label())

    def log(self):
        """Return the natural logarithm of the value."""
        return _r.double_function(self._function_input(), _r.ScalarFunction.LOG, 'Log ' + self._function_label())

    def mod(self, n: int):
        """Return the remainder after division by ``n``. Equivalent to ``value % n``."""
        return self._same_type(_r.ScalarFunction.MOD, 'Mod ' + self._function_label(), second_arg=n)

    def __mod__(self, n: int):
        """Return the remainder after division by ``n``. Equivalent to ``mod(n)``."""
        return self.mod(n)

    def power(self, n: int) -> 'IntegerFunctionOperation | DoubleFunctionOperation':
        """Return the value raised to the power of ``n``. Equivalent to ``value ** n``.

        An integer raised to a whole, non-negative ``n`` is an integer, as in Python. SQL's ``POWER`` gives a double, so
        the result is cast to ``BIGINT``. Any other power is a floating-point number."""
        label = 'Power ' + self._function_label()
        if self._number_type is _r.IntegerFunctionOperation and isinstance(n, int) and not isinstance(n, bool) and n >= 0:
            return _r.IntegerFunctionOperation(self._function_input(), _r.ScalarFunction.POWER, label, second_arg=n, cast='BIGINT')
        return _r.double_function(self._function_input(), _r.ScalarFunction.POWER, label, second_arg=n)

    def __pow__(self, n: int) -> 'IntegerFunctionOperation | DoubleFunctionOperation':
        """Return the value raised to the power of ``n``. Equivalent to ``power(n)``."""
        return self.power(n)

    def round(self, d: int | None = None):
        """Round to ``d`` decimal places. Equivalent to ``round(value, d)``.

        When ``d`` is omitted, rounds to the nearest integer.
        """
        return self._same_type(_r.ScalarFunction.ROUND, 'Round ' + self._function_label(), second_arg=d)


class DateFunctions:
    """Functions of a date: parts, the first day of a period, days, months and years added or taken away, and the
    difference between two dates. Shared by a date column and a date constant; the user of this class provides
    ``_function_input()`` and ``_function_label()``.

    Extract methods mirror ``datetime.date`` attributes::

        trade_date.year()   # d.year
        trade_date.month()  # d.month
        trade_date.day()    # d.day
        trade_date.week()   # d.isocalendar().week
        trade_date.quarter()  # (d.month - 1) // 3 + 1

    Arithmetic methods mirror ``datetime.timedelta`` addition/subtraction::

        trade_date.add_days(n)       # d + timedelta(days=n)
        trade_date.subtract_days(n)  # d - timedelta(days=n)

    Diff methods return the integer difference between the value and ``other``::

        trade_date.diff_days(other)   # (other - d).days
    """

    if TYPE_CHECKING:  # provided by the class this is mixed into; declared here only, so it does not hide the real one
        def _function_input(self) -> Any: ...
        def _function_label(self) -> str: ...

    def first_day_of_week(self) -> DateTruncOperation:
        """The first day of the week the date falls in (a Monday)."""
        return _r.DateTruncOperation(self._function_input(), _r.DatePart.WEEK, 'First Day Of Week ' + self._function_label())

    def first_day_of_month(self) -> DateTruncOperation:
        """The first day of the month the date falls in."""
        return _r.DateTruncOperation(self._function_input(), _r.DatePart.MONTH, 'First Day Of Month ' + self._function_label())

    def first_day_of_quarter(self) -> DateTruncOperation:
        """The first day of the quarter the date falls in."""
        return _r.DateTruncOperation(self._function_input(), _r.DatePart.QUARTER, 'First Day Of Quarter ' + self._function_label())

    def first_day_of_year(self) -> DateTruncOperation:
        """The first day of the year the date falls in."""
        return _r.DateTruncOperation(self._function_input(), _r.DatePart.YEAR, 'First Day Of Year ' + self._function_label())

    def year(self) -> DateExtractOperation:
        """Extract the year. Equivalent to ``d.year``."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.YEAR, 'Year ' + self._function_label())

    def month(self) -> DateExtractOperation:
        """Extract the month (1–12). Equivalent to ``d.month``."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.MONTH, 'Month ' + self._function_label())

    def day(self) -> DateExtractOperation:
        """Extract the day of the month (1–31). Equivalent to ``d.day``."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.DAY, 'Day ' + self._function_label())

    def quarter(self) -> DateExtractOperation:
        """Extract the quarter (1–4). Equivalent to ``(d.month - 1) // 3 + 1``."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.QUARTER, 'Quarter ' + self._function_label())

    def week(self) -> DateExtractOperation:
        """Extract the ISO week number. Equivalent to ``d.isocalendar().week``."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.WEEK, 'Week ' + self._function_label())

    def day_of_week(self) -> DateExtractOperation:
        """Extract the day of the week (0=Sunday in SQL). See ``d.weekday()`` for Python's Monday-based equivalent."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.DOW, 'Day Of Week ' + self._function_label())

    def add_days(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Add ``n`` days. Equivalent to ``d + timedelta(days=n)``."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.DAY, True, 'Add Days ' + self._function_label())

    def add_months(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Add ``n`` calendar months."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.MONTH, True, 'Add Months ' + self._function_label())

    def add_years(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Add ``n`` calendar years."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.YEAR, True, 'Add Years ' + self._function_label())

    def subtract_days(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Subtract ``n`` days. Equivalent to ``d - timedelta(days=n)``."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.DAY, False, 'Subtract Days ' + self._function_label())

    def subtract_months(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Subtract ``n`` calendar months."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.MONTH, False, 'Subtract Months ' + self._function_label())

    def subtract_years(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Subtract ``n`` calendar years."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.YEAR, False, 'Subtract Years ' + self._function_label())

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
        return _r.DateDiffOperation(self._function_input(), _diff_other(other), _r.DatePart.DAY, 'Diff Days ' + self._function_label())

    def diff_months(self, other: datetime.date | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole calendar months between the column value and ``other``."""
        return _r.DateDiffOperation(self._function_input(), _diff_other(other), _r.DatePart.MONTH, 'Diff Months ' + self._function_label())

    def diff_years(self, other: datetime.date | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole calendar years between the column value and ``other``."""
        return _r.DateDiffOperation(self._function_input(), _diff_other(other), _r.DatePart.YEAR, 'Diff Years ' + self._function_label())


class DateTimeFunctions:
    """Functions of a datetime: those of a date, with hour, minute and second granularity. Shared by a datetime column and
    a datetime constant; the user of this class provides ``_function_input()`` and ``_function_label()``.

    Extract methods mirror ``datetime.datetime`` attributes::

        ts.year()    # dt.year
        ts.month()   # dt.month
        ts.hour()    # dt.hour
        ts.minute()  # dt.minute
        ts.second()  # dt.second

    Arithmetic and diff methods work the same as for a date but also support hours, minutes, and seconds::

        ts.add_hours(n)       # dt + timedelta(hours=n)
        ts.subtract_hours(n)  # dt - timedelta(hours=n)
        ts.diff_seconds(other)
    """

    if TYPE_CHECKING:  # provided by the class this is mixed into; declared here only, so it does not hide the real one
        def _function_input(self) -> Any: ...
        def _function_label(self) -> str: ...

    def first_day_of_week(self) -> DateTruncOperation:
        """The date of the first day of the week the date falls in (a Monday)."""
        return _r.DateTruncOperation(self._function_input(), _r.DatePart.WEEK, 'First Day Of Week ' + self._function_label())

    def first_day_of_month(self) -> DateTruncOperation:
        """The date of the first day of the month the date falls in."""
        return _r.DateTruncOperation(self._function_input(), _r.DatePart.MONTH, 'First Day Of Month ' + self._function_label())

    def first_day_of_quarter(self) -> DateTruncOperation:
        """The date of the first day of the quarter the date falls in."""
        return _r.DateTruncOperation(self._function_input(), _r.DatePart.QUARTER, 'First Day Of Quarter ' + self._function_label())

    def first_day_of_year(self) -> DateTruncOperation:
        """The date of the first day of the year the date falls in."""
        return _r.DateTruncOperation(self._function_input(), _r.DatePart.YEAR, 'First Day Of Year ' + self._function_label())

    def year(self) -> DateExtractOperation:
        """Extract the year. Equivalent to ``dt.year``."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.YEAR, 'Year ' + self._function_label())

    def month(self) -> DateExtractOperation:
        """Extract the month (1–12). Equivalent to ``dt.month``."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.MONTH, 'Month ' + self._function_label())

    def day(self) -> DateExtractOperation:
        """Extract the day of the month (1–31). Equivalent to ``dt.day``."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.DAY, 'Day ' + self._function_label())

    def hour(self) -> DateExtractOperation:
        """Extract the hour (0–23). Equivalent to ``dt.hour``."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.HOUR, 'Hour ' + self._function_label())

    def minute(self) -> DateExtractOperation:
        """Extract the minute (0–59). Equivalent to ``dt.minute``."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.MINUTE, 'Minute ' + self._function_label())

    def second(self) -> DateExtractOperation:
        """Extract the second (0–59). Equivalent to ``dt.second``."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.SECOND, 'Second ' + self._function_label())

    def quarter(self) -> DateExtractOperation:
        """Extract the quarter (1–4). Equivalent to ``(dt.month - 1) // 3 + 1``."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.QUARTER, 'Quarter ' + self._function_label())

    def week(self) -> DateExtractOperation:
        """Extract the ISO week number. Equivalent to ``dt.isocalendar().week``."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.WEEK, 'Week ' + self._function_label())

    def day_of_week(self) -> DateExtractOperation:
        """Extract the day of the week (0=Sunday in SQL). See ``dt.weekday()`` for Python's Monday-based equivalent."""
        return _r.DateExtractOperation(self._function_input(), _r.DatePart.DOW, 'Day Of Week ' + self._function_label())

    def add_days(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` days. Equivalent to ``dt + timedelta(days=n)``."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.DAY, True, 'Add Days ' + self._function_label())

    def add_months(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` calendar months."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.MONTH, True, 'Add Months ' + self._function_label())

    def add_years(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` calendar years."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.YEAR, True, 'Add Years ' + self._function_label())

    def add_hours(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` hours. Equivalent to ``dt + timedelta(hours=n)``."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.HOUR, True, 'Add Hours ' + self._function_label())

    def add_minutes(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` minutes. Equivalent to ``dt + timedelta(minutes=n)``."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.MINUTE, True, 'Add Minutes ' + self._function_label())

    def add_seconds(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` seconds. Equivalent to ``dt + timedelta(seconds=n)``."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.SECOND, True, 'Add Seconds ' + self._function_label())

    def subtract_days(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` days. Equivalent to ``dt - timedelta(days=n)``."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.DAY, False, 'Subtract Days ' + self._function_label())

    def subtract_months(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` calendar months."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.MONTH, False, 'Subtract Months ' + self._function_label())

    def subtract_years(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` calendar years."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.YEAR, False, 'Subtract Years ' + self._function_label())

    def subtract_hours(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` hours. Equivalent to ``dt - timedelta(hours=n)``."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.HOUR, False, 'Subtract Hours ' + self._function_label())

    def subtract_minutes(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` minutes. Equivalent to ``dt - timedelta(minutes=n)``."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.MINUTE, False, 'Subtract Minutes ' + self._function_label())

    def subtract_seconds(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` seconds. Equivalent to ``dt - timedelta(seconds=n)``."""
        return _r.DateArithmeticOperation(self._function_input(), n, _r.DatePart.SECOND, False, 'Subtract Seconds ' + self._function_label())

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
        return _r.DateDiffOperation(self._function_input(), _diff_other(other), _r.DatePart.DAY, 'Diff Days ' + self._function_label())

    def diff_months(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole calendar months between the column value and ``other``."""
        return _r.DateDiffOperation(self._function_input(), _diff_other(other), _r.DatePart.MONTH, 'Diff Months ' + self._function_label())

    def diff_years(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole calendar years between the column value and ``other``."""
        return _r.DateDiffOperation(self._function_input(), _diff_other(other), _r.DatePart.YEAR, 'Diff Years ' + self._function_label())

    def diff_hours(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole hours between the column value and ``other``. Equivalent to ``int((other - dt).total_seconds() // 3600)``."""
        return _r.DateDiffOperation(self._function_input(), _diff_other(other), _r.DatePart.HOUR, 'Diff Hours ' + self._function_label())

    def diff_minutes(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole minutes between the column value and ``other``. Equivalent to ``int((other - dt).total_seconds() // 60)``."""
        return _r.DateDiffOperation(self._function_input(), _diff_other(other), _r.DatePart.MINUTE, 'Diff Minutes ' + self._function_label())

    def diff_seconds(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole seconds between the column value and ``other``. Equivalent to ``int((other - dt).total_seconds())``."""
        return _r.DateDiffOperation(self._function_input(), _diff_other(other), _r.DatePart.SECOND, 'Diff Seconds ' + self._function_label())
