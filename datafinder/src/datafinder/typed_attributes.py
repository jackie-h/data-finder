import datetime

from datafinder import Attribute
import decimal

from model.relational import ComparisonOperation, StringConstantOperation, Operation, ComparisonOperator, \
    FloatConstantOperation, IntegerConstantOperation, DateConstantOperation, DateTimeConstantOperation, \
    BooleanConstantOperation, DecimalConstantOperation, AggregateOperation, AggregateOperator, ColumnWithJoin, \
    ScalarFunction, ScalarFunctionOperation, DatePart, DateExtractOperation, DateArithmeticOperation, DateDiffOperation


class StringAttribute(Attribute):
    """A string column attribute supporting scalar functions and filter operations.

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

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def _cwj(self):
        return ColumnWithJoin(self.column(), self.parent())

    def upper(self):
        """Return the value converted to uppercase. Equivalent to ``str.upper()``."""
        return ScalarFunctionOperation(self._cwj(), ScalarFunction.UPPER, 'Upper ' + self.display_name())

    def lower(self):
        """Return the value converted to lowercase. Equivalent to ``str.lower()``."""
        return ScalarFunctionOperation(self._cwj(), ScalarFunction.LOWER, 'Lower ' + self.display_name())

    def strip(self):
        """Remove leading and trailing whitespace. Equivalent to ``str.strip()``."""
        return ScalarFunctionOperation(self._cwj(), ScalarFunction.TRIM, 'Strip ' + self.display_name())

    def lstrip(self):
        """Remove leading whitespace. Equivalent to ``str.lstrip()``."""
        return ScalarFunctionOperation(self._cwj(), ScalarFunction.LTRIM, 'Lstrip ' + self.display_name())

    def rstrip(self):
        """Remove trailing whitespace. Equivalent to ``str.rstrip()``."""
        return ScalarFunctionOperation(self._cwj(), ScalarFunction.RTRIM, 'Rstrip ' + self.display_name())

    def length(self):
        """Return the number of characters. Equivalent to ``len(s)``."""
        return ScalarFunctionOperation(self._cwj(), ScalarFunction.LENGTH, 'Length ' + self.display_name())

    def reverse(self):
        """Return the value with characters in reverse order. Equivalent to ``s[::-1]``."""
        return ScalarFunctionOperation(self._cwj(), ScalarFunction.REVERSE, 'Reverse ' + self.display_name())

    def left(self, n: int):
        """Return the first ``n`` characters. Equivalent to ``s[:n]``."""
        return ScalarFunctionOperation(self._cwj(), ScalarFunction.LEFT, 'Left ' + self.display_name(), extra_args=[n])

    def right(self, n: int):
        """Return the last ``n`` characters. Equivalent to ``s[-n:]``."""
        return ScalarFunctionOperation(self._cwj(), ScalarFunction.RIGHT, 'Right ' + self.display_name(), extra_args=[n])

    def repeat(self, n: int):
        """Return the value repeated ``n`` times. Equivalent to ``s * n``."""
        return ScalarFunctionOperation(self._cwj(), ScalarFunction.REPEAT, 'Repeat ' + self.display_name(), extra_args=[n])

    def __mul__(self, n: int):
        """Repeat the value ``n`` times. Equivalent to ``repeat(n)``."""
        return self.repeat(n)

    def replace(self, from_str: str, to_str: str):
        """Replace all occurrences of ``from_str`` with ``to_str``. Equivalent to ``str.replace()``."""
        return ScalarFunctionOperation(self._cwj(), ScalarFunction.REPLACE, 'Replace ' + self.display_name(), extra_args=[from_str, to_str])

    def substring(self, start: int, length: int = None):
        """Return a substring using 0-based ``start`` index.

        ``substring(start)`` returns from ``start`` to end of string.
        ``substring(start, length)`` returns ``length`` characters from ``start``.
        Equivalent to ``s[start:]`` and ``s[start:start+length]`` respectively.
        """
        args = [start + 1] if length is None else [start + 1, length]
        return ScalarFunctionOperation(self._cwj(), ScalarFunction.SUBSTRING, 'Substring ' + self.display_name(), extra_args=args)

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

    def eq(self, value: str) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()), ComparisonOperator.EQUAL, StringConstantOperation(value))

    def __eq__(self, value: str) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()), ComparisonOperator.EQUAL, StringConstantOperation(value))

    def ne(self, value: str) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()), ComparisonOperator.NOT_EQUAL, StringConstantOperation(value))

    def contains(self, value: str) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()), ComparisonOperator.LIKE, StringConstantOperation(f"%{value}%"))

    def starts_with(self, prefix: str) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()), ComparisonOperator.LIKE, StringConstantOperation(f"{prefix}%"))

    def ends_with(self, suffix: str) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()), ComparisonOperator.LIKE, StringConstantOperation(f"%{suffix}"))

class NumericAttribute(Attribute):
    """A numeric column attribute supporting aggregate and scalar functions.

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

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def sum(self):
        """Return the sum of all values in the group."""
        return AggregateOperation(ColumnWithJoin(self.column(), self.parent()), AggregateOperator.SUM, 'Sum ' + self.display_name())

    def min(self):
        """Return the minimum value in the group."""
        return AggregateOperation(ColumnWithJoin(self.column(), self.parent()), AggregateOperator.MIN, 'Min ' + self.display_name())

    def max(self):
        """Return the maximum value in the group."""
        return AggregateOperation(ColumnWithJoin(self.column(), self.parent()), AggregateOperator.MAX, 'Max ' + self.display_name())

    def average(self):
        """Return the mean of all values in the group."""
        return AggregateOperation(ColumnWithJoin(self.column(), self.parent()), AggregateOperator.AVERAGE, 'Average ' + self.display_name())

    def abs(self):
        """Return the absolute value. Equivalent to ``abs(n)``."""
        return ScalarFunctionOperation(ColumnWithJoin(self.column(), self.parent()), ScalarFunction.ABS, 'Abs ' + self.display_name())

    def ceil(self):
        """Return the smallest integer >= the value. Equivalent to ``math.ceil(n)``."""
        return ScalarFunctionOperation(ColumnWithJoin(self.column(), self.parent()), ScalarFunction.CEILING, 'Ceil ' + self.display_name())

    def floor(self):
        """Return the largest integer <= the value. Equivalent to ``math.floor(n)``."""
        return ScalarFunctionOperation(ColumnWithJoin(self.column(), self.parent()), ScalarFunction.FLOOR, 'Floor ' + self.display_name())

    def sqrt(self):
        """Return the square root. Equivalent to ``math.sqrt(n)``."""
        return ScalarFunctionOperation(ColumnWithJoin(self.column(), self.parent()), ScalarFunction.SQRT, 'Sqrt ' + self.display_name())

    def mod(self, n: int):
        """Return the remainder after division by ``n``. Equivalent to ``value % n``."""
        return ScalarFunctionOperation(ColumnWithJoin(self.column(), self.parent()), ScalarFunction.MOD, 'Mod ' + self.display_name(), second_arg=n)

    def __mod__(self, n: int):
        """Return the remainder after division by ``n``. Equivalent to ``mod(n)``."""
        return self.mod(n)

    def power(self, n: int):
        """Return the value raised to the power of ``n``. Equivalent to ``value ** n``."""
        return ScalarFunctionOperation(ColumnWithJoin(self.column(), self.parent()), ScalarFunction.POWER, 'Power ' + self.display_name(), second_arg=n)

    def __pow__(self, n: int):
        """Return the value raised to the power of ``n``. Equivalent to ``power(n)``."""
        return self.power(n)

    def round(self, d: int = None):
        """Round to ``d`` decimal places. Equivalent to ``round(value, d)``.

        When ``d`` is omitted, rounds to the nearest integer.
        """
        return ScalarFunctionOperation(ColumnWithJoin(self.column(), self.parent()), ScalarFunction.ROUND, 'Round ' + self.display_name(), second_arg=d)


class DoubleAttribute(NumericAttribute):

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def eq(self, value: float) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, FloatConstantOperation(value))

    def __eq__(self, value: float) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, FloatConstantOperation(value))

    def __gt__(self, value: float) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.GREATER_THAN, FloatConstantOperation(value))

    def __lt__(self, value: float):
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.LESS_THAN, FloatConstantOperation(value))

    def __ge__(self, value: float) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, FloatConstantOperation(value))

    def __le__(self, value: float):
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.LESS_THAN_OR_EQUAL_TO, FloatConstantOperation(value))


FloatAttribute = DoubleAttribute



class DecimalAttribute(NumericAttribute):

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner: str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def eq(self, value: decimal.Decimal) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, DecimalConstantOperation(value))

    def __eq__(self, value: decimal.Decimal) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, DecimalConstantOperation(value))

    def __gt__(self, value: decimal.Decimal) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.GREATER_THAN, DecimalConstantOperation(value))

    def __lt__(self, value: decimal.Decimal) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.LESS_THAN, DecimalConstantOperation(value))

    def __ge__(self, value: decimal.Decimal) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, DecimalConstantOperation(value))

    def __le__(self, value: decimal.Decimal) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.LESS_THAN_OR_EQUAL_TO, DecimalConstantOperation(value))


class BooleanAttribute(Attribute):

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner: str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def eq(self, value: bool) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, BooleanConstantOperation(value))

    def __eq__(self, value: bool) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, BooleanConstantOperation(value))

    def is_true(self) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, BooleanConstantOperation(True))

    def is_false(self) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, BooleanConstantOperation(False))


class IntegerAttribute(NumericAttribute):

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def eq(self, value: int) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, IntegerConstantOperation(value))

    def __eq__(self, value: int) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, IntegerConstantOperation(value))

    def __gt__(self, value: int) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.GREATER_THAN, IntegerConstantOperation(value))

    def __lt__(self, value: int):
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.LESS_THAN, IntegerConstantOperation(value))

    def __ge__(self, value: int) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.GREATER_THAN_OR_EQUAL_TO,
                                   IntegerConstantOperation(value))

    def __le__(self, value: int):
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.LESS_THAN_OR_EQUAL_TO,
                                   IntegerConstantOperation(value))


class DateAttribute(Attribute):
    """A date column attribute (YYYY-MM-DD) supporting extract, arithmetic, and diff operations.

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

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def eq(self, value: datetime.date) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, DateConstantOperation(value))

    def __eq__(self, value: datetime.date) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, DateConstantOperation(value))

    def __gt__(self, value: datetime.date) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.GREATER_THAN, DateConstantOperation(value))

    def __lt__(self, value: datetime.date):
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.LESS_THAN, DateConstantOperation(value))

    def __ge__(self, value: datetime.date) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.GREATER_THAN_OR_EQUAL_TO,
                                   DateConstantOperation(value))

    def __le__(self, value: datetime.date):
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.LESS_THAN_OR_EQUAL_TO,
                                   DateConstantOperation(value))

    def _cwj(self):
        return ColumnWithJoin(self.column(), self.parent())

    def year(self):
        """Extract the year. Equivalent to ``d.year``."""
        return DateExtractOperation(self._cwj(), DatePart.YEAR, 'Year ' + self.display_name())

    def month(self):
        """Extract the month (1–12). Equivalent to ``d.month``."""
        return DateExtractOperation(self._cwj(), DatePart.MONTH, 'Month ' + self.display_name())

    def day(self):
        """Extract the day of the month (1–31). Equivalent to ``d.day``."""
        return DateExtractOperation(self._cwj(), DatePart.DAY, 'Day ' + self.display_name())

    def quarter(self):
        """Extract the quarter (1–4). Equivalent to ``(d.month - 1) // 3 + 1``."""
        return DateExtractOperation(self._cwj(), DatePart.QUARTER, 'Quarter ' + self.display_name())

    def week(self):
        """Extract the ISO week number. Equivalent to ``d.isocalendar().week``."""
        return DateExtractOperation(self._cwj(), DatePart.WEEK, 'Week ' + self.display_name())

    def day_of_week(self):
        """Extract the day of the week (0=Sunday in SQL). See ``d.weekday()`` for Python's Monday-based equivalent."""
        return DateExtractOperation(self._cwj(), DatePart.DOW, 'Day Of Week ' + self.display_name())

    def add_days(self, n: int):
        """Add ``n`` days. Equivalent to ``d + timedelta(days=n)``."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.DAY, True, 'Add Days ' + self.display_name())

    def add_months(self, n: int):
        """Add ``n`` calendar months."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.MONTH, True, 'Add Months ' + self.display_name())

    def add_years(self, n: int):
        """Add ``n`` calendar years."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.YEAR, True, 'Add Years ' + self.display_name())

    def subtract_days(self, n: int):
        """Subtract ``n`` days. Equivalent to ``d - timedelta(days=n)``."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.DAY, False, 'Subtract Days ' + self.display_name())

    def subtract_months(self, n: int):
        """Subtract ``n`` calendar months."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.MONTH, False, 'Subtract Months ' + self.display_name())

    def subtract_years(self, n: int):
        """Subtract ``n`` calendar years."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.YEAR, False, 'Subtract Years ' + self.display_name())

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

    def diff_days(self, other: datetime.date):
        """Return the number of days between the column value and ``other``. Equivalent to ``(other - d).days``."""
        return DateDiffOperation(self._cwj(), other, DatePart.DAY, 'Diff Days ' + self.display_name())

    def diff_months(self, other: datetime.date):
        """Return the number of whole calendar months between the column value and ``other``."""
        return DateDiffOperation(self._cwj(), other, DatePart.MONTH, 'Diff Months ' + self.display_name())

    def diff_years(self, other: datetime.date):
        """Return the number of whole calendar years between the column value and ``other``."""
        return DateDiffOperation(self._cwj(), other, DatePart.YEAR, 'Diff Years ' + self.display_name())


class DateTimeAttribute(Attribute):
    """A datetime column attribute (YYYY-MM-DD HH:MM:SS) supporting extract, arithmetic, and diff operations.

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

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def eq(self, value: datetime.datetime) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, DateTimeConstantOperation(value))

    def __eq__(self, value: datetime.datetime) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.EQUAL, DateTimeConstantOperation(value))

    def __gt__(self, value: datetime.datetime) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.GREATER_THAN, DateTimeConstantOperation(value))

    def __lt__(self, value: datetime.datetime):
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.LESS_THAN, DateTimeConstantOperation(value))

    def __ge__(self, value: datetime.datetime) -> Operation:
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.GREATER_THAN_OR_EQUAL_TO,
                                   DateTimeConstantOperation(value))

    def __le__(self, value: datetime.datetime):
        return ComparisonOperation(ColumnWithJoin(self.column(), self.parent()),ComparisonOperator.LESS_THAN_OR_EQUAL_TO,
                                   DateTimeConstantOperation(value))

    def _cwj(self):
        return ColumnWithJoin(self.column(), self.parent())

    def year(self):
        """Extract the year. Equivalent to ``dt.year``."""
        return DateExtractOperation(self._cwj(), DatePart.YEAR, 'Year ' + self.display_name())

    def month(self):
        """Extract the month (1–12). Equivalent to ``dt.month``."""
        return DateExtractOperation(self._cwj(), DatePart.MONTH, 'Month ' + self.display_name())

    def day(self):
        """Extract the day of the month (1–31). Equivalent to ``dt.day``."""
        return DateExtractOperation(self._cwj(), DatePart.DAY, 'Day ' + self.display_name())

    def hour(self):
        """Extract the hour (0–23). Equivalent to ``dt.hour``."""
        return DateExtractOperation(self._cwj(), DatePart.HOUR, 'Hour ' + self.display_name())

    def minute(self):
        """Extract the minute (0–59). Equivalent to ``dt.minute``."""
        return DateExtractOperation(self._cwj(), DatePart.MINUTE, 'Minute ' + self.display_name())

    def second(self):
        """Extract the second (0–59). Equivalent to ``dt.second``."""
        return DateExtractOperation(self._cwj(), DatePart.SECOND, 'Second ' + self.display_name())

    def quarter(self):
        """Extract the quarter (1–4). Equivalent to ``(dt.month - 1) // 3 + 1``."""
        return DateExtractOperation(self._cwj(), DatePart.QUARTER, 'Quarter ' + self.display_name())

    def week(self):
        """Extract the ISO week number. Equivalent to ``dt.isocalendar().week``."""
        return DateExtractOperation(self._cwj(), DatePart.WEEK, 'Week ' + self.display_name())

    def day_of_week(self):
        """Extract the day of the week (0=Sunday in SQL). See ``dt.weekday()`` for Python's Monday-based equivalent."""
        return DateExtractOperation(self._cwj(), DatePart.DOW, 'Day Of Week ' + self.display_name())

    def add_days(self, n: int):
        """Add ``n`` days. Equivalent to ``dt + timedelta(days=n)``."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.DAY, True, 'Add Days ' + self.display_name())

    def add_months(self, n: int):
        """Add ``n`` calendar months."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.MONTH, True, 'Add Months ' + self.display_name())

    def add_years(self, n: int):
        """Add ``n`` calendar years."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.YEAR, True, 'Add Years ' + self.display_name())

    def add_hours(self, n: int):
        """Add ``n`` hours. Equivalent to ``dt + timedelta(hours=n)``."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.HOUR, True, 'Add Hours ' + self.display_name())

    def add_minutes(self, n: int):
        """Add ``n`` minutes. Equivalent to ``dt + timedelta(minutes=n)``."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.MINUTE, True, 'Add Minutes ' + self.display_name())

    def add_seconds(self, n: int):
        """Add ``n`` seconds. Equivalent to ``dt + timedelta(seconds=n)``."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.SECOND, True, 'Add Seconds ' + self.display_name())

    def subtract_days(self, n: int):
        """Subtract ``n`` days. Equivalent to ``dt - timedelta(days=n)``."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.DAY, False, 'Subtract Days ' + self.display_name())

    def subtract_months(self, n: int):
        """Subtract ``n`` calendar months."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.MONTH, False, 'Subtract Months ' + self.display_name())

    def subtract_years(self, n: int):
        """Subtract ``n`` calendar years."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.YEAR, False, 'Subtract Years ' + self.display_name())

    def subtract_hours(self, n: int):
        """Subtract ``n`` hours. Equivalent to ``dt - timedelta(hours=n)``."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.HOUR, False, 'Subtract Hours ' + self.display_name())

    def subtract_minutes(self, n: int):
        """Subtract ``n`` minutes. Equivalent to ``dt - timedelta(minutes=n)``."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.MINUTE, False, 'Subtract Minutes ' + self.display_name())

    def subtract_seconds(self, n: int):
        """Subtract ``n`` seconds. Equivalent to ``dt - timedelta(seconds=n)``."""
        return DateArithmeticOperation(self._cwj(), n, DatePart.SECOND, False, 'Subtract Seconds ' + self.display_name())

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

    def diff_days(self, other: datetime.datetime):
        """Return the number of days between the column value and ``other``. Equivalent to ``(other - dt).days``."""
        return DateDiffOperation(self._cwj(), other, DatePart.DAY, 'Diff Days ' + self.display_name())

    def diff_months(self, other: datetime.datetime):
        """Return the number of whole calendar months between the column value and ``other``."""
        return DateDiffOperation(self._cwj(), other, DatePart.MONTH, 'Diff Months ' + self.display_name())

    def diff_years(self, other: datetime.datetime):
        """Return the number of whole calendar years between the column value and ``other``."""
        return DateDiffOperation(self._cwj(), other, DatePart.YEAR, 'Diff Years ' + self.display_name())

    def diff_hours(self, other: datetime.datetime):
        """Return the number of whole hours between the column value and ``other``. Equivalent to ``int((other - dt).total_seconds() // 3600)``."""
        return DateDiffOperation(self._cwj(), other, DatePart.HOUR, 'Diff Hours ' + self.display_name())

    def diff_minutes(self, other: datetime.datetime):
        """Return the number of whole minutes between the column value and ``other``. Equivalent to ``int((other - dt).total_seconds() // 60)``."""
        return DateDiffOperation(self._cwj(), other, DatePart.MINUTE, 'Diff Minutes ' + self.display_name())

    def diff_seconds(self, other: datetime.datetime):
        """Return the number of whole seconds between the column value and ``other``. Equivalent to ``int((other - dt).total_seconds())``."""
        return DateDiffOperation(self._cwj(), other, DatePart.SECOND, 'Diff Seconds ' + self.display_name())
