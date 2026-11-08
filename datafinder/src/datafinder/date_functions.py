"""Date and datetime functions as methods, shared by columns and constants of those types, as the string and numeric
functions are: ``f.trade_date().year()``, ``constant(datetime.date(2024, 1, 31)).add_months(1)``.

The class they are mixed into provides ``_function_input()``, what the function is applied to, and ``_function_label()``,
the name its result is labelled with."""
import datetime
from typing import TYPE_CHECKING, Any

from model.relational import (
    DateArithmeticOperation, DateDiffOperation, DateExtractOperation, DatePart, DateTruncOperation,
    RelationalOperationElement,
)


def _diff_other(other):
    """The other side of a date difference: a value stays as it is, a column or expression becomes its operand."""
    return other._arithmetic_operand() if isinstance(other, RelationalOperationElement) else other


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
        return DateTruncOperation(self._function_input(), DatePart.WEEK, 'First Day Of Week ' + self._function_label())

    def first_day_of_month(self) -> DateTruncOperation:
        """The first day of the month the date falls in."""
        return DateTruncOperation(self._function_input(), DatePart.MONTH, 'First Day Of Month ' + self._function_label())

    def first_day_of_quarter(self) -> DateTruncOperation:
        """The first day of the quarter the date falls in."""
        return DateTruncOperation(self._function_input(), DatePart.QUARTER, 'First Day Of Quarter ' + self._function_label())

    def first_day_of_year(self) -> DateTruncOperation:
        """The first day of the year the date falls in."""
        return DateTruncOperation(self._function_input(), DatePart.YEAR, 'First Day Of Year ' + self._function_label())

    def year(self) -> DateExtractOperation:
        """Extract the year. Equivalent to ``d.year``."""
        return DateExtractOperation(self._function_input(), DatePart.YEAR, 'Year ' + self._function_label())

    def month(self) -> DateExtractOperation:
        """Extract the month (1–12). Equivalent to ``d.month``."""
        return DateExtractOperation(self._function_input(), DatePart.MONTH, 'Month ' + self._function_label())

    def day(self) -> DateExtractOperation:
        """Extract the day of the month (1–31). Equivalent to ``d.day``."""
        return DateExtractOperation(self._function_input(), DatePart.DAY, 'Day ' + self._function_label())

    def quarter(self) -> DateExtractOperation:
        """Extract the quarter (1–4). Equivalent to ``(d.month - 1) // 3 + 1``."""
        return DateExtractOperation(self._function_input(), DatePart.QUARTER, 'Quarter ' + self._function_label())

    def week(self) -> DateExtractOperation:
        """Extract the ISO week number. Equivalent to ``d.isocalendar().week``."""
        return DateExtractOperation(self._function_input(), DatePart.WEEK, 'Week ' + self._function_label())

    def day_of_week(self) -> DateExtractOperation:
        """Extract the day of the week (0=Sunday in SQL). See ``d.weekday()`` for Python's Monday-based equivalent."""
        return DateExtractOperation(self._function_input(), DatePart.DOW, 'Day Of Week ' + self._function_label())

    def add_days(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Add ``n`` days. Equivalent to ``d + timedelta(days=n)``."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.DAY, True, 'Add Days ' + self._function_label())

    def add_months(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Add ``n`` calendar months."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.MONTH, True, 'Add Months ' + self._function_label())

    def add_years(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Add ``n`` calendar years."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.YEAR, True, 'Add Years ' + self._function_label())

    def subtract_days(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Subtract ``n`` days. Equivalent to ``d - timedelta(days=n)``."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.DAY, False, 'Subtract Days ' + self._function_label())

    def subtract_months(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Subtract ``n`` calendar months."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.MONTH, False, 'Subtract Months ' + self._function_label())

    def subtract_years(self, n: int) -> 'DateArithmeticOperation[datetime.date]':
        """Subtract ``n`` calendar years."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.YEAR, False, 'Subtract Years ' + self._function_label())

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
        return DateDiffOperation(self._function_input(), _diff_other(other), DatePart.DAY, 'Diff Days ' + self._function_label())

    def diff_months(self, other: datetime.date | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole calendar months between the column value and ``other``."""
        return DateDiffOperation(self._function_input(), _diff_other(other), DatePart.MONTH, 'Diff Months ' + self._function_label())

    def diff_years(self, other: datetime.date | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole calendar years between the column value and ``other``."""
        return DateDiffOperation(self._function_input(), _diff_other(other), DatePart.YEAR, 'Diff Years ' + self._function_label())


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
        return DateTruncOperation(self._function_input(), DatePart.WEEK, 'First Day Of Week ' + self._function_label())

    def first_day_of_month(self) -> DateTruncOperation:
        """The date of the first day of the month the date falls in."""
        return DateTruncOperation(self._function_input(), DatePart.MONTH, 'First Day Of Month ' + self._function_label())

    def first_day_of_quarter(self) -> DateTruncOperation:
        """The date of the first day of the quarter the date falls in."""
        return DateTruncOperation(self._function_input(), DatePart.QUARTER, 'First Day Of Quarter ' + self._function_label())

    def first_day_of_year(self) -> DateTruncOperation:
        """The date of the first day of the year the date falls in."""
        return DateTruncOperation(self._function_input(), DatePart.YEAR, 'First Day Of Year ' + self._function_label())

    def year(self) -> DateExtractOperation:
        """Extract the year. Equivalent to ``dt.year``."""
        return DateExtractOperation(self._function_input(), DatePart.YEAR, 'Year ' + self._function_label())

    def month(self) -> DateExtractOperation:
        """Extract the month (1–12). Equivalent to ``dt.month``."""
        return DateExtractOperation(self._function_input(), DatePart.MONTH, 'Month ' + self._function_label())

    def day(self) -> DateExtractOperation:
        """Extract the day of the month (1–31). Equivalent to ``dt.day``."""
        return DateExtractOperation(self._function_input(), DatePart.DAY, 'Day ' + self._function_label())

    def hour(self) -> DateExtractOperation:
        """Extract the hour (0–23). Equivalent to ``dt.hour``."""
        return DateExtractOperation(self._function_input(), DatePart.HOUR, 'Hour ' + self._function_label())

    def minute(self) -> DateExtractOperation:
        """Extract the minute (0–59). Equivalent to ``dt.minute``."""
        return DateExtractOperation(self._function_input(), DatePart.MINUTE, 'Minute ' + self._function_label())

    def second(self) -> DateExtractOperation:
        """Extract the second (0–59). Equivalent to ``dt.second``."""
        return DateExtractOperation(self._function_input(), DatePart.SECOND, 'Second ' + self._function_label())

    def quarter(self) -> DateExtractOperation:
        """Extract the quarter (1–4). Equivalent to ``(dt.month - 1) // 3 + 1``."""
        return DateExtractOperation(self._function_input(), DatePart.QUARTER, 'Quarter ' + self._function_label())

    def week(self) -> DateExtractOperation:
        """Extract the ISO week number. Equivalent to ``dt.isocalendar().week``."""
        return DateExtractOperation(self._function_input(), DatePart.WEEK, 'Week ' + self._function_label())

    def day_of_week(self) -> DateExtractOperation:
        """Extract the day of the week (0=Sunday in SQL). See ``dt.weekday()`` for Python's Monday-based equivalent."""
        return DateExtractOperation(self._function_input(), DatePart.DOW, 'Day Of Week ' + self._function_label())

    def add_days(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` days. Equivalent to ``dt + timedelta(days=n)``."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.DAY, True, 'Add Days ' + self._function_label())

    def add_months(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` calendar months."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.MONTH, True, 'Add Months ' + self._function_label())

    def add_years(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` calendar years."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.YEAR, True, 'Add Years ' + self._function_label())

    def add_hours(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` hours. Equivalent to ``dt + timedelta(hours=n)``."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.HOUR, True, 'Add Hours ' + self._function_label())

    def add_minutes(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` minutes. Equivalent to ``dt + timedelta(minutes=n)``."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.MINUTE, True, 'Add Minutes ' + self._function_label())

    def add_seconds(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Add ``n`` seconds. Equivalent to ``dt + timedelta(seconds=n)``."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.SECOND, True, 'Add Seconds ' + self._function_label())

    def subtract_days(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` days. Equivalent to ``dt - timedelta(days=n)``."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.DAY, False, 'Subtract Days ' + self._function_label())

    def subtract_months(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` calendar months."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.MONTH, False, 'Subtract Months ' + self._function_label())

    def subtract_years(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` calendar years."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.YEAR, False, 'Subtract Years ' + self._function_label())

    def subtract_hours(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` hours. Equivalent to ``dt - timedelta(hours=n)``."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.HOUR, False, 'Subtract Hours ' + self._function_label())

    def subtract_minutes(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` minutes. Equivalent to ``dt - timedelta(minutes=n)``."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.MINUTE, False, 'Subtract Minutes ' + self._function_label())

    def subtract_seconds(self, n: int) -> 'DateArithmeticOperation[datetime.datetime]':
        """Subtract ``n`` seconds. Equivalent to ``dt - timedelta(seconds=n)``."""
        return DateArithmeticOperation(self._function_input(), n, DatePart.SECOND, False, 'Subtract Seconds ' + self._function_label())

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
        return DateDiffOperation(self._function_input(), _diff_other(other), DatePart.DAY, 'Diff Days ' + self._function_label())

    def diff_months(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole calendar months between the column value and ``other``."""
        return DateDiffOperation(self._function_input(), _diff_other(other), DatePart.MONTH, 'Diff Months ' + self._function_label())

    def diff_years(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole calendar years between the column value and ``other``."""
        return DateDiffOperation(self._function_input(), _diff_other(other), DatePart.YEAR, 'Diff Years ' + self._function_label())

    def diff_hours(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole hours between the column value and ``other``. Equivalent to ``int((other - dt).total_seconds() // 3600)``."""
        return DateDiffOperation(self._function_input(), _diff_other(other), DatePart.HOUR, 'Diff Hours ' + self._function_label())

    def diff_minutes(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole minutes between the column value and ``other``. Equivalent to ``int((other - dt).total_seconds() // 60)``."""
        return DateDiffOperation(self._function_input(), _diff_other(other), DatePart.MINUTE, 'Diff Minutes ' + self._function_label())

    def diff_seconds(self, other: datetime.datetime | RelationalOperationElement) -> DateDiffOperation:
        """Return the number of whole seconds between the column value and ``other``. Equivalent to ``int((other - dt).total_seconds())``."""
        return DateDiffOperation(self._function_input(), _diff_other(other), DatePart.SECOND, 'Diff Seconds ' + self._function_label())
