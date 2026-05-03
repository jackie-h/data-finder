import datetime

from datafinder import Attribute
import decimal

from model.relational import ComparisonOperation, StringConstantOperation, Operation, ComparisonOperator, \
    FloatConstantOperation, IntegerConstantOperation, DateConstantOperation, DateTimeConstantOperation, \
    BooleanConstantOperation, DecimalConstantOperation, AggregateOperation, AggregateOperator, ColumnWithJoin, \
    ScalarFunction, ScalarFunctionOperation, DatePart, DateExtractOperation, DateArithmeticOperation, DateDiffOperation


class StringAttribute(Attribute):

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

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

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def sum(self):
        return AggregateOperation(ColumnWithJoin(self.column(), self.parent()), AggregateOperator.SUM, 'Sum ' + self.display_name())

    def min(self):
        return AggregateOperation(ColumnWithJoin(self.column(), self.parent()), AggregateOperator.MIN, 'Min ' + self.display_name())

    def max(self):
        return AggregateOperation(ColumnWithJoin(self.column(), self.parent()), AggregateOperator.MAX, 'Max ' + self.display_name())

    def average(self):
        return AggregateOperation(ColumnWithJoin(self.column(), self.parent()), AggregateOperator.AVERAGE, 'Average ' + self.display_name())

    def abs(self):
        return ScalarFunctionOperation(ColumnWithJoin(self.column(), self.parent()), ScalarFunction.ABS, 'Abs ' + self.display_name())

    def ceiling(self):
        return ScalarFunctionOperation(ColumnWithJoin(self.column(), self.parent()), ScalarFunction.CEILING, 'Ceiling ' + self.display_name())

    def floor(self):
        return ScalarFunctionOperation(ColumnWithJoin(self.column(), self.parent()), ScalarFunction.FLOOR, 'Floor ' + self.display_name())

    def sqrt(self):
        return ScalarFunctionOperation(ColumnWithJoin(self.column(), self.parent()), ScalarFunction.SQRT, 'Sqrt ' + self.display_name())

    def mod(self, n: int):
        return ScalarFunctionOperation(ColumnWithJoin(self.column(), self.parent()), ScalarFunction.MOD, 'Mod ' + self.display_name(), second_arg=n)

    def power(self, n: int):
        return ScalarFunctionOperation(ColumnWithJoin(self.column(), self.parent()), ScalarFunction.POWER, 'Power ' + self.display_name(), second_arg=n)

    def round(self, d: int = None):
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
    """
    Represents a date attribute without a time YYYY-MM-DD
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
        return DateExtractOperation(self._cwj(), DatePart.YEAR, 'Year ' + self.display_name())

    def month(self):
        return DateExtractOperation(self._cwj(), DatePart.MONTH, 'Month ' + self.display_name())

    def day(self):
        return DateExtractOperation(self._cwj(), DatePart.DAY, 'Day ' + self.display_name())

    def quarter(self):
        return DateExtractOperation(self._cwj(), DatePart.QUARTER, 'Quarter ' + self.display_name())

    def week(self):
        return DateExtractOperation(self._cwj(), DatePart.WEEK, 'Week ' + self.display_name())

    def day_of_week(self):
        return DateExtractOperation(self._cwj(), DatePart.DOW, 'Day Of Week ' + self.display_name())

    def add_days(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.DAY, True, 'Add Days ' + self.display_name())

    def add_months(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.MONTH, True, 'Add Months ' + self.display_name())

    def add_years(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.YEAR, True, 'Add Years ' + self.display_name())

    def subtract_days(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.DAY, False, 'Subtract Days ' + self.display_name())

    def subtract_months(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.MONTH, False, 'Subtract Months ' + self.display_name())

    def subtract_years(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.YEAR, False, 'Subtract Years ' + self.display_name())

    def diff_days(self, other: datetime.date):
        return DateDiffOperation(self._cwj(), other, DatePart.DAY, 'Diff Days ' + self.display_name())

    def diff_months(self, other: datetime.date):
        return DateDiffOperation(self._cwj(), other, DatePart.MONTH, 'Diff Months ' + self.display_name())

    def diff_years(self, other: datetime.date):
        return DateDiffOperation(self._cwj(), other, DatePart.YEAR, 'Diff Years ' + self.display_name())


class DateTimeAttribute(Attribute):

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
        return DateExtractOperation(self._cwj(), DatePart.YEAR, 'Year ' + self.display_name())

    def month(self):
        return DateExtractOperation(self._cwj(), DatePart.MONTH, 'Month ' + self.display_name())

    def day(self):
        return DateExtractOperation(self._cwj(), DatePart.DAY, 'Day ' + self.display_name())

    def hour(self):
        return DateExtractOperation(self._cwj(), DatePart.HOUR, 'Hour ' + self.display_name())

    def minute(self):
        return DateExtractOperation(self._cwj(), DatePart.MINUTE, 'Minute ' + self.display_name())

    def second(self):
        return DateExtractOperation(self._cwj(), DatePart.SECOND, 'Second ' + self.display_name())

    def quarter(self):
        return DateExtractOperation(self._cwj(), DatePart.QUARTER, 'Quarter ' + self.display_name())

    def week(self):
        return DateExtractOperation(self._cwj(), DatePart.WEEK, 'Week ' + self.display_name())

    def day_of_week(self):
        return DateExtractOperation(self._cwj(), DatePart.DOW, 'Day Of Week ' + self.display_name())

    def add_days(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.DAY, True, 'Add Days ' + self.display_name())

    def add_months(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.MONTH, True, 'Add Months ' + self.display_name())

    def add_years(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.YEAR, True, 'Add Years ' + self.display_name())

    def add_hours(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.HOUR, True, 'Add Hours ' + self.display_name())

    def add_minutes(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.MINUTE, True, 'Add Minutes ' + self.display_name())

    def add_seconds(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.SECOND, True, 'Add Seconds ' + self.display_name())

    def subtract_days(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.DAY, False, 'Subtract Days ' + self.display_name())

    def subtract_months(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.MONTH, False, 'Subtract Months ' + self.display_name())

    def subtract_years(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.YEAR, False, 'Subtract Years ' + self.display_name())

    def subtract_hours(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.HOUR, False, 'Subtract Hours ' + self.display_name())

    def subtract_minutes(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.MINUTE, False, 'Subtract Minutes ' + self.display_name())

    def subtract_seconds(self, n: int):
        return DateArithmeticOperation(self._cwj(), n, DatePart.SECOND, False, 'Subtract Seconds ' + self.display_name())

    def diff_days(self, other: datetime.datetime):
        return DateDiffOperation(self._cwj(), other, DatePart.DAY, 'Diff Days ' + self.display_name())

    def diff_months(self, other: datetime.datetime):
        return DateDiffOperation(self._cwj(), other, DatePart.MONTH, 'Diff Months ' + self.display_name())

    def diff_years(self, other: datetime.datetime):
        return DateDiffOperation(self._cwj(), other, DatePart.YEAR, 'Diff Years ' + self.display_name())

    def diff_hours(self, other: datetime.datetime):
        return DateDiffOperation(self._cwj(), other, DatePart.HOUR, 'Diff Hours ' + self.display_name())

    def diff_minutes(self, other: datetime.datetime):
        return DateDiffOperation(self._cwj(), other, DatePart.MINUTE, 'Diff Minutes ' + self.display_name())

    def diff_seconds(self, other: datetime.datetime):
        return DateDiffOperation(self._cwj(), other, DatePart.SECOND, 'Diff Seconds ' + self.display_name())
