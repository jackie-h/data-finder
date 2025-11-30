import datetime

from datafinder import Attribute
from model.relational import ComparisonOperation, StringConstantOperation, Operation, ComparisonOperator, \
    FloatConstantOperation, IntegerConstantOperation, DateConstantOperation, DateTimeConstantOperation


class StringAttribute(Attribute):

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def eq(self, value: str) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.EQUAL, StringConstantOperation(value))

    def __eq__(self, value: str) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.EQUAL, StringConstantOperation(value))


class FloatAttribute(Attribute):

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def eq(self, value: float) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.EQUAL, FloatConstantOperation(value))

    def __eq__(self, value: float) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.EQUAL, FloatConstantOperation(value))

    def __gt__(self, value: float) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.GREATER_THAN, FloatConstantOperation(value))

    def __lt__(self, value: float):
        return ComparisonOperation(self.column(), ComparisonOperator.LESS_THAN, FloatConstantOperation(value))

    def __ge__(self, value: float) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, FloatConstantOperation(value))

    def __le__(self, value: float):
        return ComparisonOperation(self.column(), ComparisonOperator.LESS_THAN_OR_EQUAL_TO, FloatConstantOperation(value))



class IntegerAttribute(Attribute):

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def eq(self, value: int) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.EQUAL, IntegerConstantOperation(value))

    def __eq__(self, value: int) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.EQUAL, IntegerConstantOperation(value))

    def __gt__(self, value: int) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.GREATER_THAN, IntegerConstantOperation(value))

    def __lt__(self, value: int):
        return ComparisonOperation(self.column(), ComparisonOperator.LESS_THAN, IntegerConstantOperation(value))

    def __ge__(self, value: int) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.GREATER_THAN_OR_EQUAL_TO,
                                   IntegerConstantOperation(value))

    def __le__(self, value: int):
        return ComparisonOperation(self.column(), ComparisonOperator.LESS_THAN_OR_EQUAL_TO,
                                   IntegerConstantOperation(value))


class DateAttribute(Attribute):
    """
    Represents a date attribute without a time YYYY-MM-DD
    """

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def eq(self, value: datetime.date) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.EQUAL, DateConstantOperation(value))

    def __eq__(self, value: datetime.date) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.EQUAL, DateConstantOperation(value))

    def __gt__(self, value: datetime.date) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.GREATER_THAN, DateConstantOperation(value))

    def __lt__(self, value: datetime.date):
        return ComparisonOperation(self.column(), ComparisonOperator.LESS_THAN, DateConstantOperation(value))

    def __ge__(self, value: datetime.date) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.GREATER_THAN_OR_EQUAL_TO,
                                   DateConstantOperation(value))

    def __le__(self, value: datetime.date):
        return ComparisonOperation(self.column(), ComparisonOperator.LESS_THAN_OR_EQUAL_TO,
                                   DateConstantOperation(value))


class DateTimeAttribute(Attribute):

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(display_name, column_name, column_db_type, owner, parent)

    def eq(self, value: datetime.datetime) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.EQUAL, DateTimeConstantOperation(value))

    def __eq__(self, value: datetime.datetime) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.EQUAL, DateTimeConstantOperation(value))

    def __gt__(self, value: datetime.datetime) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.GREATER_THAN, DateTimeConstantOperation(value))

    def __lt__(self, value: datetime.datetime):
        return ComparisonOperation(self.column(), ComparisonOperator.LESS_THAN, DateTimeConstantOperation(value))

    def __ge__(self, value: datetime.datetime) -> Operation:
        return ComparisonOperation(self.column(), ComparisonOperator.GREATER_THAN_OR_EQUAL_TO,
                                   DateTimeConstantOperation(value))

    def __le__(self, value: datetime.datetime):
        return ComparisonOperation(self.column(), ComparisonOperator.LESS_THAN_OR_EQUAL_TO,
                                   DateTimeConstantOperation(value))
