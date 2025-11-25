from .operation import *
from .typed_operations import *


class StringAttribute(Attribute):

    def __init__(self, name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(name, column_db_type, owner, parent)

    def eq(self, value: str) -> Operation:
        return StringEqOperation(self, value)

    def __eq__(self, value: str) -> Operation:
        return StringEqOperation(self, value)


class FloatAttribute(Attribute):

    def __init__(self, name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(name, column_db_type, owner, parent)

    def eq(self, value: float) -> Operation:
        return PrimitiveEqOperation(self, value)

    def __eq__(self, value: float) -> Operation:
        return PrimitiveEqOperation(self, value)

    def __gt__(self, value: float) -> Operation:
        return PrimitiveGreaterThanOperation(self, value)

    def __lt__(self, value: float):
        return PrimitiveLessThanOperation(self, value)


class IntegerAttribute(Attribute):

    def __init__(self, name: str, column_db_type: str, owner:str, parent=None):
        super().__init__(name, column_db_type, owner, parent)

    def eq(self, value: int) -> Operation:
        return PrimitiveEqOperation(self, value)

    def __eq__(self, value: int) -> Operation:
        return PrimitiveEqOperation(self, value)

    def __gt__(self, value: int) -> Operation:
        return PrimitiveGreaterThanOperation(self, value)

    def __lt__(self, value: int):
        return PrimitiveLessThanOperation(self, value)


class DateAttribute(Attribute):
    """
    Represents a date attribute without a time YYYY-MM-DD
    """

    def __init__(self, name: str, column_db_type: str, owner: str, parent=None):
        super().__init__(name, column_db_type, owner, parent)

    def eq(self, value: datetime.date) -> Operation:
        return PrimitiveEqOperation(self, value)

    def __eq__(self, value: datetime.date) -> Operation:
        return PrimitiveEqOperation(self, value)

    def __gt__(self, value: datetime.date) -> Operation:
        return PrimitiveGreaterThanOperation(self, value)

    def __lt__(self, value: datetime.date):
        return PrimitiveLessThanOperation(self, value)


class DateTimeAttribute(Attribute):

    def __init__(self, name: str, column_db_type: str, owner: str, parent=None):
        super().__init__(name, column_db_type, owner, parent)

    def eq(self, value: datetime.datetime) -> Operation:
        return DateTimeEqOperation(self, value)

    def __eq__(self, value: datetime.datetime) -> Operation:
        return DateTimeEqOperation(self, value)

    def __gt__(self, value: datetime.datetime) -> Operation:
        return DateTimeGreaterThanOperation(self, value)

    def __lt__(self, value: datetime.datetime):
        return DateTimeLessThanOperation(self, value)

    def __ge__(self, value: datetime.datetime) -> Operation:
        return DateTimeGreaterThanOrEqualToOperation(self, value)

    def __le__(self, value: datetime.datetime):
        return DateTimeLessThanOrEqualToOperation(self, value)
