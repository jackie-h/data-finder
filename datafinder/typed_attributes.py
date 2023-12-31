from .attribute import *
from .operation import *
from .typed_operations import *


class StringAttribute(Attribute):

    def __init__(self, name: str, column_db_type: str):
        super().__init__(name, column_db_type)

    def eq(self, value: str) -> Operation:
        return StringEqOperation(self, value)

    def __eq__(self, value: str) -> Operation:
        return StringEqOperation(self, value)


class FloatAttribute(Attribute):

    def __init__(self, name: str, column_db_type: str):
        super().__init__(name, column_db_type)

    def eq(self, value: float) -> Operation:
        return PrimitiveEqOperation(self, value)

    def __eq__(self, value: float) -> Operation:
        return PrimitiveEqOperation(self, value)

    def __gt__(self, value: float) -> Operation:
        return PrimitiveGreaterThanOperation(self, value)
