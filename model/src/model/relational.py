import datetime
from enum import Enum


# Interface
class RelationalOperationElement:
    def __init__(self):
        pass

class Operation(RelationalOperationElement):
    def __init__(self):
        super().__init__()

    def and_op(self, other) -> RelationalOperationElement:
        return LogicalOperation(self, LogicalOperator.AND, other)

class NoOperation(RelationalOperationElement):
    def __init__(self):
        super().__init__()

class ConstantOperation(RelationalOperationElement):
    def __init__(self):
        super().__init__()

class IntegerConstantOperation(ConstantOperation):
    value:int

    def __init__(self, value:int):
        super().__init__()
        self.value = value

class FloatConstantOperation(ConstantOperation):
    value:float

    def __init__(self, value:float):
        super().__init__()
        self.value = value

class StringConstantOperation(ConstantOperation):
    value:str

    def __init__(self, value:str):
        super().__init__()
        self.value = value

class DateConstantOperation(ConstantOperation):
    value:datetime.date

    def __init__(self, value:datetime.date):
        super().__init__()
        self.value = value


class DateTimeConstantOperation(ConstantOperation):
    value:datetime.datetime

    def __init__(self, value:datetime.datetime):
        super().__init__()
        self.value = value


class BinaryOperation(Operation):
    left: RelationalOperationElement
    right: RelationalOperationElement

    def __init__(self, left: RelationalOperationElement, right: RelationalOperationElement):
        super().__init__()
        self.left = left
        self.right = right


class BooleanOperation:
    def __init__(self):
        pass


class ComparisonOperator(Enum):
    EQUAL = 1
    NOT_EQUAL = 2
    LESS_THAN = 3
    GREATER_THAN = 4
    LESS_THAN_OR_EQUAL_TO =5
    GREATER_THAN_OR_EQUAL_TO = 6


class ComparisonOperation(BinaryOperation, BooleanOperation):
    operator: ComparisonOperator

    def __init__(self, left: RelationalOperationElement, op: ComparisonOperator, right: RelationalOperationElement):
        super().__init__(left, right)
        self.operator = op


class LogicalOperator(Enum):
    AND = 1
    OR = 2


class LogicalOperation(BinaryOperation, BooleanOperation):
    operator: LogicalOperator

    def __init__(self, left: RelationalOperationElement, op: LogicalOperator, right: RelationalOperationElement):
        super().__init__(left, right)
        self.operator = op

class Relation:
    def __init__(self):
        pass

class Column(RelationalOperationElement):
    #TODO owner should be Relation
    def __init__(self, name: str, _type: str, owner:str = None):
        super().__init__()
        self.name = name
        self.type = _type
        self.owner = owner


class Table(Relation):
    def __init__(self, name: str, columns: list[Column]):
        super().__init__()
        self.name = name
        self.columns = columns
        for col in columns:
            col.table = self


class Join(RelationalOperationElement):
    def __init__(self, lhs: Column, rhs: Column):
        super().__init__()
        self.lhs = lhs
        self.rhs = rhs






