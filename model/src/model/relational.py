import datetime
import decimal
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

class DecimalConstantOperation(ConstantOperation):
    value: decimal.Decimal

    def __init__(self, value: decimal.Decimal):
        super().__init__()
        self.value = value

class BooleanConstantOperation(ConstantOperation):
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


class AggregateOperator(Enum):
    COUNT = 1
    SUM = 2
    MIN = 3
    MAX = 4
    AVERAGE = 5


class AggregateOperation(UnaryOperation):
    operator: AggregateOperator

    def __init__(self, element: RelationalOperationElement, operator: AggregateOperator, display_name: str = None):
        super().__init__(element)
        self.operator = operator
        self.display_name = display_name


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


class ScalarFunctionOperation(UnaryOperation):
    def __init__(self, element: RelationalOperationElement, function: ScalarFunction,
                 display_name: str = None, second_arg: int = None, extra_args: list = None):
        super().__init__(element)
        self.function = function
        self.display_name = display_name
        self.second_arg = second_arg
        self.extra_args = extra_args or []


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


class DateExtractOperation(UnaryOperation):
    def __init__(self, element: RelationalOperationElement, part: DatePart, display_name: str = None):
        super().__init__(element)
        self.part = part
        self.display_name = display_name


class DateArithmeticOperation(UnaryOperation):
    def __init__(self, element: RelationalOperationElement, n: int, unit: DatePart,
                 is_add: bool = True, display_name: str = None):
        super().__init__(element)
        self.n = n
        self.unit = unit
        self.is_add = is_add
        self.display_name = display_name


class DateDiffOperation(UnaryOperation):
    def __init__(self, element: RelationalOperationElement, other, unit: DatePart, display_name: str = None):
        super().__init__(element)
        self.other = other
        self.unit = unit
        self.display_name = display_name


class Relation:
    def __init__(self):
        pass


class MilestoningScheme:
    def __init__(self, name: str, processing_start: str = None, processing_end: str = None,
                 business_date: str = None, business_date_from: str = None, business_date_to: str = None):
        self.name = name
        self.processing_start = processing_start
        self.processing_end = processing_end
        self.business_date = business_date
        self.business_date_from = business_date_from
        self.business_date_to = business_date_to


class Repository:
    def __init__(self, name: str, location: str = None):
        self.name = name
        self.location = location
        self.schemas: list = []
        self.milestoning_schemes: list = []


class Schema:
    def __init__(self, name: str, repository: Repository = None):
        self.name = name
        self.repository = repository
        self.tables: list = []
        if repository is not None:
            repository.schemas.append(self)


class Column(RelationalOperationElement):
    #TODO owner should be Relation
    def __init__(self, name: str, _type: str, owner: str = None, primary_key: bool = False):
        super().__init__()
        self.name = name
        self.type = _type
        self.owner = owner
        self.primary_key = primary_key


class ForeignKey:
    def __init__(self, column: Column, references: Column):
        self.column = column
        self.references = references


class Table(Relation):
    def __init__(self, name: str, columns: list[Column], schema: Schema = None):
        super().__init__()
        self._columns_by_name: dict[str, Column] = {}
        for col in columns:
            if col.name in self._columns_by_name:
                raise ValueError(f"Duplicate column name '{col.name}' in table '{name}'")
            self._columns_by_name[col.name] = col
        self.name = name
        self.schema = schema
        self.foreign_keys: list[ForeignKey] = []
        for col in self._columns_by_name.values():
            col.table = self
        if schema is not None:
            schema.tables.append(self)

    @property
    def columns(self) -> list[Column]:
        return list(self._columns_by_name.values())


class JoinOperation:
    def __init__(self, name: str, target:Table, lhs:Column, rhs:Column, _filter:RelationalOperationElement = None):
        self.name = name
        self.target = target
        self.left = lhs
        self.right = rhs
        self.filter = _filter


class ColumnWithJoin(RelationalOperationElement):
    def __init__(self, column: Column, join: JoinOperation):
        super().__init__()
        self.column = column
        self.parent = join


class SortDirection(Enum):
    ASC = 1
    DESC = 2


class SortOperation:
    def __init__(self, column: ColumnWithJoin, direction: SortDirection):
        self.column = column
        self.direction = direction


class CountAllOperation(RelationalOperationElement):
    """Represents COUNT(*) — counts all rows without reference to a specific column."""
    def __init__(self, table: str):
        super().__init__()
        self.table = table






