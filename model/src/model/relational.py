import datetime
import decimal
from enum import Enum
from typing import Sequence


# Interface
class RelationalOperationElement:
    def __init__(self):
        pass

    def _arithmetic_operand(self) -> 'RelationalOperationElement':
        """This expression as an operand of arithmetic; an attribute reads its column."""
        return self


class Aliasable:
    """Something that can be a column of the result, so can be given a name there: ``f.price().sum().alias("Total")``.
    As in Pure, the name is held by an ``Alias`` that wraps the column."""

    def alias(self, name: str) -> 'Alias':
        return Alias(self, name)  # type: ignore[arg-type]


class Operation(RelationalOperationElement):
    def __init__(self):
        super().__init__()

    def and_(self, other) -> 'LogicalOperation':
        return LogicalOperation(self, LogicalOperator.AND, other)

    def or_(self, other) -> 'LogicalOperation':
        return LogicalOperation(self, LogicalOperator.OR, other)

class NoOperation(Operation):
    def __init__(self):
        super().__init__()

class ConstantOperation(RelationalOperationElement, Aliasable):
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


class IsNullOperation(UnaryOperation):
    pass


class IsNotNullOperation(UnaryOperation):
    pass


class InOperation(UnaryOperation):
    """The element is one of ``values`` (``element IN (...)``)."""
    values: list[ConstantOperation]

    def __init__(self, element: RelationalOperationElement, values: list[ConstantOperation]):
        super().__init__(element)
        self.values = values


class NotInOperation(UnaryOperation):
    """The element is none of ``values`` (``element NOT IN (...)``)."""
    values: list[ConstantOperation]

    def __init__(self, element: RelationalOperationElement, values: list[ConstantOperation]):
        super().__init__(element)
        self.values = values


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


# The character that makes the next one literal in a LIKE pattern built from a value (contains, starts_with, ...).
LIKE_ESCAPE = '!'


def escape_like(value: str) -> str:
    """``value`` as part of a LIKE pattern that matches it literally: %, _ and the escape character itself would
    otherwise be wildcards."""
    return value.replace(LIKE_ESCAPE, LIKE_ESCAPE * 2).replace('%', LIKE_ESCAPE + '%').replace('_', LIKE_ESCAPE + '_')


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


def _value_operand(value) -> 'ConstantOperation':
    """A Python value as the constant it compares with."""
    if isinstance(value, bool):  # before int: bool is a subclass of int
        return BooleanConstantOperation(value)
    if isinstance(value, int):
        return IntegerConstantOperation(value)
    if isinstance(value, float):
        return FloatConstantOperation(value)
    if isinstance(value, decimal.Decimal):
        return DecimalConstantOperation(value)
    if isinstance(value, str):
        return StringConstantOperation(value)
    if isinstance(value, datetime.datetime):  # before date: datetime is a subclass of date
        return DateTimeConstantOperation(value)
    if isinstance(value, datetime.date):
        return DateConstantOperation(value)
    raise TypeError(f"A value to compare with is a bool, number, string, date or datetime, not {type(value).__name__}")


class Comparable:
    """Comparisons for an expression, as for a column: ``f.name().upper().eq("ACME")``, ``f.price() * 2 > 10``,
    ``f.first() + f.last() == "AnnLee"``. The other side is a value or another expression. Each gives a filter.
    ``==`` and ``!=`` build a comparison too, as they do on attributes."""

    def _compared(self, operator: 'ComparisonOperator', other) -> 'ComparisonOperation':
        right = other._arithmetic_operand() if isinstance(other, RelationalOperationElement) else _value_operand(other)
        return ComparisonOperation(self._arithmetic_operand(), operator, right)  # type: ignore[attr-defined]

    def eq(self, other) -> 'ComparisonOperation':
        return self._compared(ComparisonOperator.EQUAL, other)

    def ne(self, other) -> 'ComparisonOperation':
        return self._compared(ComparisonOperator.NOT_EQUAL, other)

    def __eq__(self, other) -> 'ComparisonOperation':  # type: ignore[override]
        return self.eq(other)

    def __ne__(self, other) -> 'ComparisonOperation':  # type: ignore[override]
        return self.ne(other)

    __hash__ = object.__hash__  # defining __eq__ would otherwise make an expression unhashable

    def __lt__(self, other) -> 'ComparisonOperation':
        return self._compared(ComparisonOperator.LESS_THAN, other)

    def __le__(self, other) -> 'ComparisonOperation':
        return self._compared(ComparisonOperator.LESS_THAN_OR_EQUAL_TO, other)

    def __gt__(self, other) -> 'ComparisonOperation':
        return self._compared(ComparisonOperator.GREATER_THAN, other)

    def __ge__(self, other) -> 'ComparisonOperation':
        return self._compared(ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, other)


class ArithmeticOperator(Enum):
    ADD = '+'
    SUBTRACT = '-'
    MULTIPLY = '*'
    DIVIDE = '/'


def _constant_operand(value) -> 'ConstantOperation':
    if isinstance(value, bool) or not isinstance(value, (int, float, decimal.Decimal)):
        raise TypeError(f"Arithmetic is between numbers and numeric columns, not {type(value).__name__}")
    if isinstance(value, int):
        return IntegerConstantOperation(value)
    if isinstance(value, float):
        return FloatConstantOperation(value)
    return DecimalConstantOperation(value)


class Arithmetic:
    """The operators ``+ - * /`` for an expression that holds a number: ``a + b``, ``a * 2``, ``1 - a``. The other
    operand is a number or another numeric expression."""

    def _numeric(self) -> None:
        """Raise if this expression is not a number."""

    def _arithmetic(self, operator: 'ArithmeticOperator', other, reflected: bool) -> 'ArithmeticOperation':
        self._numeric()
        if isinstance(other, RelationalOperationElement):
            if isinstance(other, Arithmetic):
                other._numeric()
            operand = other._arithmetic_operand()
        else:
            operand = _constant_operand(other)
        me = self._arithmetic_operand()  # type: ignore[attr-defined]
        left, right = (operand, me) if reflected else (me, operand)
        return ArithmeticOperation(left, operator, right)

    def __add__(self, other) -> 'ArithmeticOperation':
        return self._arithmetic(ArithmeticOperator.ADD, other, False)

    def __radd__(self, other) -> 'ArithmeticOperation':
        return self._arithmetic(ArithmeticOperator.ADD, other, True)

    def __sub__(self, other) -> 'ArithmeticOperation':
        return self._arithmetic(ArithmeticOperator.SUBTRACT, other, False)

    def __rsub__(self, other) -> 'ArithmeticOperation':
        return self._arithmetic(ArithmeticOperator.SUBTRACT, other, True)

    def __mul__(self, other) -> 'ArithmeticOperation':
        return self._arithmetic(ArithmeticOperator.MULTIPLY, other, False)

    def __rmul__(self, other) -> 'ArithmeticOperation':
        return self._arithmetic(ArithmeticOperator.MULTIPLY, other, True)

    def __truediv__(self, other) -> 'ArithmeticOperation':
        return self._arithmetic(ArithmeticOperator.DIVIDE, other, False)

    def __rtruediv__(self, other) -> 'ArithmeticOperation':
        return self._arithmetic(ArithmeticOperator.DIVIDE, other, True)


class ArithmeticOperation(BinaryOperation, Arithmetic, Comparable, Aliasable):
    """``left operator right`` on numbers, e.g. ``price * quantity``."""
    operator: ArithmeticOperator

    def __init__(self, left: RelationalOperationElement, operator: ArithmeticOperator,
                 right: RelationalOperationElement, display_name: str | None = None):
        super().__init__(left, right)
        self.operator = operator
        self.display_name = display_name


class ConcatOperation(Operation, Comparable, Aliasable):
    """Text joined end to end: ``first + " " + last``. NULL in any part makes the whole NULL, as with SQL's ``||``."""
    parts: list[RelationalOperationElement]

    def __init__(self, parts: list[RelationalOperationElement], display_name: str | None = None):
        super().__init__()
        self.parts = parts
        self.display_name = display_name

    def __add__(self, other) -> 'ConcatOperation':
        return concat(self, other)

    def __radd__(self, other) -> 'ConcatOperation':
        return concat(other, self)


def concat(left, right) -> ConcatOperation:
    """``left + right`` for text. A string is a constant; a concatenation on either side is flattened into one."""
    def parts_of(value) -> list[RelationalOperationElement]:
        if isinstance(value, ConcatOperation):
            return value.parts
        if isinstance(value, str):
            return [StringConstantOperation(value)]
        if isinstance(value, RelationalOperationElement):
            return [value._arithmetic_operand()]
        raise TypeError(f"Text is joined with text, not {type(value).__name__}")
    return ConcatOperation([*parts_of(left), *parts_of(right)])


class AggregateOperator(Enum):
    COUNT = 1
    SUM = 2
    MIN = 3
    MAX = 4
    AVERAGE = 5
    STRING_AGG = 6
    COUNT_DISTINCT = 7


class WindowSpecification:
    def __init__(self, partition_by=None, order_by=None):
        self.partition_by = partition_by or []
        self.order_by = order_by or []


class AggregateOperation(UnaryOperation, Arithmetic, Aliasable):
    operator: AggregateOperator

    def __init__(self, element: RelationalOperationElement, operator: AggregateOperator, display_name: str | None = None,
                 window=None, separator: str | None = None, order_by: list | None = None):
        super().__init__(element)
        self.operator = operator
        self.display_name = display_name
        self.window = window
        # for STRING_AGG: what goes between the values, and the order they are joined in
        self.separator = separator
        self.order_by = order_by or []

    def over(self, partition_by=None, order_by=None):
        return AggregateOperation(self.element, self.operator, self.display_name,
                                  WindowSpecification(partition_by, order_by), self.separator, self.order_by)


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
    COALESCE = 20


class WindowFunction(Enum):
    ROW_NUMBER = 1
    RANK = 2
    DENSE_RANK = 3
    NTILE = 4
    LAG = 5
    LEAD = 6
    FIRST_VALUE = 7
    LAST_VALUE = 8
    CUME_DIST = 9
    PERCENT_RANK = 10


class WindowFunctionOperation(Operation, Aliasable):
    element: RelationalOperationElement | None

    def __init__(self, element: RelationalOperationElement | None, function: WindowFunction,
                 display_name: str | None = None, second_arg: int | None = None, extra_args: list | None = None, window=None):
        super().__init__()
        self.element = element
        self.function = function
        self.display_name = display_name
        self.second_arg = second_arg
        self.extra_args = extra_args or []
        self.window = window

    def over(self, partition_by=None, order_by=None):
        return WindowFunctionOperation(self.element, self.function, self.display_name,
                                       second_arg=self.second_arg, extra_args=list(self.extra_args),
                                       window=WindowSpecification(partition_by, order_by))


_NUMERIC_FUNCTIONS = {ScalarFunction.ABS, ScalarFunction.CEILING, ScalarFunction.FLOOR, ScalarFunction.MOD,
                      ScalarFunction.POWER, ScalarFunction.SQRT, ScalarFunction.ROUND, ScalarFunction.LENGTH}


class ScalarFunctionOperation(UnaryOperation, Arithmetic, Comparable, Aliasable):
    def _numeric(self) -> None:
        if self.function not in _NUMERIC_FUNCTIONS:
            raise TypeError(f"{self.function.name} does not give a number, so it cannot be used in arithmetic")

    def __add__(self, other):  # type: ignore[override]
        # a function that gives text joins text; one that gives a number adds
        return Arithmetic.__add__(self, other) if self.function in _NUMERIC_FUNCTIONS else concat(self, other)

    def __radd__(self, other):  # type: ignore[override]
        return Arithmetic.__radd__(self, other) if self.function in _NUMERIC_FUNCTIONS else concat(other, self)

    def __init__(self, element: RelationalOperationElement, function: ScalarFunction,
                 display_name: str | None = None, second_arg: int | None = None, extra_args: list | None = None):
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


class DateExtractOperation(UnaryOperation, Aliasable):
    def __init__(self, element: RelationalOperationElement, part: DatePart, display_name: str | None = None):
        super().__init__(element)
        self.part = part
        self.display_name = display_name


class DateTruncOperation(UnaryOperation, Comparable, Aliasable):
    """The first day of the week, month, quarter or year that the date falls in, as a date."""

    def __init__(self, element: RelationalOperationElement, part: DatePart, display_name: str | None = None):
        super().__init__(element)
        self.part = part
        self.display_name = display_name


class DateArithmeticOperation(UnaryOperation, Aliasable):
    def __init__(self, element: RelationalOperationElement, n: int, unit: DatePart,
                 is_add: bool = True, display_name: str | None = None):
        super().__init__(element)
        self.n = n
        self.unit = unit
        self.is_add = is_add
        self.display_name = display_name


class DateDiffOperation(UnaryOperation, Aliasable):
    def __init__(self, element: RelationalOperationElement, other, unit: DatePart, display_name: str | None = None):
        super().__init__(element)
        self.other = other
        self.unit = unit
        self.display_name = display_name


class Relation:
    def __init__(self):
        pass


class MilestoningScheme:
    def __init__(self, name: str, processing_start: str | None = None, processing_end: str | None = None,
                 business_date: str | None = None, business_date_from: str | None = None,
                 business_date_to: str | None = None, infinite_datetime: str | None = None):
        self.name = name
        self.processing_start = processing_start
        self.processing_end = processing_end
        self.business_date = business_date
        self.business_date_from = business_date_from
        self.business_date_to = business_date_to
        self.infinite_datetime = infinite_datetime


from abc import ABC, abstractmethod


class DataStore(ABC):
    def __init__(self, name: str):
        self.name = name
        self.schemas: list = []
        self.milestoning_schemes: list = []

    @abstractmethod
    def namespace(self, schema_name: str) -> str:
        pass


class Database(DataStore):
    def __init__(self, name: str, location: str | None = None):
        super().__init__(name)
        self.location = location

    def namespace(self, schema_name: str) -> str:
        return schema_name


class DataCatalog(DataStore):
    def __init__(self, name: str):
        super().__init__(name)

    def namespace(self, schema_name: str) -> str:
        return f"{self.name}.{schema_name}"


class Schema:
    def __init__(self, name: str, datastore: DataStore | None = None):
        self.name = name
        self.datastore = datastore
        self.tables: list = []
        if datastore is not None:
            datastore.schemas.append(self)


class Column(RelationalOperationElement):
    table: 'Table | None'

    #TODO owner should be Relation
    def __init__(self, name: str, _type: str, owner: str | None = None, primary_key: bool = False,
                 nullable: bool = True):
        self.table = None
        super().__init__()
        self.name = name
        self.type = _type
        self.owner = owner
        self.primary_key = primary_key
        self.nullable = nullable and not primary_key  # as in Legend, a primary key is never null

    def qualified_name(self) -> str:
        assert self.table is not None, f"Column {self.name} has no table"
        return self.table.qualified_name


class ForeignKey:
    def __init__(self, column: Column, references: Column):
        self.column = column
        self.references = references


class Table(Relation):
    schema: 'Schema | None'
    foreign_keys: 'list[ForeignKey]'

    def __init__(self, name: str, columns: list[Column], schema: 'Schema | None' = None):
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

    @property
    def qualified_name(self) -> str:
        if self.schema is None:
            return self.name
        if self.schema.datastore is None:
            return f"{self.schema.name}.{self.name}"
        return f"{self.schema.datastore.namespace(self.schema.name)}.{self.name}"


class JoinOperation:
    def __init__(self, name: str, target:Table, lhs:Column, rhs:Column, _filter:RelationalOperationElement | None = None):
        self.name = name
        self.target = target
        self.left = lhs
        self.right = rhs
        self.filter = _filter
        self.filter_kwargs: dict = {}
        self.filter_column_map: dict = {}


class JoinTreeNodeOperation:
    """Wraps a JoinOperation and links it to its parent node in the join tree.

    Enables multi-hop traversal (e.g. Employee.manager.reports) by ensuring
    intermediate joins are emitted before the joins that depend on them.
    """
    def __init__(self, join: JoinOperation, parent: 'JoinTreeNodeOperation | None' = None, many: bool = False):
        self.join = join
        self.parent = parent
        # True when the property this step follows is to-many, so a value read through it repeats the row. As in
        # Pure, that is the multiplicity of the property, not something the join itself says.
        self.many = many


class ColumnWithJoin(RelationalOperationElement):
    def __init__(self, column: Column, join: 'JoinTreeNodeOperation | None', embedded: 'ColumnWithJoin | None' = None):
        super().__init__()
        self.column = column
        self.parent = join
        self.embedded = embedded


class SortDirection(Enum):
    ASC = 1
    DESC = 2


class SortOperation(RelationalOperationElement):
    def __init__(self, column: ColumnWithJoin, direction: SortDirection):
        super().__init__()
        self.column = column
        self.direction = direction


class CountAllOperation(RelationalOperationElement, Aliasable):
    """Represents COUNT(*) — counts all rows without reference to a specific column."""
    def __init__(self, table: str):
        super().__init__()
        self.table = table


class Alias(RelationalOperationElement, Aliasable):
    def __init__(self, element: RelationalOperationElement, name: str):
        super().__init__()
        self.element = element
        self.name = name

    def alias(self, name: str) -> 'Alias':
        return Alias(self.element, name)


class ExistsOperation(RelationalOperationElement):
    """Returned by RelatedFinder.exists(). Backend translates this to a join-presence check."""

    def __init__(self, node):
        super().__init__()
        self.node = node

    def and_(self, other) -> 'LogicalOperation':
        return LogicalOperation(self, LogicalOperator.AND, other)

    def or_(self, other) -> 'LogicalOperation':
        return LogicalOperation(self, LogicalOperator.OR, other)


class NotExistsOperation(RelationalOperationElement):
    """Returned by RelatedFinder.not_exists(). Backend translates this to a join-absence check."""

    def __init__(self, node):
        super().__init__()
        self.node = node

    def and_(self, other) -> 'LogicalOperation':
        return LogicalOperation(self, LogicalOperator.AND, other)

    def or_(self, other) -> 'LogicalOperation':
        return LogicalOperation(self, LogicalOperator.OR, other)


class QueryExpression:
    """A query expression over a root table: what to show, which rows qualify and how they are grouped.
    This is the part of a SELECT that ANSI SQL also allows in a view definition.

    ``order_by`` and ``limit`` exist so a query can be rendered uniformly, but they are always empty here:
    a view definition has no ORDER BY or row limit. A SelectOperation, which is run rather than stored, can set them.
    """
    order_by: list[SortOperation]
    limit: int | None

    def __init__(self, display: Sequence[RelationalOperationElement], filter: Operation,
                 group_by: list | None = None, table: 'Table | None' = None):
        self.display = display
        self.filter = filter
        self.group_by = group_by or []
        self.table = table
        self.order_by = []
        self.limit = None
        # ON-clause conditions this query adds to its joins (milestoning, kwargs filters), keyed by id(node).
        # Join objects are shared between queries, so per-query conditions live here and not on the join.
        self.join_filters: dict[int, Operation] = {}

    def expressions(self) -> list[RelationalOperationElement]:
        """Every expression of the query, in the order they are rendered."""
        return [*self.display, *self.group_by, *self.order_by, self.filter]


class SelectOperation(QueryExpression):
    """A query that is run: a query expression plus the ordering and row limit applied to its result."""

    def __init__(self, display: Sequence[RelationalOperationElement], filter: Operation,
                 order_by: list[SortOperation] | None = None, group_by: list | None = None,
                 limit: int | None = None, table: 'Table | None' = None, distinct: bool = False,
                 offset: int | None = None):
        super().__init__(display, filter, group_by, table)
        self.order_by = order_by or []
        self.limit = limit
        self.distinct = distinct
        self.offset = offset
