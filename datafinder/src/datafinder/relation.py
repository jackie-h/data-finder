"""A step of a chain as a relation: the rows a ``find_all(...)`` returns have named columns, and the next step (``filter``,
``extend``, ``select``) reads them as it reads a table's.

As in Legend, where ``filter``, ``extend`` and ``aggregate`` each take a relation and return one, a step is a query over the
query before it: ``result.filter(...)`` is ``SELECT ... FROM (<result>) WHERE ...``. A window function over a result can then
be filtered on, which ``WHERE`` alone cannot do.
"""
from __future__ import annotations

from typing import Sequence

from model.naming import to_snake_case
from model.relational import (
    AggregateOperation, AggregateOperator, Alias, ArithmeticOperation, BooleanConstantOperation, CaseOperation, Column,
    ColumnWithJoin,
    ConcatOperation, ConstantOperation, CountAllOperation, DateConstantOperation, DateDiffOperation, DateExtractOperation,
    DateTimeConstantOperation, DecimalConstantOperation, DerivedTable, FloatConstantOperation, IntegerConstantOperation,
    QueryExpression, RelationalOperationElement, ScalarFunctionOperation, StringConstantOperation, WindowFunctionOperation,
    DateArithmeticOperation, DateTruncOperation,
)

from datafinder.attribute import Attribute
from datafinder.sql_generator import display_alias
from datafinder.typed_attributes import (
    BooleanAttribute, DateAttribute, DateTimeAttribute, DecimalAttribute, DoubleAttribute, IntegerAttribute, StringAttribute,
)

_CONSTANT_TYPES = {
    IntegerConstantOperation: "BIGINT", FloatConstantOperation: "DOUBLE", StringConstantOperation: "VARCHAR",
    DateConstantOperation: "DATE", DateTimeConstantOperation: "TIMESTAMP", DecimalConstantOperation: "DECIMAL",
    BooleanConstantOperation: "BOOLEAN",
}


def column_name(element: RelationalOperationElement) -> str:
    """The name a column of a result has for the step after it: the name it was given, or its display name in snake_case
    (as a view names its columns)."""
    return element.name if isinstance(element, Alias) else to_snake_case(display_alias(element))


def column_type(element: RelationalOperationElement) -> str:
    """The database type of what a result column holds, so the step after it can compare it as that type."""
    if isinstance(element, Alias):
        return column_type(element.element)
    if isinstance(element, Attribute):
        return element.column().type
    if isinstance(element, ColumnWithJoin):
        return element.column.type
    if isinstance(element, Column):
        return element.type
    if isinstance(element, CountAllOperation):
        return "BIGINT"
    if isinstance(element, AggregateOperation):
        if element.operator in (AggregateOperator.COUNT, AggregateOperator.COUNT_DISTINCT):
            return "BIGINT"
        if element.operator == AggregateOperator.STRING_AGG:
            return "VARCHAR"
        if element.operator in (AggregateOperator.MIN, AggregateOperator.MAX, AggregateOperator.SUM):
            return column_type(element.element)
        if element.operator in (AggregateOperator.BOOL_AND, AggregateOperator.BOOL_OR):
            return "BOOLEAN"
        return "DOUBLE"
    if isinstance(element, WindowFunctionOperation):
        return column_type(element.element) if element.element is not None else "BIGINT"
    if isinstance(element, ConstantOperation):
        return _CONSTANT_TYPES.get(type(element), "VARCHAR")
    if isinstance(element, CaseOperation):
        return column_type(element.then)
    if isinstance(element, ArithmeticOperation):
        return "DOUBLE"
    if isinstance(element, ConcatOperation):
        return "VARCHAR"
    if isinstance(element, DateExtractOperation):
        return "BIGINT"
    if isinstance(element, (DateArithmeticOperation, DateTruncOperation)):
        return column_type(element.element)
    if isinstance(element, DateDiffOperation):
        return "BIGINT"
    if isinstance(element, ScalarFunctionOperation):
        return "DOUBLE"
    return "VARCHAR"


def attribute(name: str, db_type: str, owner: str) -> Attribute:
    """A column of a relation, as the typed attribute that compares, does arithmetic and aggregates as its type does."""
    kind = db_type.upper().split("(")[0].strip()
    if kind in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT"):
        factory = IntegerAttribute
    elif kind in ("DOUBLE", "FLOAT", "REAL"):
        factory = DoubleAttribute
    elif kind in ("DECIMAL", "NUMERIC"):
        factory = DecimalAttribute
    elif kind == "DATE":
        factory = DateAttribute
    elif kind.startswith("TIMESTAMP") or kind == "DATETIME":
        factory = DateTimeAttribute
    elif kind in ("BOOLEAN", "BOOL"):
        factory = BooleanAttribute
    else:
        factory = StringAttribute
    return factory(name, name, db_type, owner)


def derived_table(name: str, display: Sequence[RelationalOperationElement], query: QueryExpression) -> DerivedTable:
    """The relation a query is: a column for each of its outputs, named as ``column_name`` names them."""
    names = [column_name(element) for element in display]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ValueError(f"A chain needs each column named once, but {duplicates} are repeated: name them with alias()")
    return DerivedTable(name, [Column(n, column_type(e), name) for n, e in zip(names, display)], query)


def attributes(table: DerivedTable) -> dict[str, Attribute]:
    return {column.name: attribute(column.name, column.type, table.name) for column in table.columns}
