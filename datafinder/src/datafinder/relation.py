"""A result as a relation: the rows a ``find_all(...)`` returns have named columns, and a query over it (a ``concatenate``,
and what orders and limits that) reads them as it reads a table's.

As in Legend, where ``concatenate`` takes relations and returns one, the result is a query over a query: the two results are a
``UNION``, and ordering the union is a ``SELECT ... FROM (<union>) ORDER BY ...``.
"""
from __future__ import annotations

import decimal

from model.naming import to_snake_case
from model.relational import (
    AggregateOperation, AggregateOperator, Alias, ArithmeticOperation, BooleanConstantOperation, CaseOperation, Column,
    ColumnWithJoin,
    ConcatOperation, ConstantOperation, CountAllOperation, DateConstantOperation, DateDiffOperation, DateExtractOperation,
    DateTimeConstantOperation, DecimalConstantOperation, FloatConstantOperation, IntegerConstantOperation,
    Relation, RelationalOperationElement, TableAlias, ScalarFunctionOperation, StringConstantOperation, WindowFunctionOperation,
    DateArithmeticOperation, DateTruncOperation,
)

from datafinder.attribute import Attribute
from datafinder.scalar_functions import StringFunctionOperation
from datafinder.sql_generator import display_alias
from datafinder.typed_attributes import (
    BooleanAttribute, DateAttribute, DateTimeAttribute, DecimalAttribute, DoubleAttribute, IntegerAttribute, StringAttribute,
)

_NUMBER_TYPES = {int: "BIGINT", float: "DOUBLE", decimal.Decimal: "DECIMAL"}

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
    """The database type of what a result column holds, so a query over the result can compare it as that type."""
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
    # a number of a known kind: an integer, double or decimal arithmetic result, function, aggregate or constant
    kind = getattr(element, "_number_kind", None)
    if kind in _NUMBER_TYPES:
        return _NUMBER_TYPES[kind]
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
    if isinstance(element, (ConcatOperation, StringFunctionOperation)):
        return "VARCHAR"
    if isinstance(element, (DateExtractOperation, DateDiffOperation)):
        return "BIGINT"
    if isinstance(element, (DateArithmeticOperation, DateTruncOperation)):
        return column_type(element.element)
    if isinstance(element, ScalarFunctionOperation):  # an untyped one, such as fill_null: what it is applied to
        return column_type(element.element)
    if isinstance(element, ArithmeticOperation):  # one of unknown kind
        return "DOUBLE"
    return "VARCHAR"


def attribute(name: str, db_type: str, owner: Relation) -> Attribute:
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


def attributes(alias: TableAlias) -> dict[str, Attribute]:
    """The columns of a relation under a name, as a query that reads from it sees them: a typed attribute for each, named
    as ``column_name`` names it and owned by the alias."""
    names = [column_name(element) for element in alias.columns]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ValueError(f"A result needs each column named once, but {duplicates} are repeated: name them with alias()")
    return {name: attribute(name, column_type(element), alias) for name, element in zip(names, alias.columns)}
