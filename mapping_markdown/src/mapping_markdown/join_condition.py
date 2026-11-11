"""A condition of a join in a mapping, written as SQL over the columns of the two tables it joins:

    firmTable.FLAG IS NULL AND personTable.FLAG IS NOT NULL

It is read into the same condition objects a query is made of, so the generated finders put it into the ``ON`` of the join
beside its key columns. Columns are written ``table.column`` (or ``schema.table.column``) and say which side they are on by
their table, so the condition reads the same from either end of the join. When a table is joined to itself its name is
both sides, so the joined side is written ``{target}``, as Legend writes it: ``personTable.MANAGERID = {target}.ID``. Comparisons, ``IS [NOT] NULL``, ``AND``, ``OR``,
``NOT``, parentheses and string and number literals are read; a function, or anything else, is refused."""
from __future__ import annotations

import decimal

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from model.relational import (
    ComparisonOperation, ComparisonOperator, Column, DecimalConstantOperation, FloatConstantOperation,
    IntegerConstantOperation, IsNotNullOperation, IsNullOperation, LogicalOperation, LogicalOperator, NotOperation,
    Operation, RelationalOperationElement, StringConstantOperation, Table, TableAlias,
)

TARGET = "{target}"
_TARGET_NAME = "__target__"  # SQL has no {target}, so it is read as a name

_COMPARISONS: dict[type, ComparisonOperator] = {
    exp.EQ: ComparisonOperator.EQUAL, exp.NEQ: ComparisonOperator.NOT_EQUAL, exp.LT: ComparisonOperator.LESS_THAN,
    exp.LTE: ComparisonOperator.LESS_THAN_OR_EQUAL_TO, exp.GT: ComparisonOperator.GREATER_THAN,
    exp.GTE: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO,
}


def parse_condition(text: str, tables: list[Table], where: str) -> Operation:
    """``text`` as a condition over the columns of ``tables``, the two a join joins (the same table twice for a join of a
    table to itself). ``where`` names the join for the error message.

    ``{target}`` is the table the join reaches. When that is another table it is that table; when it is the table the
    join starts from, the joined side's columns belong to an alias of the table, that tells them from the starting
    side's, which the table's own name is."""
    self_join = len(tables) == 2 and (tables[0] is tables[1] or tables[0].qualified_name == tables[1].qualified_name)
    target_alias = TableAlias(tables[-1], TARGET) if self_join else None
    try:
        tree = sqlglot.parse_one(text.replace(TARGET, _TARGET_NAME), read="")
    except SqlglotError as error:
        raise ValueError(f"{where}: cannot read the condition {text!r}: {error}") from error

    def column(node: exp.Column) -> Column:
        table_name, column_name = node.table, node.name
        if table_name == _TARGET_NAME:
            target = tables[-1]
            match = next((c for c in target.columns if c.name.lower() == column_name.lower()), None)
            if match is None:
                raise ValueError(f"{where}: table '{target.name}' has no column '{column_name}' (in {text!r})")
            if target_alias is None:
                return match
            return target_alias.column(match.name, match.type)
        if not table_name:
            raise ValueError(f"{where}: the column {column_name!r} in {text!r} needs its table: write table.{column_name}")
        schema = node.args.get("db")
        wanted = (f"{schema.name}." if schema is not None else "") + table_name
        for table in tables:
            if table.name.lower() in (wanted.lower(), table_name.lower()) or table.qualified_name.lower() == wanted.lower():
                match = next((c for c in table.columns if c.name.lower() == column_name.lower()), None)
                if match is None:
                    raise ValueError(f"{where}: table '{table.name}' has no column '{column_name}' (in {text!r})")
                return match
        names = ", ".join(f"'{t.name}'" for t in tables)
        raise ValueError(f"{where}: the table {wanted!r} in {text!r} is not one of the two tables of the join: {names}")

    def operand(node: exp.Expr) -> RelationalOperationElement:
        if isinstance(node, exp.Paren):
            return operand(node.this)
        if isinstance(node, exp.Column):
            return column(node)
        if isinstance(node, exp.Literal):
            if node.is_string:
                return StringConstantOperation(node.name)
            number = decimal.Decimal(node.name)
            if number == number.to_integral_value() and "." not in node.name and "e" not in node.name.lower():
                return IntegerConstantOperation(int(number))
            return FloatConstantOperation(float(number)) if "e" in node.name.lower() else DecimalConstantOperation(number)
        if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal) and not node.this.is_string:
            inner = operand(node.this)
            if isinstance(inner, (IntegerConstantOperation, FloatConstantOperation, DecimalConstantOperation)):
                return type(inner)(-inner.value)  # type: ignore[arg-type]
        if isinstance(node, exp.Func):
            raise ValueError(f"{where}: a function in a join condition is not supported: {node.sql()!r} in {text!r}")
        raise ValueError(f"{where}: {node.sql()!r} in {text!r} is not a column or a literal")

    def condition(node: exp.Expr) -> Operation:
        if isinstance(node, exp.Paren):
            return condition(node.this)
        if isinstance(node, exp.And):
            return LogicalOperation(condition(node.this), LogicalOperator.AND, condition(node.expression))
        if isinstance(node, exp.Or):
            return LogicalOperation(condition(node.this), LogicalOperator.OR, condition(node.expression))
        if isinstance(node, exp.Not):
            inner = node.this
            if isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null):
                return IsNotNullOperation(operand(inner.this))
            return NotOperation(condition(inner))
        if isinstance(node, exp.Is) and isinstance(node.expression, exp.Null):
            return IsNullOperation(operand(node.this))
        if type(node) in _COMPARISONS:
            return ComparisonOperation(operand(node.this), _COMPARISONS[type(node)], operand(node.expression))
        if isinstance(node, exp.Func):
            raise ValueError(f"{where}: a function in a join condition is not supported: {node.sql()!r} in {text!r}")
        raise ValueError(f"{where}: {node.sql()!r} in {text!r} is not a condition that can be read: use comparisons, "
                         f"IS [NOT] NULL, AND, OR, NOT")

    return condition(tree)
