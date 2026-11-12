"""A query written as SQL, read into the query model: ``SELECT DISTINCT FIRM_ID, FIRM_LEGALNAME FROM PERSON_FIRM_DENORM``.

A mapping can map a class to a query written as SQL, as it maps one to a table. It is not a database view: nothing is created in the
database, the query is a statement that is run. The SQL is checked and read here, with the PostgreSQL dialect as the standard (it is
close to ANSI SQL and is what sqlglot reads best), into the same objects a query is made of, so that it is written into a query over the
class as a derived table: ``FROM (SELECT ...) AS t0``.

What is read: ``SELECT [DISTINCT]`` over one relation and any joins (``INNER``, ``LEFT``, ``RIGHT`` and ``FULL`` with an ``ON``),
with ``WHERE``, ``GROUP BY`` and ``HAVING``, and ``UNION [ALL]`` of such queries. A select item is a column, ``*``, or an
expression named with ``AS``: a literal, arithmetic, ``||`` and ``CONCAT``, ``CASE``, ``UPPER``, ``LOWER``, ``TRIM``,
``LENGTH`` and ``ABS``, or an aggregate (``SUM``, ``COUNT``, ``MIN``, ``MAX``, ``AVG``). A condition is a comparison,
``LIKE``, ``IN`` a list, ``IS [NOT] NULL``, ``AND``, ``OR`` or ``NOT``. ``ORDER BY`` and ``LIMIT`` are not part of the query, and
anything else is refused with the part of the SQL it is."""
from __future__ import annotations

import decimal
from collections.abc import Sequence

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from model.relational import (
    AggregateOperation, AggregateOperator, Alias, ArithmeticOperator, BooleanConstantOperation, CaseOperation,
    Column, ComparisonOperation, ComparisonOperator, ConcatOperation, ConstantOperation, CountAllOperation,
    DecimalConstantOperation, FloatConstantOperation, InOperation, IntegerConstantOperation, IsNotNullOperation,
    IsNullOperation, JoinKind, JoinOperation, LogicalOperation, LogicalOperator, NamedRelation, NoOperation,
    NotInOperation, NotOperation, Operation, QueryExpression, QuerySpecification, Relation, RelationalOperationElement,
    ScalarFunction, ScalarFunctionOperation, StringConstantOperation, Table, TableAlias, UnionOperation,
    UntypedArithmeticOperation,
)

DIALECT = "postgres"


class SqlQueryError(ValueError):
    """The SQL of a query cannot be read, or reads tables or columns that there are not."""


_COMPARISONS = {
    exp.EQ: ComparisonOperator.EQUAL, exp.NEQ: ComparisonOperator.NOT_EQUAL, exp.LT: ComparisonOperator.LESS_THAN,
    exp.LTE: ComparisonOperator.LESS_THAN_OR_EQUAL_TO, exp.GT: ComparisonOperator.GREATER_THAN,
    exp.GTE: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO,
}
_ARITHMETIC = {exp.Add: ArithmeticOperator.ADD, exp.Sub: ArithmeticOperator.SUBTRACT,
               exp.Mul: ArithmeticOperator.MULTIPLY, exp.Div: ArithmeticOperator.DIVIDE}
_SCALARS = {exp.Upper: ScalarFunction.UPPER, exp.Lower: ScalarFunction.LOWER, exp.Length: ScalarFunction.LENGTH,
            exp.Abs: ScalarFunction.ABS}
_AGGREGATES = {exp.Sum: AggregateOperator.SUM, exp.Min: AggregateOperator.MIN, exp.Max: AggregateOperator.MAX,
               exp.Avg: AggregateOperator.AVERAGE}
_JOINS = {"INNER": JoinKind.INNER, "": JoinKind.INNER, "LEFT": JoinKind.LEFT, "RIGHT": JoinKind.RIGHT, "FULL": JoinKind.FULL}


class _Scope:
    """What the names of one SELECT mean: the relations it reads, by the name each is read by."""

    def __init__(self, where: str):
        self.where = where
        self.relations: dict[str, Relation] = {}

    def add(self, name: str, relation: Relation) -> None:
        if name.lower() in self.relations:
            raise SqlQueryError(f"{self.where}: {name!r} is read twice in the FROM: name one of them with AS")
        self.relations[name.lower()] = relation

    def column(self, table: str, name: str) -> Column:
        if table:
            relation = self.relations.get(table.lower())
            if relation is None:
                raise SqlQueryError(f"{self.where}: {table!r} in {table}.{name} is not a table of the query: "
                                    f"{sorted(self.relations)}")
            found = _column_of(relation, name)
            if found is None:
                raise SqlQueryError(f"{self.where}: {_label(relation)} has no column {name!r}")
            return found
        matches = [(rel, col) for rel in self.relations.values() if (col := _column_of(rel, name)) is not None]
        if not matches:
            raise SqlQueryError(f"{self.where}: no table of the query has a column {name!r}")
        if len(matches) > 1:
            raise SqlQueryError(f"{self.where}: the column {name!r} is in more than one table: write table.{name}")
        return matches[0][1]


def _label(relation: Relation) -> str:
    return f"'{relation.name}'" if isinstance(relation, NamedRelation) else repr(relation)


def _column_of(relation: Relation, name: str) -> Column | None:
    for column in relation.columns:
        if isinstance(column, Column) and column.name.lower() == name.lower():
            return column
        if isinstance(column, Alias) and column.name.lower() == name.lower():
            return Column(column.name, "", relation)  # a derived table's column, read by the name it is given
    return None


def output_names(query: QueryExpression) -> list[str]:
    """The names of the columns a query gives: the name of a column, or what an expression is named with AS."""
    names = []
    for column in query.columns:
        if isinstance(column, Alias):
            names.append(column.name)
        elif isinstance(column, Column):
            names.append(column.name)
        else:
            raise SqlQueryError(f"The select item {column!r} has no name: write AS name")
    return names


def parse_query(sql: str, tables: Sequence[NamedRelation], where: str = "The query") -> QueryExpression:
    """``sql`` as a query over ``tables``, the relations it can read by name. ``where`` names it for an error message."""
    return parse_query_reads(sql, tables, where)[0]


def parse_query_reads(sql: str, tables: Sequence[NamedRelation], where: str = "The query") \
        -> tuple[QueryExpression, list[NamedRelation]]:
    """``sql`` as a query, and the relations of ``tables`` it reads, each once, in the order it reads them."""
    by_name: dict[str, NamedRelation] = {}
    for table in tables:
        by_name.setdefault(table.name.split(".")[-1].lower(), table)  # a table in a finder is named with its schema
        by_name[table.name.lower()] = table
        by_name[table.qualified_name.lower()] = table
    reads: list[NamedRelation] = []
    try:
        tree = sqlglot.parse_one(sql, read=DIALECT)
    except SqlglotError as error:
        raise SqlQueryError(f"{where}: the SQL cannot be read as {DIALECT} SQL: {error}") from error
    return _query(tree, by_name, where, reads), reads


def _query(node: exp.Expr, tables: dict[str, NamedRelation], where: str, reads: list) -> QueryExpression:
    if isinstance(node, exp.Subquery):
        return _query(node.this, tables, where, reads)
    if isinstance(node, (exp.Intersect, exp.Except)):
        raise SqlQueryError(f"{where}: only UNION [ALL] is read, not {node.key.upper()}")
    if isinstance(node, exp.Union):
        left, right = _query(node.this, tables, where, reads), _query(node.expression, tables, where, reads)
        if len(left.columns) != len(right.columns):
            raise SqlQueryError(f"{where}: the two sides of a UNION have {len(left.columns)} and {len(right.columns)} columns")
        return UnionOperation(left, right, all=not node.args.get("distinct"))
    if not isinstance(node, exp.Select):
        raise SqlQueryError(f"{where}: a query is a SELECT, or a UNION of them, not {node.sql(dialect=DIALECT)!r}")
    return _select(node, tables, where, reads)


def _select(node: exp.Select, tables: dict[str, NamedRelation], where: str, reads: list) -> QuerySpecification:
    for part in ("limit", "offset", "order", "qualify", "windows", "with_"):
        if node.args.get(part):
            raise SqlQueryError(f"{where}: {part.upper().strip('_')} is not part of a mapped query: it belongs to a query over it")
    from_ = node.args.get("from_")
    if from_ is None:
        raise SqlQueryError(f"{where}: the SELECT reads from no table")
    scope = _Scope(where)
    root = _relation(from_.this, tables, scope, where, reads)
    joins: list[JoinOperation] = []
    for join in node.args.get("joins") or []:
        kind = _JOINS.get((join.args.get("side") or join.args.get("kind") or "").upper())
        if kind is None or join.args.get("kind") == "CROSS" or join.args.get("on") is None:
            raise SqlQueryError(f"{where}: a join is INNER, LEFT, RIGHT or FULL, with an ON condition: "
                                f"{join.sql(dialect=DIALECT)!r}")
        target = _relation(join.this, tables, scope, where, reads)
        joins.append(JoinOperation(_name_of(target), target, _condition(join.args["on"], scope), kind))
    columns = _select_items(node, scope)
    filter_: Operation = _condition(node.args["where"].this, scope) if node.args.get("where") else NoOperation()
    group = node.args.get("group")
    group_by = [_value(e, scope) for e in group.expressions] if group else []
    having = _condition(node.args["having"].this, scope) if node.args.get("having") else None
    return QuerySpecification(columns, filter_, group_by, root, bool(node.args.get("distinct")), having, joins)


def _name_of(relation: Relation) -> str:
    return relation.name if isinstance(relation, NamedRelation) else ""


def _relation(node: exp.Expr, tables: dict[str, NamedRelation], scope: _Scope, where: str, reads: list) -> Relation:
    if isinstance(node, exp.Table):
        schema = node.args.get("db")
        name = (f"{schema.name}." if schema is not None else "") + node.name
        table = tables.get(name.lower()) or tables.get(node.name.lower())
        if table is None:
            raise SqlQueryError(f"{where}: the table {name!r} is not one of the tables there are: "
                                f"{sorted({t.name for t in tables.values()})}")
        if not any(r is table for r in reads):
            reads.append(table)
        read_as = node.alias or table.name
        if not node.alias:
            relation: Relation = table
        elif isinstance(table, TableAlias):
            # another query read under a name of its own: the name is of the query, with columns it owns
            relation = TableAlias(table.relation, node.alias, [(c.name, c.type) for c in table.columns if isinstance(c, Column)])
        else:
            relation = TableAlias(table, node.alias)
        scope.add(read_as, relation)
        return relation
    if isinstance(node, exp.Subquery):
        if not node.alias:
            raise SqlQueryError(f"{where}: a query in a FROM needs a name: (SELECT ...) AS name")
        query = _query(node.this, tables, where, reads)
        alias = TableAlias(query, node.alias)
        scope.add(node.alias, alias)
        return alias
    raise SqlQueryError(f"{where}: {node.sql(dialect=DIALECT)!r} in a FROM is not a table or a query")


def _select_items(node: exp.Select, scope: _Scope) -> list[RelationalOperationElement]:
    items: list[RelationalOperationElement] = []
    for item in node.expressions:
        if isinstance(item, exp.Star):
            for relation in scope.relations.values():
                items.extend(_all_columns(relation))
        elif isinstance(item, exp.Column) and isinstance(item.this, exp.Star):
            relation = scope.relations.get(item.table.lower())
            if relation is None:
                raise SqlQueryError(f"{scope.where}: {item.table!r} in {item.table}.* is not a table of the query")
            items.extend(_all_columns(relation))
        elif isinstance(item, exp.Alias):
            element = _value(item.this, scope)
            items.append(Alias(element, item.alias))
        elif isinstance(item, exp.Column):
            column = scope.column(item.table, item.name)
            items.append(Alias(column, column.name))  # named, so that it is written AS name
        else:
            raise SqlQueryError(f"{scope.where}: the select item {item.sql(dialect=DIALECT)!r} has no name: write AS name")
    names = [n for n in (_item_name(i) for i in items)]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise SqlQueryError(f"{scope.where}: the select list names {duplicates} more than once: name them with AS")
    return items


def _all_columns(relation: Relation) -> list[RelationalOperationElement]:
    return [Alias(c, c.name) if isinstance(c, Column) else c for c in relation.columns if isinstance(c, (Column, Alias))]


def _item_name(item: RelationalOperationElement) -> str:
    assert isinstance(item, (Alias, Column))
    return item.name


def _value(node: exp.Expr, scope: _Scope) -> RelationalOperationElement:
    """A value: a column, a literal, or an expression of them."""
    if isinstance(node, exp.Paren):
        return _value(node.this, scope)
    if isinstance(node, exp.Column):
        return scope.column(node.table, node.name)
    if isinstance(node, exp.Literal):
        return _literal(node, scope)
    if isinstance(node, exp.Boolean):
        return BooleanConstantOperation(bool(node.this))
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal) and not node.this.is_string:
        inner = _literal(node.this, scope)
        return type(inner)(-inner.value)  # type: ignore[attr-defined,call-arg]
    if type(node) in _ARITHMETIC:
        return UntypedArithmeticOperation(_value(node.this, scope), _ARITHMETIC[type(node)], _value(node.expression, scope))
    if isinstance(node, exp.DPipe):
        return ConcatOperation([_value(node.this, scope), _value(node.expression, scope)])
    if isinstance(node, exp.Concat):
        return ConcatOperation([_value(e, scope) for e in node.expressions])
    if type(node) in _SCALARS:
        return ScalarFunctionOperation(_value(node.this, scope), _SCALARS[type(node)])
    if isinstance(node, exp.Trim):
        return ScalarFunctionOperation(_value(node.this, scope), ScalarFunction.TRIM)
    if isinstance(node, exp.Case):
        return _case(node, scope)
    if isinstance(node, exp.Count):
        if isinstance(node.this, exp.Star):
            return CountAllOperation("")
        if isinstance(node.this, exp.Distinct):
            return AggregateOperation(_value(node.this.expressions[0], scope), AggregateOperator.COUNT_DISTINCT)
        return AggregateOperation(_value(node.this, scope), AggregateOperator.COUNT)
    if type(node) in _AGGREGATES:
        return AggregateOperation(_value(node.this, scope), _AGGREGATES[type(node)])
    raise SqlQueryError(f"{scope.where}: {node.sql(dialect=DIALECT)!r} is not something a view can read yet: a column, a "
                        f"literal, arithmetic, ||, CONCAT, CASE, UPPER, LOWER, TRIM, LENGTH, ABS or an aggregate")


def _literal(node: exp.Literal, scope: _Scope) -> ConstantOperation:
    if node.is_string:
        return StringConstantOperation(node.name)
    text = node.name
    if "." not in text and "e" not in text.lower():
        return IntegerConstantOperation(int(text))
    return FloatConstantOperation(float(text)) if "e" in text.lower() else DecimalConstantOperation(decimal.Decimal(text))


def _case(node: exp.Case, scope: _Scope) -> RelationalOperationElement:
    if node.this is not None:
        raise SqlQueryError(f"{scope.where}: CASE <value> WHEN is not read: write CASE WHEN <condition>")
    default = _value(node.args["default"], scope) if node.args.get("default") is not None else None
    result: RelationalOperationElement | None = default
    for branch in reversed(node.args["ifs"]):
        result = CaseOperation(_condition(branch.this, scope), _value(branch.args["true"], scope), result)
    assert result is not None
    return result


def _condition(node: exp.Expr, scope: _Scope) -> Operation:
    if isinstance(node, exp.Paren):
        return _condition(node.this, scope)
    if isinstance(node, exp.And):
        return LogicalOperation(_condition(node.this, scope), LogicalOperator.AND, _condition(node.expression, scope))
    if isinstance(node, exp.Or):
        return LogicalOperation(_condition(node.this, scope), LogicalOperator.OR, _condition(node.expression, scope))
    if isinstance(node, exp.Not):
        inner = node.this
        if isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null):
            return IsNotNullOperation(_value(inner.this, scope))
        if isinstance(inner, exp.In):
            return _in(inner, scope, negate=True)
        return NotOperation(_condition(inner, scope))
    if isinstance(node, exp.Is) and isinstance(node.expression, exp.Null):
        element = _value(node.this, scope)  # PostgreSQL reads IS NOT NULL as an IS NULL that is negated
        return IsNotNullOperation(element) if node.args.get("negate") else IsNullOperation(element)
    if type(node) in _COMPARISONS:
        return ComparisonOperation(_value(node.this, scope), _COMPARISONS[type(node)], _value(node.expression, scope))
    if isinstance(node, exp.Like):
        operator = ComparisonOperator.NOT_LIKE if node.args.get("negate") else ComparisonOperator.LIKE
        return ComparisonOperation(_value(node.this, scope), operator, _value(node.expression, scope))
    if isinstance(node, exp.In):
        return _in(node, scope, negate=False)
    raise SqlQueryError(f"{scope.where}: {node.sql(dialect=DIALECT)!r} is not a condition a view can read: use comparisons, "
                        f"LIKE, IN a list, IS [NOT] NULL, AND, OR, NOT")


def _in(node: exp.In, scope: _Scope, negate: bool) -> Operation:
    values = [_value(e, scope) for e in node.expressions]
    if not values or not all(isinstance(v, ConstantOperation) for v in values):
        raise SqlQueryError(f"{scope.where}: IN reads a list of literals: {node.sql(dialect=DIALECT)!r}")
    constants: list[ConstantOperation] = [v for v in values if isinstance(v, ConstantOperation)]
    element = _value(node.this, scope)
    return NotInOperation(element, constants) if negate else InOperation(element, constants)
