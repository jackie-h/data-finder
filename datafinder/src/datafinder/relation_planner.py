"""Lowering a tree of relations (``model.relation_tree``) to the queries SQL says it with.

The tree is what was written, in the order it was written. A SELECT says its parts in one order only: DISTINCT, then ORDER BY, then
OFFSET, then LIMIT. A step that comes next in that order, or the same again, is said by the SELECT of the node before it (``order_by``
after ``find_all``, ``limit`` after ``order_by``, a second ``limit``). A step that comes earlier than the last one said, an order after a
``limit`` or a ``distinct`` after an order, is of the rows that SELECT gives, so it is a SELECT over it, as every other step is.

``parts_of`` is the SELECT a node is, as the arguments a runner is given; ``alias_of`` is that query under a name, which the columns a
person reads from the node (``result["id"]``, ``r.price``) belong to. Both are worked out once for a node and kept, so a node is the
same query, and the same name, each time it is asked for.
"""
from __future__ import annotations

import datetime
import decimal
import itertools
from dataclasses import dataclass, replace
from typing import Any, Sequence
from weakref import WeakKeyDictionary

from datafinder import Attribute, Operation
from datafinder.sql_generator import build_query_specification, display_alias
from datafinder.typed_attributes import typed_attribute
from model.naming import to_snake_case
from model.relation_tree import (
    Concatenate, Distinct, Drop, Join, Project, Read, RelationNode, Sort, Take)
from model.relational import (
    AggregateOperation, AggregateOperator, Alias, ArithmeticOperation, BooleanConstantOperation, CaseOperation, Column,
    ColumnWithJoin, ConcatOperation, ConstantOperation, CountAllOperation, DateArithmeticOperation, DateConstantOperation,
    DateDiffOperation, DateExtractOperation, DateTimeConstantOperation, DateTruncOperation, DecimalConstantOperation,
    FloatConstantOperation, IntegerConstantOperation, IntervalConstantOperation, IntervalOperation, JoinOperation, NoOperation,
    OrderedQuerySpecification, QueryExpression, QuerySpecification, Relation, RelationalOperationElement, ScalarFunctionOperation,
    SortOperation, StringConstantOperation, StringFunctionOperation, TableAlias, UnionOperation, WindowFunctionOperation)

relation_numbers = itertools.count(1)


def _column_of(element) -> Any:
    """The column an attribute, or a column reached through joins, reads; none for any other expression."""
    if isinstance(element, Attribute):
        return element.column()
    if isinstance(element, ColumnWithJoin):
        return element.column
    return None


def _sort_target(sort_op: SortOperation) -> tuple:
    """What a sort orders by: a column and the joins to reach it. Any other expression, such as an aggregate, is its
    own target, so sorting by an expression again adds a sort."""
    column = sort_op.column
    if not isinstance(column, ColumnWithJoin):
        return ("expression", id(column))
    joins = []
    node = column.parent
    while node is not None:
        joins.append(node.join.name)
        node = node.parent
    return column.column.owner_name, column.column.name, tuple(joins)




# --- the columns of a result, for a query over it ---------------------------------------------------------------

_NUMBER_TYPES = {int: "BIGINT", float: "DOUBLE", decimal.Decimal: "DECIMAL"}

_CONSTANT_TYPES = {
    IntegerConstantOperation: "BIGINT", FloatConstantOperation: "DOUBLE", StringConstantOperation: "VARCHAR",
    DateConstantOperation: "DATE", DateTimeConstantOperation: "TIMESTAMP", DecimalConstantOperation: "DECIMAL",
    BooleanConstantOperation: "BOOLEAN", IntervalConstantOperation: "INTERVAL",
}


def _column_name(element: RelationalOperationElement) -> str:
    """The name a column of a result has for the step after it: the name it was given, or its display name in snake_case
    (as a view names its columns)."""
    return element.name if isinstance(element, Alias) else to_snake_case(display_alias(element))


def _column_type(element: RelationalOperationElement) -> str:
    """The database type of what a result column holds, so a query over the result can compare it as that type."""
    if isinstance(element, Alias):
        return _column_type(element.element)
    if isinstance(element, Attribute):
        return element.column().type
    if isinstance(element, ColumnWithJoin):
        return element.column.type
    if isinstance(element, Column):
        return element.type
    if isinstance(element, CountAllOperation):
        return "BIGINT"
    if isinstance(element, IntervalOperation):  # before the number kinds: it is arithmetic's subclass, but not a number
        return "INTERVAL"
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
            return _column_type(element.element)
        if element.operator in (AggregateOperator.BOOL_AND, AggregateOperator.BOOL_OR):
            return "BOOLEAN"
        return "DOUBLE"
    if isinstance(element, WindowFunctionOperation):
        return _column_type(element.element) if element.element is not None else "BIGINT"
    if isinstance(element, ConstantOperation):
        return _CONSTANT_TYPES.get(type(element), "VARCHAR")
    if isinstance(element, CaseOperation):
        return _column_type(element.then)
    if isinstance(element, (ConcatOperation, StringFunctionOperation)):
        return "VARCHAR"
    if isinstance(element, (DateExtractOperation, DateDiffOperation)):
        return "BIGINT"
    if isinstance(element, (DateArithmeticOperation, DateTruncOperation)):
        return _column_type(element.element)
    if isinstance(element, ScalarFunctionOperation):  # an untyped one, such as fill_null: what it is applied to
        return _column_type(element.element)
    if isinstance(element, ArithmeticOperation):  # one of unknown kind
        return "DOUBLE"
    return "VARCHAR"


def _attributes(alias: TableAlias) -> dict[str, Attribute]:
    """The columns of a relation under a name, as a query that reads from it sees them: a typed attribute for each, named
    as ``_column_name`` names it and owned by the alias."""
    names = [_column_name(element) for element in alias.columns]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ValueError(f"A result needs each column named once, but {duplicates} are repeated: name them with alias()")
    return {name: typed_attribute(name, _column_type(element), alias) for name, element in zip(names, alias.columns)}




# --- the SELECT a node is ----------------------------------------------------------------------------------------------

_DISTINCT, _SORT, _DROP, _TAKE = 1, 2, 3, 4  # what a SELECT has said last, in the order it can say them


@dataclass(frozen=True)
class SelectParts:
    """One SELECT, as the arguments a runner is given: what it shows, what it reads, and how it is distinct, ordered, dropped and
    taken. ``stage`` is the last of those it says."""
    columns: Sequence
    table: Relation
    op: Operation
    business_date: datetime.date | None = None
    processing_datetime: datetime.datetime | datetime.date | None = None
    business_date_to: datetime.date | None = None
    processing_datetime_to: datetime.datetime | datetime.date | None = None
    order_by: tuple = ()
    group_by: tuple = ()
    limit: int | None = None
    distinct: bool = False
    offset: int | None = None
    having: Operation | None = None
    stage: int = 0

    def specification(self, columns: Sequence | None = None) -> OrderedQuerySpecification:
        return build_query_specification(
            self.business_date, self.processing_datetime, self.columns if columns is None else columns, self.table, self.op,
            list(self.order_by), list(self.group_by), self.limit, business_date_to=self.business_date_to, distinct=self.distinct,
            offset=self.offset, having=self.having, processing_datetime_to=self.processing_datetime_to)


def _display(columns: Sequence) -> list[Alias]:
    """The columns a query is read under: each named for what a query over it reads it as."""
    return [c if isinstance(c, Alias) else Alias(c, _column_name(c)) for c in columns]


def _named(alias: TableAlias) -> list[Alias]:
    return [Alias(a, n) for n, a in _attributes(alias).items()]


class Planner:
    """Works out the SELECT of each node of a tree, and keeps it."""

    def __init__(self) -> None:
        self._parts: WeakKeyDictionary[RelationNode, SelectParts] = WeakKeyDictionary()
        self._aliases: WeakKeyDictionary[RelationNode, TableAlias] = WeakKeyDictionary()
        self._unions: WeakKeyDictionary[RelationNode, UnionOperation] = WeakKeyDictionary()
        self._inner: WeakKeyDictionary[RelationNode, TableAlias] = WeakKeyDictionary()

    def parts_of(self, node: RelationNode) -> SelectParts:
        found = self._parts.get(node)
        if found is None:
            found = self._parts[node] = self._plan(node)
        return found

    def alias_of(self, node: RelationNode) -> TableAlias:
        """The query of a node under a name of its own, which the columns of the node belong to."""
        found = self._aliases.get(node)
        if found is None:
            found = self._aliases[node] = TableAlias(self.parts_of(node).specification(_display(self.parts_of(node).columns)),
                                                     f"relation{next(relation_numbers)}")
        return found

    def attributes_of(self, node: RelationNode) -> dict[str, Attribute]:
        return _attributes(self.alias_of(node))

    # --- one node ------------------------------------------------------------------------------------------------------

    def _plan(self, node: RelationNode) -> SelectParts:
        if isinstance(node, Read):
            return SelectParts(node.columns, node.table, node.op, node.business_date, node.processing_datetime,
                               node.business_date_to, node.processing_datetime_to, group_by=tuple(node.group_by), having=node.having)
        if isinstance(node, Project):
            return SelectParts(node.columns, self.alias_of(node.source), NoOperation() if node.op is None else node.op,
                               group_by=tuple(node.group_by))
        if isinstance(node, Join):
            left, right = self.alias_of(node.left), self.alias_of(node.right)
            joined = QuerySpecification(node.columns, NoOperation(), table=left,
                                        joins=[JoinOperation('', right, node.condition, node.kind)])
            return self._of_relation(node, joined)
        if isinstance(node, Concatenate):
            return self._of_relation(node, self._union(node))
        if isinstance(node, Distinct):
            return self._distinct(node)
        if isinstance(node, Sort):
            return self._sort(node)
        if isinstance(node, Drop):
            return self._drop(node)
        if isinstance(node, Take):
            return self._take(node)
        raise TypeError(f"{type(node).__name__} is not a relation that can be lowered")

    def _of_relation(self, node: RelationNode, relation: QueryExpression) -> SelectParts:
        """A SELECT of every column of a relation that is not a table, a join or a union, which is read under a name of its own."""
        inner = self._inner[node] = TableAlias(relation, f"relation{next(relation_numbers)}")
        return SelectParts(_named(inner), inner, NoOperation())

    def _union(self, node: Concatenate) -> UnionOperation:
        found = self._unions.get(node)
        if found is None:
            found = self._unions[node] = UnionOperation(
                self._union_side(node.left, node.all), self._union_side(node.right, node.all), node.all)  # type: ignore[arg-type]
        return found

    def _union_side(self, node: RelationNode, all: bool) -> QueryExpression:
        """A node as one side of a union. A union of the same operator is that union itself, not a SELECT over it, so
        ``a.concatenate(b).concatenate(c)`` is one flat chain of unions."""
        if isinstance(node, Concatenate) and node.all == all:
            return self._union(node)
        return self.alias_of(node).relation  # type: ignore[return-value]

    # --- what follows a SELECT, which it says if it can ------------------------------------------------------------

    def _over(self, node: RelationNode) -> SelectParts:
        """A SELECT over the rows of a node, to say what the SELECT of the node cannot say after what it has."""
        alias = self.alias_of(node)
        return SelectParts(list(_attributes(alias).values()), alias, NoOperation())

    def _sorts(self, source: RelationNode, sorts: Sequence[SortOperation], parts: SelectParts, wrapped: bool) -> list[SortOperation]:
        """The sorts as the SELECT that says them reads their columns. A column of the node before reads as what that node shows under
        that name; one of the table a finder reads is as it is, when the SELECT is the finder's, and a column of the rows when it is
        over them."""
        alias = self._aliases.get(source)
        shown = {} if alias is None else {_column_name(c): c.element if isinstance(c, Alias) else c
                                         for c in self.parts_of(source).columns}
        result = []
        for sort in sorts:
            column = _column_of(sort.column)
            owned = column is not None and alias is not None and column.owner is alias
            if wrapped:
                attributes = _attributes(parts.table) if isinstance(parts.table, TableAlias) else {}
                if owned:
                    own = sort.column if isinstance(sort.column, ColumnWithJoin) else sort.column._column_with_join()  # type: ignore[attr-defined]
                    result.append(SortOperation(own, sort.direction))
                    continue
                name = next((n for n, c in zip(attributes, self.parts_of(source).columns)
                             if column is not None and _column_of(c.element if isinstance(c, Alias) else c) is column), None)
                if name is None:
                    raise ValueError("The rows after a limit or an offset are ordered by the columns of the result: "
                                     f"{list(attributes)} (result[\"name\"].ascending())")
                result.append(SortOperation(attributes[name]._column_with_join(), sort.direction))
            elif owned and column is not None and column.name in shown:
                found = shown[column.name]
                result.append(SortOperation(found._column_with_join() if isinstance(found, Attribute) else found, sort.direction))
            else:
                result.append(sort)
        return result

    def _distinct(self, node: Distinct) -> SelectParts:
        below = self.parts_of(node.source)
        if below.stage > _DISTINCT:
            below = self._over(node.source)
        return replace(below, distinct=True, stage=_DISTINCT)

    def _sort(self, node: Sort) -> SelectParts:
        below = self.parts_of(node.source)
        wrapped = below.stage > _SORT
        if wrapped:
            below = self._over(node.source)
        sorts = self._sorts(node.source, node.sorts, below, wrapped)
        resorted = {_sort_target(s) for s in sorts}
        return replace(below, order_by=(*sorts, *(s for s in below.order_by if _sort_target(s) not in resorted)), stage=_SORT)

    def _drop(self, node: Drop) -> SelectParts:
        below = self.parts_of(node.source)
        if below.stage >= _TAKE:
            below = self._over(node.source)
        return replace(below, offset=node.count if below.offset is None else below.offset + node.count, stage=_DROP)

    def _take(self, node: Take) -> SelectParts:
        below = self.parts_of(node.source)
        return replace(below, limit=node.count if below.limit is None else min(below.limit, node.count), stage=_TAKE)


PLANNER = Planner()


def parts_of(node: RelationNode) -> SelectParts:
    return PLANNER.parts_of(node)


def alias_of(node: RelationNode) -> TableAlias:
    return PLANNER.alias_of(node)


def attributes_of(node: RelationNode) -> dict[str, Attribute]:
    return PLANNER.attributes_of(node)
