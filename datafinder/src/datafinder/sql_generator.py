import copy
import decimal
import importlib
import itertools
import re
import datetime
from functools import reduce
from typing import Any, Callable, Iterable, Sequence

import sqlglot
import sqlglot.errors

from datafinder import DateTimeAttribute, DateAttribute
from datafinder.attribute import Attribute
from datafinder.pgvector_dialect import PgVector
from model.milestoning import AsOf, MilestoningColumns, ProcessingTemporalColumns, SingleBusinessDateColumn, \
    SingleProcessingDatetimeColumn, \
    BusinessDateAndProcessingTemporalColumns, BiTemporalColumns, BusinessDateRangeColumns, MilestonedTable
from model.milestoning import milestoning_columns_of
from model.naming import to_snake_case
from model.view import View, ViewDefinitionError, contains_aggregate, contains_aggregate_or_window
from model.relational import LIKE_ESCAPE, Table, Relation, NamedRelation, TableAlias, QueryExpression, UnionOperation, Operation, LogicalOperator, LogicalOperation, RelationalOperationElement, \
    ComparisonOperation, ConstantOperation, ComparisonOperator, StringConstantOperation, DateConstantOperation, \
    DateTimeConstantOperation, IntegerConstantOperation, FloatConstantOperation, BooleanConstantOperation, DecimalConstantOperation, Column, NoOperation, JoinOperation, JoinTreeNodeOperation, \
    UnaryOperation, ColumnWithJoin, AggregateOperation, AggregateOperator, SortOperation, SortDirection, CountAllOperation, \
    ScalarFunction, ScalarFunctionOperation, VectorValue, DatePart, DateExtractOperation, DateTruncOperation, DateArithmeticOperation, DateDiffOperation, \
    RecentDayOfWeekOperation, CurrentDateOperation, CurrentTimestampOperation, \
    IsNullOperation, IsNotNullOperation, InOperation, NotInOperation, NotOperation, \
    WindowFunctionOperation, WindowFunction, WindowSpecification, Alias, ExistsOperation, NotExistsOperation, RelationExistsOperation, \
    OrderedQuerySpecification, QuerySpecification, BinaryOperation, ArithmeticOperation, ArithmeticOperator, ConcatOperation, CaseOperation, IntervalOperation, IntervalConstantOperation, \
    JoinKind, StringFunctionOperation

_JOIN_WORDS = {JoinKind.INNER: 'INNER', JoinKind.LEFT: 'LEFT OUTER', JoinKind.RIGHT: 'RIGHT OUTER', JoinKind.FULL: 'FULL OUTER'}


def _join_word(kind: JoinKind) -> str:
    return ' ' + _JOIN_WORDS[kind] + ' JOIN '


class TableAliasColumn(RelationalOperationElement):
    def __init__(self, column: Column, table_alias: TableAlias):
        super().__init__()
        self.column = column
        self.table_alias = table_alias


class Join:
    """A join as the query writes it: what is joined, how, and the condition and filter in ON."""
    def __init__(self, kind: JoinKind, target: TableAlias, condition: str, filter_sql: str | None = None):
        self.kind = kind
        self.target = target
        self.condition = condition
        self.filter_sql = filter_sql

def _open_end_clause(end_attr, value, infinite_datetime: str | None) -> Operation:
    """Returns `end > value` normally, or `(end > value OR end IS NULL)` when infinite_datetime is None."""
    gt_op = end_attr > value
    if infinite_datetime is None:
        col_ref = ColumnWithJoin(end_attr.column(), end_attr.parent())
        return LogicalOperation(gt_op, LogicalOperator.OR, IsNullOperation(col_ref))
    return gt_op


def _and_all(ops: Iterable[Operation | None]) -> Operation | None:
    """The operations ANDed together left to right, ignoring the missing ones; None if there are none."""
    present = [op for op in ops if op is not None]
    return reduce(lambda left, right: LogicalOperation(left, LogicalOperator.AND, right),
                  present[1:], present[0]) if present else None


def _table_of(column: Column) -> Table | TableAlias:
    """The table a milestoning or view column is a column of; for a query that a class is mapped to, the query."""
    assert isinstance(column.table, (Table, TableAlias)), f"Column {column.name} has no table"
    return column.table


def _processing_predicate(mc: MilestoningColumns, processing_datetime: datetime.datetime | datetime.date | None,
                          join_node: 'JoinTreeNodeOperation | None') -> Operation | None:
    """The table's processing range contains the processing time, or its processing snapshot column equals it."""
    if processing_datetime is None:
        return None
    if isinstance(mc, SingleProcessingDatetimeColumn):
        snapshot = DateTimeAttribute('processing_datetime', mc.processing_datetime_column.name, mc.processing_datetime_column.type, _table_of(mc.processing_datetime_column), join_node)
        return snapshot == processing_datetime
    if not isinstance(mc, ProcessingTemporalColumns):
        return None
    start_at = DateTimeAttribute('start_at', mc.start_at_column.name, mc.start_at_column.type, _table_of(mc.start_at_column), join_node)
    end_at = DateTimeAttribute('end_at', mc.end_at_column.name, mc.end_at_column.type, _table_of(mc.end_at_column), join_node)
    return LogicalOperation(start_at <= processing_datetime, LogicalOperator.AND, _open_end_clause(end_at, processing_datetime, mc.processing_infinite_datetime))


def _processing_range_predicate(mc: MilestoningColumns, processing_from: datetime.datetime | datetime.date,
                                processing_to: datetime.datetime | datetime.date,
                                join_node: 'JoinTreeNodeOperation | None') -> Operation | None:
    """The table's processing range overlaps the processing range, or its processing snapshot column is within it."""
    if isinstance(mc, SingleProcessingDatetimeColumn):
        snapshot = DateTimeAttribute('processing_datetime', mc.processing_datetime_column.name, mc.processing_datetime_column.type, _table_of(mc.processing_datetime_column), join_node)
        return LogicalOperation(snapshot >= processing_from, LogicalOperator.AND, snapshot <= processing_to)
    if not isinstance(mc, ProcessingTemporalColumns):
        return None
    start_at = DateTimeAttribute('start_at', mc.start_at_column.name, mc.start_at_column.type, _table_of(mc.start_at_column), join_node)
    end_at = DateTimeAttribute('end_at', mc.end_at_column.name, mc.end_at_column.type, _table_of(mc.end_at_column), join_node)
    return LogicalOperation(start_at <= processing_to, LogicalOperator.AND, _open_end_clause(end_at, processing_from, mc.processing_infinite_datetime))


def _business_predicate(mc: MilestoningColumns, business_date: datetime.date | None,
                        join_node: 'JoinTreeNodeOperation | None') -> Operation | None:
    """The table's business range contains the business date, or its business date column equals it."""
    if business_date is None:
        return None
    if isinstance(mc, BusinessDateRangeColumns):
        date_from = DateAttribute('business_date_from', mc.business_date_from_column.name, mc.business_date_from_column.type, _table_of(mc.business_date_from_column), join_node)
        date_to = DateAttribute('business_date_to', mc.business_date_to_column.name, mc.business_date_to_column.type, _table_of(mc.business_date_to_column), join_node)
        return LogicalOperation(date_from <= business_date, LogicalOperator.AND, _open_end_clause(date_to, business_date, mc.business_infinite_datetime))
    if isinstance(mc, SingleBusinessDateColumn):
        business_att = DateAttribute('business_date', mc.business_date_column.name, mc.business_date_column.type, _table_of(mc.business_date_column), join_node)
        return business_att == business_date
    return None


def _business_range_predicate(mc: MilestoningColumns, business_date_from: datetime.date | None,
                              business_date_to: datetime.date | None,
                              join_node: 'JoinTreeNodeOperation | None') -> Operation | None:
    """The table's business range overlaps the business date range, or its business date column is within it."""
    if business_date_from is None or business_date_to is None:
        return None
    if isinstance(mc, BusinessDateRangeColumns):
        date_from = DateAttribute('business_date_from', mc.business_date_from_column.name, mc.business_date_from_column.type, _table_of(mc.business_date_from_column), join_node)
        date_to = DateAttribute('business_date_to', mc.business_date_to_column.name, mc.business_date_to_column.type, _table_of(mc.business_date_to_column), join_node)
        return LogicalOperation(date_from <= business_date_to, LogicalOperator.AND, _open_end_clause(date_to, business_date_from, mc.business_infinite_datetime))
    if isinstance(mc, SingleBusinessDateColumn):
        business_att = DateAttribute('business_date', mc.business_date_column.name, mc.business_date_column.type, _table_of(mc.business_date_column), join_node)
        return LogicalOperation(business_att >= business_date_from, LogicalOperator.AND, business_att <= business_date_to)
    return None


def build_milestoning_filter_operation_for_date_range(
        business_date_from: datetime.date | None, business_date_to: datetime.date | None,
        processing_datetime: datetime.datetime | datetime.date | None,
        table: MilestonedTable | TableAlias, join_node: 'JoinTreeNodeOperation | None' = None,
        processing_datetime_to: datetime.datetime | datetime.date | None = None) -> 'Operation | None':
    """Like build_milestoning_filter_operation but applies a business-date range filter.

    For BusinessDateRangeColumns the range uses interval overlap:
        business_date_from_col <= business_date_to AND business_date_to_col > business_date_from
    For single-date columns:
        business_date >= business_date_from AND business_date <= business_date_to
    Processing-date filtering is point-in-time (unchanged from the normal path), unless ``processing_datetime_to`` is
    given: then ``processing_datetime`` to ``processing_datetime_to`` is a range, which a processing range overlaps and
    a processing snapshot column is within.
    """
    mc = milestoning_columns_of(table)
    assert mc is not None, f"{table.name} is not milestoned"
    if processing_datetime is not None and processing_datetime_to is not None:
        processing = _processing_range_predicate(mc, processing_datetime, processing_datetime_to, join_node)
    else:
        processing = _processing_predicate(mc, processing_datetime, join_node)
    return _and_all([processing, _business_range_predicate(mc, business_date_from, business_date_to, join_node)])


def build_milestoning_filter_operation(business_date: datetime.date | None,
                                       processing_datetime: datetime.datetime | datetime.date | None,
                                       table: MilestonedTable | TableAlias,
                                       join_node: 'JoinTreeNodeOperation | None' = None) -> 'Operation | None':
    """The milestoning filter for a point in time: the processing range, then the business range, ANDed together. The relation is a
    milestoned table, or a query that a class is mapped to with a milestoning scheme."""
    #TODO this should not reference attribute
    mc = milestoning_columns_of(table)
    assert mc is not None, f"{table.name} is not milestoned"
    return _and_all([_processing_predicate(mc, processing_datetime, join_node),
                     _business_predicate(mc, business_date, join_node)])

def find_column(operation: RelationalOperationElement) -> ColumnWithJoin:
    if isinstance(operation, UnaryOperation):
        return find_column(operation.element)
    elif isinstance(operation, ColumnWithJoin):
        return operation
    else:
        raise TypeError(operation)

def _build_join_kwargs_filter(node, filter_kwargs: dict, filter_column_map: dict):
    """Translate raw property-equality kwargs into a relational filter for a join node.

    ``filter_column_map`` maps property IDs to ``(col_name, col_type, table)`` tuples.
    Multiple kwargs are combined with AND.  Raises ``ValueError`` for unknown keys.
    """
    import decimal
    ops = []
    for key, value in filter_kwargs.items():
        if key not in filter_column_map:
            raise ValueError(f"Unknown property '{key}' — valid properties are: {sorted(filter_column_map)}")
        col_name, col_type, table = filter_column_map[key]
        col = ColumnWithJoin(Column(col_name, col_type, table), node)  # table: the relation the column is of
        if isinstance(value, bool):
            const = BooleanConstantOperation(value)
        elif isinstance(value, int):
            const = IntegerConstantOperation(value)
        elif isinstance(value, float):
            const = FloatConstantOperation(value)
        elif isinstance(value, decimal.Decimal):
            const = DecimalConstantOperation(value)
        elif isinstance(value, datetime.datetime):
            const = DateTimeConstantOperation(value)
        elif isinstance(value, datetime.date):
            const = DateConstantOperation(value)
        else:
            const = StringConstantOperation(str(value))
        ops.append(ComparisonOperation(col, ComparisonOperator.EQUAL, const))
    result = ops[0]
    for op in ops[1:]:
        result = LogicalOperation(result, LogicalOperator.AND, op)
    return result


def collect_required_joins(op: RelationalOperationElement | None, required_joins: dict,
                            node_refs: dict | None = None):
    """Walk an operation tree collecting the JoinTreeNodeOperations it depends on.

    When ``node_refs`` is provided, also records — per node id — every Attribute/ColumnWithJoin
    that references that node, so callers can decide whether *all* references to a node can be
    satisfied by an embedded (join-less) alternate (see _compute_elidable_nodes).
    """
    if op is None:
        return
    if isinstance(op, Attribute):
        parent = op.parent()
        if parent is not None:
            required_joins[id(parent)] = parent
            if node_refs is not None:
                node_refs.setdefault(id(parent), []).append(op)
    elif isinstance(op, ColumnWithJoin):
        if op.parent is not None:
            required_joins[id(op.parent)] = op.parent
            if node_refs is not None:
                node_refs.setdefault(id(op.parent), []).append(op)
    elif isinstance(op, AggregateOperation):
        collect_required_joins(op.element, required_joins, node_refs)
        collect_required_joins(op.window, required_joins, node_refs)
        for sort_op in op.order_by:
            collect_required_joins(sort_op.column, required_joins, node_refs)
    elif isinstance(op, CaseOperation):
        collect_required_joins(op.condition, required_joins, node_refs)
        collect_required_joins(op.then, required_joins, node_refs)
        collect_required_joins(op.default, required_joins, node_refs)
    elif isinstance(op, ScalarFunctionOperation):
        collect_required_joins(op.element, required_joins, node_refs)
    elif isinstance(op, (DateExtractOperation, DateTruncOperation)):
        collect_required_joins(op.element, required_joins, node_refs)
    elif isinstance(op, DateArithmeticOperation):
        collect_required_joins(op.element, required_joins, node_refs)
    elif isinstance(op, DateDiffOperation):
        collect_required_joins(op.element, required_joins, node_refs)
        if isinstance(op.other, RelationalOperationElement):
            collect_required_joins(op.other, required_joins, node_refs)
    elif isinstance(op, WindowFunctionOperation):
        if op.element is not None:
            collect_required_joins(op.element, required_joins, node_refs)
        collect_required_joins(op.window, required_joins, node_refs)
    elif isinstance(op, WindowSpecification):
        for part in op.partition_by:
            collect_required_joins(part, required_joins, node_refs)
        for sort_op in op.order_by:
            collect_required_joins(sort_op.column, required_joins, node_refs)
    elif isinstance(op, SortOperation):
        collect_required_joins(op.column, required_joins, node_refs)
    elif isinstance(op, Alias):
        collect_required_joins(op.element, required_joins, node_refs)
    elif isinstance(op, LogicalOperation):
        collect_required_joins(op.left, required_joins, node_refs)
        collect_required_joins(op.right, required_joins, node_refs)
    elif isinstance(op, ComparisonOperation):
        collect_required_joins(op.left, required_joins, node_refs)
        collect_required_joins(op.right, required_joins, node_refs)
    elif isinstance(op, (IsNullOperation, IsNotNullOperation, InOperation, NotInOperation, NotOperation)):
        collect_required_joins(op.element, required_joins, node_refs)
    elif isinstance(op, ArithmeticOperation):
        collect_required_joins(op.left, required_joins, node_refs)
        collect_required_joins(op.right, required_joins, node_refs)
    elif isinstance(op, ConcatOperation):
        for part in op.parts:
            collect_required_joins(part, required_joins, node_refs)
    elif isinstance(op, (ExistsOperation, NotExistsOperation)):
        # The join must be constrained like any other. It isn't recorded in node_refs: it is not an
        # attribute reference, so it doesn't affect whether other references to the node can be embedded.
        required_joins[id(op.node)] = op.node


def _embedded_of(ref: RelationalOperationElement):
    """Return the join-less alternate for an Attribute (a method) or ColumnWithJoin (a field)."""
    if isinstance(ref, Attribute):
        return ref.embedded()
    if isinstance(ref, ColumnWithJoin):
        return ref.embedded
    return None


def _compute_elidable_nodes(node_refs: dict) -> set:
    """Return the ids of join-tree nodes where every reference has a usable embedded alternate.

    A ColumnWithJoin/Attribute built without an ``embedded`` alternate (e.g. a milestoning column,
    or a raw filter kwarg column) forces the join to stay for every reference to that node —
    "if one projected property isn't embedded, take all of them from the join instead".
    """
    elidable = set()
    for node_id, refs in node_refs.items():
        if refs and all(_embedded_of(ref) is not None for ref in refs):
            elidable.add(node_id)
    return elidable

def _required_joins(query: QuerySpecification, node_refs: dict | None = None) -> dict:
    """Join-tree nodes the query's expressions read through, keyed by node id."""
    required_joins: dict = {}
    for expression in query.expressions():
        collect_required_joins(expression, required_joins, node_refs)
    return required_joins


def _joined_nodes(query: QuerySpecification) -> list[JoinTreeNodeOperation]:
    """Every join the query needs, ancestors first.

    The SQL generator joins a node's ancestors whenever it joins the node, so each of them needs its
    join filter set too, not only the nodes an expression refers to directly.
    """
    ordered: dict = {}

    def add(node: JoinTreeNodeOperation):
        if id(node) in ordered:
            return
        if node.parent is not None:
            add(node.parent)
        ordered[id(node)] = node

    for node in _required_joins(query).values():
        add(node)
    return list(ordered.values())


def _apply_join_filters(query: QuerySpecification,
                        milestoning_condition: Callable[[JoinTreeNodeOperation], Operation | None]) -> None:
    """Set ``query.join_filters`` to each joined node's ON-clause condition: the join's own fixed filter and its raw
    kwargs filter ANDed with ``milestoning_condition(node)``.

    Join objects are shared between queries, so the conditions are kept on the query and the join is not modified.
    """
    query.join_filters = {}
    for node in _joined_nodes(query):
        parts = []
        if node.join.filter is not None:
            parts.append(node.join.filter)
        if node.join.filter_kwargs:
            parts.append(_build_join_kwargs_filter(node, node.join.filter_kwargs, node.join.filter_column_map))
        milestoned = milestoning_condition(node)
        if milestoned is not None:
            parts.append(milestoned)
        if parts:
            combined = parts[0]
            for part in parts[1:]:
                combined = LogicalOperation(combined, LogicalOperator.AND, part)
            query.join_filters[id(node)] = combined


def _exists_only_nodes(query: QuerySpecification) -> list[JoinTreeNodeOperation]:
    """The to-many steps the query reaches only to ask whether a row exists (``exists`` or ``not_exists``).

    Joining such a step repeats the parent once per matching row. A step the query also reads a value through is left as a
    join: those values are one row per child, and the existence then holds for a child with those values. Nor can a step
    stand for a query when a step below it is reached through it, or when its join has a filter object of its own, whose
    columns are not known to be the table's. A step given property values, as in ``product(id=2)``, can: they are
    conditions on the table. A to-one step cannot repeat a row, so it stays a join.
    """
    required: dict = {}
    node_refs: dict = {}
    expressions = list(query.expressions())
    having = query.having
    if having is not None:
        expressions.append(having)
    for expression in expressions:
        collect_required_joins(expression, required, node_refs)
    ancestors: set = set()
    for node in required.values():
        parent = node.parent
        while parent is not None:
            ancestors.add(id(parent))
            parent = parent.parent
    return [node for node in required.values()
            if node.many and id(node) not in node_refs and id(node) not in ancestors
            and node.join.filter is None]


def _apply_sub_selects(query: QuerySpecification, business_date: datetime.date | None,
                       business_date_to: datetime.date | None,
                       processing_datetime: datetime.datetime | datetime.date | None,
                       processing_datetime_to: datetime.datetime | datetime.date | None = None) -> None:
    """Set ``query.sub_selects``: for each step only asked the existence of, ``SELECT DISTINCT <join column> FROM <its
    table>`` with the table's milestoning at the dates the step uses. Distinct on the join column makes it at most one
    row for a parent, however many rows of the table match. Property values the step was given are conditions of the
    query, on the table's own columns."""
    query.sub_selects = {}
    for node in _exists_only_nodes(query):
        key = node.join.keys()[1]
        right = Column(key.name, key.type, node.join.table, primary_key=key.primary_key)  # of the table, not of an alias of it
        bd, bd_to, pdt, pdt_to = _dates_for_node(node, business_date, business_date_to, processing_datetime,
                                                 processing_datetime_to)
        values = node.join.filter_kwargs
        condition = _build_join_kwargs_filter(None, values, node.join.filter_column_map) if values else NoOperation()
        query.sub_selects[id(node)] = build_query_specification(
            bd, pdt, [Alias(right, right.name)], node.join.table, condition, business_date_to=bd_to, distinct=True,
            processing_datetime_to=pdt_to)


def _dates_for_node(node: JoinTreeNodeOperation, business_date: datetime.date | None,
                    business_date_to: datetime.date | None, processing_datetime: datetime.datetime | datetime.date | None,
                    processing_datetime_to: datetime.datetime | datetime.date | None = None):
    """The business date, its range end, the processing time and its range end for the table a step reaches.

    A step uses the dates it was given, else those of the nearest step above it that was given any, else the query's.
    Each axis is taken separately, and a date given to a step is a point in time, so it ends any range the query asked
    for on that axis.
    """
    explicit_business = explicit_processing = None
    step: JoinTreeNodeOperation | None = node
    while step is not None:
        if explicit_business is None:
            explicit_business = step.business_date
        if explicit_processing is None:
            explicit_processing = step.processing_datetime
        step = step.parent
    if explicit_business is not None:
        business_date, business_date_to = explicit_business, None
    if explicit_processing is not None:
        processing_datetime, processing_datetime_to = explicit_processing, None
    return business_date, business_date_to, processing_datetime, processing_datetime_to


def build_query_specification(business_date: datetime.date | None, processing_datetime: datetime.datetime | datetime.date | None,
                         columns: Sequence[RelationalOperationElement], table: Relation, op: Operation,
                         order_by: list[SortOperation] | None = None, group_by: list | None = None,
                         limit: int | None = None, business_date_to: datetime.date | None = None,
                         distinct: bool = False, offset: int | None = None,
                         having: Operation | None = None,
                         processing_datetime_to: datetime.datetime | datetime.date | None = None) -> OrderedQuerySpecification:
    """The query for the rows asked of ``table``, as of the dates. ``filter`` of the query is the ``op`` that was asked, and what the
    dates add to it, by the milestoning of the table, is ``as_of.filter``."""
    as_of = None
    if milestoning_columns_of(table) is not None:
        assert isinstance(table, (MilestonedTable, TableAlias))
        if business_date_to is not None or processing_datetime_to is not None:
            as_of = build_milestoning_filter_operation_for_date_range(
                business_date, business_date_to, processing_datetime, table,
                processing_datetime_to=processing_datetime_to)
        else:
            as_of = build_milestoning_filter_operation(business_date, processing_datetime, table)

    query = OrderedQuerySpecification(columns, op, order_by or [], group_by or [], limit, table, distinct, offset, having)
    query.as_of = AsOf(business_date, business_date_to, processing_datetime, processing_datetime_to, as_of)
    _derive_joins(query)
    return query


def _derive_joins(query: QuerySpecification) -> None:
    """What the joins a query needs add to it, from the columns it reads and the dates it reads them as of: the ON conditions of the
    joins, and the queries that stand for the steps it only asks the existence of. A step folded into a query can read another join."""
    as_of = query.as_of or AsOf()

    def milestoning_condition(node):
        if milestoning_columns_of(node.join.table) is None:
            return None
        assert isinstance(node.join.table, (MilestonedTable, TableAlias))
        bd, bd_to, pdt, pdt_to = _dates_for_node(node, as_of.business_date, as_of.business_date_to, as_of.processing_datetime,
                                                 as_of.processing_datetime_to)
        if bd_to is not None or pdt_to is not None:
            return build_milestoning_filter_operation_for_date_range(bd, bd_to, pdt, node.join.table, node,
                                                                     processing_datetime_to=pdt_to)
        return build_milestoning_filter_operation(bd, pdt, node.join.table, node)

    _apply_join_filters(query, milestoning_condition)
    _apply_sub_selects(query, as_of.business_date, as_of.business_date_to, as_of.processing_datetime, as_of.processing_datetime_to)


# --- the steps written after a query, folded into SELECTs ---------------------------------------------------------------------

_NUMBER_TYPES = {int: "BIGINT", float: "DOUBLE", decimal.Decimal: "DECIMAL"}

_CONSTANT_TYPES = {
    IntegerConstantOperation: "BIGINT", FloatConstantOperation: "DOUBLE", StringConstantOperation: "VARCHAR",
    DateConstantOperation: "DATE", DateTimeConstantOperation: "TIMESTAMP", DecimalConstantOperation: "DECIMAL",
    BooleanConstantOperation: "BOOLEAN", IntervalConstantOperation: "INTERVAL",
    CurrentDateOperation: "DATE", CurrentTimestampOperation: "TIMESTAMP",
}


def _column_name(element: RelationalOperationElement) -> str:
    """The name a column of a result has for the step after it: the name it was given, or its display name in snake_case
    (as a view names its columns)."""
    if isinstance(element, Alias):
        return element.name
    if isinstance(element, Attribute) and isinstance(element.column().owner, TableAlias):
        return element.column().name  # a column that a result already named: it keeps that name
    return to_snake_case(display_alias(element))


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
        if element.operator in (AggregateOperator.MIN, AggregateOperator.MAX, AggregateOperator.SUM, AggregateOperator.PERCENTILE_DISC):
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
    if _is_condition(element):  # a comparison, a test for null, an IN ...: true or false
        return "BOOLEAN"
    return "VARCHAR"



def _sort_target(sort_op: SortOperation) -> tuple:
    """What a sort orders by: a column and the joins to reach it. Any other expression, such as an aggregate, is its
    own target, so sorting by an expression again adds a sort."""
    column = sort_op.column
    if isinstance(column, Alias):
        return ("alias", column.name)
    if not isinstance(column, ColumnWithJoin):
        return ("expression", id(column))
    joins = []
    node = column.parent
    while node is not None:
        joins.append(node.join.name)
        node = node.parent
    return column.column.owner_name, column.column.name, tuple(joins)



def _column_of(element) -> 'Column | None':
    """The column an attribute, or a column reached through joins, reads; none for any other expression."""
    if isinstance(element, Attribute):
        return element.column()
    if isinstance(element, ColumnWithJoin):
        return element.column
    return None


def display_columns(columns: Sequence) -> list[Alias]:
    """The columns of a query that something reads from, each under the name that reads it."""
    return [c if isinstance(c, Alias) else Alias(c, _column_name(c)) for c in columns]


def labelled(query: QuerySpecification) -> QuerySpecification:
    """The query as something reads from it: its columns under the names that read them."""
    result = copy.copy(query)
    result.columns = display_columns(query.columns)
    return result


relation_numbers = itertools.count(1)

# A SELECT applies DISTINCT, then the order, then the offset, then the limit, in that order only. A step that comes next, or again, is folded
# into the SELECT it is written after; one that comes earlier than the last clause applied is a SELECT over the rows that SELECT gives, as
# every other step is. The last clause a SELECT applies is read from the SELECT.
_DISTINCT, _SORT, _DROP, _TAKE = 1, 2, 3, 4


def _stage(query: QuerySpecification) -> int:
    if not isinstance(query, OrderedQuerySpecification):
        return _DISTINCT if query.distinct else 0
    if query.limit is not None:
        return _TAKE
    if query.offset is not None:
        return _DROP
    return _SORT if query.order_by else (_DISTINCT if query.distinct else 0)


def accepts_group_by(query: QuerySpecification) -> bool:
    """Whether group by and having can still be applied: nothing that comes after them (distinct, an order, an offset, a limit) has been
    applied yet."""
    return _stage(query) == 0


def _with_clauses(query: OrderedQuerySpecification, **changes) -> OrderedQuerySpecification:
    """The query with some of its clauses changed (``order_by``, ``limit``, ``offset``, ``distinct``, ``group_by``, ``having``). A sort can read a join the query did not, so what the joins add is worked out again."""
    result = copy.copy(query)
    for name, value in changes.items():
        setattr(result, name, value)
    _derive_joins(result)
    return result


def select_over(query: QuerySpecification) -> tuple[OrderedQuerySpecification, TableAlias]:
    """A SELECT of every column of ``query``, which it reads under a name of its own."""
    alias = TableAlias(labelled(query), f"relation{next(relation_numbers)}")
    columns = [Alias(alias.column(_column_name(c), _column_type(c)), _column_name(c)) for c in query.columns]
    return build_query_specification(None, None, columns, alias, NoOperation()), alias


def _own_sorts(query: QuerySpecification, alias: TableAlias, sorts: Sequence[SortOperation]) -> list[SortOperation]:
    """The sorts, which are of the columns ``query`` shows, as a SELECT over it reads them: the columns of ``alias``."""
    result = []
    for sort in sorts:
        found = None
        ref = _column_of(sort.column)
        for shown in query.columns:
            element = shown.element if isinstance(shown, Alias) else shown
            if shown is sort.column or element is sort.column or (ref is not None and _column_of(element) is ref):
                found = shown
                break
        if found is None:
            names = [_column_name(c) for c in query.columns]
            raise ValueError(f"The rows after a limit or an offset are ordered by the columns of the result: {names} "
                             "(result[\"name\"].ascending())")
        name = _column_name(found)
        result.append(SortOperation(ColumnWithJoin(alias.column(name, _column_type(found)), None), sort.direction, sort.nulls_first))
    return result


def _orders_by_what_it_shows(query: OrderedQuerySpecification) -> bool:
    """Whether every sort of the query is of a column it shows, as SELECT DISTINCT ... ORDER BY needs."""
    for sort in query.order_by:
        ref = _column_of(sort.column)
        if not any((shown.element if isinstance(shown, Alias) else shown) is sort.column
                   or (ref is not None and _column_of(shown.element if isinstance(shown, Alias) else shown) is ref) for shown in query.columns):
            return False
    return True


def made_distinct(query: OrderedQuerySpecification) -> OrderedQuerySpecification:
    """Each distinct row of the query once. A SELECT is distinct and ordered at once, so a query that is only ordered, by columns it shows, is distinct in
    its own select."""
    if _stage(query) > _DISTINCT and not (_stage(query) == _SORT and _orders_by_what_it_shows(query)):
        query = select_over(query)[0]
    return _with_clauses(query, distinct=True)


def sorted_by(query: OrderedQuerySpecification, sorts: Sequence[SortOperation]) -> OrderedQuerySpecification:
    """The rows of the query in the order of ``sorts``, which are of the columns it shows. A column sorted again is ordered as the later sort
    asks, and the earlier sorts order the rows it leaves tied."""
    sorts = list(sorts)
    if _stage(query) > _SORT:
        inner = query
        query, alias = select_over(inner)
        sorts = _own_sorts(inner, alias, sorts)
    resorted = {_sort_target(s) for s in sorts}
    return _with_clauses(query, order_by=[*sorts, *(s for s in query.order_by if _sort_target(s) not in resorted)])


def dropped(query: OrderedQuerySpecification, count: int) -> OrderedQuerySpecification:
    """The rows of the query after the first ``count`` of them."""
    if _stage(query) >= _TAKE:
        query = select_over(query)[0]
    return _with_clauses(query, offset=count if query.offset is None else query.offset + count)


def taken(query: OrderedQuerySpecification, count: int) -> OrderedQuerySpecification:
    """The first ``count`` rows of the query."""
    return _with_clauses(query, limit=count if query.limit is None else min(query.limit, count))


def sql_format_datetime(value:datetime.datetime) -> str:
    return value.strftime("'%Y-%m-%d %H:%M:%S'")

def sql_format_date(value:datetime.date) -> str:
    return value.strftime("'%Y-%m-%d'")

LOGICAL_OPERATOR_STR = {
    LogicalOperator.AND: ' AND ',
    LogicalOperator.OR: ' OR '
}

COMPARISON_OPERATOR_STR = {
    ComparisonOperator.EQUAL: ' = ',
    ComparisonOperator.LESS_THAN: ' < ',
    ComparisonOperator.GREATER_THAN: ' > ',
    ComparisonOperator.LESS_THAN_OR_EQUAL_TO: ' <= ',
    ComparisonOperator.GREATER_THAN_OR_EQUAL_TO: ' >= ',
    ComparisonOperator.NOT_EQUAL: ' <> ',
    ComparisonOperator.LIKE: ' LIKE ',
    ComparisonOperator.NOT_LIKE: ' NOT LIKE ',
    ComparisonOperator.MATCHES: ' MATCHES ',
}

def needs_parentheses(parent: LogicalOperation, child: RelationalOperationElement) -> bool:
    """Whether ``child`` must be parenthesised where it is an operand of ``parent``.

    A logical operation nested in one with a different operator is parenthesised: OR inside AND would otherwise
    change meaning, and AND inside OR is easier to read. The same operator needs none, since AND and OR are
    associative, so a AND (b AND c) is a AND b AND c.
    """
    return isinstance(child, LogicalOperation) and child.operator != parent.operator


# Comparisons that are never true for a missing value. These are guarded on an optional property with IS NOT NULL;
# it does not guard = (which a missing value never matches either). A <> against a value is not guarded but widened: <> or IS NULL.
_GUARDED_COMPARISONS = (
    ComparisonOperator.LESS_THAN, ComparisonOperator.GREATER_THAN, ComparisonOperator.LESS_THAN_OR_EQUAL_TO,
    ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, ComparisonOperator.LIKE,
)


_NEGATED_EQUALITY = {ComparisonOperator.EQUAL: ComparisonOperator.NOT_EQUAL, ComparisonOperator.NOT_EQUAL: ComparisonOperator.EQUAL}


def logical_operator_string(op:LogicalOperator) -> str:
    return LOGICAL_OPERATOR_STR[op]

def comparison_operator_string(op:ComparisonOperator) -> str:
    return COMPARISON_OPERATOR_STR[op]

def sql_string_literal(value: str) -> str:
    """A string as a SQL literal: a quote inside it is doubled, so a value can never end the literal early."""
    return "'" + value.replace("'", "''") + "'"


def constant_value_string(op:ConstantOperation) -> str:
    if isinstance(op, StringConstantOperation):
        return sql_string_literal(op.value)
    elif isinstance(op, DateConstantOperation):
        return sql_format_date(op.value)
    elif isinstance(op, DateTimeConstantOperation):
        return sql_format_datetime(op.value)
    elif isinstance(op, IntegerConstantOperation):
        return str(op.value)
    elif isinstance(op, FloatConstantOperation):
        return str(op.value)
    elif isinstance(op, BooleanConstantOperation):
        return "TRUE" if op.value else "FALSE"
    elif isinstance(op, DecimalConstantOperation):
        return str(op.value)
    elif isinstance(op, CurrentDateOperation):
        return 'CURRENT_DATE'
    elif isinstance(op, CurrentTimestampOperation):
        return 'CURRENT_TIMESTAMP'
    elif isinstance(op, IntervalConstantOperation):
        return "INTERVAL '" + format(op.value.total_seconds(), 'f').rstrip('0').rstrip('.') + "' SECOND"
    else:
        raise ValueError

_PLAIN_IDENTIFIER = re.compile(r'[A-Za-z_][A-Za-z0-9_]*$')


# The SQL the generator writes is Postgres SQL (which is ANSI SQL where ANSI has the construct). Every engine translates it to the SQL of
# its own database with sqlglot, reading it as this dialect.
SOURCE_DIALECT = "postgres"


_DIALECTS: dict[str, Any] = {"postgres": PgVector}
# The module of the engine that registers the dialect of a database, which is imported the first time SQL is written for that dialect, so that
# a view or a changelog written for Databricks is written as the Databricks engine writes it. An engine that is not installed is not needed.
_DIALECT_MODULES: dict[str, str] = {"databricks": "datafinder_databricks.databricks_engine", "spark": "datafinder_spark.spark_engine",
                                    "duckdb": "datafinder_duckdb.duckdb_dialect"}


def register_dialect(name: str, dialect: Any) -> None:
    """Have ``sql_for_dialect`` write ``name`` with ``dialect`` (a sqlglot dialect) in place of sqlglot's own, as an engine needs it to."""
    _DIALECTS[name] = dialect


def sql_for_dialect(sql: str, dialect: str) -> str:
    """The generated SQL as ``dialect`` (a sqlglot dialect: ``duckdb``, ``databricks``, ...) spells it."""
    if dialect not in _DIALECTS and dialect in _DIALECT_MODULES:
        try:
            importlib.import_module(_DIALECT_MODULES[dialect])  # registers the dialect
        except ImportError:
            pass  # the engine is not installed: sqlglot's own dialect
    return sqlglot.transpile(sql, read=PgVector, write=_DIALECTS.get(dialect, dialect))[0]


_DAY_NAMES = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')


def _date_diff_string(unit: DatePart, element: str, other: str) -> str:
    """The number of ``unit`` boundaries from ``element`` to ``other`` (a year is a change of year, a day a change of date, a week the start of a
    week, on a Monday, an hour a change of hour), as DuckDB's DATE_DIFF counts them for all but a week, in standard operators and EXTRACT, DATE_TRUNC and CAST, which Postgres and the other
    databases all have, where DATE_DIFF is DuckDB's own."""
    if unit in (DatePart.YEAR, DatePart.MONTH):
        a, b = f'CAST({element} AS DATE)', f'CAST({other} AS DATE)'
        years = f'(EXTRACT(YEAR FROM {b}) - EXTRACT(YEAR FROM {a}))'
        if unit == DatePart.YEAR:
            return years
        return f'({years} * 12 + EXTRACT(MONTH FROM {b}) - EXTRACT(MONTH FROM {a}))'
    if unit == DatePart.DAY:
        return f'(CAST({other} AS DATE) - CAST({element} AS DATE))'
    if unit == DatePart.WEEK:  # the Mondays between: the days from the Monday of one week to the Monday of the other, in weeks
        weeks = lambda date: f"CAST(DATE_TRUNC('week', CAST({date} AS DATE)) AS DATE)"  # noqa: E731
        return f'CAST(({weeks(other)} - {weeks(element)}) / 7 AS BIGINT)'
    if unit in (DatePart.HOUR, DatePart.MINUTE, DatePart.SECOND):
        part, seconds = unit.value.lower(), {DatePart.HOUR: 3600, DatePart.MINUTE: 60, DatePart.SECOND: 1}[unit]
        return (f"CAST(EXTRACT(EPOCH FROM (DATE_TRUNC('{part}', CAST({other} AS TIMESTAMP)) - "
                f"DATE_TRUNC('{part}', CAST({element} AS TIMESTAMP)))) / {seconds} AS BIGINT)")
    raise ValueError(f"A difference between dates is counted in years, months, weeks, days, hours, minutes or seconds, not {unit.value}")


def _interval_string(n: int, unit: DatePart) -> str:
    """``INTERVAL '5' DAY`` in the units ANSI has (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND): a week is 7 days and a microsecond a millionth of
    a second."""
    if unit == DatePart.WEEK:
        n, unit = n * 7, DatePart.DAY
    elif unit == DatePart.MICROSECOND:
        return "INTERVAL '" + format(decimal.Decimal(n).scaleb(-6), 'f') + "' SECOND"
    elif unit in (DatePart.QUARTER, DatePart.ISODOW, DatePart.DOY):
        raise ValueError(f"A length of time is not counted in {unit.value}")
    return "INTERVAL '" + str(n) + "' " + unit.value


def _vector_literal(vector: VectorValue) -> str:
    """A vector as pgvector has it: a list of numbers cast to ``VECTOR(n)``."""
    return 'CAST(ARRAY[' + ', '.join(repr(number) for number in vector.values) + f'] AS VECTOR({len(vector)}))'


def _column_reference(alias_name: str, column_name: str) -> str:
    """``t0.name``, with the column quoted when its name is not a plain identifier (``"First Name"``): a result's columns have the names
    it was asked to give them."""
    if _PLAIN_IDENTIFIER.match(column_name):
        return alias_name + '.' + column_name
    return alias_name + '."' + column_name.replace('"', '""') + '"'


def table_alias_column_string(tac: TableAliasColumn) -> str:
    return _column_reference(tac.table_alias.name, tac.column.name)

_AGGREGATE_SQL_NAMES = {
    AggregateOperator.AVERAGE: 'AVG',
}

_SCALAR_SQL_NAMES = {
    ScalarFunction.ABS:       'ABS',
    ScalarFunction.CEILING:   'CEILING',
    ScalarFunction.FLOOR:     'FLOOR',
    ScalarFunction.MOD:       'MOD',
    ScalarFunction.POWER:     'POWER',
    ScalarFunction.SQRT:      'SQRT',
    ScalarFunction.ROUND:     'ROUND',
    ScalarFunction.UPPER:     'UPPER',
    ScalarFunction.LOWER:     'LOWER',
    ScalarFunction.TRIM:      'TRIM',
    ScalarFunction.LTRIM:     'LTRIM',
    ScalarFunction.RTRIM:     'RTRIM',
    ScalarFunction.LENGTH:    'LENGTH',
    ScalarFunction.REVERSE:   'REVERSE',
    ScalarFunction.LEFT:      'LEFT',
    ScalarFunction.RIGHT:     'RIGHT',
    ScalarFunction.REPEAT:    'REPEAT',
    ScalarFunction.REPLACE:   'REPLACE',
    ScalarFunction.SUBSTRING: 'SUBSTRING',
    ScalarFunction.COALESCE:  'COALESCE',
    ScalarFunction.SIN:       'SIN',
    ScalarFunction.COS:       'COS',
    ScalarFunction.TAN:       'TAN',
    ScalarFunction.ASIN:      'ASIN',
    ScalarFunction.ACOS:      'ACOS',
    ScalarFunction.ATAN:      'ATAN',
    ScalarFunction.ATAN2:     'ATAN2',
    ScalarFunction.EXP:       'EXP',
    ScalarFunction.LOG:       'LN',
    ScalarFunction.COSINE_DISTANCE: 'COSINE_DISTANCE',  # pgvector's function of that name
    ScalarFunction.L2_DISTANCE:     'L2_DISTANCE',
    ScalarFunction.INNER_PRODUCT:   'INNER_PRODUCT',
}

_WINDOW_SQL_NAMES = {
    WindowFunction.ROW_NUMBER:  'ROW_NUMBER',
    WindowFunction.RANK:        'RANK',
    WindowFunction.DENSE_RANK:  'DENSE_RANK',
    WindowFunction.NTILE:       'NTILE',
    WindowFunction.LAG:         'LAG',
    WindowFunction.LEAD:        'LEAD',
    WindowFunction.FIRST_VALUE: 'FIRST_VALUE',
    WindowFunction.LAST_VALUE:  'LAST_VALUE',
    WindowFunction.CUME_DIST:   'CUME_DIST',
    WindowFunction.PERCENT_RANK:'PERCENT_RANK',
}

def _sort_suffix(sort: SortOperation) -> str:
    """`` ASC`` or `` DESC`` and where a missing value goes, written every time. The SQL is Postgres, where it is last when ascending and first
    when descending, and each engine it is translated for has its own default, so the sort says it: the same rows come in the same order
    on every engine."""
    return (' ASC' if sort.direction == SortDirection.ASC else ' DESC') + (' NULLS FIRST' if sort.nulls_first else ' NULLS LAST')


def _window_spec_to_string(window: WindowSpecification) -> str:
    parts = []
    if window.partition_by:
        parts.append('PARTITION BY ' + ', '.join(sql_operation_to_string(part) for part in window.partition_by))
    if window.order_by:
        parts.append('ORDER BY ' + ', '.join(
            sql_operation_to_string(sort.column) + _sort_suffix(sort)
            for sort in window.order_by
        ))
    return 'OVER (' + ' '.join(parts) + ')'

def _selected_constant_string(operation: ConstantOperation) -> str:
    """A constant in the select list. A bare quoted literal would come back as text, so dates are typed."""
    if isinstance(operation, DateTimeConstantOperation):
        return 'TIMESTAMP ' + constant_value_string(operation)
    if isinstance(operation, DateConstantOperation):
        return 'DATE ' + constant_value_string(operation)
    return constant_value_string(operation)


_ARITHMETIC_PRECEDENCE = {ArithmeticOperator.ADD: 1, ArithmeticOperator.SUBTRACT: 1,
                          ArithmeticOperator.MULTIPLY: 2, ArithmeticOperator.DIVIDE: 2}


def arithmetic_needs_parentheses(parent: ArithmeticOperation, child: RelationalOperationElement, on_right: bool) -> bool:
    """Whether ``child`` must be parenthesised as an operand of ``parent``: it binds more loosely than the parent, or
    it is the right operand of a - or / and binds equally (a - (b - c) is not a - b - c)."""
    if not isinstance(child, ArithmeticOperation):
        return False
    here, there = _ARITHMETIC_PRECEDENCE[parent.operator], _ARITHMETIC_PRECEDENCE[child.operator]
    return there < here or (there == here and on_right and parent.operator in (ArithmeticOperator.SUBTRACT,
                                                                                 ArithmeticOperator.DIVIDE))


def _operand_string(operation: RelationalOperationElement) -> str:
    return constant_value_string(operation) if isinstance(operation, ConstantOperation) \
        else sql_operation_to_string(operation)


class _ProjectionJoin:
    """The key of the join that a node has for what the query reads outside its WHERE, where the node is joined twice."""

    def __init__(self, node: JoinTreeNodeOperation):
        self.node = node


def _is_milestoned_step(node: JoinTreeNodeOperation) -> bool:
    return milestoning_columns_of(node.join.table) is not None


def _split_milestoned_joins(query: QuerySpecification, asked: Operation, elidable: set) -> list[JoinTreeNodeOperation]:
    """The steps that are joined once for the WHERE and once for the rest of the query, as Legend joins them.

    A to-one milestoned step reached through another milestoned step, whose columns the WHERE reads and the select (or a group by, an order
    or a having) reads too, is two independent joins: the filter holds for one row of the table and the column is read from another. Where the
    table has several rows for the key, as the sets of a union do, the rows of the query are each of the one multiplied by each of the other.
    The step it is reached through, and any step that is to-many, not milestoned, or reached through none, stays one join that both read: for
    a to-many step the filter and the values read are of the same row, as a query over its rows means.
    """
    in_where: dict = {}
    collect_required_joins(asked, {}, in_where)
    if not in_where:
        return []
    read: dict = {}
    reached: dict = {}
    reads = [*query.columns, *query.group_by, *(sort.column for sort in getattr(query, "order_by", [])), query.having]
    for expression in reads:
        collect_required_joins(expression, reached, read)
    split: dict = {}
    for node_id in in_where:
        node = reached.get(node_id)
        if node is None or node_id in elidable or node_id not in read or node.many or not _is_milestoned_step(node):
            continue
        ancestor = node.parent
        while ancestor is not None and not _is_milestoned_step(ancestor):
            ancestor = ancestor.parent
        if ancestor is not None:
            split[node_id] = node
    # a step that is read through a step that is joined twice is joined with that one
    grew = True
    while grew:
        grew = False
        for node_id, node in reached.items():
            if node_id not in split and node.parent is not None and id(node.parent) in split and node_id not in elidable:
                split[node_id] = node
                grew = True
    return list(split.values())


class _RenderedSql(RelationalOperationElement):
    """A condition already written as SQL, to be selected as a column. A condition is rendered where its columns can
    be resolved to table aliases, which is while the query is being built, not when its text is put together."""

    def __init__(self, sql: str):
        super().__init__()
        self.sql = sql


def _is_condition(op: RelationalOperationElement) -> bool:
    return isinstance(op, (ComparisonOperation, LogicalOperation, IsNullOperation, IsNotNullOperation, InOperation,
                           NotInOperation, ExistsOperation, NotExistsOperation, RelationExistsOperation, NotOperation))


def sql_operation_to_string(operation: RelationalOperationElement) -> str:
    if isinstance(operation, TableAliasColumn):
        return table_alias_column_string(operation)
    elif isinstance(operation, AggregateOperation) and operation.operator == AggregateOperator.COUNT_DISTINCT:
        return 'COUNT(DISTINCT ' + sql_operation_to_string(operation.element) + ')'
    elif isinstance(operation, AggregateOperation) and operation.operator == AggregateOperator.STRING_AGG:
        sql = 'STRING_AGG(' + sql_operation_to_string(operation.element) + ", '" + (operation.separator or '').replace("'", "''") + "'"
        if operation.order_by:
            sql += ' ORDER BY ' + ', '.join(
                sql_operation_to_string(s.column) + _sort_suffix(s)
                for s in operation.order_by)
        sql += ')'
        if operation.window is not None:
            sql += ' ' + _window_spec_to_string(operation.window)
        return sql
    elif isinstance(operation, AggregateOperation) and operation.operator in (AggregateOperator.PERCENTILE_CONT, AggregateOperator.PERCENTILE_DISC):
        # ANSI's ordered-set aggregate: the fraction is the argument, and the values it is taken from are ordered in the group. A continuous
        # percentile interpolates, so it takes the values as floating point
        values = sql_operation_to_string(operation.element)
        if operation.operator == AggregateOperator.PERCENTILE_CONT:
            values = f'CAST({values} AS DOUBLE PRECISION)'
        function = 'PERCENTILE_CONT' if operation.operator == AggregateOperator.PERCENTILE_CONT else 'PERCENTILE_DISC'
        return f"{function}({operation.percentile!r}) WITHIN GROUP (ORDER BY {values} {'DESC' if operation.from_largest else 'ASC'})"
    elif isinstance(operation, AggregateOperation):
        fn = _AGGREGATE_SQL_NAMES.get(operation.operator, operation.operator.name)
        sql = fn + '(' + sql_operation_to_string(operation.element) + ')'
        if operation.window is not None:
            sql += ' ' + _window_spec_to_string(operation.window)
        return sql
    elif isinstance(operation, WindowFunctionOperation):
        fn = _WINDOW_SQL_NAMES[operation.function]
        parts = []
        if operation.element is not None:
            parts.append(sql_operation_to_string(operation.element))
        if operation.second_arg is not None:
            parts.append(str(operation.second_arg))
        for arg in operation.extra_args:
            parts.append(sql_string_literal(arg) if isinstance(arg, str) else str(arg))
        sql = fn + '(' + ', '.join(parts) + ')'
        sql += ' ' + _window_spec_to_string(operation.window) if operation.window is not None else ' OVER ()'
        return sql
    elif isinstance(operation, ScalarFunctionOperation) and operation.function == ScalarFunction.DAY_NAME:
        # the name of the day is spelled out, as there is no standard function for it
        whens = ' '.join(f"WHEN {number} THEN '{name}'" for number, name in enumerate(_DAY_NAMES, 1))
        return f'CASE EXTRACT(ISODOW FROM {sql_operation_to_string(operation.element)}) {whens} END'
    elif isinstance(operation, ScalarFunctionOperation):
        fn = _SCALAR_SQL_NAMES[operation.function]
        parts = [sql_operation_to_string(operation.element)]
        if operation.second_arg is not None:
            parts.append(str(operation.second_arg))
        for arg in operation.extra_args:
            parts.append(sql_string_literal(arg) if isinstance(arg, str) else _vector_literal(arg) if isinstance(arg, VectorValue) else str(arg))
        sql = fn + '(' + ', '.join(parts) + ')'
        return f'CAST({sql} AS {operation.cast})' if operation.cast else sql
    elif isinstance(operation, DateExtractOperation):
        return 'EXTRACT(' + operation.part.value + ' FROM ' + sql_operation_to_string(operation.element) + ')'
    elif isinstance(operation, RecentDayOfWeekOperation):
        date = 'CAST(' + sql_operation_to_string(operation.element) + ' AS DATE)'
        # the days back to that day of the week: 0 to 6 on or before the date, 1 to 7 before it
        back = f'((EXTRACT(ISODOW FROM {date}) - {operation.day.value} + {7 if operation.inclusive else 6}) % 7' + ('' if operation.inclusive else ' + 1') + ')'
        return f'({date} - CAST({back} AS INTEGER))'
    elif isinstance(operation, DateTruncOperation):
        return "CAST(DATE_TRUNC('" + operation.part.value.lower() + "', " + sql_operation_to_string(operation.element) + ') AS DATE)'
    elif isinstance(operation, DateArithmeticOperation):
        op = '+' if operation.is_add else '-'
        return sql_operation_to_string(operation.element) + ' ' + op + ' ' + _interval_string(operation.n, operation.unit)
    elif isinstance(operation, DateDiffOperation):
        if isinstance(operation.other, RelationalOperationElement):
            other_sql = sql_operation_to_string(operation.other)
        elif isinstance(operation.other, datetime.datetime):
            other_sql = sql_format_datetime(operation.other)
        else:
            other_sql = sql_format_date(operation.other)
        return _date_diff_string(operation.unit, sql_operation_to_string(operation.element), other_sql)
    elif isinstance(operation, CountAllOperation):
        return 'COUNT(*)'
    elif isinstance(operation, ConstantOperation):
        return _selected_constant_string(operation)
    elif isinstance(operation, ConcatOperation):
        return ' || '.join(_operand_string(part) for part in operation.parts)
    elif isinstance(operation, IntervalOperation):  # the datetimes are typed, as a select list's constants are
        return sql_operation_to_string(operation.left) + ' - ' + sql_operation_to_string(operation.right)
    elif isinstance(operation, ArithmeticOperation):
        left = _operand_string(operation.left)
        right = _operand_string(operation.right)
        if arithmetic_needs_parentheses(operation, operation.left, False):
            left = '(' + left + ')'
        if arithmetic_needs_parentheses(operation, operation.right, True):
            right = '(' + right + ')'
        return left + ' ' + operation.operator.value + ' ' + right
    elif isinstance(operation, _RenderedSql):
        return operation.sql
    elif isinstance(operation, Alias):
        return sql_operation_to_string(operation.element) + ' AS "' + operation.name + '"'
    else:
        raise TypeError(operation)

def display_alias(col: RelationalOperationElement) -> str:
    """The name a display expression gets in a SELECT when it has not been given an explicit alias."""
    if isinstance(col, CountAllOperation):
        return 'Count'
    if isinstance(col, ConstantOperation):
        return 'Constant'
    if isinstance(col, Attribute):
        return col.display_name()
    if isinstance(col, (ArithmeticOperation, ConcatOperation)):
        return col.display_name if col.display_name else 'Expression'
    if _is_condition(col) or isinstance(col, CaseOperation):
        return 'Expression'
    if isinstance(col, AggregateOperation):
        return col.display_name if col.display_name else col.operator.name + ' ' + find_column(col).column.name
    if isinstance(col, ScalarFunctionOperation):
        return col.display_name if col.display_name else col.function.name + ' ' + find_column(col).column.name
    if isinstance(col, (DateExtractOperation, DateTruncOperation, DateArithmeticOperation, DateDiffOperation)):
        return col.display_name if col.display_name else find_column(col).column.name
    if isinstance(col, WindowFunctionOperation):
        if col.display_name:
            return col.display_name
        if col.function in (WindowFunction.ROW_NUMBER, WindowFunction.RANK, WindowFunction.DENSE_RANK):
            return col.function.name.replace('_', ' ').title()
        return col.function.name + ('' if col.element is None else ' ' + find_column(col).column.name)
    return str(col)


def _reaches_to_many(op: RelationalOperationElement) -> bool:
    """Whether a value is read through a to-many association, so it repeats the row it is read for."""
    if isinstance(op, ColumnWithJoin):
        node = op.parent
        while node is not None:
            if node.many:
                return True
            node = node.parent
        return False
    if isinstance(op, (UnaryOperation, Alias)):
        return _reaches_to_many(op.element)
    if isinstance(op, BinaryOperation):
        return _reaches_to_many(op.left) or _reaches_to_many(op.right)
    return False


class SQLQueryGenerator:
    _select: list[Alias]
    _from: set[TableAlias]
    _join: list[Join]
    __table_alias_incr: int
    _where: str

    def __init__(self, first_alias: int = 0):
        self._having = ''
        self._select = []
        self._from = set()
        self._join = []
        self._group_by_parts = []
        self._order_by_parts = []
        self.__table_alias_incr = first_alias
        self.__table_aliases_by_table = {}
        self.__added_join_ids = set()
        self.__split_ids: set = set()  # nodes joined once for the WHERE and once for everything else
        self.__projection_keys: dict = {}
        self.__reading_where = False
        self.__elidable_ids: set = set()
        self._sub_selects: dict = {}
        self._root_table = None
        self._query_joins: list[JoinOperation] = []
        self._join_filters: dict = {}

    def __compute_elidable_ids(self, query: QuerySpecification) -> set:
        """Determine upfront which join nodes can be satisfied entirely by embedded (join-less)
        alternates, so every later leaf resolution can build the right TableAliasColumn directly —
        see __resolve."""
        node_refs: dict = {}
        _required_joins(query, node_refs)
        return _compute_elidable_nodes(node_refs)

    def __resolve(self, op: 'Attribute | ColumnWithJoin') -> tuple[Column, JoinTreeNodeOperation | None]:
        """Return the (column, parent) an Attribute/ColumnWithJoin should actually reference,
        substituting its embedded alternate when the join it depends on has been elided."""
        if isinstance(op, Attribute):
            column, parent = op.column(), op.parent()
        else:
            column, parent = op.column, op.parent
        if parent is not None and id(parent) in self.__elidable_ids:
            embedded = _embedded_of(op)
            if embedded is not None:
                return self.__resolve(embedded)
        return column, parent

    def __guarded(self, op: RelationalOperationElement, guard: bool = True) -> RelationalOperationElement:
        """The condition with an explicit ``IS NOT NULL`` before each ordering or pattern comparison on a value that can
        be missing, for what a query asks of a class. The guard changes no row. A ``<>`` of a column and a value also
        matches a missing value, for any column or expression: ``<> x OR IS NULL``, and one of two columns that is missing matches
        the other that is not. An ``=`` of two columns that can be missing also matches when both are. It is not added
        to a join's own filter (its milestoning dates and kwargs), which is not that, so it is never passed through here. HAVING gets
        the ``<>`` alternative only (``guard=False``): its own ordering comparisons are left as written."""
        if isinstance(op, LogicalOperation):
            return LogicalOperation(self.__guarded(op.left, guard), op.operator, self.__guarded(op.right, guard))
        if isinstance(op, NotOperation):
            negated = self.__negated(op.element)
            if negated is not None:
                return self.__guarded(negated, guard)
            return op
        if guard and isinstance(op, ComparisonOperation) and op.operator in _GUARDED_COMPARISONS \
                and isinstance(op.left, ColumnWithJoin) and self.__resolve_optional(op.left):
            return LogicalOperation(IsNotNullOperation(op.left), LogicalOperator.AND, op)
        if isinstance(op, ComparisonOperation) and op.operator == ComparisonOperator.NOT_EQUAL \
                and not isinstance(op.left, ConstantOperation) and isinstance(op.right, ConstantOperation):
            # a missing value is different from any value: <> x OR is null, for a column and for a function or other
            # expression of one, whose value is missing when a column it reads is
            return LogicalOperation(op, LogicalOperator.OR, IsNullOperation(op.left))
        if isinstance(op, ComparisonOperation) and op.operator == ComparisonOperator.NOT_EQUAL \
                and not isinstance(op.left, ConstantOperation) and not isinstance(op.right, ConstantOperation):
            # two values are different when one is missing and the other is not, as well as when they differ
            one_missing = LogicalOperation(
                LogicalOperation(IsNullOperation(op.left), LogicalOperator.AND, IsNotNullOperation(op.right)),
                LogicalOperator.OR,
                LogicalOperation(IsNotNullOperation(op.left), LogicalOperator.AND, IsNullOperation(op.right)))
            return LogicalOperation(op, LogicalOperator.OR, one_missing)
        if isinstance(op, ComparisonOperation) and op.operator == ComparisonOperator.EQUAL \
                and not isinstance(op.left, ConstantOperation) and not isinstance(op.right, ConstantOperation) \
                and self.__may_be_missing(op.left) and self.__may_be_missing(op.right):
            # two missing values are the same: a = b OR both are missing, as IS NOT DISTINCT FROM is
            both_missing = LogicalOperation(IsNullOperation(op.left), LogicalOperator.AND, IsNullOperation(op.right))
            return LogicalOperation(op, LogicalOperator.OR, both_missing)
        return op

    def __negated(self, op: RelationalOperationElement) -> RelationalOperationElement | None:
        """``NOT op`` as a condition with no NOT, when it can be, or ``None``. ``NOT (a = b)`` is ``a <> b``, so that a missing value is
        different, as the ``<>`` says; ``NOT (a <> b)`` is ``a = b``, and a NOT over AND or OR is pushed in to what they join. A NOT over an
        ordering or a pattern is left as it is: a missing value is not less than anything, and not greater either."""
        if isinstance(op, ComparisonOperation) and op.operator in _NEGATED_EQUALITY:
            return ComparisonOperation(op.left, _NEGATED_EQUALITY[op.operator], op.right)
        if isinstance(op, NotOperation):
            return op.element
        if isinstance(op, LogicalOperation):
            left, right = self.__negated(op.left), self.__negated(op.right)
            if left is None and right is None:
                return None
            left = NotOperation(op.left) if left is None else left
            right = NotOperation(op.right) if right is None else right
            flipped = LogicalOperator.OR if op.operator == LogicalOperator.AND else LogicalOperator.AND
            return LogicalOperation(left, flipped, right)
        return None

    def __may_be_missing(self, op: RelationalOperationElement) -> bool:
        """Whether ``op`` can be missing: a column that is nullable, or read through a join that can leave it out, can; a count cannot; anything else is taken to."""
        if isinstance(op, CountAllOperation) or (isinstance(op, AggregateOperation)
                                                  and op.operator in (AggregateOperator.COUNT, AggregateOperator.COUNT_DISTINCT)):
            return False
        return self.__resolve(op)[0].nullable or self.__resolve_optional(op) if isinstance(op, ColumnWithJoin) else True

    def __resolve_optional(self, op: ColumnWithJoin) -> bool:
        """Whether the value ``op`` reads can be missing, in the join or embedded form that is used for it."""
        if op.parent is not None and id(op.parent) in self.__elidable_ids and op.embedded is not None:
            return self.__resolve_optional(op.embedded)
        return op.optional

    def generate(self, query: QuerySpecification):
        self._root_table = query.table
        self._query_joins = query.joins
        self._join_filters = query.join_filters
        self.__elidable_ids = self.__compute_elidable_ids(query)
        self._sub_selects = query.sub_selects
        ordered = isinstance(query, OrderedQuerySpecification)
        order_by = query.order_by if ordered else []
        asked = query.filter
        if query.as_of is not None and query.as_of.filter is not None:
            asked = query.as_of.filter if isinstance(asked, NoOperation) else LogicalOperation(asked, LogicalOperator.AND, query.as_of.filter)
        split = _split_milestoned_joins(query, asked, self.__elidable_ids)
        self.__split_ids = {id(node) for node in split}
        self.__reading_where = True  # the joins of the WHERE come first, as the query's filter is read before its columns
        try:
            for node in split:
                self.__add_join(node)
        finally:
            self.__reading_where = False
        self.select(query.columns)
        self.__reading_where = True
        try:
            self._where = self.build_filter(self.__guarded(asked))
        finally:
            self.__reading_where = False
        required_joins: dict = {}
        for group_by_expr in query.group_by:
            self.__collect_required_joins(group_by_expr, required_joins)
        for sort_op in order_by:
            self.__collect_required_joins(sort_op.column, required_joins)
        for parent in required_joins.values():
            self.__add_join(parent)
        self._group_by_parts = self.__group_by_parts(query)
        self._having = ''
        having = query.having
        if having is not None:
            having_joins: dict = {}
            self.__collect_required_joins(having, having_joins)
            for parent in having_joins.values():
                self.__add_join(parent)
            self._having = self.build_filter(self.__guarded(having, guard=False))
        constants = {id(c.element): c.name for c in query.columns if isinstance(c, Alias) and isinstance(c.element, ConstantOperation)}
        self._order_by_parts = [
            # a constant is sorted by the name it is selected under: a literal in ORDER BY is not a column (a number is a position)
            ('"' + constants[id(s.column)].replace('"', '""') + '"' if id(s.column) in constants else self.__sort_expression(s.column))
            + _sort_suffix(s)
            for s in order_by
        ]
        self._limit = query.limit if ordered else None
        self._distinct = query.distinct
        self._offset = query.offset if ordered else None

    def __group_by_parts(self, query: QuerySpecification) -> list[str]:
        """The GROUP BY of a query: the grouping it was given, else the grouping its columns imply.

        Columns that are not aggregated are what the aggregates are computed for, so a query that selects both is
        grouped by those columns (as ``df.groupby(columns).agg(...)`` does). An aggregate read through a to-many
        association is computed for each row of the root table, so the root's primary key is grouped
        by too. A query that selects only aggregates over the root's own columns is not grouped: the aggregate covers
        every row."""
        if query.group_by or not isinstance(query, OrderedQuerySpecification):
            return [self.__attr_to_col_string(a) if isinstance(a, (Attribute, ColumnWithJoin, Column))
                    else sql_operation_to_string(self.__rewrite_operation(a)) for a in query.group_by]
        aggregates = [c for c in query.columns if contains_aggregate(c)]
        if not aggregates:
            return []
        per_root = query.table is not None and any(_reaches_to_many(c) for c in aggregates)
        keys = [self.__attr_to_col_string(c) for c in query.table.primary_key] \
            if per_root and isinstance(query.table, Table) else []
        grouped = [sql_operation_to_string(shown.element) for column, shown in zip(query.columns, self._select)
                   if not contains_aggregate_or_window(column)]
        return list(dict.fromkeys([*keys, *grouped]))

    def __sort_expression(self, expression: RelationalOperationElement) -> str:
        """What a sort orders by: a column, an expression such as an aggregate, or a column of the select by the name it is given there."""
        if isinstance(expression, Alias):
            return '"' + expression.name.replace('"', '""') + '"'
        if isinstance(expression, (ColumnWithJoin, Attribute)):
            return self.build_filter(expression)
        return sql_operation_to_string(self.__rewrite_operation(expression))

    def __in_operand(self, element: RelationalOperationElement) -> str:
        """What IN is of: a condition is bracketed, as a comparison and an AND are looser than IN."""
        sql = self.build_filter(element)
        return '(' + sql + ')' if _is_condition(element) and not isinstance(element, (InOperation, NotInOperation)) else sql

    def __attr_to_col_string(self, attr: 'Attribute | ColumnWithJoin | Column') -> str:
        if isinstance(attr, Column):
            return _column_reference(self.__table_alias_for_table(attr.owner_relation).name, attr.name)
        column, parent = self.__resolve(attr)
        ta = self.__table_alias_for_table(column.owner_relation, key=self.__join_target_key(parent) if parent is not None else None)
        return _column_reference(ta.name, column.name)

    def __join_target_key(self, parent: JoinTreeNodeOperation | None):
        """Cache key for the join target alias. Always keyed by node identity so the
        alias in SELECT matches the alias registered in __add_join. A node that is joined twice is keyed by its own
        identity where the WHERE reads it, and by another where anything else does."""
        if parent is not None and id(parent) in self.__split_ids and not self.__reading_where:
            return self.__projection_keys.setdefault(id(parent), _ProjectionJoin(parent))
        return parent

    def __table_alias_for_column(self, column: Column, parent: JoinTreeNodeOperation | None = None) -> TableAlias:
        key = self.__join_target_key(parent) if parent is not None else None
        return self.__table_alias_for_table(column.owner_relation, key=key)

    def __rewrite_window_spec(self, window: WindowSpecification | None):
        if window is None:
            return None
        partition_by = [self.__rewrite_operation(p) for p in window.partition_by]
        order_by = [SortOperation(self.__rewrite_operation(s.column), s.direction, s.nulls_first) for s in window.order_by]
        return WindowSpecification(partition_by, order_by)

    def __case_sql(self, case: CaseOperation) -> str:
        """The CASE written out now: its condition can only be rendered while the query is being built."""
        def branch(value: RelationalOperationElement) -> str:
            return constant_value_string(value) if isinstance(value, ConstantOperation) \
                else sql_operation_to_string(self.__rewrite_operation(value))
        sql = 'CASE WHEN ' + self.build_filter(self.__guarded(case.condition)) + ' THEN ' + branch(case.then)
        if case.default is not None:
            sql += ' ELSE ' + branch(case.default)
        return sql + ' END'

    def __rewrite_operation(self, op: RelationalOperationElement):
        if isinstance(op, (Attribute, ColumnWithJoin)):
            column, parent = self.__resolve(op)
            return TableAliasColumn(column, self.__table_alias_for_column(column, parent))
        elif isinstance(op, Column):
            return TableAliasColumn(op, self.__table_alias_for_table(op.owner_relation))
        elif isinstance(op, AggregateOperation):
            return AggregateOperation(self.__rewrite_operation(op.element), op.operator, op.display_name,
                                      self.__rewrite_window_spec(op.window), op.separator,
                                      [SortOperation(self.__rewrite_operation(s.column), s.direction, s.nulls_first) for s in op.order_by],
                                      op.percentile, op.from_largest)
        elif isinstance(op, ScalarFunctionOperation):
            return ScalarFunctionOperation(self.__rewrite_operation(op.element), op.function, op.display_name,
                                           second_arg=op.second_arg, extra_args=list(op.extra_args), cast=op.cast)
        elif isinstance(op, IntervalOperation):
            return IntervalOperation(self.__rewrite_operation(op.left), self.__rewrite_operation(op.right), op.display_name)
        elif isinstance(op, ArithmeticOperation):
            return ArithmeticOperation(self.__rewrite_operation(op.left), op.operator,
                                       self.__rewrite_operation(op.right), op.display_name)
        elif isinstance(op, ConcatOperation):
            return ConcatOperation([self.__rewrite_operation(part) for part in op.parts], op.display_name)
        elif isinstance(op, CaseOperation):
            return _RenderedSql(self.__case_sql(op))
        elif _is_condition(op):  # a condition inside an expression, such as the argument of an aggregate
            return _RenderedSql(self.build_filter(op))
        elif isinstance(op, DateExtractOperation):
            return DateExtractOperation(self.__rewrite_operation(op.element), op.part, op.display_name)
        elif isinstance(op, RecentDayOfWeekOperation):
            return RecentDayOfWeekOperation(self.__rewrite_operation(op.element), op.day, op.inclusive, op.display_name)
        elif isinstance(op, DateTruncOperation):
            return DateTruncOperation(self.__rewrite_operation(op.element), op.part, op.display_name)
        elif isinstance(op, DateArithmeticOperation):
            return DateArithmeticOperation(self.__rewrite_operation(op.element), op.n, op.unit, op.is_add,
                                           op.display_name)
        elif isinstance(op, DateDiffOperation):
            other = self.__rewrite_operation(op.other) if isinstance(op.other, RelationalOperationElement) else op.other
            return DateDiffOperation(self.__rewrite_operation(op.element), other, op.unit, op.display_name)
        elif isinstance(op, WindowFunctionOperation):
            element = None if op.element is None else self.__rewrite_operation(op.element)
            return WindowFunctionOperation(element, op.function, op.display_name, second_arg=op.second_arg,
                                           extra_args=list(op.extra_args),
                                           window=self.__rewrite_window_spec(op.window))
        elif isinstance(op, Alias):
            return Alias(self.__rewrite_operation(op.element), op.name)
        return op

    def __collect_required_joins(self, op: RelationalOperationElement | None, required_joins: dict):
        if op is None:
            return
        if isinstance(op, (Attribute, ColumnWithJoin)):
            _, parent = self.__resolve(op)
            if parent is not None:
                required_joins[id(parent)] = parent
        elif isinstance(op, AggregateOperation):
            self.__collect_required_joins(op.element, required_joins)
            self.__collect_required_joins(op.window, required_joins)
            for sort_op in op.order_by:
                self.__collect_required_joins(sort_op.column, required_joins)
        elif isinstance(op, ScalarFunctionOperation):
            self.__collect_required_joins(op.element, required_joins)
        elif isinstance(op, ArithmeticOperation):
            self.__collect_required_joins(op.left, required_joins)
            self.__collect_required_joins(op.right, required_joins)
        elif isinstance(op, ConcatOperation):
            for part in op.parts:
                self.__collect_required_joins(part, required_joins)
        elif isinstance(op, CaseOperation):
            self.__collect_required_joins(op.condition, required_joins)
            self.__collect_required_joins(op.then, required_joins)
            if op.default is not None:
                self.__collect_required_joins(op.default, required_joins)
        elif isinstance(op, (DateExtractOperation, DateTruncOperation)):
            self.__collect_required_joins(op.element, required_joins)
        elif isinstance(op, DateArithmeticOperation):
            self.__collect_required_joins(op.element, required_joins)
        elif isinstance(op, DateDiffOperation):
            self.__collect_required_joins(op.element, required_joins)
            if isinstance(op.other, RelationalOperationElement):
                self.__collect_required_joins(op.other, required_joins)
        elif isinstance(op, WindowFunctionOperation):
            if op.element is not None:
                self.__collect_required_joins(op.element, required_joins)
            self.__collect_required_joins(op.window, required_joins)
        elif isinstance(op, WindowSpecification):
            for part in op.partition_by:
                self.__collect_required_joins(part, required_joins)
            for sort_op in op.order_by:
                self.__collect_required_joins(sort_op.column, required_joins)
        elif isinstance(op, SortOperation):
            self.__collect_required_joins(op.column, required_joins)
        elif isinstance(op, Alias):
            self.__collect_required_joins(op.element, required_joins)
        elif isinstance(op, LogicalOperation):
            self.__collect_required_joins(op.left, required_joins)
            self.__collect_required_joins(op.right, required_joins)
        elif isinstance(op, ComparisonOperation):
            self.__collect_required_joins(op.left, required_joins)
            self.__collect_required_joins(op.right, required_joins)
        elif isinstance(op, (InOperation, NotInOperation, NotOperation)):
            self.__collect_required_joins(op.element, required_joins)

    def select(self, cols: Sequence[RelationalOperationElement]):
        required_joins: dict = {}
        if self._root_table is not None:
            self._from.add(self.__table_alias_for_table(self._root_table))
        for join in self._query_joins:  # the query's own joins, always written: the relations are named in the order they are joined
            target = self.__table_alias_for_table(join.target)
            self._join.append(Join(join.kind, target, self.build_filter(join.operation)))

        for col in cols:
            self.__collect_required_joins(col, required_joins)
            rewritten = self.__rewrite_operation(col)
            if _is_condition(col):
                rewritten = _RenderedSql(self.build_filter(self.__guarded(col)))

            if isinstance(col, Alias):
                inner = _RenderedSql(self.build_filter(self.__guarded(col.element))) if _is_condition(col.element) \
                    else self.__rewrite_operation(col.element)
                self._select.append(Alias(inner, col.name))
            else:
                self._select.append(Alias(rewritten, display_alias(col)))

        for node in required_joins.values():
            self.__add_join(node)

    def __add_join(self, node: JoinTreeNodeOperation):
        key = self.__join_target_key(node)
        if id(key) in self.__added_join_ids:
            return
        # Add ancestor joins first so intermediate tables are available
        if node.parent is not None:
            self.__add_join(node.parent)
        self.__added_join_ids.add(id(key))
        join = node.join
        if node.parent is None:
            source = next((c for c in join.columns() if not join.is_target(c)), None)
            assert source is not None, f"Join '{join.name}' reads nothing of what it is joined from"
            self._from.add(self.__table_alias_for_table(source.owner_relation))
        target_alias = self.__table_alias_for_table(join.target, key=key)
        # a sub-select carries the step's filter itself, so it is not repeated in the ON clause
        on_filter = None if isinstance(target_alias.relation, OrderedQuerySpecification) else self._join_filters.get(id(node))
        condition = self.build_filter(self.__sided(join.operation, node))
        self._join.append(Join(join.kind, target_alias, condition, self.build_filter(on_filter) if on_filter else None))

    def __sided(self, op: RelationalOperationElement, node: JoinTreeNodeOperation) -> RelationalOperationElement:
        """A join's condition with each column under the alias of its side: the joined table's under the step's, any
        other under the one it is joined from. An equality of two columns has the key first, as a join is
        defined as key = foreign key."""
        def alias_of(column: Column) -> TableAliasColumn:
            key = self.__join_target_key(node if node.join.is_target(column) else node.parent)
            return TableAliasColumn(column, self.__table_alias_for_table(column.owner_relation, key=key))

        def side(op: RelationalOperationElement) -> RelationalOperationElement:
            if isinstance(op, Column):
                return alias_of(op)
            if isinstance(op, ComparisonOperation):
                return ComparisonOperation(side(op.left), op.operator, side(op.right))
            if isinstance(op, LogicalOperation):
                return LogicalOperation(side(op.left), op.operator, side(op.right))
            if isinstance(op, (NotOperation, IsNullOperation, IsNotNullOperation)):
                return type(op)(side(op.element))
            return op

        if isinstance(op, ComparisonOperation) and op.operator == ComparisonOperator.EQUAL \
                and isinstance(op.left, Column) and isinstance(op.right, Column) \
                and op.right.primary_key and not op.left.primary_key:
            op = ComparisonOperation(op.right, op.operator, op.left)
        return side(op)

    def build_filter(self, op:RelationalOperationElement) -> str:
        if isinstance(op, NoOperation):
            return ''
        elif isinstance(op, ExistsOperation):
            col = op.node.join.keys()[1]
            return self.build_filter(ColumnWithJoin(col, op.node)) + ' IS NOT NULL'
        elif isinstance(op, RelationExistsOperation):
            inner = self.__table_alias_for_table(op.relation)
            return 'EXISTS (SELECT 1 FROM ' + self.__relation_sql(inner) + ' AS ' + inner.name + ' WHERE ' \
                + self.build_filter(op.condition) + ')'
        elif isinstance(op, NotExistsOperation):
            col = op.node.join.keys()[1]
            return self.build_filter(ColumnWithJoin(col, op.node)) + ' IS NULL'
        elif isinstance(op, InOperation):
            return self.__in_operand(op.element) + ' IN (' + ', '.join(constant_value_string(v) for v in op.values) + ')'
        elif isinstance(op, NotInOperation):
            column = self.__in_operand(op.element)
            sql = column + ' NOT IN (' + ', '.join(constant_value_string(v) for v in op.values) + ')'
            # NULL NOT IN (...) is not true in SQL, so a nullable column has to match its NULLs explicitly
            if isinstance(op.element, ColumnWithJoin) and self.__resolve(op.element)[0].nullable:
                return '(' + sql + ' OR ' + column + ' IS NULL)'
            return sql
        elif isinstance(op, NotOperation):
            return 'NOT (' + self.build_filter(op.element) + ')'
        elif isinstance(op, IsNotNullOperation):
            return self.build_filter(op.element) + ' IS NOT NULL'
        elif isinstance(op, IsNullOperation):
            return self.build_filter(op.element) + ' IS NULL'
        elif isinstance(op, LogicalOperation):
            left_str = self.build_filter(op.left)
            right_str = self.build_filter(op.right)
            if needs_parentheses(op, op.left):
                left_str = '(' + left_str + ')'
            if needs_parentheses(op, op.right):
                right_str = '(' + right_str + ')'
            return left_str + logical_operator_string(op.operator) + right_str
        elif isinstance(op, ComparisonOperation) and op.operator == ComparisonOperator.MATCHES:
            return self.build_filter(op.left) + ' ~ ' + self.build_filter(op.right)
        elif isinstance(op, ComparisonOperation):
            sql = self.build_filter(op.left) + comparison_operator_string(op.operator) + self.build_filter(op.right)
            if op.operator in (ComparisonOperator.LIKE, ComparisonOperator.NOT_LIKE) \
                    and isinstance(op.right, StringConstantOperation) and LIKE_ESCAPE in op.right.value:
                sql += " ESCAPE '" + LIKE_ESCAPE + "'"  # the pattern has a literal %, _ or escape character in it
            return sql
        elif isinstance(op, ConstantOperation):
            return constant_value_string(op)
        elif isinstance(op, TableAliasColumn):
            return table_alias_column_string(op)
        elif isinstance(op, ColumnWithJoin):
            column, node = self.__resolve(op)
            if node is not None:
                ta = self.__table_alias_for_table(column.owner_relation, key=self.__join_target_key(node))
                self.__add_join(node)
            else:
                ta = self.__table_alias_for_table(column.owner_relation)
            return _column_reference(ta.name, column.name)
        elif isinstance(op, Column):
            ta = self.__table_alias_for_table(op.owner_relation)
            return _column_reference(ta.name, op.name)
        elif isinstance(op, (ArithmeticOperation, ConcatOperation, ScalarFunctionOperation, DateTruncOperation,
                             DateExtractOperation, DateArithmeticOperation, DateDiffOperation,
                             AggregateOperation, CountAllOperation, CaseOperation)):
            # an expression compared in a filter, or a conditional that is the filter: it needs its columns' joins, then it is written as in a select list
            required_joins: dict = {}
            self.__collect_required_joins(op, required_joins)
            for parent in required_joins.values():
                self.__add_join(parent)
            return sql_operation_to_string(self.__rewrite_operation(op))
        else:
            raise ValueError(op)

    def __table_alias_for_table(self, table: Relation, key=None) -> TableAlias:
        if not isinstance(table, NamedRelation):
            raise TypeError(f"A relation read by name needs a name: {table!r}")
        cache_key = key if key is not None else table.qualified_name
        if cache_key in self.__table_aliases_by_table:
            return self.__table_aliases_by_table[cache_key]
        if key is not None and id(key) in self._sub_selects:
            ta = self.__new_alias(self._sub_selects[id(key)])
        elif isinstance(table, TableAlias):
            ta = self.__new_alias(table.relation)  # a relation read under a name: the relation itself is what is written
        else:
            ta = self.__new_alias(table)
        self.__table_aliases_by_table[cache_key] = ta
        return ta

    def __new_alias(self, relation: Relation) -> TableAlias:
        ta = TableAlias(relation, "t" + str(self.__table_alias_incr))
        self.__table_alias_incr += 1
        return ta

    def __relation_sql(self, alias: TableAlias) -> str:
        """What follows FROM or JOIN: the table's name, or the query in parentheses, written by a generator of its own
        that numbers its aliases after this one's."""
        relation = alias.relation
        if isinstance(relation, Table):
            return relation.qualified_name
        assert isinstance(relation, QueryExpression), f"Not a relation to read from: {relation!r}"
        return '(' + self.__query_sql(relation) + ')'

    def __query_sql(self, relation: 'QueryExpression') -> str:
        """A query written by a generator of its own that numbers its aliases after this one's; a union is its two queries."""
        if isinstance(relation, UnionOperation):
            return self.__union_side_sql(relation.left) + (' UNION ALL ' if relation.all else ' UNION ') \
                + self.__union_side_sql(relation.right)
        assert isinstance(relation, QuerySpecification)
        nested = SQLQueryGenerator(first_alias=self.__table_alias_incr)
        nested.generate(relation)
        sql = nested.build_query_string()  # it names its own relations as it is written, so read its count after
        self.__table_alias_incr = nested.__table_alias_incr
        return sql

    def __union_side_sql(self, side: 'QueryExpression') -> str:
        """One side of a union: in brackets if it has an ORDER BY, a row limit or an offset, as SQL needs for them to belong
        to that side and not to the whole union."""
        sql = self.__query_sql(side)
        if isinstance(side, OrderedQuerySpecification) and (side.order_by or side.limit is not None or side.offset):
            return '(' + sql + ')'
        return sql

    def build_query_string(self) -> str:
        def join_sql(j: Join) -> str:
            return _join_word(j.kind) + self.__relation_sql(j.target) + ' AS ' + j.target.name + ' ON (' + j.condition \
                + (' AND ' + j.filter_sql if j.filter_sql else '') + ')'

        joins = map(join_sql, self._join)
        from_sql = ','.join(map(lambda ta: self.__relation_sql(ta) + ' AS ' + ta.name, self._from))
        return ('SELECT DISTINCT ' if self._distinct else 'SELECT ') + ','.join(map(lambda ca: sql_operation_to_string(ca), self._select)) \
            + ' FROM ' + from_sql \
            + ''.join(joins) \
            + self.__build_where() \
            + self.__build_group_by() \
            + self.__build_having() \
            + self.__build_order_by() \
            + self.__build_limit() \
            + self.__build_offset()

    def __build_where(self) -> str:
        where = ''.join(self._where)
        return ' WHERE ' + where if where else ''

    def __build_group_by(self) -> str:
        if not self._group_by_parts:
            return ''
        return ' GROUP BY ' + ', '.join(self._group_by_parts)

    def __build_having(self) -> str:
        return ' HAVING ' + self._having if self._having else ''

    def __build_order_by(self) -> str:
        if not self._order_by_parts:
            return ''
        return ' ORDER BY ' + ', '.join(self._order_by_parts)

    def __build_offset(self) -> str:
        return '' if self._offset is None else ' OFFSET ' + str(self._offset)

    def __build_limit(self) -> str:
        if self._limit is None:
            return ''
        return ' LIMIT ' + str(self._limit)


def to_sql(business_date: datetime.date | None, processing_datetime: datetime.datetime | datetime.date | None,
           columns: Sequence, table, op,
           order_by: list | None = None, group_by: list | None = None,
           limit: int | None = None, validate_sqlglot: bool = True,
           business_date_to: datetime.date | None = None, distinct: bool = False,
           offset: int | None = None, having=None,
           processing_datetime_to: datetime.datetime | datetime.date | None = None) -> str:
    query = build_query_specification(business_date, processing_datetime, columns, table, op,
                                  order_by or [], group_by or [], limit,
                                  business_date_to=business_date_to, distinct=distinct, offset=offset, having=having,
                                  processing_datetime_to=processing_datetime_to)
    return query_sql_to_string(query, validate_sqlglot=validate_sqlglot)


def query_sql_to_string(query: QuerySpecification, validate_sqlglot: bool = True) -> str:
    qe = SQLQueryGenerator()
    qe.generate(query)
    sql = qe.build_query_string()
    if validate_sqlglot:
        sqlglot.transpile(sql, read=SOURCE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)
    return sql


# --- views -------------------------------------------------------------------------------------------------------

def _as_of_column(column: Column, node: JoinTreeNodeOperation) -> ColumnWithJoin:
    """The joined table's column, read through the join."""
    return ColumnWithJoin(Column(column.name, column.type, _table_of(column)), node)


def _as_of_root_column(column: Column) -> Column:
    """The root table's column: it is read directly, with no join."""
    return Column(column.name, column.type, _table_of(column))


def _as_of_and(ops: list):
    result = ops[0]
    for op in ops[1:]:
        result = LogicalOperation(result, LogicalOperator.AND, op)
    return result


def _as_of_interval_contains(start: Column, end: Column, point: Column, infinite_datetime: str | None,
                             node: JoinTreeNodeOperation):
    """``start <= point AND end > point`` (or ``end IS NULL`` when open ends are NULL)."""
    after_start = ComparisonOperation(_as_of_column(start, node), ComparisonOperator.LESS_THAN_OR_EQUAL_TO, point)
    before_end: Operation = ComparisonOperation(_as_of_column(end, node), ComparisonOperator.GREATER_THAN, point)
    if infinite_datetime is None:
        before_end = LogicalOperation(before_end, LogicalOperator.OR, IsNullOperation(_as_of_column(end, node)))
    return LogicalOperation(after_start, LogicalOperator.AND, before_end)


def _as_of_business_point(root: MilestoningColumns | None) -> Column | None:
    """The root row's point on the business-date axis: its date, or the start of its interval."""
    if isinstance(root, SingleBusinessDateColumn):
        return root.business_date_column
    if isinstance(root, BusinessDateRangeColumns):
        return root.business_date_from_column
    return None


def _as_of_processing_point(root: MilestoningColumns | None) -> Column | None:
    """The root row's point on the processing axis: its snapshot time, or the start of its interval."""
    if isinstance(root, SingleProcessingDatetimeColumn):
        return root.processing_datetime_column
    if isinstance(root, ProcessingTemporalColumns):
        return root.start_at_column
    return None


def build_as_of_join_condition(root_table: Table, node: JoinTreeNodeOperation) -> Operation | None:
    """Join condition pinning a joined milestoned table to the root row's own point in time.

    The child version chosen is the one that applied when the root row was recorded (processing axis)
    and for the root row's business date (business axis), so later changes to the child never alter
    rows already in the view. Raises ViewDefinitionError when the root has no corresponding axis.
    """
    child_table = node.join.table
    if not isinstance(child_table, MilestonedTable):
        return None
    child = child_table.milestoning_columns
    root = root_table.milestoning_columns if isinstance(root_table, MilestonedTable) else None
    where = f"'{node.join.name}' ({child_table.name}) joined from {root_table.name}"
    conditions = []

    if isinstance(child, (ProcessingTemporalColumns, SingleProcessingDatetimeColumn)):
        processing_point = _as_of_processing_point(root)
        if processing_point is None:
            raise ViewDefinitionError(
                f"{where} is processing-temporal, but {root_table.name} has no processing time to anchor it to")
        if isinstance(child, SingleProcessingDatetimeColumn):
            conditions.append(ComparisonOperation(_as_of_column(child.processing_datetime_column, node),
                                                  ComparisonOperator.EQUAL, _as_of_root_column(processing_point)))
        else:
            conditions.append(_as_of_interval_contains(child.start_at_column, child.end_at_column,
                                                       _as_of_root_column(processing_point), child.processing_infinite_datetime,
                                                       node))

    if isinstance(child, (SingleBusinessDateColumn, BusinessDateRangeColumns)):
        point = _as_of_business_point(root)
        if point is None:
            raise ViewDefinitionError(
                f"{where} has a business date, but {root_table.name} has no business date to anchor it to")
        if isinstance(child, SingleBusinessDateColumn):
            conditions.append(ComparisonOperation(_as_of_column(child.business_date_column, node),
                                                  ComparisonOperator.EQUAL, _as_of_root_column(point)))
        else:
            conditions.append(_as_of_interval_contains(child.business_date_from_column, child.business_date_to_column,
                                                       _as_of_root_column(point), child.business_infinite_datetime, node))
    return _as_of_and(conditions) if conditions else None


def check_view_joins(view: View) -> None:
    """Raise ViewDefinitionError unless every joined milestoned table can be pinned to the root row's point in time."""
    assert view.table is not None
    for node in _joined_nodes(view):
        step: JoinTreeNodeOperation | None = node
        while step is not None:
            if step.business_date is not None or step.processing_datetime is not None:
                raise ViewDefinitionError(
                    f"'{step.join.name}' was given a date, but a view has no dates: each joined table is pinned to the "
                    f"point in time of the row it is joined from")
            step = step.parent
        build_as_of_join_condition(view.table, node)


def view_select_to_string(view: View) -> str:
    """The ``SELECT`` a view is defined by, in generic SQL.

    The view's join filters are set here, immediately before rendering, and not when the view is built.
    """
    check_view_joins(view)
    table = view.table
    assert table is not None
    _apply_join_filters(view, lambda node: build_as_of_join_condition(table, node))
    return query_sql_to_string(view, validate_sqlglot=False)


def view_sql_to_string(view: View, dialect: str | None = None) -> str:
    """``CREATE OR REPLACE VIEW`` DDL for the view, in the SQL the generator writes (Postgres) or transpiled to ``dialect`` (e.g. 'databricks')."""
    sql = f"CREATE OR REPLACE VIEW {view.qualified_name} AS " + view_select_to_string(view)
    if dialect is None:
        sqlglot.transpile(sql, read=SOURCE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)
        return sql
    return sql_for_dialect(sql, dialect)
