import datetime
from dataclasses import dataclass, field
from functools import reduce
from typing import Any, Callable, Iterable, Sequence

import sqlglot
import sqlglot.errors

from datafinder import DateTimeAttribute, DateAttribute
from datafinder.attribute import Attribute
from model.milestoning import MilestoningColumns, ProcessingTemporalColumns, SingleBusinessDateColumn, \
    BusinessDateAndProcessingTemporalColumns, BiTemporalColumns, MilestonedTable
from model.view import View, ViewDefinitionError, contains_aggregate, contains_aggregate_or_window
from model.relational import LIKE_ESCAPE, Table, Operation, LogicalOperator, LogicalOperation, RelationalOperationElement, \
    ComparisonOperation, ConstantOperation, ComparisonOperator, StringConstantOperation, DateConstantOperation, \
    DateTimeConstantOperation, IntegerConstantOperation, FloatConstantOperation, BooleanConstantOperation, DecimalConstantOperation, Column, NoOperation, JoinOperation, JoinTreeNodeOperation, \
    UnaryOperation, ColumnWithJoin, AggregateOperation, AggregateOperator, SortOperation, SortDirection, CountAllOperation, \
    ScalarFunction, ScalarFunctionOperation, DatePart, DateExtractOperation, DateTruncOperation, DateArithmeticOperation, DateDiffOperation, \
    IsNullOperation, IsNotNullOperation, InOperation, NotInOperation, \
    WindowFunctionOperation, WindowFunction, WindowSpecification, Alias, ExistsOperation, NotExistsOperation, \
    SelectOperation, QueryExpression, BinaryOperation, ArithmeticOperation, ArithmeticOperator, ConcatOperation, CaseOperation

class TableAlias:
    def __init__(self, table: str, alias: str):
        self.table = table
        self.alias = alias


class TableAliasColumn(RelationalOperationElement):
    def __init__(self, column: Column, table_alias: TableAlias):
        super().__init__()
        self.column = column
        self.table_alias = table_alias


class Join:
    def __init__(self, source: TableAliasColumn, target: TableAliasColumn, filter_op: RelationalOperationElement | None = None):
        self.source = source
        self.target = target
        self.filter_op = filter_op

    def condition(self) -> str:
        """``source = target``, written with the key column first as Legend writes a join defined as key = foreign key."""
        first, second = self.source, self.target
        if second.column.primary_key and not first.column.primary_key:
            first, second = second, first
        return (first.table_alias.alias + '.' + first.column.name + ' = ' +
                second.table_alias.alias + '.' + second.column.name)

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


def _processing_predicate(mc: MilestoningColumns, processing_datetime: datetime.datetime | datetime.date | None,
                          join_node: 'JoinTreeNodeOperation | None') -> Operation | None:
    """The table's processing range contains the processing date."""
    if not isinstance(mc, ProcessingTemporalColumns) or processing_datetime is None:
        return None
    start_at = DateTimeAttribute('start_at', mc.start_at_column.name, mc.start_at_column.type, mc.start_at_column.qualified_name(), join_node)
    end_at = DateTimeAttribute('end_at', mc.end_at_column.name, mc.end_at_column.type, mc.end_at_column.qualified_name(), join_node)
    return LogicalOperation(start_at <= processing_datetime, LogicalOperator.AND, _open_end_clause(end_at, processing_datetime, mc.infinite_datetime))


def _business_predicate(mc: MilestoningColumns, business_date: datetime.date | None,
                        join_node: 'JoinTreeNodeOperation | None') -> Operation | None:
    """The table's business range contains the business date, or its business date column equals it."""
    if business_date is None:
        return None
    if isinstance(mc, BiTemporalColumns):
        date_from = DateAttribute('business_date_from', mc.business_date_from_column.name, mc.business_date_from_column.type, mc.business_date_from_column.qualified_name(), join_node)
        date_to = DateAttribute('business_date_to', mc.business_date_to_column.name, mc.business_date_to_column.type, mc.business_date_to_column.qualified_name(), join_node)
        return LogicalOperation(date_from <= business_date, LogicalOperator.AND, _open_end_clause(date_to, business_date, mc.infinite_datetime))
    if isinstance(mc, SingleBusinessDateColumn):
        business_att = DateAttribute('business_date', mc.business_date_column.name, mc.business_date_column.type, mc.business_date_column.qualified_name(), join_node)
        return business_att == business_date
    return None


def _business_range_predicate(mc: MilestoningColumns, business_date_from: datetime.date | None,
                              business_date_to: datetime.date | None,
                              join_node: 'JoinTreeNodeOperation | None') -> Operation | None:
    """The table's business range overlaps the business date range, or its business date column is within it."""
    if business_date_from is None or business_date_to is None:
        return None
    if isinstance(mc, BiTemporalColumns):
        date_from = DateAttribute('business_date_from', mc.business_date_from_column.name, mc.business_date_from_column.type, mc.business_date_from_column.qualified_name(), join_node)
        date_to = DateAttribute('business_date_to', mc.business_date_to_column.name, mc.business_date_to_column.type, mc.business_date_to_column.qualified_name(), join_node)
        return LogicalOperation(date_from <= business_date_to, LogicalOperator.AND, _open_end_clause(date_to, business_date_from, mc.infinite_datetime))
    if isinstance(mc, SingleBusinessDateColumn):
        business_att = DateAttribute('business_date', mc.business_date_column.name, mc.business_date_column.type, mc.business_date_column.qualified_name(), join_node)
        return LogicalOperation(business_att >= business_date_from, LogicalOperator.AND, business_att <= business_date_to)
    return None


def build_milestoning_filter_operation_for_date_range(
        business_date_from: datetime.date | None, business_date_to: datetime.date | None,
        processing_datetime: datetime.datetime | datetime.date | None,
        table: MilestonedTable, join_node: 'JoinTreeNodeOperation | None' = None) -> 'Operation | None':
    """Like build_milestoning_filter_operation but applies a business-date range filter.

    For BiTemporalColumns the range uses interval overlap:
        business_date_from_col <= business_date_to AND business_date_to_col > business_date_from
    For single-date columns:
        business_date >= business_date_from AND business_date <= business_date_to
    Processing-date filtering is point-in-time (unchanged from the normal path).
    """
    mc = table.milestoning_columns
    return _and_all([_processing_predicate(mc, processing_datetime, join_node),
                     _business_range_predicate(mc, business_date_from, business_date_to, join_node)])


def build_milestoning_filter_operation(business_date: datetime.date | None,
                                       processing_datetime: datetime.datetime | datetime.date | None,
                                       table:MilestonedTable, join_node:'JoinTreeNodeOperation | None' = None) -> 'Operation | None':
    """The milestoning filter for a point in time: the processing range, then the business range, ANDed together."""
    #TODO this should not reference attribute
    mc = table.milestoning_columns
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
        col = ColumnWithJoin(Column(col_name, col_type, table), node)
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
    elif isinstance(op, ScalarFunctionOperation):
        collect_required_joins(op.element, required_joins, node_refs)
    elif isinstance(op, (DateExtractOperation, DateTruncOperation)):
        collect_required_joins(op.element, required_joins, node_refs)
    elif isinstance(op, DateArithmeticOperation):
        collect_required_joins(op.element, required_joins, node_refs)
    elif isinstance(op, DateDiffOperation):
        collect_required_joins(op.element, required_joins, node_refs)
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
    elif isinstance(op, (IsNullOperation, IsNotNullOperation, InOperation, NotInOperation)):
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

def _required_joins(query: QueryExpression, node_refs: dict | None = None) -> dict:
    """Join-tree nodes the query's expressions read through, keyed by node id."""
    required_joins: dict = {}
    for expression in query.expressions():
        collect_required_joins(expression, required_joins, node_refs)
    return required_joins


def _joined_nodes(query: QueryExpression) -> list[JoinTreeNodeOperation]:
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


def _apply_join_filters(query: QueryExpression,
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


def build_query_operation(business_date: datetime.date | None, processing_datetime: datetime.datetime | datetime.date | None,
                         columns: Sequence[Attribute], table: Table, op: Operation,
                         order_by: list[SortOperation] | None = None, group_by: list | None = None,
                         limit: int | None = None, business_date_to: datetime.date | None = None,
                         distinct: bool = False, offset: int | None = None,
                         having: Operation | None = None) -> SelectOperation:
    if isinstance(table, MilestonedTable):
        if business_date_to is not None:
            milestoned_op = build_milestoning_filter_operation_for_date_range(business_date, business_date_to, processing_datetime, table)
        else:
            milestoned_op = build_milestoning_filter_operation(business_date, processing_datetime, table)
        if milestoned_op is not None:
            op = milestoned_op if isinstance(op, NoOperation) else LogicalOperation(op, LogicalOperator.AND, milestoned_op)

    select = SelectOperation(columns, op, order_by or [], group_by or [], limit, table, distinct, offset, having)

    def milestoning_condition(node):
        if not isinstance(node.join.target, MilestonedTable):
            return None
        if business_date_to is not None:
            return build_milestoning_filter_operation_for_date_range(business_date, business_date_to, processing_datetime, node.join.target, node)
        return build_milestoning_filter_operation(business_date, processing_datetime, node.join.target, node)

    _apply_join_filters(select, milestoning_condition)
    return select

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
}

def needs_parentheses(parent: LogicalOperation, child: RelationalOperationElement) -> bool:
    """Whether ``child`` must be parenthesised where it is an operand of ``parent``.

    A logical operation nested in one with a different operator is parenthesised: OR inside AND would otherwise
    change meaning, and AND inside OR is easier to read. The same operator needs none, since AND and OR are
    associative, so a AND (b AND c) is a AND b AND c.
    """
    return isinstance(child, LogicalOperation) and child.operator != parent.operator


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
    else:
        raise ValueError

def table_alias_column_string(tac: TableAliasColumn) -> str:
    return tac.table_alias.alias + '.' + tac.column.name

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

def _window_spec_to_string(window: WindowSpecification) -> str:
    parts = []
    if window.partition_by:
        parts.append('PARTITION BY ' + ', '.join(sql_operation_to_string(part) for part in window.partition_by))
    if window.order_by:
        parts.append('ORDER BY ' + ', '.join(
            sql_operation_to_string(sort.column) + (' ASC' if sort.direction == SortDirection.ASC else ' DESC')
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


class _RenderedSql(RelationalOperationElement):
    """A condition already written as SQL, to be selected as a column. A condition is rendered where its columns can
    be resolved to table aliases, which is while the query is being built, not when its text is put together."""

    def __init__(self, sql: str):
        super().__init__()
        self.sql = sql


def _is_condition(op: RelationalOperationElement) -> bool:
    return isinstance(op, (ComparisonOperation, LogicalOperation, IsNullOperation, IsNotNullOperation, InOperation,
                           NotInOperation, ExistsOperation, NotExistsOperation))


def sql_operation_to_string(operation: RelationalOperationElement) -> str:
    if isinstance(operation, TableAliasColumn):
        return table_alias_column_string(operation)
    elif isinstance(operation, AggregateOperation) and operation.operator == AggregateOperator.COUNT_DISTINCT:
        return 'COUNT(DISTINCT ' + sql_operation_to_string(operation.element) + ')'
    elif isinstance(operation, AggregateOperation) and operation.operator == AggregateOperator.STRING_AGG:
        sql = 'STRING_AGG(' + sql_operation_to_string(operation.element) + ", '" + (operation.separator or '').replace("'", "''") + "'"
        if operation.order_by:
            sql += ' ORDER BY ' + ', '.join(
                sql_operation_to_string(s.column) + (' ASC' if s.direction == SortDirection.ASC else ' DESC')
                for s in operation.order_by)
        sql += ')'
        if operation.window is not None:
            sql += ' ' + _window_spec_to_string(operation.window)
        return sql
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
    elif isinstance(operation, ScalarFunctionOperation):
        fn = _SCALAR_SQL_NAMES[operation.function]
        parts = [sql_operation_to_string(operation.element)]
        if operation.second_arg is not None:
            parts.append(str(operation.second_arg))
        for arg in operation.extra_args:
            parts.append(sql_string_literal(arg) if isinstance(arg, str) else str(arg))
        return fn + '(' + ', '.join(parts) + ')'
    elif isinstance(operation, DateExtractOperation):
        return 'EXTRACT(' + operation.part.value + ' FROM ' + sql_operation_to_string(operation.element) + ')'
    elif isinstance(operation, DateTruncOperation):
        return "CAST(DATE_TRUNC('" + operation.part.value.lower() + "', " + sql_operation_to_string(operation.element) + ') AS DATE)'
    elif isinstance(operation, DateArithmeticOperation):
        op = '+' if operation.is_add else '-'
        return sql_operation_to_string(operation.element) + ' ' + op + " INTERVAL '" + str(operation.n) + "' " + operation.unit.value
    elif isinstance(operation, DateDiffOperation):
        other_sql = sql_format_datetime(operation.other) if isinstance(operation.other, datetime.datetime) else sql_format_date(operation.other)
        return "DATE_DIFF('" + operation.unit.value.lower() + "', " + sql_operation_to_string(operation.element) + ', ' + other_sql + ')'
    elif isinstance(operation, CountAllOperation):
        return 'COUNT(*)'
    elif isinstance(operation, ConstantOperation):
        return _selected_constant_string(operation)
    elif isinstance(operation, ConcatOperation):
        return ' || '.join(_operand_string(part) for part in operation.parts)
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

    def __init__(self):
        self._having = ''
        self._select = []
        self._from = set()
        self._join = []
        self._group_by_parts = []
        self._order_by_parts = []
        self.__table_alias_incr = 0
        self.__table_aliases_by_table = {}
        self.__added_join_ids = set()
        self.__elidable_ids: set = set()
        self._root_table = None
        self._join_filters: dict = {}

    def __compute_elidable_ids(self, select: QueryExpression) -> set:
        """Determine upfront which join nodes can be satisfied entirely by embedded (join-less)
        alternates, so every later leaf resolution can build the right TableAliasColumn directly —
        see __resolve."""
        node_refs: dict = {}
        _required_joins(select, node_refs)
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

    def generate(self, select: QueryExpression):
        self._root_table = select.table
        self._join_filters = select.join_filters
        self.__elidable_ids = self.__compute_elidable_ids(select)
        self.select(select.display)
        self._where = self.build_filter(select.filter)
        required_joins: dict = {}
        for group_by_expr in select.group_by:
            self.__collect_required_joins(group_by_expr, required_joins)
        for sort_op in select.order_by:
            self.__collect_required_joins(sort_op.column, required_joins)
        for parent in required_joins.values():
            self.__add_join(parent)
        self._group_by_parts = self.__group_by_parts(select)
        self._having = ''
        having = select.having if isinstance(select, SelectOperation) else None
        if having is not None:
            having_joins: dict = {}
            self.__collect_required_joins(having, having_joins)
            for parent in having_joins.values():
                self.__add_join(parent)
            self._having = self.build_filter(having)
        self._order_by_parts = [
            self.__sort_expression(s.column) + (' ASC' if s.direction == SortDirection.ASC else ' DESC')
            for s in select.order_by
        ]
        self._limit = select.limit
        self._distinct = isinstance(select, SelectOperation) and select.distinct
        self._offset = select.offset if isinstance(select, SelectOperation) else None

    def __group_by_parts(self, select: QueryExpression) -> list[str]:
        """The GROUP BY of a query: the grouping it was given, else the grouping its columns imply.

        Columns that are not aggregated are what the aggregates are computed for, so a query that selects both is
        grouped by those columns (as ``df.groupby(columns).agg(...)`` does). An aggregate read through a to-many
        association is computed for each row of the root table, as Legend does, so the root's primary key is grouped
        by too. A query that selects only aggregates over the root's own columns is not grouped: the aggregate covers
        every row."""
        if select.group_by or not isinstance(select, SelectOperation):
            return [self.__attr_to_col_string(a) for a in select.group_by]
        aggregates = [c for c in select.display if contains_aggregate(c)]
        if not aggregates:
            return []
        per_root = select.table is not None and any(_reaches_to_many(c) for c in aggregates)
        keys = [self.__attr_to_col_string(c) for c in select.table.columns if c.primary_key] if per_root and select.table else []
        grouped = [sql_operation_to_string(shown.element) for column, shown in zip(select.display, self._select)
                   if not contains_aggregate_or_window(column)]
        return list(dict.fromkeys([*keys, *grouped]))

    def __sort_expression(self, expression: RelationalOperationElement) -> str:
        """What a sort orders by: a column, or an expression such as an aggregate."""
        if isinstance(expression, (ColumnWithJoin, Attribute)):
            return self.build_filter(expression)
        return sql_operation_to_string(self.__rewrite_operation(expression))

    def __attr_to_col_string(self, attr: 'Attribute | ColumnWithJoin | Column') -> str:
        if isinstance(attr, Column):
            return self.__table_alias_for_table(attr.owner).alias + '.' + attr.name
        column, parent = self.__resolve(attr)
        ta = self.__table_alias_for_table(column.owner, key=self.__join_target_key(parent) if parent is not None else None)
        return ta.alias + '.' + column.name

    @staticmethod
    def __is_self_join(parent: JoinTreeNodeOperation) -> bool:
        return parent.join.left.owner == parent.join.right.owner

    def __join_target_key(self, parent: JoinTreeNodeOperation):
        """Cache key for the join target alias. Always keyed by node identity so the
        alias in SELECT matches the alias registered in __add_join."""
        return parent

    def __table_alias_for_column(self, column: Column, parent: JoinTreeNodeOperation | None = None) -> TableAlias:
        key = self.__join_target_key(parent) if parent is not None else None
        return self.__table_alias_for_table(column.owner, key=key)

    def __rewrite_window_spec(self, window: WindowSpecification | None):
        if window is None:
            return None
        partition_by = [self.__rewrite_operation(p) for p in window.partition_by]
        order_by = [SortOperation(self.__rewrite_operation(s.column), s.direction) for s in window.order_by]
        return WindowSpecification(partition_by, order_by)

    def __case_sql(self, case: CaseOperation) -> str:
        """The CASE written out now: its condition can only be rendered while the query is being built."""
        def branch(value: RelationalOperationElement) -> str:
            return constant_value_string(value) if isinstance(value, ConstantOperation) \
                else sql_operation_to_string(self.__rewrite_operation(value))
        sql = 'CASE WHEN ' + self.build_filter(case.condition) + ' THEN ' + branch(case.then)
        if case.default is not None:
            sql += ' ELSE ' + branch(case.default)
        return sql + ' END'

    def __rewrite_operation(self, op: RelationalOperationElement):
        if isinstance(op, (Attribute, ColumnWithJoin)):
            column, parent = self.__resolve(op)
            return TableAliasColumn(column, self.__table_alias_for_column(column, parent))
        elif isinstance(op, Column):
            return TableAliasColumn(op, self.__table_alias_for_table(op.owner))
        elif isinstance(op, AggregateOperation):
            return AggregateOperation(self.__rewrite_operation(op.element), op.operator, op.display_name,
                                      self.__rewrite_window_spec(op.window), op.separator,
                                      [SortOperation(self.__rewrite_operation(s.column), s.direction) for s in op.order_by])
        elif isinstance(op, ScalarFunctionOperation):
            return ScalarFunctionOperation(self.__rewrite_operation(op.element), op.function, op.display_name,
                                           second_arg=op.second_arg, extra_args=list(op.extra_args))
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
        elif isinstance(op, DateTruncOperation):
            return DateTruncOperation(self.__rewrite_operation(op.element), op.part, op.display_name)
        elif isinstance(op, DateArithmeticOperation):
            return DateArithmeticOperation(self.__rewrite_operation(op.element), op.n, op.unit, op.is_add,
                                           op.display_name)
        elif isinstance(op, DateDiffOperation):
            return DateDiffOperation(self.__rewrite_operation(op.element), op.other, op.unit, op.display_name)
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
        elif isinstance(op, (InOperation, NotInOperation)):
            self.__collect_required_joins(op.element, required_joins)

    def select(self, cols: Sequence[RelationalOperationElement]):
        required_joins: dict = {}
        if self._root_table is not None:
            self._from.add(self.__table_alias_for_table(self._root_table.name))

        for col in cols:
            self.__collect_required_joins(col, required_joins)
            rewritten = self.__rewrite_operation(col)
            if _is_condition(col):
                rewritten = _RenderedSql(self.build_filter(col))

            if isinstance(col, Alias):
                inner = _RenderedSql(self.build_filter(col.element)) if _is_condition(col.element) \
                    else self.__rewrite_operation(col.element)
                self._select.append(Alias(inner, col.name))
            else:
                self._select.append(Alias(rewritten, display_alias(col)))

        for node in required_joins.values():
            self.__add_join(node)

    def __add_join(self, node: JoinTreeNodeOperation):
        if id(node) in self.__added_join_ids:
            return
        # Add ancestor joins first so intermediate tables are available
        if node.parent is not None:
            self.__add_join(node.parent)
        self.__added_join_ids.add(id(node))
        join = node.join
        left = join.left
        src_key = node.parent if node.parent is not None else None
        src_ta = self.__table_alias_for_table(left.owner, key=src_key)
        if node.parent is None:
            self._from.add(src_ta)
        sc = TableAliasColumn(left, src_ta)
        right = join.right
        tc = TableAliasColumn(right, self.__table_alias_for_table(right.owner, key=node))
        self._join.append(Join(sc, tc, self._join_filters.get(id(node))))

    def build_filter(self, op:RelationalOperationElement) -> str:
        if isinstance(op, NoOperation):
            return ''
        elif isinstance(op, ExistsOperation):
            col = op.node.join.right
            return self.build_filter(ColumnWithJoin(col, op.node)) + ' IS NOT NULL'
        elif isinstance(op, NotExistsOperation):
            col = op.node.join.right
            return self.build_filter(ColumnWithJoin(col, op.node)) + ' IS NULL'
        elif isinstance(op, InOperation):
            return self.build_filter(op.element) + ' IN (' + ', '.join(constant_value_string(v) for v in op.values) + ')'
        elif isinstance(op, NotInOperation):
            column = self.build_filter(op.element)
            sql = column + ' NOT IN (' + ', '.join(constant_value_string(v) for v in op.values) + ')'
            # NULL NOT IN (...) is not true in SQL, so a nullable column has to match its NULLs explicitly
            if isinstance(op.element, ColumnWithJoin) and self.__resolve(op.element)[0].nullable:
                return '(' + sql + ' OR ' + column + ' IS NULL)'
            return sql
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
        elif isinstance(op, ComparisonOperation):
            sql = self.build_filter(op.left) + comparison_operator_string(op.operator) + self.build_filter(op.right)
            if op.operator in (ComparisonOperator.LIKE, ComparisonOperator.NOT_LIKE) \
                    and isinstance(op.right, StringConstantOperation) and LIKE_ESCAPE in op.right.value:
                sql += " ESCAPE '" + LIKE_ESCAPE + "'"  # the pattern has a literal %, _ or escape character in it
            return sql
        elif isinstance(op, ConstantOperation):
            return constant_value_string(op)
        elif isinstance(op, ColumnWithJoin):
            column, node = self.__resolve(op)
            if node is not None:
                ta = self.__table_alias_for_table(column.owner, key=node)
                self.__add_join(node)
            else:
                ta = self.__table_alias_for_table(column.owner)
            return ta.alias + '.' + column.name
        elif isinstance(op, Column):
            ta = self.__table_alias_for_table(op.owner)
            return ta.alias + '.' + op.name
        elif isinstance(op, (ArithmeticOperation, ConcatOperation, ScalarFunctionOperation, DateTruncOperation,
                             AggregateOperation, CountAllOperation)):
            # an expression compared in a filter: it needs its columns' joins, then it is written as in a select list
            required_joins: dict = {}
            self.__collect_required_joins(op, required_joins)
            for parent in required_joins.values():
                self.__add_join(parent)
            return sql_operation_to_string(self.__rewrite_operation(op))
        else:
            raise ValueError(op)

    def __table_alias_for_table(self, table: str | None, key=None) -> TableAlias:
        if table is None:
            raise TypeError
        cache_key = key if key is not None else table
        if cache_key in self.__table_aliases_by_table:
            return self.__table_aliases_by_table[cache_key]
        ta = TableAlias(table, "t" + str(self.__table_alias_incr))
        self.__table_alias_incr += 1
        self.__table_aliases_by_table[cache_key] = ta
        return ta

    def build_query_string(self) -> str:
        joins = map(lambda j: ' LEFT OUTER JOIN ' + j.target.table_alias.table + ' AS ' + j.target.table_alias.alias +
                              ' ON (' + j.condition()
                              + ( ' AND ' + self.build_filter(j.filter_op) if j.filter_op else '') + ')', self._join)
        return ('SELECT DISTINCT ' if self._distinct else 'SELECT ') + ','.join(map(lambda ca: sql_operation_to_string(ca), self._select)) \
            + ' FROM ' + ','.join(map(lambda ta: ta.table + ' AS ' + ta.alias, self._from)) \
            + ''.join(joins) \
            + self.__build_where() \
            + self.__build_group_by() \
            + self.__build_having() \
            + self.__build_order_by() \
            + self.__build_limit() \
            + self.__build_offset()

    def __build_where(self) -> str:
        if len(self._where) > 0:
            return ' WHERE ' + ''.join(self._where)
        else:
            return ''

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


@dataclass
class QueryParts:
    """What makes up one query that is combined with others by ``UNION``: the pieces ``to_sql`` takes, apart from the
    order, limit and offset, which apply to the combined rows."""
    business_date: datetime.date | None
    processing_datetime: datetime.datetime | datetime.date | None
    columns: Sequence
    table: Any
    op: Any
    group_by: list = field(default_factory=list)
    business_date_to: datetime.date | None = None
    distinct: bool = False
    having: Any = None
    combined: list[tuple['QueryParts', bool]] = field(default_factory=list)  # (query, keep_duplicates)


def sort_target(sort_op: SortOperation) -> tuple:
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
    return column.column.owner, column.column.name, tuple(joins)


def to_sql(business_date: datetime.date | None, processing_datetime: datetime.datetime | datetime.date | None,
           columns: Sequence, table, op,
           order_by: list | None = None, group_by: list | None = None,
           limit: int | None = None, validate_sqlglot: bool = True,
           business_date_to: datetime.date | None = None, distinct: bool = False,
           offset: int | None = None, having=None,
           combined: list[tuple[QueryParts, bool]] | None = None) -> str:
    """The SQL for the query. ``combined`` holds queries whose rows follow this one's, each with whether duplicates are
    kept (``UNION ALL``) or not (``UNION``); the order, limit and offset then apply to all of the rows."""
    if combined:
        first = to_sql(business_date, processing_datetime, columns, table, op, [], group_by, None, validate_sqlglot,
                       business_date_to=business_date_to, distinct=distinct, having=having)
        return _combined_sql(first, columns, combined, order_by or [], limit, offset, validate_sqlglot)
    select_op = build_query_operation(business_date, processing_datetime, columns, table, op,
                                      order_by or [], group_by or [], limit,
                                      business_date_to=business_date_to, distinct=distinct, offset=offset, having=having)
    return select_sql_to_string(select_op, validate_sqlglot=validate_sqlglot)


def _combined_sql(first: str, columns: Sequence, combined: list[tuple[QueryParts, bool]], order_by: list,
                  limit: int | None, offset: int | None, validate_sqlglot: bool) -> str:
    """The queries joined by ``UNION`` in a derived table, which the order, limit and offset apply to."""
    for part, _ in combined:
        if len(part.columns) != len(columns):
            raise ValueError(f"The results combined have different numbers of columns: "
                             f"{sorted({len(columns), *(len(p.columns) for p, _ in combined)})}")
    inner = first
    for part, keep_duplicates in combined:
        part_sql = to_sql(part.business_date, part.processing_datetime, part.columns, part.table, part.op, [],
                          part.group_by, None, validate_sqlglot, business_date_to=part.business_date_to,
                          distinct=part.distinct, having=part.having, combined=part.combined)
        inner += (' UNION ALL ' if keep_duplicates else ' UNION ') + part_sql
    names = [column.name if isinstance(column, Alias) else display_alias(column) for column in columns]
    quoted = ['"' + name.replace('"', '""') + '"' for name in names]
    sql = 'SELECT ' + ', '.join(f'unionalias_0.{q} AS {q}' for q in quoted) + ' FROM (' + inner + ') AS unionalias_0'
    if order_by:
        sql += ' ORDER BY ' + ', '.join(
            f'unionalias_0.{quoted[_column_position(columns, sort_op)]}'
            + (' DESC' if sort_op.direction == SortDirection.DESC else ' ASC') for sort_op in order_by)
    if limit is not None:
        sql += f' LIMIT {limit}'
    if offset is not None:
        sql += f' OFFSET {offset}'
    if validate_sqlglot:
        sqlglot.transpile(sql, error_level=sqlglot.ErrorLevel.RAISE)
    return sql


def _column_position(columns: Sequence, sort_op: SortOperation) -> int:
    """Where in the columns the sort's column is: the combined rows are ordered by position in the select list."""
    target = sort_target(sort_op)
    for position, column in enumerate(columns):
        column = column.element if isinstance(column, Alias) else column
        as_column = getattr(column, "_column_with_join", None)
        if sort_target(SortOperation(as_column() if as_column else column, sort_op.direction)) == target:
            return position
    raise ValueError("The sort is not by one of the columns of the combined results")


def select_sql_to_string(select_operation: QueryExpression, validate_sqlglot: bool = True) -> str:
    qe = SQLQueryGenerator()
    qe.generate(select_operation)
    sql = qe.build_query_string()
    if validate_sqlglot:
        sqlglot.transpile(sql, error_level=sqlglot.ErrorLevel.RAISE)
    return sql


# --- views -------------------------------------------------------------------------------------------------------

def _as_of_column(column: Column, node: JoinTreeNodeOperation) -> ColumnWithJoin:
    """The joined table's column, read through the join."""
    return ColumnWithJoin(Column(column.name, column.type, column.qualified_name()), node)


def _as_of_root_column(column: Column) -> Column:
    """The root table's column: it is read directly, with no join."""
    return Column(column.name, column.type, column.qualified_name())


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
    if isinstance(root, BiTemporalColumns):
        return root.business_date_from_column
    return None


def build_as_of_join_condition(root_table: Table, node: JoinTreeNodeOperation) -> Operation | None:
    """Join condition pinning a joined milestoned table to the root row's own point in time.

    The child version chosen is the one that applied when the root row was recorded (processing axis)
    and for the root row's business date (business axis), so later changes to the child never alter
    rows already in the view. Raises ViewDefinitionError when the root has no corresponding axis.
    """
    child_table = node.join.target
    if not isinstance(child_table, MilestonedTable):
        return None
    child = child_table.milestoning_columns
    root = root_table.milestoning_columns if isinstance(root_table, MilestonedTable) else None
    where = f"'{node.join.name}' ({child_table.name}) joined from {root_table.name}"
    conditions = []

    if isinstance(child, ProcessingTemporalColumns):
        if not isinstance(root, ProcessingTemporalColumns):
            raise ViewDefinitionError(
                f"{where} is processing-temporal, but {root_table.name} has no processing time to anchor it to")
        conditions.append(_as_of_interval_contains(child.start_at_column, child.end_at_column,
                                                   _as_of_root_column(root.start_at_column), child.infinite_datetime,
                                                   node))

    if isinstance(child, (SingleBusinessDateColumn, BiTemporalColumns)):
        point = _as_of_business_point(root)
        if point is None:
            raise ViewDefinitionError(
                f"{where} has a business date, but {root_table.name} has no business date to anchor it to")
        if isinstance(child, SingleBusinessDateColumn):
            conditions.append(ComparisonOperation(_as_of_column(child.business_date_column, node),
                                                  ComparisonOperator.EQUAL, _as_of_root_column(point)))
        else:
            conditions.append(_as_of_interval_contains(child.business_date_from_column, child.business_date_to_column,
                                                       _as_of_root_column(point), child.infinite_datetime, node))
    return _as_of_and(conditions) if conditions else None


def check_view_joins(view: View) -> None:
    """Raise ViewDefinitionError unless every joined milestoned table can be pinned to the root row's point in time."""
    assert view.table is not None
    for node in _joined_nodes(view):
        build_as_of_join_condition(view.table, node)


def view_sql_to_string(view: View, dialect: str | None = None) -> str:
    """``CREATE OR REPLACE VIEW`` DDL for the view, in generic SQL or transpiled to ``dialect`` (e.g. 'databricks').

    The view's join filters are set here, immediately before rendering, and not when the view is built.
    """
    check_view_joins(view)
    table = view.table
    assert table is not None
    _apply_join_filters(view, lambda node: build_as_of_join_condition(table, node))
    sql = f"CREATE OR REPLACE VIEW {view.qualified_name} AS " + select_sql_to_string(view, validate_sqlglot=False)
    if dialect is None:
        sqlglot.transpile(sql, error_level=sqlglot.ErrorLevel.RAISE)
        return sql
    return sqlglot.transpile(sql, read='', write=dialect)[0]
