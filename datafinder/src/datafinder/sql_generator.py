import datetime

import sqlglot
import sqlglot.errors

from datafinder import DateTimeAttribute, DateAttribute
from datafinder.attribute import Attribute
from datafinder.finder import ExistsOperation, NotExistsOperation
from model.milestoning import ProcessingTemporalColumns, SingleBusinessDateColumn, \
    BusinessDateAndProcessingTemporalColumns, BiTemporalColumns, MilestonedTable
from model.relational import Table, Operation, LogicalOperator, LogicalOperation, RelationalOperationElement, \
    ComparisonOperation, ConstantOperation, ComparisonOperator, StringConstantOperation, DateConstantOperation, \
    DateTimeConstantOperation, IntegerConstantOperation, FloatConstantOperation, BooleanConstantOperation, DecimalConstantOperation, Column, NoOperation, JoinOperation, JoinTreeNodeOperation, \
    UnaryOperation, ColumnWithJoin, AggregateOperation, AggregateOperator, SortOperation, SortDirection, CountAllOperation, \
    ScalarFunction, ScalarFunctionOperation, DatePart, DateExtractOperation, DateArithmeticOperation, DateDiffOperation, \
    IsNullOperation, IsNotNullOperation, \
    WindowFunctionOperation, WindowFunction, WindowSpecification

class Alias:
    def __init__(self, element: RelationalOperationElement, name: str):
        self.element = element
        self.name = name

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
    def __init__(self, source: TableAliasColumn, target: TableAliasColumn, filter_op: RelationalOperationElement = None):
        self.source = source
        self.target = target
        self.filter_op = filter_op

class SelectOperation:
    def __init__(self, display: list[Attribute], filter: Operation, order_by: list[SortOperation] = None,
                 group_by: list = None, limit: int = None, table: Table = None):
        self.display = display
        self.filter = filter
        self.order_by = order_by or []
        self.group_by = group_by or []
        self.limit = limit
        self.table = table

def _open_end_clause(end_attr, value, infinite_datetime: str) -> Operation:
    """Returns `end > value` normally, or `(end > value OR end IS NULL)` when infinite_datetime is None."""
    gt_op = end_attr > value
    if infinite_datetime is None:
        col_ref = ColumnWithJoin(end_attr.column(), end_attr.parent())
        return LogicalOperation(gt_op, LogicalOperator.OR, IsNullOperation(col_ref))
    return gt_op


def build_milestoning_filter_operation(business_date:datetime.date, processing_datetime: datetime.datetime,
                               table:MilestonedTable, join_node:'JoinTreeNodeOperation' = None) -> Operation:
    op = None
    #TODO this should not reference attribute
    mc = table.milestoning_columns
    if isinstance(mc, BiTemporalColumns):
        ops = []
        if business_date is not None:
            date_from = DateAttribute('business_date_from', mc.business_date_from_column.name, mc.business_date_from_column.type, mc.business_date_from_column.table.qualified_name, join_node)
            date_to = DateAttribute('business_date_to', mc.business_date_to_column.name, mc.business_date_to_column.type, mc.business_date_to_column.table.qualified_name, join_node)
            ops.append(LogicalOperation(date_from <= business_date, LogicalOperator.AND, _open_end_clause(date_to, business_date, mc.infinite_datetime)))
        if processing_datetime is not None:
            start_at = DateTimeAttribute('start_at', mc.start_at_column.name, mc.start_at_column.type, mc.start_at_column.table.qualified_name, join_node)
            end_at = DateTimeAttribute('end_at', mc.end_at_column.name, mc.end_at_column.type, mc.end_at_column.table.qualified_name, join_node)
            ops.append(LogicalOperation(start_at <= processing_datetime, LogicalOperator.AND, _open_end_clause(end_at, processing_datetime, mc.infinite_datetime)))
        op = ops[0] if len(ops) == 1 else LogicalOperation(ops[0], LogicalOperator.AND, ops[1]) if ops else None
    elif isinstance(mc, BusinessDateAndProcessingTemporalColumns):
        ops = []
        if business_date is not None:
            business_att = DateAttribute('business_date', mc.business_date_column.name, mc.business_date_column.type, mc.business_date_column.table.qualified_name, join_node)
            ops.append(business_att == business_date)
        if processing_datetime is not None:
            start_at = DateTimeAttribute('start_at', mc.start_at_column.name, mc.start_at_column.type, mc.start_at_column.table.qualified_name, join_node)
            end_at = DateTimeAttribute('end_at', mc.end_at_column.name, mc.end_at_column.type, mc.end_at_column.table.qualified_name, join_node)
            ops.append(LogicalOperation(start_at <= processing_datetime, LogicalOperator.AND, _open_end_clause(end_at, processing_datetime, mc.infinite_datetime)))
        op = ops[0] if len(ops) == 1 else LogicalOperation(ops[0], LogicalOperator.AND, ops[1]) if ops else None
    elif isinstance(mc, ProcessingTemporalColumns) and processing_datetime is not None:
        start_at = DateTimeAttribute('start_at', mc.start_at_column.name, mc.start_at_column.type, mc.start_at_column.table.qualified_name, join_node)
        end_at = DateTimeAttribute('end_at', mc.end_at_column.name, mc.end_at_column.type, mc.end_at_column.table.qualified_name, join_node)
        op = LogicalOperation(start_at <= processing_datetime, LogicalOperator.AND, _open_end_clause(end_at, processing_datetime, mc.infinite_datetime))
    elif isinstance(mc, SingleBusinessDateColumn) and business_date is not None:
        business_att = DateAttribute('business_date', mc.business_date_column.name, mc.business_date_column.type, mc.business_date_column.table.qualified_name, join_node)
        op = business_att == business_date
    return op

def find_column(operation: RelationalOperationElement) -> ColumnWithJoin:
    if isinstance(operation, UnaryOperation):
        return find_column(operation.element)
    elif isinstance(operation, ColumnWithJoin):
        return operation
    else:
        raise TypeError(operation)

def collect_required_joins(op: RelationalOperationElement, required_joins: set):
    if op is None:
        return
    if isinstance(op, Attribute):
        parent = op.parent()
        if parent is not None:
            required_joins.add(parent)
    elif isinstance(op, ColumnWithJoin):
        if op.parent is not None:
            required_joins.add(op.parent)
    elif isinstance(op, AggregateOperation):
        collect_required_joins(op.element, required_joins)
        collect_required_joins(op.window, required_joins)
    elif isinstance(op, ScalarFunctionOperation):
        collect_required_joins(op.element, required_joins)
    elif isinstance(op, DateExtractOperation):
        collect_required_joins(op.element, required_joins)
    elif isinstance(op, DateArithmeticOperation):
        collect_required_joins(op.element, required_joins)
    elif isinstance(op, DateDiffOperation):
        collect_required_joins(op.element, required_joins)
    elif isinstance(op, WindowFunctionOperation):
        if op.element is not None:
            collect_required_joins(op.element, required_joins)
        collect_required_joins(op.window, required_joins)
    elif isinstance(op, WindowSpecification):
        for part in op.partition_by:
            collect_required_joins(part, required_joins)
        for sort_op in op.order_by:
            collect_required_joins(sort_op.column, required_joins)
    elif isinstance(op, SortOperation):
        collect_required_joins(op.column, required_joins)
    elif isinstance(op, Alias):
        collect_required_joins(op.element, required_joins)
    elif isinstance(op, LogicalOperation):
        collect_required_joins(op.left, required_joins)
        collect_required_joins(op.right, required_joins)
    elif isinstance(op, ComparisonOperation):
        collect_required_joins(op.left, required_joins)
        collect_required_joins(op.right, required_joins)

def build_query_operation(business_date:datetime.date, processing_datetime: datetime.datetime,
                         columns: list[Attribute], table: Table, op: Operation,
                         order_by: list[SortOperation] = None, group_by: list = None,
                         limit: int = None) -> SelectOperation:
    if isinstance(table, MilestonedTable):
        milestoned_op = build_milestoning_filter_operation(business_date, processing_datetime, table)
        op = milestoned_op if isinstance(op, NoOperation) else LogicalOperation(op, LogicalOperator.AND, milestoned_op)

    required_joins: list[JoinTreeNodeOperation] = []
    required_join_ids: set = set()
    for col in columns:
        collect_required_joins(col, required_joins)
    for group_by_expr in group_by or []:
        collect_required_joins(group_by_expr, required_joins)
    for sort_op in order_by or []:
        collect_required_joins(sort_op.column, required_joins)
    collect_required_joins(op, required_joins)

    for node in required_joins:
        if isinstance(node.join.target, MilestonedTable):
            milestoned_op = build_milestoning_filter_operation(business_date, processing_datetime, node.join.target, node)
            node.join.filter = milestoned_op

    select = SelectOperation(columns, op, order_by or [], group_by or [], limit, table)
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

def logical_operator_string(op:LogicalOperator) -> str:
    return LOGICAL_OPERATOR_STR.get(op)

def comparison_operator_string(op:ComparisonOperator) -> str:
    return COMPARISON_OPERATOR_STR.get(op)

def constant_value_string(op:ConstantOperation) -> str:
    if isinstance(op, StringConstantOperation):
        return "'" + op.value + "'"
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

def sql_operation_to_string(operation: RelationalOperationElement) -> str:
    if isinstance(operation, TableAliasColumn):
        return table_alias_column_string(operation)
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
            parts.append("'" + arg + "'" if isinstance(arg, str) else str(arg))
        sql = fn + '(' + ', '.join(parts) + ')'
        sql += ' ' + _window_spec_to_string(operation.window) if operation.window is not None else ' OVER ()'
        return sql
    elif isinstance(operation, ScalarFunctionOperation):
        fn = _SCALAR_SQL_NAMES[operation.function]
        parts = [sql_operation_to_string(operation.element)]
        if operation.second_arg is not None:
            parts.append(str(operation.second_arg))
        for arg in operation.extra_args:
            parts.append("'" + arg + "'" if isinstance(arg, str) else str(arg))
        return fn + '(' + ', '.join(parts) + ')'
    elif isinstance(operation, DateExtractOperation):
        return 'EXTRACT(' + operation.part.value + ' FROM ' + sql_operation_to_string(operation.element) + ')'
    elif isinstance(operation, DateArithmeticOperation):
        op = '+' if operation.is_add else '-'
        return sql_operation_to_string(operation.element) + ' ' + op + ' INTERVAL ' + str(operation.n) + ' ' + operation.unit.value
    elif isinstance(operation, DateDiffOperation):
        other_sql = sql_format_datetime(operation.other) if isinstance(operation.other, datetime.datetime) else sql_format_date(operation.other)
        return "DATE_DIFF('" + operation.unit.value.lower() + "', " + sql_operation_to_string(operation.element) + ', ' + other_sql + ')'
    elif isinstance(operation, CountAllOperation):
        return 'COUNT(*)'
    elif isinstance(operation, Alias):
        return sql_operation_to_string(operation.element) + ' AS "' + operation.name + '"'
    else:
        raise TypeError(operation)

class SQLQueryGenerator:
    _select: list[Alias]
    _from: set[TableAlias]
    _join: list[Join]
    __table_alias_incr: int
    _where: str

    def __init__(self):
        self._select = []
        self._from = set()
        self._join = []
        self._group_by_parts = []
        self._order_by_parts = []
        self.__table_alias_incr = 0
        self.__table_aliases_by_table = {}
        self.__added_join_ids = set()
        self._root_table = None

    def generate(self, select:SelectOperation):
        self._root_table = select.table
        self.select(select.display)
        self._where = self.build_filter(select.filter)
        required_joins = set()
        for group_by_expr in select.group_by:
            self.__collect_required_joins(group_by_expr, required_joins)
        for sort_op in select.order_by:
            self.__collect_required_joins(sort_op.column, required_joins)
        for parent in required_joins:
            self.__add_join(parent)
        self._group_by_parts = [
            self.__attr_to_col_string(a) for a in select.group_by
        ]
        self._order_by_parts = [
            self.build_filter(s.column) + (' ASC' if s.direction == SortDirection.ASC else ' DESC')
            for s in select.order_by
        ]
        self._limit = select.limit

    def __attr_to_col_string(self, attr: Attribute) -> str:
        parent = attr.parent()
        ta = self.__table_alias_for_table(attr.owner(), key=self.__join_target_key(parent) if parent is not None else None)
        return ta.alias + '.' + attr.column().name

    @staticmethod
    def __is_self_join(parent: JoinOperation) -> bool:
        return parent.left.owner == parent.right.owner

    def __join_target_key(self, parent: JoinOperation):
        """Cache key for the join target alias. Use join op identity only for self-joins
        so the target gets a distinct alias from the source table."""
        return parent if self.__is_self_join(parent) else None

    def __table_alias_for_column(self, column: Column, parent: JoinOperation = None) -> TableAlias:
        key = self.__join_target_key(parent) if parent is not None else None
        return self.__table_alias_for_table(column.owner, key=key)

    def __rewrite_window_spec(self, window: WindowSpecification):
        if window is None:
            return None
        partition_by = [self.__rewrite_operation(p) for p in window.partition_by]
        order_by = [SortOperation(self.__rewrite_operation(s.column), s.direction) for s in window.order_by]
        return WindowSpecification(partition_by, order_by)

    def __rewrite_operation(self, op: RelationalOperationElement):
        if isinstance(op, Attribute):
            return TableAliasColumn(op.column(), self.__table_alias_for_column(op.column(), op.parent()))
        elif isinstance(op, ColumnWithJoin):
            return TableAliasColumn(op.column, self.__table_alias_for_column(op.column, op.parent))
        elif isinstance(op, Column):
            return TableAliasColumn(op, self.__table_alias_for_table(op.owner))
        elif isinstance(op, AggregateOperation):
            return AggregateOperation(self.__rewrite_operation(op.element), op.operator, op.display_name,
                                      self.__rewrite_window_spec(op.window))
        elif isinstance(op, ScalarFunctionOperation):
            return ScalarFunctionOperation(self.__rewrite_operation(op.element), op.function, op.display_name,
                                           second_arg=op.second_arg, extra_args=list(op.extra_args))
        elif isinstance(op, DateExtractOperation):
            return DateExtractOperation(self.__rewrite_operation(op.element), op.part, op.display_name)
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

    def __collect_required_joins(self, op: RelationalOperationElement, required_joins: set):
        if op is None:
            return
        if isinstance(op, Attribute):
            parent = op.parent()
            if parent is not None:
                required_joins.add(parent)
        elif isinstance(op, ColumnWithJoin):
            if op.parent is not None:
                required_joins.add(op.parent)
        elif isinstance(op, AggregateOperation):
            self.__collect_required_joins(op.element, required_joins)
            self.__collect_required_joins(op.window, required_joins)
        elif isinstance(op, ScalarFunctionOperation):
            self.__collect_required_joins(op.element, required_joins)
        elif isinstance(op, DateExtractOperation):
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

    def select(self, cols: list[Attribute]):
        required_joins = set()
        if self._root_table is not None:
            self._from.add(self.__table_alias_for_table(self._root_table.name))

        for col in cols:
            self.__collect_required_joins(col, required_joins)
            rewritten = self.__rewrite_operation(col)

            if isinstance(col, CountAllOperation):
                self._select.append(Alias(rewritten, 'Count'))
            elif isinstance(col, Attribute):
                self._select.append(Alias(rewritten, col.display_name()))
            elif isinstance(col, AggregateOperation):
                col_nested = find_column(col)
                alias = col.display_name if col.display_name else col.operator.name + ' ' + col_nested.column.name
                self._select.append(Alias(rewritten, alias))
            elif isinstance(col, ScalarFunctionOperation):
                col_nested = find_column(col)
                alias = col.display_name if col.display_name else col.function.name + ' ' + col_nested.column.name
                self._select.append(Alias(rewritten, alias))
            elif isinstance(col, (DateExtractOperation, DateArithmeticOperation, DateDiffOperation)):
                col_nested = find_column(col)
                alias = col.display_name if col.display_name else col_nested.column.name
                self._select.append(Alias(rewritten, alias))
            elif isinstance(col, WindowFunctionOperation):
                if col.display_name:
                    alias = col.display_name
                elif col.function in (WindowFunction.ROW_NUMBER, WindowFunction.RANK, WindowFunction.DENSE_RANK):
                    alias = col.function.name.replace('_', ' ').title()
                else:
                    alias = col.function.name + ('' if col.element is None else ' ' + find_column(col).column.name)
                self._select.append(Alias(rewritten, alias))
            else:
                self._select.append(Alias(rewritten, str(col)))

        for node in required_joins:
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
        self._join.append(Join(sc, tc, join.filter))

    def build_filter(self, op:RelationalOperationElement) -> str:
        if isinstance(op, NoOperation):
            return ''
        elif isinstance(op, ExistsOperation):
            col = op.node.join.right
            return self.build_filter(ColumnWithJoin(col, op.node)) + ' IS NOT NULL'
        elif isinstance(op, NotExistsOperation):
            col = op.node.join.right
            return self.build_filter(ColumnWithJoin(col, op.node)) + ' IS NULL'
        elif isinstance(op, IsNotNullOperation):
            return self.build_filter(op.element) + ' IS NOT NULL'
        elif isinstance(op, IsNullOperation):
            return self.build_filter(op.element) + ' IS NULL'
        elif isinstance(op, LogicalOperation):
            left_str = self.build_filter(op.left)
            right_str = self.build_filter(op.right)
            if isinstance(op.left, LogicalOperation):
                left_str = '(' + left_str + ')'
            if isinstance(op.right, LogicalOperation):
                right_str = '(' + right_str + ')'
            return left_str + logical_operator_string(op.operator) + right_str
        elif isinstance(op, ComparisonOperation):
            return self.build_filter(op.left) + comparison_operator_string(op.operator) + self.build_filter(op.right)
        elif isinstance(op, ConstantOperation):
            return constant_value_string(op)
        elif isinstance(op, ColumnWithJoin):
            node = op.parent
            if node is not None:
                ta = self.__table_alias_for_table(op.column.owner, key=node)
                self.__add_join(node)
            else:
                ta = self.__table_alias_for_table(op.column.owner)
            return ta.alias + '.' + op.column.name
        elif isinstance(op, Column):
            ta = self.__table_alias_for_table(op.owner)
            return ta.alias + '.' + op.name
        else:
            raise ValueError(op)

    def __table_alias_for_table(self, table: str, key=None) -> TableAlias:
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
                              ' ON ' + j.source.table_alias.alias + '.' + j.source.column.name + ' = ' +
                              j.target.table_alias.alias + '.' + j.target.column.name
                              + ( ' AND ' + self.build_filter(j.filter_op) if j.filter_op else ''), self._join)
        return 'SELECT ' + ','.join(map(lambda ca: sql_operation_to_string(ca), self._select)) \
            + ' FROM ' + ','.join(map(lambda ta: ta.table + ' AS ' + ta.alias, self._from)) \
            + ''.join(joins) \
            + self.__build_where() \
            + self.__build_group_by() \
            + self.__build_order_by() \
            + self.__build_limit()

    def __build_where(self) -> str:
        if len(self._where) > 0:
            return ' WHERE ' + ''.join(self._where)
        else:
            return ''

    def __build_group_by(self) -> str:
        if not self._group_by_parts:
            return ''
        return ' GROUP BY ' + ', '.join(self._group_by_parts)

    def __build_order_by(self) -> str:
        if not self._order_by_parts:
            return ''
        return ' ORDER BY ' + ', '.join(self._order_by_parts)

    def __build_limit(self) -> str:
        if self._limit is None:
            return ''
        return ' LIMIT ' + str(self._limit)


def to_sql(business_date: datetime.date, processing_datetime: datetime.datetime,
           columns: list, table, op,
           order_by: list = None, group_by: list = None,
           limit: int = None, validate_sqlglot: bool = True) -> str:
    select_op = build_query_operation(business_date, processing_datetime, columns, table, op,
                                      order_by or [], group_by or [], limit)
    return select_sql_to_string(select_op, validate_sqlglot=validate_sqlglot)


def select_sql_to_string(select_operation: SelectOperation, validate_sqlglot: bool = True) -> str:
    qe = SQLQueryGenerator()
    qe.generate(select_operation)
    sql = qe.build_query_string()
    if validate_sqlglot:
        sqlglot.transpile(sql, error_level=sqlglot.ErrorLevel.RAISE)
    return sql

