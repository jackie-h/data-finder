import datetime

import sqlglot
import sqlglot.errors

from datafinder import DateTimeAttribute, DateAttribute
from datafinder.attribute import Attribute
from model.milestoning import ProcessingTemporalColumns, SingleBusinessDateColumn, \
    BusinessDateAndProcessingTemporalColumns, BiTemporalColumns, MilestonedTable
from model.relational import Table, Operation, LogicalOperator, LogicalOperation, RelationalOperationElement, \
    ComparisonOperation, ConstantOperation, ComparisonOperator, StringConstantOperation, DateConstantOperation, \
    DateTimeConstantOperation, IntegerConstantOperation, FloatConstantOperation, BooleanConstantOperation, DecimalConstantOperation, Column, NoOperation, JoinOperation, JoinTreeNodeOperation, \
    UnaryOperation, ColumnWithJoin, AggregateOperation, AggregateOperator, SortOperation, SortDirection, CountAllOperation, \
    ScalarFunction, ScalarFunctionOperation, DatePart, DateExtractOperation, DateArithmeticOperation, DateDiffOperation, \
    IsNullOperation, IsNotNullOperation

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
                 group_by: list = None, limit: int = None):
        self.display = display
        self.filter = filter
        self.order_by = order_by or []
        self.group_by = group_by or []
        self.limit = limit

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
        if isinstance(col, CountAllOperation):
            continue
        if isinstance(col, Attribute):
            node = col.parent()
        else:
            node = find_column(col).parent
        if node is not None and id(node) not in required_join_ids:
            required_join_ids.add(id(node))
            required_joins.append(node)

    for node in required_joins:
        if isinstance(node.join.target, MilestonedTable):
            milestoned_op = build_milestoning_filter_operation(business_date, processing_datetime, node.join.target, node)
            node.join.filter = milestoned_op

    select = SelectOperation(columns, op, order_by or [], group_by or [], limit)
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

def sql_operation_to_string(operation: RelationalOperationElement) -> str:
    if isinstance(operation, TableAliasColumn):
        return table_alias_column_string(operation)
    elif isinstance(operation, AggregateOperation):
        fn = _AGGREGATE_SQL_NAMES.get(operation.operator, operation.operator.name)
        return fn + '(' + sql_operation_to_string(operation.element) + ')'
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

    def generate(self, select:SelectOperation):
        self.select(select.display)
        self._where = self.build_filter(select.filter)
        self._group_by_parts = [
            self.__attr_to_col_string(a) for a in select.group_by
        ]
        self._order_by_parts = [
            self.build_filter(s.column) + (' ASC' if s.direction == SortDirection.ASC else ' DESC')
            for s in select.order_by
        ]
        self._limit = select.limit

    def __attr_to_col_string(self, attr: Attribute) -> str:
        node = attr.parent()
        ta = self.__table_alias_for_table(attr.owner(), key=node)
        return ta.alias + '.' + attr.column().name

    def select(self, cols: list[Attribute]):
        required_joins: list[JoinTreeNodeOperation] = []
        required_join_ids: set = set()

        def _require(node: JoinTreeNodeOperation):
            if node is not None and id(node) not in required_join_ids:
                required_join_ids.add(id(node))
                required_joins.append(node)

        for col in cols:
            if isinstance(col, Attribute):
                table = col.owner()
                node = col.parent()
                if node is not None:
                    _require(node)
                    ta = self.__table_alias_for_table(table, key=node)
                else:
                    ta = self.__table_alias_for_table(table)
                    self._from.add(ta)
                ca = Alias(TableAliasColumn(col.column(), ta), col.display_name())
                self._select.append(ca)
            elif isinstance(col, AggregateOperation):
                col_nested = find_column(col)
                table = col_nested.column.owner
                node = col_nested.parent
                if node is not None:
                    _require(node)
                    ta = self.__table_alias_for_table(table, key=node)
                else:
                    ta = self.__table_alias_for_table(table)
                    self._from.add(ta)
                alias = col.display_name if col.display_name else col.operator.name + ' ' + col_nested.column.name
                ca = Alias(AggregateOperation(TableAliasColumn(col_nested.column, ta), col.operator), alias)
                self._select.append(ca)
            elif isinstance(col, ScalarFunctionOperation):
                col_nested = find_column(col)
                table = col_nested.column.owner
                node = col_nested.parent
                if node is not None:
                    _require(node)
                    ta = self.__table_alias_for_table(table, key=node)
                else:
                    ta = self.__table_alias_for_table(table)
                    self._from.add(ta)
                alias = col.display_name if col.display_name else col.function.name + ' ' + col_nested.column.name
                ca = Alias(ScalarFunctionOperation(TableAliasColumn(col_nested.column, ta), col.function,
                                                   second_arg=col.second_arg, extra_args=col.extra_args), alias)
                self._select.append(ca)
            elif isinstance(col, (DateExtractOperation, DateArithmeticOperation, DateDiffOperation)):
                col_nested = find_column(col)
                table = col_nested.column.owner
                node = col_nested.parent
                if node is not None:
                    _require(node)
                    ta = self.__table_alias_for_table(table, key=node)
                else:
                    ta = self.__table_alias_for_table(table)
                    self._from.add(ta)
                alias = col.display_name if col.display_name else col_nested.column.name
                if isinstance(col, DateExtractOperation):
                    ca = Alias(DateExtractOperation(TableAliasColumn(col_nested.column, ta), col.part), alias)
                elif isinstance(col, DateArithmeticOperation):
                    ca = Alias(DateArithmeticOperation(TableAliasColumn(col_nested.column, ta), col.n, col.unit, col.is_add), alias)
                else:
                    ca = Alias(DateDiffOperation(TableAliasColumn(col_nested.column, ta), col.other, col.unit), alias)
                self._select.append(ca)
            elif isinstance(col, CountAllOperation):
                ta = self.__table_alias_for_table(col.table)
                self._from.add(ta)
                self._select.append(Alias(col, 'Count'))

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

