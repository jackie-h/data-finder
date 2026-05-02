import datetime

from datafinder import DateTimeAttribute, DateAttribute
from datafinder.attribute import Attribute
from model.milestoning import ProcessingTemporalColumns, SingleBusinessDateColumn, \
    BusinessDateAndProcessingTemporalColumns, BiTemporalColumns, MilestonedTable
from model.relational import Table, Operation, LogicalOperator, LogicalOperation, RelationalOperationElement, \
    ComparisonOperation, ConstantOperation, ComparisonOperator, StringConstantOperation, DateConstantOperation, \
    DateTimeConstantOperation, IntegerConstantOperation, FloatConstantOperation, BooleanConstantOperation, DecimalConstantOperation, Column, NoOperation, JoinOperation, \
    UnaryOperation, ColumnWithJoin, AggregateOperation, AggregateOperator, SortOperation, SortDirection, CountAllOperation

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
                 group_by: list = None):
        self.display = display
        self.filter = filter
        self.order_by = order_by or []
        self.group_by = group_by or []

def build_milestoning_filter_operation(business_date:datetime.date, processing_datetime: datetime.datetime,
                               table:MilestonedTable) -> Operation:
    op = None
    #TODO this should not reference attribute
    mc = table.milestoning_columns
    if isinstance(mc, BiTemporalColumns):
        ops = []
        if business_date is not None:
            date_from = DateAttribute('business_date_from', mc.business_date_from_column.name, mc.business_date_from_column.type, mc.business_date_from_column.table.name)
            date_to = DateAttribute('business_date_to', mc.business_date_to_column.name, mc.business_date_to_column.type, mc.business_date_to_column.table.name)
            ops.append(LogicalOperation(date_from <= business_date, LogicalOperator.AND, date_to > business_date))
        if processing_datetime is not None:
            start_at = DateTimeAttribute('start_at', mc.start_at_column.name, mc.start_at_column.type, mc.start_at_column.table.name)
            end_at = DateTimeAttribute('end_at', mc.end_at_column.name, mc.end_at_column.type, mc.end_at_column.table.name)
            ops.append(LogicalOperation(start_at <= processing_datetime, LogicalOperator.AND, end_at > processing_datetime))
        op = ops[0] if len(ops) == 1 else LogicalOperation(ops[0], LogicalOperator.AND, ops[1]) if ops else None
    elif isinstance(mc, BusinessDateAndProcessingTemporalColumns):
        ops = []
        if business_date is not None:
            business_att = DateAttribute('business_date', mc.business_date_column.name, mc.business_date_column.type, mc.business_date_column.table.name)
            ops.append(business_att == business_date)
        if processing_datetime is not None:
            start_at = DateTimeAttribute('start_at', mc.start_at_column.name, mc.start_at_column.type, mc.start_at_column.table.name)
            end_at = DateTimeAttribute('end_at', mc.end_at_column.name, mc.end_at_column.type, mc.end_at_column.table.name)
            ops.append(LogicalOperation(start_at <= processing_datetime, LogicalOperator.AND, end_at > processing_datetime))
        op = ops[0] if len(ops) == 1 else LogicalOperation(ops[0], LogicalOperator.AND, ops[1]) if ops else None
    elif isinstance(mc, ProcessingTemporalColumns) and processing_datetime is not None:
        start_at = DateTimeAttribute('start_at', mc.start_at_column.name, mc.start_at_column.type, mc.start_at_column.table.name)
        end_at = DateTimeAttribute('end_at', mc.end_at_column.name, mc.end_at_column.type, mc.end_at_column.table.name)
        op = LogicalOperation(start_at <= processing_datetime, LogicalOperator.AND, end_at > processing_datetime)
    elif isinstance(mc, SingleBusinessDateColumn) and business_date is not None:
        business_att = DateAttribute('business_date', mc.business_date_column.name, mc.business_date_column.type, mc.business_date_column.table.name)
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
                         order_by: list[SortOperation] = None, group_by: list = None) -> SelectOperation:
    if isinstance(table, MilestonedTable):
        milestoned_op = build_milestoning_filter_operation(business_date, processing_datetime, table)
        op = milestoned_op if isinstance(op, NoOperation) else LogicalOperation(op, LogicalOperator.AND, milestoned_op)

    required_joins = set()
    for col in columns:
        if isinstance(col, CountAllOperation):
            continue
        if isinstance(col, Attribute):
            parent: JoinOperation = col.parent()
        else:
            parent: JoinOperation = find_column(col).parent
        if parent is not None:
            required_joins.add(parent)

    for j in required_joins:
        if isinstance(j.target, MilestonedTable):
            milestoned_op = build_milestoning_filter_operation(business_date, processing_datetime, j.target)
            j.filter = milestoned_op

    select = SelectOperation(columns, op, order_by or [], group_by or [])
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
    ComparisonOperator.EQUAL: ' == ',
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

def sql_operation_to_string(operation: RelationalOperationElement) -> str:
    if isinstance(operation, TableAliasColumn):
        return table_alias_column_string(operation)
    elif isinstance(operation, AggregateOperation):
        fn = _AGGREGATE_SQL_NAMES.get(operation.operator, operation.operator.name)
        return fn + '(' + sql_operation_to_string(operation.element) + ')'
    elif isinstance(operation, CountAllOperation):
        return 'COUNT(*)'
    elif isinstance(operation, Alias):
        return sql_operation_to_string(operation.element) + ' AS \'' + operation.name + '\''
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

    def __attr_to_col_string(self, attr: Attribute) -> str:
        ta = self.__table_alias_for_table(attr.owner())
        return ta.alias + '.' + attr.column().name

    @staticmethod
    def __is_self_join(parent: JoinOperation) -> bool:
        return parent.left.owner == parent.right.owner

    def __join_target_key(self, parent: JoinOperation):
        """Cache key for the join target alias. Use join op identity only for self-joins
        so the target gets a distinct alias from the source table."""
        return parent if self.__is_self_join(parent) else None

    def select(self, cols: list[Attribute]):
        required_joins = set()

        for col in cols:
            if isinstance(col, Attribute):
                table = col.owner()
                parent: JoinOperation = col.parent()
                if parent is not None:
                    required_joins.add(parent)
                    ta = self.__table_alias_for_table(table, key=self.__join_target_key(parent))
                else:
                    ta = self.__table_alias_for_table(table)
                    self._from.add(ta)
                ca = Alias(TableAliasColumn(col.column(), ta), col.display_name())
                self._select.append(ca)
            elif isinstance(col, AggregateOperation):
                col_nested = find_column(col)
                table = col_nested.column.owner
                parent: JoinOperation = col_nested.parent
                if parent is not None:
                    required_joins.add(parent)
                    ta = self.__table_alias_for_table(table, key=self.__join_target_key(parent))
                else:
                    ta = self.__table_alias_for_table(table)
                    self._from.add(ta)
                alias = col.display_name if col.display_name else col.operator.name + ' ' + col_nested.column.name
                ca = Alias(AggregateOperation(TableAliasColumn(col_nested.column, ta), col.operator), alias)
                self._select.append(ca)
            elif isinstance(col, CountAllOperation):
                ta = self.__table_alias_for_table(col.table)
                self._from.add(ta)
                self._select.append(Alias(col, 'Count'))

        for parent in required_joins:
            self.__add_join(parent)

    def __add_join(self, parent: JoinOperation):
        if id(parent) in self.__added_join_ids:
            return
        self.__added_join_ids.add(id(parent))
        left = parent.left
        sc = TableAliasColumn(left, self.__table_alias_for_table(left.owner))
        right = parent.right
        tc = TableAliasColumn(right, self.__table_alias_for_table(right.owner, key=self.__join_target_key(parent)))
        self._join.append(Join(sc, tc, parent.filter))

    def build_filter(self, op:RelationalOperationElement) -> str:
        if isinstance(op, NoOperation):
            return ''
        elif isinstance(op, LogicalOperation):
            return self.build_filter(op.left) + logical_operator_string(op.operator) + self.build_filter(op.right)
        elif isinstance(op, ComparisonOperation):
            return self.build_filter(op.left) + comparison_operator_string(op.operator) + self.build_filter(op.right)
        elif isinstance(op, ConstantOperation):
            return constant_value_string(op)
        elif isinstance(op, ColumnWithJoin):
            parent = op.parent
            if parent is not None:
                ta = self.__table_alias_for_table(op.column.owner, key=self.__join_target_key(parent))
                self.__add_join(parent)
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
            + self.__build_order_by()

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


def select_sql_to_string(select_operation: SelectOperation) -> str:
    qe = SQLQueryGenerator()
    qe.generate(select_operation)
    return qe.build_query_string()

