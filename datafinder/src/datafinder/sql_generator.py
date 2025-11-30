import datetime

from datafinder import DateTimeAttribute, DateAttribute
from datafinder.attribute import Attribute
from model.milestoning import ProcessingTemporalColumns, SingleBusinessDateColumn, MilestonedTable
from model.relational import Table, Operation, LogicalOperator, LogicalOperation, RelationalOperationElement, \
    ComparisonOperation, ConstantOperation, ComparisonOperator, StringConstantOperation, DateConstantOperation, \
    DateTimeConstantOperation, IntegerConstantOperation, FloatConstantOperation, Column, NoOperation, JoinOperation


class TableAlias:
    def __init__(self, table: str, alias: str):
        self.table = table
        self.alias = alias


class TableAliasColumn:
    def __init__(self, column: Column, table_alias: TableAlias, column_alias: str = None):
        self.column = column
        self.table_alias = table_alias
        self.column_alias = column_alias


class Join:
    def __init__(self, source: TableAliasColumn, target: TableAliasColumn, filter_op: RelationalOperationElement = None):
        self.source = source
        self.target = target
        self.filter_op = filter_op

class SelectOperation:
    def __init__(self, display: list[Attribute], table: str, filter: Operation):
        self.display = display
        self.table = table
        self.filter = filter

def build_milestoning_filter_operation(business_date:datetime.date, processing_datetime: datetime.datetime,
                               table:MilestonedTable) -> Operation:
    op = None
    #TODO this should not reference attribute
    if isinstance(table.milestoning_columns, ProcessingTemporalColumns) and processing_datetime is not None:
        ptc:ProcessingTemporalColumns = table.milestoning_columns
        start_at = DateTimeAttribute('start_at', ptc.start_at_column.name, ptc.start_at_column.type, ptc.start_at_column.table.name)
        end_at = DateTimeAttribute('end_at', ptc.end_at_column.name, ptc.end_at_column.type, ptc.end_at_column.table.name)
        op = LogicalOperation(start_at <= processing_datetime,LogicalOperator.AND, (end_at > processing_datetime))
    elif isinstance(table.milestoning_columns, SingleBusinessDateColumn) and business_date is not None:
        sbdc:SingleBusinessDateColumn = table.milestoning_columns
        business_att = DateAttribute('business_date', sbdc.business_date_column.name, sbdc.business_date_column.type, sbdc.business_date_column.table.name)
        op = business_att == business_date
    return op

def build_query_operation(business_date:datetime.date, processing_datetime: datetime.datetime,
                         columns: list[Attribute], table: Table, op: Operation) -> SelectOperation:
    if isinstance(table, MilestonedTable):
        milestoned_op = build_milestoning_filter_operation(business_date, processing_datetime, table)
        op = milestoned_op if isinstance(op, NoOperation) else LogicalOperation(op, LogicalOperator.AND, milestoned_op)

    required_joins = set()
    for col in columns:
        parent: JoinOperation = col.parent()
        if parent is not None:
            required_joins.add(parent)

    for j in required_joins:
        if isinstance(j.target, MilestonedTable):
            milestoned_op = build_milestoning_filter_operation(business_date, processing_datetime, j.target)
            j.filter = milestoned_op

    select = SelectOperation(columns, table.name, op)
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
    ComparisonOperator.NOT_EQUAL: ' <> '
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
    else:
        raise ValueError

def table_alias_column_string(tac: TableAliasColumn) -> str:
    col_str = tac.table_alias.alias + '.' + tac.column.name
    return col_str if tac.column_alias is None else col_str + ' AS \'' + tac.column_alias + '\''

class SQLQueryGenerator:
    _select: list[TableAliasColumn]
    _from: set[TableAlias]
    _join: list[Join]
    __table_alias_incr: int
    _where: str

    def __init__(self):
        self._select = []
        self._from = set()
        self._join = []
        self.__table_alias_incr = 0
        self.__table_aliases_by_table = {}

    def generate(self, select:SelectOperation):
        self.select(select.display)
        self._where = self.build_filter(select.filter)

    def select(self, cols: list[Attribute]):
        required_joins = set()

        for col in cols:
            table = col.owner()
            ta = self.__table_alias_for_table(table)
            parent: JoinOperation = col.parent()
            if parent is not None:
                required_joins.add(parent)
            else:
                self._from.add(ta)
            ca = TableAliasColumn(col.column(), ta, col.display_name())
            self._select.append(ca)

        for parent in required_joins:
            left = parent.left
            sc = TableAliasColumn(left, self.__table_alias_for_table(left.owner))
            right = parent.right
            tc = TableAliasColumn(right, self.__table_alias_for_table(right.owner))
            self._join.append(Join(sc, tc, parent.filter))

    def build_filter(self, op:RelationalOperationElement) -> str:
        if isinstance(op, LogicalOperation):
            return self.build_filter(op.left) + logical_operator_string(op.operator) + self.build_filter(op.right)
        elif isinstance(op, ComparisonOperation):
            return self.build_filter(op.left) + comparison_operator_string(op.operator) + self.build_filter(op.right)
        elif isinstance(op, ConstantOperation):
            return constant_value_string(op)
        elif isinstance(op, Column):
            ta = self.__table_alias_for_table(op.owner)
            return ta.alias + '.' + op.name
        else:
            raise ValueError(op)

    def __table_alias_for_table(self, table: str) -> TableAlias:
        ta = None
        if table is None:
            raise TypeError

        if table in self.__table_aliases_by_table:
            ta = self.__table_aliases_by_table[table]
        else:
            ta = TableAlias(table, "t" + str(self.__table_alias_incr))
            self.__table_alias_incr = self.__table_alias_incr + 1
            self.__table_aliases_by_table[table] = ta
        return ta

    def build_query_string(self) -> str:
        joins = map(lambda j: ' LEFT OUTER JOIN ' + j.target.table_alias.table + ' AS ' + j.target.table_alias.alias +
                              ' ON ' + j.source.table_alias.alias + '.' + j.source.column.name + ' = ' +
                              j.target.table_alias.alias + '.' + j.target.column.name
                              + ( ' AND ' + self.build_filter(j.filter_op) if j.filter_op else ''), self._join)
        return 'SELECT ' + ','.join(map(lambda ca: table_alias_column_string(ca), self._select)) \
            + ' FROM ' + ','.join(map(lambda ta: ta.table + ' AS ' + ta.alias, self._from)) \
            + ''.join(joins) \
            + self.__build_where()

    def __build_where(self) -> str:
        if len(self._where) > 0:
            return ' WHERE ' + ''.join(self._where)
        else:
            return ''


def select_sql_to_string(select_operation: SelectOperation) -> str:
    qe = SQLQueryGenerator()
    qe.generate(select_operation)
    return qe.build_query_string()

