import datetime

from datafinder.attribute import Attribute
from model.relational import Table, Operation, LogicalOperator, LogicalOperation, RelationalOperationElement, \
    ComparisonOperation, ConstantOperation, ComparisonOperator, StringConstantOperation, DateConstantOperation, \
    DateTimeConstantOperation, IntegerConstantOperation, FloatConstantOperation, Column


class TableAlias:
    def __init__(self, table: str, alias: str):
        self.table = table
        self.alias = alias


class ColumnAlias:
    def __init__(self, column_name: str, table_alias: TableAlias):
        self.column_name = column_name
        self.table_alias = table_alias


class Join:
    def __init__(self, source: ColumnAlias, target: ColumnAlias):
        self.source = source
        self.target = target

class SelectOperation:
    def __init__(self, display: list[Attribute], table: str, filter: Operation):
        self.display = display
        self.table = table
        self.filter = filter

    # def generate_query(self, qe: QueryEngine):
    #     qe.select(self.__display)
    #     self.__filter.generate_query(qe)

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

class SQLQueryGenerator:
    _select: list[ColumnAlias]
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
            ca = ColumnAlias(col.column().name, ta)
            self._select.append(ca)

        for parent in required_joins:
            left = parent.left
            sc = ColumnAlias(left.column().name, self.__table_alias_for_table(left.owner()))
            right = parent.right
            tc = ColumnAlias(right.column().name, self.__table_alias_for_table(right.owner()))
            self._join.append(Join(sc, tc))

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
                              ' ON ' + j.source.table_alias.alias + '.' + j.source.column_name + ' = ' +
                              j.target.table_alias.alias + '.' + j.target.column_name, self._join)
        return 'SELECT ' + ','.join(map(lambda ca: ca.table_alias.alias + '.' + ca.column_name, self._select)) \
            + ' FROM ' + ','.join(map(lambda ta: ta.table + ' AS ' + ta.alias, self._from)) \
            + ''.join(joins) \
            + self.__build_where()

    def __build_where(self) -> str:
        if len(self._where) > 0:
            return ' WHERE ' + ''.join(self._where)
        else:
            return ''

class JoinOperation:
    def __init__(self, name: str, target:Table, lhs:Attribute, rhs:Attribute):
        self.name = name
        self.target = target
        self.left = lhs
        self.right = rhs


def select_sql_to_string(select_operation: SelectOperation) -> str:
    qe = SQLQueryGenerator()
    qe.generate(select_operation)
    return qe.build_query_string()

