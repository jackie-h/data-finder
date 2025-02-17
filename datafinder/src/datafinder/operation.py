import datetime

from datafinder.attribute import Attribute


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


class QueryEngine:
    _select: list[ColumnAlias]
    _from: set[TableAlias]
    _where: list[str]
    _join: list[Join]
    __table_alias_incr: int

    def __init__(self):
        self._where = []
        self._select = []
        self._from = set()
        self._join = []
        self.__table_alias_incr = 0
        self.__table_aliases_by_table = {}

    def select(self, cols: list[Attribute]):
        for col in cols:
            table = col.owner()
            ta = self.__table_alias_for_table(table)
            parent: JoinOperation = col.parent()
            if parent is not None:
                left = parent.left
                sc = ColumnAlias(left.column_name(), self.__table_alias_for_table(left.owner()))
                right = parent.right
                tc = ColumnAlias(right.column_name(), self.__table_alias_for_table(right.owner()))
                self._join.append(Join(sc, tc))
            else:
                self._from.add(ta)
            ca = ColumnAlias(col.column_name(), ta)
            self._select.append(ca)

    def __table_alias_for_table(self, table: str) -> TableAlias:
        ta = None
        if table in self.__table_aliases_by_table:
            ta = self.__table_aliases_by_table[table]
        else:
            ta = TableAlias(table, "t" + str(self.__table_alias_incr))
            self.__table_alias_incr = self.__table_alias_incr + 1
            self.__table_aliases_by_table[table] = ta
        return ta

    def append_where_binary_clause(self, op: str):
        self._where.append(op)

    def append_where_clause(self, attr: Attribute, op: str, value: str):
        ta = self.__table_alias_for_table(attr.owner())
        self._where.append(ta.alias + '.' + attr.column_name() + ' ' + op + ' ' + value)

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

    def where_clauses(self):
        return self._where

    def start_and(self):
        pass

    def end_and(self):
        pass


# Interface
class Operation:

    def generate_query(self, query: QueryEngine):
        pass


class NoOperation(Operation):
    def __init__(self):
        pass


class SelectOperation(Operation):
    def __init__(self, display: list[Attribute], table: str, filter: Operation):
        self.__display = display
        self.__table = table
        self.__filter = filter

    def generate_query(self, qe: QueryEngine):
        qe.select(self.__display)
        self.__filter.generate_query(qe)


class AndOperation(Operation):
    __left: Operation
    __right: Operation

    def __init__(self, lhs: Operation, rhs: Operation):
        self.__left = lhs
        self.__right = rhs

    def generate_query(self, query: QueryEngine):
        query.start_and()
        self.__left.generate_query(query)
        query.append_where_binary_clause(" and ")
        self.__right.generate_query(query)
        query.end_and()


class BusinessTemporalOperation(Operation):
    # TODO - which date format should we use
    __business_date_from_inclusive: datetime.date
    __business_date_to_inclusive: datetime.date


class BaseOperation(Operation):

    def and_op(self, rhs: Operation):
        return AndOperation(self, rhs)


class JoinOperation(Operation):
    left: Attribute
    right: Attribute

    def __init__(self, lhs: Attribute, rhs: Attribute):
        self.left = lhs
        self.right = rhs


def select_sql_to_string(columns: list[Attribute], table: str, op: Operation) -> str:
    qe = QueryEngine()
    select = SelectOperation(columns, table, op)
    select.generate_query(qe)
    return qe.build_query_string()

