import datetime


class QueryEngine:

    _where: list[str]

    def __init__(self):
        self._where = []

    def append_where_clause(self, clause: str):
        self._where.append(clause)

    def build_query_string(self) -> str:
        return ','.join(self._where)
    
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


class AndOperation(Operation):
    __left: Operation
    __right: Operation

    def __init__(self, lhs: Operation, rhs: Operation):
        self.__left = lhs
        self.__right = rhs

    def generate_query(self, query: QueryEngine):
        query.start_and()
        self.__left.generate_query(query)
        #query.append_where_clause(" and ")
        self.__right.generate_query(query)
        query.end_and()

class BusinessTemporalOperation(Operation):

    # TODO - which date format should we use
    __business_date_from_inclusive: datetime.date
    __business_date_to_inclusive: datetime.date


class BaseOperation(Operation):
    
    def and_op(self, rhs:Operation):
        return AndOperation(self, rhs)