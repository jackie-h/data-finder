import datetime

class QueryEngine:

    _where: list[str]

    def __init__(self):
        self._where = []

    def append_where_clause(self, clause: str):
        self._where.append(clause)

    def build_query_string(self) -> str:
        return ','.join(self._where)

# Interface
class Operation:

    def generate_query(self, query: QueryEngine):
        pass


class AndOperation(Operation):
    def generate_query(self, query: QueryEngine):
        pass


class BusinessTemporalOperation(Operation):

    # TODO - which date format should we use
    __business_date_from_inclusive: datetime.date
    __business_date_to_inclusive: datetime.date