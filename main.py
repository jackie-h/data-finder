
import datetime
import pykx as kx
import numpy as np
import pandas as pd


class QueryEngine:

    __where:list[str] = []

    def append_where_clause(self, clause:str):
        self.__where.append(clause)

    def build_query_string(self) -> str:
        return ','.join(self.__where)


# Interface
class Operation:

    def generate_query(self, query: QueryEngine):
        pass


class AndOperation(Operation):
    def generate_query(self, query: QueryEngine):
        pass


class BusinessTemporalOperation(Operation):

    #TODO - which date format should we use
    __business_date_from_inclusive:datetime.date
    __business_date_to_inclusive:datetime.date


class Attribute:
    __name:str
    __column_db_type:str

    def __init__(self, name:str, column_db_type:str):
        self.__name = name
        self.__column_db_type = column_db_type

    def _column_name(self) -> str:
        return self.__name

    def _column_type(self) -> str:
        return self.__column_db_type

class EqOperation(Operation):
    __attribute:Attribute

    def column_type(self) -> str:
        return self.__attribute._column_type()

    def __init__(self, attrib: Attribute):
        self.__attribute = attrib

    def generate_query(self, query: QueryEngine):
        query.append_where_clause(self.__attribute._column_name() + ' = ' + self.prepare_value())

    def prepare_value(self) -> str:
        pass




class PrimitiveEqOperation(EqOperation):
    __value: []

    def __init__(self, attrib: Attribute, value: str):
        super().__init__(attrib)
        self.__value = value

class StringEqOperation(EqOperation):
    __value:str = ''

    def __init__(self, attrib: Attribute, value: str):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        if self.column_type() == 'kx_symbol':
            return "`" + self.__value
        else:
            return "\"" + self.__value + "\""

class StringAttribute(Attribute):

    def __init__(self, name: str, column_db_type:str):
        super().__init__(name, column_db_type)

    def eq(self, value: str) -> Operation:
        return StringEqOperation(self, value)



class FloatAttribute(Attribute):

    def __init__(self, name: str, column_db_type:str):
        super().__init__(name, column_db_type)

    def eq(value: float) -> Operation:
        return EqOperation(self, value)


class QConnect:

    @staticmethod
    def select(table:str, op:Operation, columns:list[str]) -> kx.Table:
        conn = kx.QConnection('localhost', 5001)
        qe = QueryEngine()
        op.generate_query(qe)
        query = qe.build_query_string()
        print(query)
        res = conn.qsql.select(table, columns, query)
        return res


class Output:
    __table:kx.Table

    def __init__(self, t:kx.Table):
        self.__table = t

    #https://code.kx.com/pykx/1.6/getting-started/quickstart.html#converting-pykx-objects-to-common-python-types
    def to_numpy(self) -> np.array:
        return self.__table.np()

    #https://code.kx.com/pykx/1.6/getting-started/quickstart.html#converting-pykx-objects-to-common-python-types
    def to_pandas(self) -> pd.DataFrame:
        return self.__table.pd()


class TradeFinder:

    __table = 'trade'
    __symbol = StringAttribute('sym', 'kx_symbol')
    __price = FloatAttribute('price', 'kx_float')

    @staticmethod
    def symbol() -> StringAttribute:
        return TradeFinder.__symbol

    @staticmethod
    def price() -> FloatAttribute:
        return TradeFinder.__price

    @staticmethod
    def find_all(date_from:datetime.date, date_to:datetime.date, as_of:str,
                 filter_op: Operation,
                 display_columns: list[StringAttribute]) -> Output:
        cols = []
        for dc in display_columns:
            cols.append(dc._column_name())
        kx_out = QConnect.select(TradeFinder.__table, filter_op, cols)
        return Output(kx_out)


def find_trades():
    print(f'Finding trades')

    op:Operation = TradeFinder.symbol().eq("AAPL")
    trades = TradeFinder.find_all(datetime.date.today(), datetime.date.today(), "LATEST",
                                  op,
                                  [TradeFinder.symbol(), TradeFinder.price()])
    np_trades = trades.to_numpy()
    print(np_trades)
    pd = trades.to_pandas()
    print(pd)


if __name__ == '__main__':
    find_trades()


