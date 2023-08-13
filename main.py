
import datetime
import pykx as kx
import numpy as np
import pandas as pd

# Interface
class Operation:

    def load_data_source(self, path: str, file_name: str) -> str:
        """Overrides FormalParserInterface.load_data_source()"""
        pass

class BusinessTemporalOperation(Operation):

    #TODO - which date format should we use
    __business_date_from_inclusive:datetime.date
    __business_date_to_inclusive:datetime.date



class EqOperation(Operation):
    __value = ''

    def __init__(self, value):
        self.__value = value


class StringAttribute:

    @staticmethod
    def eq(value: str) -> Operation:
        return EqOperation(value)

class QConnect:

    @staticmethod
    def run(table:str, op:Operation) -> kx.Table:
        conn = kx.QConnection('localhost', 5001)
        res = conn.qsql.select(table, ['sym','price'])
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
    __sym = StringAttribute()

    @staticmethod
    def sym() -> StringAttribute():
        return TradeFinder.__sym

    @staticmethod
    def find_all(date_from:datetime.date, date_to:datetime.date, as_of:str, op:Operation) -> Output:
        kx_out = QConnect.run(TradeFinder.__table, op)
        return Output(kx_out)




def find_trades():
    print(f'Finding trades')

    op:Operation = TradeFinder.sym().eq("AAPL")
    trades = TradeFinder.find_all(datetime.date.today(), datetime.date.today(), "LATEST", op)
    np_trades = trades.to_numpy()
    print(np_trades)
    pd = trades.to_pandas()
    print(pd)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    find_trades()


