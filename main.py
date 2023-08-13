
from typed_attributes import *
import numpy as np
import pandas as pd
from kdb_engine import *


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
    def find_all(date_from: datetime.date, date_to: datetime.date, as_of: str,
                 filter_op: Operation,
                 display_columns: list[StringAttribute]) -> Output:
        cols = []
        for dc in display_columns:
            cols.append(dc._column_name())
        kx_out = QConnect.select(TradeFinder.__table, filter_op, cols)
        return Output(kx_out)


def find_trades():
    print(f'Finding trades')

    trades = TradeFinder.find_all(datetime.date.today(), datetime.date.today(), "LATEST",
                                  TradeFinder.symbol().eq("AAPL"),
                                  [TradeFinder.symbol(), TradeFinder.price()])
    np_trades = trades.to_numpy()
    print(np_trades)
    df = trades.to_pandas()
    print(df)

    trades = TradeFinder.find_all(datetime.date.today(), datetime.date.today(), "LATEST",
                                  TradeFinder.price() > 200.0,
                                  [TradeFinder.symbol(), TradeFinder.price()])
    np_trades = trades.to_numpy()
    print(np_trades)
    df = trades.to_pandas()
    print(df)
    
    trades = TradeFinder.find_all(datetime.date.today(), datetime.date.today(), "LATEST",
                                  (TradeFinder.symbol() == "AAPL").and_op(TradeFinder.price() == 84.11),
                                  [TradeFinder.symbol(), TradeFinder.price()])
    np_trades = trades.to_numpy()
    print(np_trades)
    df = trades.to_pandas()
    print(df)


if __name__ == '__main__':
    find_trades()
