
from datafinder.typed_attributes import *
import numpy as np
import pandas as pd
from datafinder_kdb.kdb_engine import *
from example_kdb.kdb_trade_finder import *

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
