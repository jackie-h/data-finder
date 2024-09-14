
import numpy as np
import pandas as pd
#from example_kdb.kdb_trade_finder import *
from example_duckdb.duckdb_trade_finder import *


def setup_duckdb():
    con = duckdb.connect('test.db')
    con.execute("DROP TABLE IF EXISTS trade;")
    con.execute("CREATE TABLE trade(id INT, account_id INT, sym VARCHAR, price DOUBLE); COPY trade FROM 'data/trades.csv'")
    con.sql("SELECT * from trade").show()
    con.sql("SELECT * from trade where sym LIKE 'AAPL'").show()

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
    setup_duckdb()
    find_trades()
