import duckdb

from duckdb_trade_finder import *

def duckdb_sample():
    con = duckdb.connect()
    con.sql("SELECT 42 AS x").show()


def find_trades():
    print(f'Finding trades')

    trades = TradeFinder.find_all(datetime.date.today(), datetime.date.today(), "LATEST",
                                  TradeFinder.symbol().eq("AAPL"),
                                  [TradeFinder.symbol(), TradeFinder.price()])
    np_trades = trades.to_numpy()
    print(np_trades)
    df = trades.to_pandas()
    print(df)


if __name__ == '__main__':
    duckdb_sample()
    find_trades()