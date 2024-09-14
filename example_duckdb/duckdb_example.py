import duckdb

from duckdb_trade_finder import *
from example import queries

def duckdb_sample():
    con = duckdb.connect('test.db')
    con.sql("SELECT 42 AS x").show()
    con.execute("DROP TABLE IF EXISTS trade;")
    con.execute(
        "CREATE TABLE trade(id INT, account_id INT, sym VARCHAR, price DOUBLE); COPY trade FROM '../data/trades.csv'")
    con.sql("SELECT * from trade").show()
    con.sql("SELECT * from trade where sym LIKE 'AAPL'").show()




if __name__ == '__main__':
    duckdb_sample()
    queries.find_trades(TradeFinder)