import duckdb

from ibis_gen import generate
#from ibis_trade_finder import *
from trade_finder import TradeFinder
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
    generate()
    duckdb_sample()
    queries.find_trades(TradeFinder)