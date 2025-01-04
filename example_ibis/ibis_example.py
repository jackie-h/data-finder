import duckdb
import datetime

from account_finder import AccountFinder
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

    con.execute("DROP TABLE IF EXISTS account;")
    con.execute(
        "CREATE TABLE account(id INT, name VARCHAR); COPY account FROM '../data/accounts.csv'")




if __name__ == '__main__':
    generate()
    duckdb_sample()
    queries.find_trades(TradeFinder)
    np_accts = AccountFinder\
        .find_all(datetime.date.today(), datetime.date.today(), "LATEST",AccountFinder.id().eq(211978),
                  [AccountFinder.id(), AccountFinder.name()])\
        .to_numpy()
    print(np_accts)