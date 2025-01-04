import duckdb
import datetime

from ibis_gen import generate
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
    # Import after generation, so we get the latest version
    from trade_finder import TradeFinder
    queries.find_trades(TradeFinder)
    from account_finder import AccountFinder
    np_accts = AccountFinder\
        .find_all(datetime.date.today(), datetime.date.today(), "LATEST",AccountFinder.id().eq(211978),
                  [AccountFinder.id(), AccountFinder.name()])\
        .to_numpy()
    print(np_accts)