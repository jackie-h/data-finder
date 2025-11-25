import duckdb

def setup_duckdb():
    con = duckdb.connect('test.db')
    con.sql("SELECT 42 AS x").show()
    con.execute("DROP TABLE IF EXISTS trades;")
    con.execute(
        "CREATE TABLE trades(id INT, account_id INT, sym VARCHAR, price DOUBLE, start_at TIMESTAMP, end_at TIMESTAMP); COPY trades FROM 'data/trades.csv'")
    con.sql("SELECT * from trades").show()
    con.sql("SELECT * from trades where sym LIKE 'AAPL'").show()

    con.execute("DROP TABLE IF EXISTS account_master;")
    con.execute(
        "CREATE TABLE account_master(id INT, name VARCHAR); COPY account_master FROM 'data/accounts.csv'")

    con.execute("DROP TABLE IF EXISTS contractualposition;")
    con.execute(
        "CREATE TABLE contractualposition(DATE DATE, INSTRUMENT VARCHAR, CPTY_ID INT, QUANTITY DOUBLE); COPY contractualposition FROM 'data/contractualpositions.csv'")
    con.sql("SELECT * from contractualposition").show()

    con.execute("DROP TABLE IF EXISTS price;")
    con.execute(
        "CREATE TABLE price(DATE_TIME DATETIME, SYM VARCHAR, PRICE DOUBLE); COPY price FROM 'data/prices.csv'")
    con.sql("SELECT * from price").show()