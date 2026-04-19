import duckdb

def setup_duckdb():
    con = duckdb.connect('test.db')
    con.sql("SELECT 42 AS x").show()

    con.execute("DROP SCHEMA IF EXISTS trading CASCADE;")
    con.execute("DROP SCHEMA IF EXISTS ref_data CASCADE;")
    con.execute("CREATE SCHEMA trading;")
    con.execute("CREATE SCHEMA ref_data;")

    con.execute(
        "CREATE TABLE trading.trades(id INT, account_id INT, sym VARCHAR, price DOUBLE, start_at TIMESTAMP, end_at TIMESTAMP); COPY trading.trades FROM 'data/trades.csv'")
    con.sql("SELECT * from trading.trades").show()
    con.sql("SELECT * from trading.trades where sym LIKE 'AAPL'").show()

    con.execute(
        "CREATE TABLE ref_data.account_master(ID INT, ACCT_NAME VARCHAR); COPY ref_data.account_master FROM 'data/accounts.csv'")

    con.execute(
        "CREATE TABLE trading.contractualposition(DATE DATE, INSTRUMENT VARCHAR, CPTY_ID INT, QUANTITY DOUBLE); COPY trading.contractualposition FROM 'data/contractualpositions.csv'")
    con.sql("SELECT * from trading.contractualposition").show()

    con.execute(
        "CREATE TABLE ref_data.price(DATE_TIME DATETIME, SYM VARCHAR, PRICE DOUBLE, START_AT DATETIME, END_AT DATETIME); COPY ref_data.price FROM 'data/prices.csv'")
    con.sql("SELECT * from ref_data.price").show()