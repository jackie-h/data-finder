import duckdb

def duckdb_sample():
    con = duckdb.connect()
    con.sql("SELECT 42 AS x").show()


if __name__ == '__main__':
    duckdb_sample()