"""The TPC-BiH tables in DuckDB: the DDL, and loading the generated CSV files into them."""
from pathlib import Path

import duckdb

_SYSTEM = "sys_time_begin TIMESTAMP, sys_time_end TIMESTAMP"
TABLES = {
    "region": "r_regionkey INT, r_name VARCHAR, r_comment VARCHAR",
    "nation": "n_nationkey INT, n_name VARCHAR, n_regionkey INT, n_comment VARCHAR",
    "supplier": "s_suppkey INT, s_name VARCHAR, s_address VARCHAR, s_nationkey INT, s_phone VARCHAR, "
                f"s_acctbal DOUBLE, s_comment VARCHAR, {_SYSTEM}",
    "part": "p_partkey INT, p_name VARCHAR, p_mfgr VARCHAR, p_brand VARCHAR, p_type VARCHAR, p_size INT, "
            f"p_container VARCHAR, p_retailprice DOUBLE, p_comment VARCHAR, availability_time_begin DATE, availability_time_end DATE, {_SYSTEM}",
    "partsupp": "ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost DOUBLE, ps_comment VARCHAR, "
                f"validity_time_begin DATE, validity_time_end DATE, {_SYSTEM}",
    "customer": "c_custkey INT, c_name VARCHAR, c_address VARCHAR, c_nationkey INT, c_phone VARCHAR, "
                "c_acctbal DOUBLE, c_mktsegment VARCHAR, c_comment VARCHAR, "
                f"visible_time_begin DATE, visible_time_end DATE, {_SYSTEM}",
    "orders": "o_orderkey INT, o_custkey INT, o_orderstatus VARCHAR, o_totalprice DOUBLE, o_orderdate DATE, "
              "o_orderpriority VARCHAR, o_clerk VARCHAR, o_shippriority INT, o_comment VARCHAR, receivable_time_begin DATE, "
              f"receivable_time_end DATE, active_time_begin DATE, active_time_end DATE, {_SYSTEM}",
    "lineitem": "l_orderkey INT, l_linenumber INT, l_partkey INT, l_suppkey INT, l_quantity DOUBLE, "
                "l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE, l_returnflag VARCHAR, l_linestatus VARCHAR, "
                "l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct VARCHAR, l_shipmode VARCHAR, "
                "l_comment VARCHAR, "
                f"active_time_begin DATE, active_time_end DATE, {_SYSTEM}",
}


def load(connection: duckdb.DuckDBPyConnection, directory: Path, schema: str | None = None) -> None:
    """Create the tables (in ``schema`` if given, which is created) and load tpch_bih_benchmark_<table>.csv into them."""
    prefix = f"{schema}." if schema else ""
    if schema:
        connection.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        connection.execute(f"CREATE SCHEMA {schema}")
    for table, columns in TABLES.items():
        connection.execute(f"CREATE TABLE {prefix}{table} ({columns})")
        connection.execute(
            f"INSERT INTO {prefix}{table} SELECT * FROM read_csv_auto('{directory / f'tpch_bih_benchmark_{table}.csv'}')")
