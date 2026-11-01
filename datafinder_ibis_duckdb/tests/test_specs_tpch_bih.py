"""
Runs the TPC-BiH specs (time travel, key in time, range-timeslice, bitemporal and TPC-H queries) against the
ibis/DuckDB backend. See tpch_bih_specs.py for the schema, the history and the queries that are skipped.
"""
import importlib
import shutil
import sys
import tempfile

import duckdb
import pytest

from datafinder import QueryRunnerBase
from datafinder_generator.generator import generate
from datafinder_ibis.ibis_engine import IbisConnect
from mapping_markdown.markdown_mapping import load
from datafinder_examples import example_path

from datafinder_examples_tests.tpch_bih_specs import ALL_TPCH_BIH_SPECS, TPCH_BIH_MAPPING

_FINDERS = {
    "RegionFinder": "tpch.bih.region_finder",
    "NationFinder": "tpch.bih.nation_finder",
    "SupplierFinder": "tpch.bih.supplier_finder",
    "PartFinder": "tpch.bih.part_finder",
    "PartSuppFinder": "tpch.bih.partsupp_finder",
    "CustomerFinder": "tpch.bih.customer_finder",
    "OrderFinder": "tpch.bih.order_finder",
    "LineItemFinder": "tpch.bih.lineitem_finder",
}

_SYSTEM = "sys_time_begin TIMESTAMP, sys_time_end TIMESTAMP"
_TABLES = {
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


def _seed_db() -> None:
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS tpch CASCADE")
    conn.execute("CREATE SCHEMA tpch")
    for table, columns in _TABLES.items():
        conn.execute(f"CREATE TABLE tpch.{table} ({columns})")
        conn.execute(
            f"INSERT INTO tpch.{table} SELECT * FROM read_csv_auto('{str(example_path(f'tpch_bih_{table}.csv'))}')"
        )
    conn.close()


def _purge_modules() -> None:
    for name in list(sys.modules):
        if name == "tpch" or name.startswith("tpch."):
            sys.modules.pop(name, None)


@pytest.fixture(scope="module")
def finders():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    _purge_modules()
    sys.path.insert(0, tmp)

    generate(load(str(example_path(TPCH_BIH_MAPPING))), tmp)
    _seed_db()

    instances = {}
    for finder_name, module_name in _FINDERS.items():
        module = importlib.import_module(module_name)
        instances[finder_name] = getattr(module, finder_name)()
    yield instances

    if tmp in sys.path:
        sys.path.remove(tmp)
    _purge_modules()
    shutil.rmtree(tmp, ignore_errors=True)


_CASES = [
    pytest.param(spec, expectation, id=f"{spec.finder_name}-{expectation.name}")
    for spec in ALL_TPCH_BIH_SPECS
    for expectation in spec.expectations
]


@pytest.mark.parametrize("spec, expectation", _CASES)
def test_tpch_bih(spec, expectation, finders):
    expectation.run(finders[spec.finder_name], backend="duckdb")
