"""
Runs the TPC-BiH specs (time travel, key in time, range-timeslice, bitemporal and TPC-H queries) against the
ibis/DuckDB backend. See tpch_bih_specs.py for the schema, the history and the queries that are skipped.
"""
import importlib
import shutil
import sys
import tempfile
from types import SimpleNamespace

import duckdb
import pytest

from datafinder import QueryRunnerBase
from datafinder_generator.generator import generate
from datafinder_ibis.ibis_engine import IbisConnect
from mapping_markdown.markdown_mapping import load
from datafinder_examples import example_path

from datafinder_examples_tests.tpch_bih_cases import CASES
from datafinder_examples_tests.tpch_bih_data import csv_dir
from datafinder_examples_tests.tpch_bih_db import load as load_tables
from datafinder_examples_tests.tpch_bih_specs import ALL_TPCH_BIH_CASES, TPCH_BIH_MAPPING

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



def _seed_db() -> None:
    conn = duckdb.connect("test.db")
    load_tables(conn, csv_dir(), schema="tpch")
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
    yield SimpleNamespace(
        order=instances["OrderFinder"],
        customer=instances["CustomerFinder"],
        partsupp=instances["PartSuppFinder"],
        lineitem=instances["LineItemFinder"],
    )

    if tmp in sys.path:
        sys.path.remove(tmp)
    _purge_modules()
    shutil.rmtree(tmp, ignore_errors=True)


_CASES = [
    pytest.param(expectation, id=expectation.name)
    for expectation in ALL_TPCH_BIH_CASES
]


@pytest.mark.parametrize("expectation", _CASES)
def test_tpch_bih(expectation, finders):
    expectation.run(finders, backend="duckdb")


@pytest.fixture(scope="module")
def sql_connection():
    connection = duckdb.connect()
    load_tables(connection, csv_dir())
    yield connection
    connection.close()


@pytest.mark.parametrize("case", [pytest.param(case, id=case.id) for case in CASES.values()])
def test_tpch_bih_sql(case, sql_connection):
    """The report's SQL, translated for DuckDB, returns the expected rows: the case is as the report asks it."""
    case.check(case.run_sql(sql_connection))
