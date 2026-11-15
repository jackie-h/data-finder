"""
Runs the specs of a class mapped to several named sets (sql_query_sets_mapping.md) against the ibis/DuckDB backend: the class is the union of its sets.
"""
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

from datafinder_examples_tests.sql_query_sets_specs import SQL_QUERY_SETS_CUSTOMER_FINDER_SPECS, SQL_QUERY_SETS_MAPPING

_SPECS = [SQL_QUERY_SETS_CUSTOMER_FINDER_SPECS]


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS org CASCADE")
    conn.execute("CREATE SCHEMA org")
    for table in ("retail_customers", "corporate_customers"):
        conn.execute(f"CREATE TABLE org.{table} (id INT, name VARCHAR)")
        conn.execute(f"INSERT INTO org.{table} SELECT * FROM read_csv_auto('{str(example_path(f'sql_query_sets_{table}.csv'))}')")
    conn.execute("CREATE TABLE org.orders (id INT, customer_id INT, amount INT)")
    conn.execute(f"INSERT INTO org.orders SELECT * FROM read_csv_auto('{str(example_path('sql_query_sets_orders.csv'))}')")
    conn.close()


@pytest.fixture(scope="module")
def finders():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in list(sys.modules):
        if m == "org" or m.startswith("org."):
            sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    generate(load(str(example_path(SQL_QUERY_SETS_MAPPING))), tmp)
    _seed_db()

    from org.sets.customer_finder import CustomerFinder  # pyright: ignore[reportMissingImports]
    yield {"CustomerFinder": CustomerFinder()}

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in list(sys.modules):
        if m == "org" or m.startswith("org."):
            sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize("spec,expectation", [pytest.param(spec, e, id=e.name) for spec in _SPECS for e in spec.expectations])
def test_sql_query_sets(spec, expectation, finders):
    expectation.run(finders[spec.finder_name], backend="duckdb")
