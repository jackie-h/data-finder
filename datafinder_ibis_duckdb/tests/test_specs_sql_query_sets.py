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
from datafinder_examples_tests.suite_data import load_duckdb

from datafinder_examples_tests.sql_query_sets_specs import (
    SQL_QUERY_SETS_APPLICATION_FINDER_SPECS, SQL_QUERY_SETS_CUSTOMER_FINDER_SPECS, SQL_QUERY_SETS_EMBEDDED_MAPPING, SQL_QUERY_SETS_MAPPING,
)

_SPECS = [SQL_QUERY_SETS_CUSTOMER_FINDER_SPECS, SQL_QUERY_SETS_APPLICATION_FINDER_SPECS]


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS org CASCADE")
    load_duckdb(conn, "sql_query_sets")
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
    generate(load(str(example_path(SQL_QUERY_SETS_EMBEDDED_MAPPING))), tmp)
    _seed_db()

    from org.sets.customer_finder import CustomerFinder  # pyright: ignore[reportMissingImports]
    from org.applications.application_finder import ApplicationFinder  # pyright: ignore[reportMissingImports]
    yield {"CustomerFinder": CustomerFinder(), "ApplicationFinder": ApplicationFinder()}

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in list(sys.modules):
        if m == "org" or m.startswith("org."):
            sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize("spec,expectation", [pytest.param(spec, e, id=e.name) for spec in _SPECS for e in spec.expectations])
def test_sql_query_sets(spec, expectation, finders):
    expectation.run(finders[spec.finder_name], backend="duckdb")
