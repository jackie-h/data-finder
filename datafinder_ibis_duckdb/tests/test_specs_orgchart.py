"""
Runs ORGCHART_FINDER_SPECS (self-join, manager navigation) against the
ibis/DuckDB backend.
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

from datafinder_examples_tests.orgchart_specs import ORGCHART_FINDER_SPECS, EMPLOYEES

_MODS = ["employee_finder", "employee_finder_base"]


@pytest.fixture(scope="module")
def employee_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    mapping = load(str(example_path(ORGCHART_FINDER_SPECS.mapping_file)))
    generate(mapping, tmp)

    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS hr CASCADE")
    conn.execute("CREATE SCHEMA hr")
    conn.execute("CREATE TABLE hr.employees (id INT, name VARCHAR, manager_id INT)")
    for r in EMPLOYEES:
        conn.execute("INSERT INTO hr.employees VALUES (?, ?, ?)", r)
    conn.close()

    from employee_finder import EmployeeFinder  # type: ignore[import]
    yield EmployeeFinder()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "expectation", ORGCHART_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_orgchart(expectation, employee_finder):
    expectation.run(employee_finder, backend="duckdb")
