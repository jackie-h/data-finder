"""
Runs EMPLOYEE_INHERITANCE_FINDER_SPECS, PROJECT_FINDER_SPECS (including join
filters and multi-hop joins) against the ibis/DuckDB backend.

Also keeps a small set of non-spec tests for error-handling behaviour
(invalid join kwarg) that cannot be expressed as data-result assertions.
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

from datafinder_examples_tests.orgchart_inheritance_specs import (
    EMPLOYEE_INHERITANCE_FINDER_SPECS,
    PROJECT_FINDER_SPECS,
)

_MODS = ["employee_finder", "employee_finder_base", "project_finder", "project_finder_base"]


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS hr CASCADE")
    conn.execute("CREATE SCHEMA hr")
    conn.execute("""
        CREATE TABLE hr.employees (
            emp_id INT, first_name VARCHAR, last_name VARCHAR,
            email VARCHAR, department VARCHAR, manager_id INT
        )
    """)
    conn.execute(f"INSERT INTO hr.employees SELECT * FROM read_csv_auto('{str(example_path('employees_inheritance.csv'))}')")
    conn.execute("""
        CREATE TABLE hr.projects (
            project_id INT, name VARCHAR, code VARCHAR, assignee_id INT
        )
    """)
    conn.execute(f"INSERT INTO hr.projects SELECT * FROM read_csv_auto('{str(example_path('projects.csv'))}')")
    conn.close()


@pytest.fixture(scope="module")
def orgchart_finders():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    mapping = load(str(example_path(EMPLOYEE_INHERITANCE_FINDER_SPECS.mapping_file)))
    generate(mapping, tmp)
    _seed_db()

    from employee_finder import EmployeeFinder  # type: ignore[import]
    from project_finder import ProjectFinder  # type: ignore[import]
    yield EmployeeFinder(), ProjectFinder()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "expectation", EMPLOYEE_INHERITANCE_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_employee(expectation, orgchart_finders):
    expectation.run(orgchart_finders[0], backend="duckdb")


@pytest.mark.parametrize(
    "expectation", PROJECT_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_project(expectation, orgchart_finders):
    expectation.run(orgchart_finders[1], backend="duckdb")


# --- non-spec tests: property existence (inherited from Person / Contactable) ---

def test_has_id(orgchart_finders):
    ef, _ = orgchart_finders
    assert ef.id_() is not None


def test_has_first_name(orgchart_finders):
    ef, _ = orgchart_finders
    assert ef.first_name() is not None


def test_has_last_name(orgchart_finders):
    ef, _ = orgchart_finders
    assert ef.last_name() is not None


def test_has_email(orgchart_finders):
    ef, _ = orgchart_finders
    assert ef.email() is not None


def test_has_department(orgchart_finders):
    ef, _ = orgchart_finders
    assert ef.department() is not None


def test_has_manager(orgchart_finders):
    ef, _ = orgchart_finders
    assert ef.manager() is not None


# --- non-spec tests: error handling and caching behaviour ---

def test_invalid_join_kwarg_raises(orgchart_finders):
    ef, _ = orgchart_finders
    with pytest.raises(ValueError, match="Unknown property 'bogus'"):
        ef.find_all(None, None, [ef.first_name(), ef.projects(bogus="x").name()]).to_pandas()


def test_unfiltered_join_result_is_cached(orgchart_finders):
    ef, _ = orgchart_finders
    assert ef.projects() is ef.projects()


def test_filtered_join_result_is_not_cached(orgchart_finders):
    ef, _ = orgchart_finders
    assert ef.projects(code="ALPHA") is not ef.projects()
