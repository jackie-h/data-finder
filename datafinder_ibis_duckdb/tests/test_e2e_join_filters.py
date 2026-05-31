import os
import shutil
import sys
import tempfile

import duckdb
import pytest

from datafinder import QueryRunnerBase
from datafinder_generator.generator import generate
from datafinder_ibis.ibis_engine import IbisConnect
from mapping_markdown.markdown_mapping import load

_MAPPING_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "orgchart_inheritance_mapping.md")
)

_FINDER_MODULES = ["employee_finder", "employee_finder_base", "project_finder", "project_finder_base"]

_EMPLOYEES = [
    (1, "Alice", "Smith",  "alice@example.com",  "Executive",   None),
    (2, "Bob",   "Jones",  "bob@example.com",    "Engineering", 1),
    (3, "Carol", "White",  "carol@example.com",  "Engineering", 1),
    (4, "Dave",  "Brown",  "dave@example.com",   "QA",          2),
]

_PROJECTS = [
    (1, "Alpha Initiative",  "ALPHA", 2),
    (2, "Beta Platform",     "BETA",  2),
    (3, "Gamma Tooling",     "GAMMA", 3),
    (4, "Delta Ops",         "DELTA", 4),
    (5, "Epsilon Research",  "EPS",   1),
]


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
    for row in _EMPLOYEES:
        conn.execute("INSERT INTO hr.employees VALUES (?, ?, ?, ?, ?, ?)", row)
    conn.execute("""
        CREATE TABLE hr.projects (
            project_id INT, name VARCHAR, code VARCHAR, assignee_id INT
        )
    """)
    for row in _PROJECTS:
        conn.execute("INSERT INTO hr.projects VALUES (?, ?, ?, ?)", row)
    conn.close()


@pytest.fixture(scope="module")
def finders():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)

    temp_dir = tempfile.mkdtemp()
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    sys.path.insert(0, temp_dir)

    mapping = load(_MAPPING_FILE)
    generate(mapping, temp_dir)
    _seed_db()

    from employee_finder import EmployeeFinder
    from project_finder import ProjectFinder
    yield EmployeeFinder(), ProjectFinder()

    sys.path.remove(temp_dir)
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestForwardJoinFilter:

    def test_unfiltered_navigation_returns_all_assigned_employees(self, finders):
        ef, _ = finders
        df = ef.find_all(None, None, [
            ef.first_name(), ef.projects().name(),
        ]).to_pandas()
        # 5 rows: Bob x2 (ALPHA, BETA), Carol x1, Dave x1, Alice x1
        assert len(df) == 5

    def test_filter_on_code_limits_join_rows(self, finders):
        ef, _ = finders
        df = ef.find_all(None, None, [
            ef.first_name(), ef.projects(code='ALPHA').name(),
        ]).to_pandas()
        # Only Bob is assigned to ALPHA; other employees get NULL for project name
        non_null = df[df["Project Name"].notna()]
        assert list(non_null["First Name"]) == ["Bob"]

    def test_filter_on_name_limits_join_rows(self, finders):
        ef, _ = finders
        df = ef.find_all(None, None, [
            ef.first_name(), ef.projects(name='Gamma Tooling').name(),
        ]).to_pandas()
        non_null = df[df["Project Name"].notna()]
        assert list(non_null["First Name"]) == ["Carol"]

    def test_multiple_kwargs_are_ANDed(self, finders):
        ef, _ = finders
        df = ef.find_all(None, None, [
            ef.first_name(), ef.projects(name='Alpha Initiative', code='ALPHA').name(),
        ]).to_pandas()
        non_null = df[df["Project Name"].notna()]
        assert list(non_null["First Name"]) == ["Bob"]

    def test_filter_matching_nothing_returns_all_nulls_in_join(self, finders):
        ef, _ = finders
        df = ef.find_all(None, None, [
            ef.first_name(), ef.projects(code='NONEXISTENT').name(),
        ]).to_pandas()
        assert df["Project Name"].isna().all()

    def test_unfiltered_call_returns_cached_instance(self, finders):
        ef, _ = finders
        assert ef.projects() is ef.projects()

    def test_filtered_call_returns_new_instance(self, finders):
        ef, _ = finders
        assert ef.projects(code='ALPHA') is not ef.projects()


class TestForwardJoinFilterOnRelatedFinder:

    def test_filter_on_assignee_department_from_project(self, finders):
        _, pf = finders
        df = pf.find_all(None, None, [
            pf.name(), pf.assignee(department='Engineering').first_name(),
        ]).to_pandas()
        # Only Engineering employees (Bob, Carol) have projects; others get NULL
        non_null = df[df["Assignee First Name"].notna()]
        assert set(non_null["Assignee First Name"].tolist()) == {"Bob", "Carol"}

    def test_filter_matching_nothing_on_related_finder(self, finders):
        _, pf = finders
        df = pf.find_all(None, None, [
            pf.name(), pf.assignee(department='UNKNOWN').first_name(),
        ]).to_pandas()
        assert df["Assignee First Name"].isna().all()


class TestInvalidKwarg:

    def test_unknown_property_raises_value_error(self, finders):
        ef, _ = finders
        with pytest.raises(ValueError, match="Unknown property 'bogus'"):
            ef.find_all(None, None, [
                ef.first_name(), ef.projects(bogus='x').name(),
            ]).to_pandas()
