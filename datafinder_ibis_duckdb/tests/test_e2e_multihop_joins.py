"""
End-to-end tests for multi-hop join traversal.

The chain under test is Employee -> Employee (manager, self-join) -> Project.

The key regression scenario is: selecting only Project columns without selecting any
Employee (manager) columns. The ancestor-join fix ensures the manager join is still
emitted as a prerequisite for the projects join — without it the generated SQL would
reference an alias that was never introduced in the FROM/JOIN clauses.
"""
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

_FINDER_MODULES = ["employee_finder", "project_finder"]

_EMPLOYEES = [
    # (emp_id, first_name, last_name, email, department, manager_id)
    (1, "Alice", "Smith",  "alice@example.com",  "Executive",   None),
    (2, "Bob",   "Jones",  "bob@example.com",    "Engineering", 1),
    (3, "Carol", "White",  "carol@example.com",  "Engineering", 1),
    (4, "Dave",  "Brown",  "dave@example.com",   "QA",          2),
]

_PROJECTS = [
    # (project_id, name, code, assignee_id)
    (1, "Alpha Initiative",  "ALPHA", 2),   # assigned to Bob   (manager = Alice)
    (2, "Beta Platform",     "BETA",  2),   # assigned to Bob   (manager = Alice)
    (3, "Gamma Tooling",     "GAMMA", 3),   # assigned to Carol (manager = Alice)
    (4, "Delta Ops",         "DELTA", 4),   # assigned to Dave  (manager = Bob)
    (5, "Epsilon Research",  "EPS",   1),   # assigned to Alice (no manager)
]


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS hr CASCADE")
    conn.execute("CREATE SCHEMA hr")
    conn.execute("""
        CREATE TABLE hr.employees (
            emp_id     INT,
            first_name VARCHAR,
            last_name  VARCHAR,
            email      VARCHAR,
            department VARCHAR,
            manager_id INT
        )
    """)
    for row in _EMPLOYEES:
        conn.execute("INSERT INTO hr.employees VALUES (?, ?, ?, ?, ?, ?)", row)
    conn.execute("""
        CREATE TABLE hr.projects (
            project_id  INT,
            name        VARCHAR,
            code        VARCHAR,
            assignee_id INT
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

    yield {"Employee": EmployeeFinder, "Project": ProjectFinder}

    sys.path.remove(temp_dir)
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestDirectEmployeeProjectJoin:
    """Two-hop join: Employee -> Project (baseline, no intermediate-table issue)."""

    def test_project_names_for_each_employee(self, finders):
        EF = finders["Employee"]
        result = EF.find_all(
            [EF.first_name(), EF.projects().name()],
        ).to_pandas()
        by_employee = {}
        for _, row in result.iterrows():
            by_employee.setdefault(row["First Name"], set()).add(row["Project Name"])
        assert by_employee["Bob"] == {"Alpha Initiative", "Beta Platform"}
        assert by_employee["Carol"] == {"Gamma Tooling"}
        assert by_employee["Dave"] == {"Delta Ops"}
        assert by_employee["Alice"] == {"Epsilon Research"}

    def test_project_code_filtered_by_employee(self, finders):
        EF = finders["Employee"]
        result = EF.find_all(
            [EF.first_name(), EF.projects().code()],
            EF.first_name().eq("Bob"),
        ).to_pandas()
        assert set(result["Project Code"].tolist()) == {"ALPHA", "BETA"}

    def test_filter_by_project_name(self, finders):
        EF = finders["Employee"]
        result = EF.find_all(
            [EF.first_name(), EF.projects().name()],
            EF.projects().name().eq("Delta Ops"),
        ).to_pandas()
        assert result.iloc[0]["First Name"] == "Dave"


class TestMultiHopManagerProjects:
    """
    Three-hop join: employees (root) -> employees (manager) -> projects.

    Exercises the ancestor-join-emission fix: when only Project columns are selected,
    the manager join (employees self-join) must still be emitted because the projects
    join references the manager's emp_id via projects.assignee_id.
    """

    def test_manager_project_name_with_employee_column(self, finders):
        """Standard multi-hop: employee name + manager's project name."""
        EF = finders["Employee"]
        result = EF.find_all(
            [EF.first_name(), EF.manager().projects().name()],
        ).to_pandas()
        by_employee = {}
        for _, row in result.iterrows():
            by_employee.setdefault(row["First Name"], set()).add(row["Project Name"])
        # Bob's manager is Alice; Alice is assigned Epsilon Research
        assert by_employee["Bob"] == {"Epsilon Research"}
        # Carol's manager is Alice; same
        assert by_employee["Carol"] == {"Epsilon Research"}
        # Dave's manager is Bob; Bob is assigned Alpha and Beta
        assert by_employee["Dave"] == {"Alpha Initiative", "Beta Platform"}

    def test_only_manager_project_column_selected(self, finders):
        """
        Ancestor-join regression test: only manager's project name in SELECT.
        No employee or manager column selected — manager join must still be emitted
        so that projects.assignee_id can reference the manager table alias.
        """
        EF = finders["Employee"]
        result = EF.find_all(
            [EF.manager().projects().name()],
            EF.first_name().eq("Dave"),
        ).to_pandas()
        # Dave's manager is Bob; Bob is assigned Alpha and Beta
        assert set(result["Project Name"].tolist()) == {"Alpha Initiative", "Beta Platform"}

    def test_filter_on_manager_project_no_select(self, finders):
        """
        Ancestor-join regression test: manager's project column only in WHERE, not SELECT.
        Both the manager join and projects join must be emitted even though neither
        appears in the SELECT list.
        """
        EF = finders["Employee"]
        result = EF.find_all(
            [EF.first_name()],
            EF.manager().projects().code().eq("ALPHA"),
        ).to_pandas()
        # ALPHA is assigned to Bob. Employees whose manager is Bob: Dave
        assert result["First Name"].tolist() == ["Dave"]

    def test_manager_project_count_is_deterministic(self, finders):
        """Join ordering must be stable across repeated calls."""
        EF = finders["Employee"]
        for _ in range(3):
            result = EF.find_all(
                [EF.first_name(), EF.manager().projects().name()],
            ).to_pandas()
            counts = result.groupby("First Name")["Project Name"].count().to_dict()
            # Bob and Carol report to Alice (1 project: Epsilon Research)
            assert counts["Bob"] == 1
            assert counts["Carol"] == 1
            # Dave reports to Bob (2 projects: Alpha, Beta)
            assert counts["Dave"] == 2

    def test_filter_by_manager_name_select_project(self, finders):
        """Filter on manager column; select manager's project — exercises both joins."""
        EF = finders["Employee"]
        result = EF.find_all(
            [EF.manager().projects().name()],
            EF.manager().first_name().eq("Alice"),
        ).to_pandas()
        # Bob and Carol report to Alice; Alice has Epsilon Research
        assert set(result["Project Name"].tolist()) == {"Epsilon Research"}


class TestProjectFinderReverse:
    """Reverse navigation: Project -> Employee (assignee)."""

    def test_project_with_assignee_name(self, finders):
        PF = finders["Project"]
        result = PF.find_all(
            [PF.name(), PF.assignee().first_name()],
        ).to_pandas()
        by_project = dict(zip(result["Name"], result["Assignee First Name"]))
        assert by_project["Alpha Initiative"] == "Bob"
        assert by_project["Gamma Tooling"] == "Carol"
        assert by_project["Delta Ops"] == "Dave"

    def test_filter_project_by_assignee_department(self, finders):
        PF = finders["Project"]
        result = PF.find_all(
            [PF.name(), PF.code()],
            PF.assignee().department().eq("Engineering"),
        ).to_pandas()
        assert set(result["Code"].tolist()) == {"ALPHA", "BETA", "GAMMA"}
