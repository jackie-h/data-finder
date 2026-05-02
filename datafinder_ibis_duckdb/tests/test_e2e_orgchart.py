import os
import shutil
import sys
import tempfile

import duckdb
import pytest
from numpy.testing import assert_array_equal

from datafinder import QueryRunnerBase
from datafinder_generator.generator import generate
from datafinder_ibis.ibis_engine import IbisConnect
from mapping_markdown.markdown_mapping import load

_MAPPING_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "mapping_markdown", "tests", "orgchart_mapping.md")
)

_FINDER_MODULES = ["employee_finder"]


def _build_test_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS hr CASCADE")
    conn.execute("CREATE SCHEMA hr")
    conn.execute("CREATE TABLE hr.employees (id INT, name VARCHAR, manager_id INT)")
    conn.execute("INSERT INTO hr.employees VALUES (1, 'Alice', NULL)")   # CEO — no manager
    conn.execute("INSERT INTO hr.employees VALUES (2, 'Bob', 1)")        # reports to Alice
    conn.execute("INSERT INTO hr.employees VALUES (3, 'Carol', 1)")      # reports to Alice
    conn.execute("INSERT INTO hr.employees VALUES (4, 'Dave', 2)")       # reports to Bob
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
    _build_test_db()

    from employee_finder import EmployeeFinder

    yield {"Employee": EmployeeFinder}

    sys.path.remove(temp_dir)
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestE2EOrgChart:

    def test_all_employees_returned(self, finders):
        EmployeeFinder = finders["Employee"]
        result = EmployeeFinder.find_all(
            [EmployeeFinder.id_(), EmployeeFinder.name()],
        ).to_pandas()
        assert len(result) == 4
        assert set(result["Name"].tolist()) == {"Alice", "Bob", "Carol", "Dave"}

    def test_self_join_manager_name(self, finders):
        EmployeeFinder = finders["Employee"]
        result = EmployeeFinder.find_all(
            [EmployeeFinder.name(), EmployeeFinder.manager().name()],
        ).to_pandas()
        # Only employees with a manager are returned (inner-like left join drops NULLs in display)
        by_name = {row["Name"]: row["Manager Name"] for _, row in result.iterrows()
                   if row["Name"] in ("Bob", "Carol", "Dave")}
        assert by_name["Bob"] == "Alice"
        assert by_name["Carol"] == "Alice"
        assert by_name["Dave"] == "Bob"

    def test_filter_by_manager(self, finders):
        EmployeeFinder = finders["Employee"]
        result = EmployeeFinder.find_all(
            [EmployeeFinder.name()],
            EmployeeFinder.manager().name().eq("Alice"),
        ).to_pandas()
        assert set(result["Name"].tolist()) == {"Bob", "Carol"}

    def test_filter_direct_report_of_bob(self, finders):
        EmployeeFinder = finders["Employee"]
        result = EmployeeFinder.find_all(
            [EmployeeFinder.name(), EmployeeFinder.manager().name()],
            EmployeeFinder.manager().name().eq("Bob"),
        ).to_pandas()
        assert result["Name"].tolist() == ["Dave"]
