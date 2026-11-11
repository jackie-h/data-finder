"""
Runs the join condition specs (a condition on a join beside its key columns, read from an Association table's Condition
column) against the ibis/DuckDB backend.
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
from model.relational import Database, Schema, Table
from datafinder_examples import example_path

from datafinder_examples_tests.join_conditions_specs import (
    JOIN_CONDITIONS_EMPLOYEE_FINDER_SPECS, JOIN_CONDITIONS_FIRM_FINDER_SPECS, JOIN_CONDITIONS_MAPPING,
    JOIN_CONDITIONS_PERSON_FINDER_SPECS,
)

_MODS = ["org", "org.staff", "org.staff.firm_finder", "org.staff.firm_finder_base", "org.staff.person_finder",
         "org.staff.person_finder_base", "org.staff.employee_finder", "org.staff.employee_finder_base"]


def _build_repo():
    repo = Database("org_db", "duckdb://test.db")
    schema = Schema("org", repo)
    Table("firms", [("id", "INT"), ("legal_name", "VARCHAR"), ("flag", "INT")], schema)
    Table("persons", [("id", "INT"), ("first_name", "VARCHAR"), ("flag", "INT"), ("firm_id", "INT")], schema)
    Table("staff", [("id", "INT"), ("name", "VARCHAR"), ("manager_id", "INT"), ("active", "INT")], schema)
    Table("memberships", [("person_id", "INT"), ("firm_id", "INT"), ("active", "INT")], schema)
    return repo


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS org CASCADE")
    conn.execute("CREATE SCHEMA org")
    conn.execute("CREATE TABLE org.firms (id INT, legal_name VARCHAR, flag INT)")
    conn.execute("CREATE TABLE org.persons (id INT, first_name VARCHAR, flag INT, firm_id INT)")
    conn.execute("CREATE TABLE org.memberships (person_id INT, firm_id INT, active INT)")
    conn.execute("CREATE TABLE org.staff (id INT, name VARCHAR, manager_id INT, active INT)")
    for table in ("firms", "persons", "memberships", "staff"):
        conn.execute(f"INSERT INTO org.{table} SELECT * FROM read_csv_auto('{str(example_path(f'join_conditions_{table}.csv'))}')")
    conn.close()


@pytest.fixture(scope="module")
def finders():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    mapping = load(str(example_path(JOIN_CONDITIONS_MAPPING)), _build_repo())
    generate(mapping, tmp)
    _seed_db()

    from org.staff.firm_finder import FirmFinder  # type: ignore[import]
    from org.staff.employee_finder import EmployeeFinder  # type: ignore[import]
    from org.staff.person_finder import PersonFinder  # type: ignore[import]
    yield {"FirmFinder": FirmFinder(), "PersonFinder": PersonFinder(), "EmployeeFinder": EmployeeFinder()}

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


_SPECS = [JOIN_CONDITIONS_FIRM_FINDER_SPECS, JOIN_CONDITIONS_PERSON_FINDER_SPECS, JOIN_CONDITIONS_EMPLOYEE_FINDER_SPECS]


@pytest.mark.parametrize(
    "spec,expectation",
    [pytest.param(spec, e, id=e.name) for spec in _SPECS for e in spec.expectations],
)
def test_join_conditions(spec, expectation, finders):
    expectation.run(finders[spec.finder_name], backend="duckdb")
