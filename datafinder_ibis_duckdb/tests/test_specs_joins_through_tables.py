"""
Runs JOINS_THROUGH_TABLES_EMPLOYEE_FINDER_SPECS (a property of an embedded object that reaches another table, and a
property mapped through another table, with dates reaching the table on the way) against the ibis/DuckDB backend.
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
from model.relational import Database, Schema, Table, Column
from datafinder_examples import example_path

from datafinder_examples_tests.joins_through_tables_specs import JOINS_THROUGH_TABLES_EMPLOYEE_FINDER_SPECS

_MODS = ["people", "people.hr", "people.hr.employee_finder", "people.hr.employee_finder_base",
         "people.hr.country_finder", "people.hr.country_finder_base"]


def _build_repo():
    repo = Database("hr_db", "duckdb://test.db")
    schema = Schema("hr", repo)
    Table("employees", [Column("id", "INT"), Column("name", "VARCHAR"), Column("street", "VARCHAR"),
                        Column("home_country", "INT"), Column("citizen_ref", "INT")], schema)
    Table("country_refs", [Column("ref", "INT"), Column("code", "VARCHAR"), Column("valid_from", "DATE"),
                           Column("valid_to", "DATE")], schema)
    Table("countries", [Column("code", "VARCHAR"), Column("name", "VARCHAR")], schema)
    return repo


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS hr CASCADE")
    conn.execute("CREATE SCHEMA hr")
    conn.execute("CREATE TABLE hr.employees (id INT, name VARCHAR, street VARCHAR, home_country INT, citizen_ref INT)")
    conn.execute("CREATE TABLE hr.country_refs (ref INT, code VARCHAR, valid_from DATE, valid_to DATE)")
    conn.execute("CREATE TABLE hr.countries (code VARCHAR, name VARCHAR)")
    for table in ("employees", "country_refs", "countries"):
        conn.execute(f"INSERT INTO hr.{table} SELECT * FROM read_csv_auto('{str(example_path(f'joins_through_tables_{table}.csv'))}')")
    conn.close()


@pytest.fixture(scope="module")
def finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    mapping = load(str(example_path(JOINS_THROUGH_TABLES_EMPLOYEE_FINDER_SPECS.mapping_file)), _build_repo())
    generate(mapping, tmp)
    _seed_db()

    from people.hr.employee_finder import EmployeeFinder  # type: ignore[import]
    yield EmployeeFinder()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize("expectation", JOINS_THROUGH_TABLES_EMPLOYEE_FINDER_SPECS.expectations,
                         ids=lambda e: e.name)
def test_joins_through_tables(expectation, finder):
    expectation.run(finder, backend="duckdb")
