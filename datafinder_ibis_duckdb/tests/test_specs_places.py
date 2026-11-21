"""
Runs the places specs (a navigation with a filter of its own, whose condition is in the ``ON`` of its join) against the
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
from model.relational import Database, Schema, Table
from datafinder_examples import example_path

from datafinder_examples_tests.places_specs import (
    PLACES_LOCATION_FINDER_SPECS, PLACES_MAPPING, PLACES_PERSON_FINDER_SPECS,
)

_MODS = ["travel", "travel.places", "travel.places.person_finder", "travel.places.person_finder_base",
         "travel.places.location_finder", "travel.places.location_finder_base", "travel.places.region_finder",
         "travel.places.region_finder_base"]


def _build_repo():
    repo = Database("places_db", "duckdb://test.db")
    schema = Schema("geo", repo)
    Table("people", [("id", "INT"), ("name", "VARCHAR")], schema)
    Table("regions", [("id", "INT"), ("name", "VARCHAR")], schema)
    Table("locations", [("id", "INT"), ("place", "VARCHAR"), ("visits", "INT"), ("person_id", "INT"), ("region_id", "INT")], schema)
    return repo


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS geo CASCADE")
    conn.execute("CREATE SCHEMA geo")
    conn.execute("CREATE TABLE geo.people (id INT, name VARCHAR)")
    conn.execute("CREATE TABLE geo.regions (id INT, name VARCHAR)")
    conn.execute("CREATE TABLE geo.locations (id INT, place VARCHAR, visits INT, person_id INT, region_id INT)")
    for table in ("people", "regions", "locations"):
        conn.execute(f"INSERT INTO geo.{table} SELECT * FROM read_csv_auto('{str(example_path(f'places_{table}.csv'))}')")
    conn.close()


@pytest.fixture(scope="module")
def finders():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    mapping = load(str(example_path(PLACES_MAPPING)), _build_repo())
    generate(mapping, tmp)
    _seed_db()

    from travel.places.person_finder import PersonFinder  # pyright: ignore[reportMissingImports]
    from travel.places.location_finder import LocationFinder  # pyright: ignore[reportMissingImports]
    yield {"PersonFinder": PersonFinder(), "LocationFinder": LocationFinder()}

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


_SPECS = [PLACES_PERSON_FINDER_SPECS, PLACES_LOCATION_FINDER_SPECS]


@pytest.mark.parametrize(
    "spec,expectation",
    [pytest.param(spec, e, id=e.name) for spec in _SPECS for e in spec.expectations],
)
def test_places(spec, expectation, finders):
    expectation.run(finders[spec.finder_name], backend="duckdb")
