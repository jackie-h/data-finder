"""
Runs PERSON_DENORMALISED_FINDER_SPECS against the ibis/DuckDB backend: embedded properties with no table behind
them are read from the person's own row, without a join.
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

from datafinder_examples_tests.people_denormalised_specs import PERSON_DENORMALISED_FINDER_SPECS

_MODS = ["person_finder", "person_finder_base"]


@pytest.fixture(scope="module")
def person_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    generate(load(str(example_path(PERSON_DENORMALISED_FINDER_SPECS.mapping_file))), tmp)

    conn = duckdb.connect("test.db")
    conn.execute("DROP TABLE IF EXISTS people")
    conn.execute("CREATE TABLE people (id INT, first_name VARCHAR, home VARCHAR, firm_name VARCHAR, firm_address VARCHAR)")
    conn.execute(f"INSERT INTO people SELECT * FROM read_csv_auto('{str(example_path('people_denormalised.csv'))}')")
    conn.close()

    from person_finder import PersonFinder  # type: ignore[import]
    yield PersonFinder()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "expectation", PERSON_DENORMALISED_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_person_denormalised(expectation, person_finder):
    expectation.run(person_finder, backend="duckdb")
