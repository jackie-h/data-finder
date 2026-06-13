"""
Runs DIAMOND_FINDER_SPECS (diamond inheritance queries) against the
ibis/DuckDB backend.

Also keeps non-spec tests for property-existence assertions that cannot
be expressed as data-result expectations.
"""
import inspect
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

from datafinder_examples_tests.diamond_specs import DIAMOND_FINDER_SPECS, DIAMOND_ITEMS

_MODS = ["record_finder", "record_finder_base"]


@pytest.fixture(scope="module")
def record_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    mapping = load(str(example_path(DIAMOND_FINDER_SPECS.mapping_file)))
    generate(mapping, tmp)

    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS records CASCADE")
    conn.execute("CREATE SCHEMA records")
    conn.execute("""
        CREATE TABLE records.items (
            item_id INT, created_at VARCHAR, updated_at VARCHAR,
            version INT, record_name VARCHAR
        )
    """)
    for r in DIAMOND_ITEMS:
        conn.execute("INSERT INTO records.items VALUES (?, ?, ?, ?, ?)", r)
    conn.close()

    from record_finder import RecordFinder  # type: ignore[import]
    yield RecordFinder()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "expectation", DIAMOND_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_diamond(expectation, record_finder):
    expectation.run(record_finder, backend="duckdb")


# --- non-spec tests: structural property existence ---

def test_has_id_from_auditable(record_finder):
    assert record_finder.id_() is not None


def test_has_created_at(record_finder):
    assert record_finder.created_at() is not None


def test_has_updated_at(record_finder):
    assert record_finder.updated_at() is not None


def test_has_version(record_finder):
    assert record_finder.version() is not None


def test_has_record_name(record_finder):
    assert record_finder.record_name() is not None


def test_no_duplicate_id_accessor(record_finder):
    members = [name for name, _ in inspect.getmembers(record_finder) if name == "id_"]
    assert len(members) == 1
