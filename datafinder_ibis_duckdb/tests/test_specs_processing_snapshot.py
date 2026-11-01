"""
Runs PROCESSING_SNAPSHOT_QUOTE_FINDER_SPECS (a processing snapshot) against the
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
from model.relational import Database, Schema, Table, Column
from datafinder_examples import example_path

from datafinder_examples_tests.processing_snapshot_specs import (
    PROCESSING_SNAPSHOT_POSITION_FINDER_SPECS, PROCESSING_SNAPSHOT_QUOTE_FINDER_SPECS,
)

_MODS = ["snapshots", "snapshots.market", "snapshots.market.quote_finder", "snapshots.market.quote_finder_base",
         "snapshots.market.position_finder", "snapshots.market.position_finder_base"]


def _build_repo():
    repo = Database("snapshots_db", "duckdb://test.db")
    schema = Schema("mkt", repo)
    Table("quotes", [
        Column("id", "INT"), Column("symbol", "VARCHAR"), Column("price", "DOUBLE"), Column("taken_at", "TIMESTAMP"),
    ], schema)
    Table("positions", [Column("id", "INT"), Column("symbol", "VARCHAR")], schema)
    return repo


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS mkt CASCADE")
    conn.execute("CREATE SCHEMA mkt")
    conn.execute("CREATE TABLE mkt.quotes (id INT, symbol VARCHAR, price DOUBLE, taken_at TIMESTAMP)")
    conn.execute("CREATE TABLE mkt.positions (id INT, symbol VARCHAR)")
    conn.execute(f"INSERT INTO mkt.quotes SELECT * FROM read_csv_auto('{str(example_path('processing_snapshot_quotes.csv'))}')")
    conn.execute(f"INSERT INTO mkt.positions SELECT * FROM read_csv_auto('{str(example_path('processing_snapshot_positions.csv'))}')")
    conn.close()


@pytest.fixture(scope="module")
def finders():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    repo = _build_repo()
    mapping = load(str(example_path(PROCESSING_SNAPSHOT_QUOTE_FINDER_SPECS.mapping_file)), repo)
    generate(mapping, tmp)
    _seed_db()

    from snapshots.market.quote_finder import QuoteFinder  # type: ignore[import]
    from snapshots.market.position_finder import PositionFinder  # type: ignore[import]
    yield {"QuoteFinder": QuoteFinder(), "PositionFinder": PositionFinder()}

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


_CASES = [pytest.param(spec, e, id=e.name) for spec in (PROCESSING_SNAPSHOT_QUOTE_FINDER_SPECS,
                                                          PROCESSING_SNAPSHOT_POSITION_FINDER_SPECS)
          for e in spec.expectations]


@pytest.mark.parametrize("spec,expectation", _CASES)
def test_processing_snapshot(spec, expectation, finders):
    expectation.run(finders[spec.finder_name], backend="duckdb")
