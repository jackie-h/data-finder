"""
Runs BUSINESS_RANGE_RATE_FINDER_SPECS (a business date range) against the
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

from datafinder_examples_tests.business_range_specs import (
    BUSINESS_RANGE_RATE_FINDER_SPECS,
)

_MODS = ["rates", "rates.market", "rates.market.rate_finder",
         "rates.market.rate_finder_base"]


def _build_repo():
    repo = Database("rates_db", "duckdb://test.db")
    schema = Schema("mkt", repo)
    Table("rates", [
        Column("code", "VARCHAR"), Column("amount", "DOUBLE"),
        Column("from_z", "DATE"), Column("thru_z", "DATE"),
    ], schema)
    return repo


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS mkt CASCADE")
    conn.execute("CREATE SCHEMA mkt")
    conn.execute(
        "CREATE TABLE mkt.rates (code VARCHAR, amount DOUBLE, from_z DATE, thru_z DATE)"
    )
    conn.execute(f"INSERT INTO mkt.rates SELECT * FROM read_csv_auto('{str(example_path('business_range_rates.csv'))}')")
    conn.close()


@pytest.fixture(scope="module")
def rate_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    repo = _build_repo()
    mapping = load(str(example_path(BUSINESS_RANGE_RATE_FINDER_SPECS.mapping_file)), repo)
    generate(mapping, tmp)
    _seed_db()

    from rates.market.rate_finder import RateFinder  # type: ignore[import]
    yield RateFinder()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "expectation", BUSINESS_RANGE_RATE_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_business_range(expectation, rate_finder):
    expectation.run(rate_finder, backend="duckdb")
