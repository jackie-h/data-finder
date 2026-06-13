"""
Runs NULL_END_PRICE_FINDER_SPECS (NULL-ended milestoning) against the
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

from datafinder_examples_tests.null_end_milestoning_specs import (
    NULL_END_PRICE_FINDER_SPECS,
)

_MODS = ["prices", "prices.market", "prices.market.price_finder",
         "prices.market.price_finder_base"]


def _build_repo():
    repo = Database("prices_db", "duckdb://test.db")
    schema = Schema("mkt", repo)
    Table("prices", [
        Column("sym", "VARCHAR"), Column("price", "DOUBLE"),
        Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP"),
    ], schema)
    return repo


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS mkt CASCADE")
    conn.execute("CREATE SCHEMA mkt")
    conn.execute(
        "CREATE TABLE mkt.prices (sym VARCHAR, price DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP)"
    )
    conn.execute(f"INSERT INTO mkt.prices SELECT * FROM read_csv_auto('{str(example_path('null_end_prices.csv'))}')")
    conn.close()


@pytest.fixture(scope="module")
def price_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    repo = _build_repo()
    mapping = load(str(example_path(NULL_END_PRICE_FINDER_SPECS.mapping_file)), repo)
    generate(mapping, tmp)
    _seed_db()

    from prices.market.price_finder import PriceFinder  # type: ignore[import]
    yield PriceFinder()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "expectation", NULL_END_PRICE_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_null_end_milestoning(expectation, price_finder):
    expectation.run(price_finder, backend="duckdb")
