"""
Runs BITEMPORAL_DATES_QUOTE_FINDER_SPECS (a bitemporal table with a datetime infinity and a date one) against the
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
from datafinder_examples_tests.suite_data import load_duckdb

from datafinder_examples_tests.bitemporal_dates_specs import (
    BITEMPORAL_DATES_QUOTE_FINDER_SPECS,
)

_MODS = ["quotes", "quotes.dated", "quotes.dated.quote_finder",
         "quotes.dated.quote_finder_base"]


def _build_repo():
    repo = Database("dated_quotes_db", "duckdb://test.db")
    schema = Schema("dated", repo)
    Table("quotes", [
        ("code", "VARCHAR"), ("amount", "DOUBLE"),
        ("in_z", "TIMESTAMP"), ("out_z", "TIMESTAMP"), ("from_z", "DATE"), ("thru_z", "DATE"),
    ], schema)
    return repo


def _seed_db():
    conn = duckdb.connect("test.db")
    load_duckdb(conn, "bitemporal_dates")
    conn.close()


@pytest.fixture(scope="module")
def quote_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    repo = _build_repo()
    mapping = load(str(example_path(BITEMPORAL_DATES_QUOTE_FINDER_SPECS.mapping_file)), repo)
    generate(mapping, tmp)
    _seed_db()

    from quotes.dated.quote_finder import QuoteFinder  # pyright: ignore[reportMissingImports]
    yield QuoteFinder()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "expectation", BITEMPORAL_DATES_QUOTE_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_business_range(expectation, quote_finder):
    expectation.run(quote_finder, backend="duckdb")
