"""
Runs the SQL view specs (a class mapped to a query written as SQL in a mapping's Query section) against the ibis/DuckDB backend.
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
from datafinder_examples_tests.suite_data import load_duckdb

from datafinder_examples_tests.sql_queries_specs import (
    SQL_QUERIES_FIRM_FINDER_SPECS, SQL_QUERIES_FIRM_X_PERSON_FINDER_SPECS, SQL_QUERIES_MAPPING, SQL_QUERIES_POSITION_FINDER_SPECS,
    SQL_QUERIES_HOLDER_FINDER_SPECS, SQL_QUERIES_QUOTE_FINDER_SPECS, SQL_QUERIES_RATE_FINDER_SPECS,
    SQL_QUERIES_SHOUT_FINDER_SPECS, SQL_QUERIES_TAG_FINDER_SPECS, SQL_QUERIES_UNIT_FINDER_SPECS,
)

_SPECS = [SQL_QUERIES_FIRM_FINDER_SPECS, SQL_QUERIES_POSITION_FINDER_SPECS, SQL_QUERIES_FIRM_X_PERSON_FINDER_SPECS,
          SQL_QUERIES_TAG_FINDER_SPECS, SQL_QUERIES_SHOUT_FINDER_SPECS, SQL_QUERIES_RATE_FINDER_SPECS,
          SQL_QUERIES_QUOTE_FINDER_SPECS, SQL_QUERIES_HOLDER_FINDER_SPECS, SQL_QUERIES_UNIT_FINDER_SPECS]
_MODS = ["org", "org.views"]


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS org CASCADE")
    load_duckdb(conn, "sql_queries")
    conn.close()


@pytest.fixture(scope="module")
def finders():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in list(sys.modules):
        if m == "org" or m.startswith("org."):
            sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    mapping = load(str(example_path(SQL_QUERIES_MAPPING)))
    generate(mapping, tmp)
    _seed_db()

    from org.views.firm_finder import FirmFinder  # pyright: ignore[reportMissingImports]
    from org.views.firmxperson_finder import FirmXPersonFinder  # pyright: ignore[reportMissingImports]
    from org.views.position_finder import PositionFinder  # pyright: ignore[reportMissingImports]
    from org.views.holder_finder import HolderFinder  # pyright: ignore[reportMissingImports]
    from org.views.quote_finder import QuoteFinder  # pyright: ignore[reportMissingImports]
    from org.views.rate_finder import RateFinder  # pyright: ignore[reportMissingImports]
    from org.views.shout_finder import ShoutFinder  # pyright: ignore[reportMissingImports]
    from org.views.tag_finder import TagFinder  # pyright: ignore[reportMissingImports]
    from org.views.unit_finder import UnitFinder  # pyright: ignore[reportMissingImports]
    yield {"FirmFinder": FirmFinder(), "PositionFinder": PositionFinder(), "FirmXPersonFinder": FirmXPersonFinder(),
           "TagFinder": TagFinder(), "ShoutFinder": ShoutFinder(), "RateFinder": RateFinder(),
           "QuoteFinder": QuoteFinder(), "HolderFinder": HolderFinder(), "UnitFinder": UnitFinder()}

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in list(sys.modules):
        if m == "org" or m.startswith("org."):
            sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize("spec,expectation", [pytest.param(spec, e, id=e.name) for spec in _SPECS for e in spec.expectations])
def test_sql_queries(spec, expectation, finders):
    expectation.run(finders[spec.finder_name], backend="duckdb")
