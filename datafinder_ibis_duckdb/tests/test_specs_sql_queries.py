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

from datafinder_examples_tests.sql_queries_specs import (
    SQL_QUERIES_FIRM_FINDER_SPECS, SQL_QUERIES_FIRM_X_PERSON_FINDER_SPECS, SQL_QUERIES_MAPPING, SQL_QUERIES_POSITION_FINDER_SPECS,
    SQL_QUERIES_HOLDER_FINDER_SPECS, SQL_QUERIES_QUOTE_FINDER_SPECS, SQL_QUERIES_RATE_FINDER_SPECS,
    SQL_QUERIES_SHOUT_FINDER_SPECS, SQL_QUERIES_TAG_FINDER_SPECS,
)

_SPECS = [SQL_QUERIES_FIRM_FINDER_SPECS, SQL_QUERIES_POSITION_FINDER_SPECS, SQL_QUERIES_FIRM_X_PERSON_FINDER_SPECS,
          SQL_QUERIES_TAG_FINDER_SPECS, SQL_QUERIES_SHOUT_FINDER_SPECS, SQL_QUERIES_RATE_FINDER_SPECS,
          SQL_QUERIES_QUOTE_FINDER_SPECS, SQL_QUERIES_HOLDER_FINDER_SPECS]
_MODS = ["org", "org.views"]


def _seed_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS org CASCADE")
    conn.execute("CREATE SCHEMA org")
    conn.execute("CREATE TABLE org.people (person_id INT, first_name VARCHAR, age INT, firm_id INT, firm_name VARCHAR)")
    conn.execute("CREATE TABLE org.firms (id INT, name VARCHAR)")
    conn.execute("CREATE TABLE org.trades (acc_num INT, gsn VARCHAR, qty INT)")
    conn.execute("CREATE TABLE org.rates (code VARCHAR, amount DOUBLE, from_z DATE, thru_z DATE)")
    conn.execute("CREATE TABLE org.quotes (code VARCHAR, amount DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP, from_z DATE, thru_z DATE)")
    conn.execute("CREATE TABLE org.holders (id INT, rate_code VARCHAR)")
    for table in ("people", "firms", "trades", "rates", "quotes", "holders"):
        conn.execute(f"INSERT INTO org.{table} SELECT * FROM read_csv_auto('{str(example_path(f'sql_queries_{table}.csv'))}')")
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

    from org.views.firm_finder import FirmFinder  # type: ignore[import]
    from org.views.firmxperson_finder import FirmXPersonFinder  # type: ignore[import]
    from org.views.position_finder import PositionFinder  # type: ignore[import]
    from org.views.holder_finder import HolderFinder  # type: ignore[import]
    from org.views.quote_finder import QuoteFinder  # type: ignore[import]
    from org.views.rate_finder import RateFinder  # type: ignore[import]
    from org.views.shout_finder import ShoutFinder  # type: ignore[import]
    from org.views.tag_finder import TagFinder  # type: ignore[import]
    yield {"FirmFinder": FirmFinder(), "PositionFinder": PositionFinder(), "FirmXPersonFinder": FirmXPersonFinder(),
           "TagFinder": TagFinder(), "ShoutFinder": ShoutFinder(), "RateFinder": RateFinder(),
           "QuoteFinder": QuoteFinder(), "HolderFinder": HolderFinder()}

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in list(sys.modules):
        if m == "org" or m.startswith("org."):
            sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize("spec,expectation", [pytest.param(spec, e, id=e.name) for spec in _SPECS for e in spec.expectations])
def test_sql_queries(spec, expectation, finders):
    expectation.run(finders[spec.finder_name], backend="duckdb")
