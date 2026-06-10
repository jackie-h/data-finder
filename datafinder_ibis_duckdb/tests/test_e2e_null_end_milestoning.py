"""
End-to-end tests for milestoning where open rows carry NULL in the end column.

With no infinite_datetime set on the scheme, the generated SQL should use
(out_z > processing_dt OR out_z IS NULL) so that NULL-ended rows are treated
as currently active.
"""
import datetime
import os
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

_MAPPING_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "null_end_milestoning_mapping.md")
)

_DB_FILE = "test.db"  # relative path — same file the ibis engine connects to

_FINDER_MODULES = ["prices", "prices.market", "prices.market.price_finder"]


def _build_repository() -> Database:
    repo = Database("prices_db", "duckdb://test.db")
    schema = Schema("mkt", repo)
    Table("prices", [
        Column("sym", "VARCHAR"),
        Column("price", "DOUBLE"),
        Column("in_z", "TIMESTAMP"),
        Column("out_z", "TIMESTAMP"),
    ], schema)
    return repo


def _seed_test_db():
    conn = duckdb.connect(_DB_FILE)
    conn.execute("DROP SCHEMA IF EXISTS mkt CASCADE")
    conn.execute("CREATE SCHEMA mkt")
    conn.execute(
        "CREATE TABLE mkt.prices(sym VARCHAR, price DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP)"
    )
    # AAPL: active from 2020, NULL end (open-ended)
    conn.execute("INSERT INTO mkt.prices VALUES ('AAPL', 150.0, '2020-01-01', NULL)")
    # MSFT: active from 2020, NULL end (open-ended)
    conn.execute("INSERT INTO mkt.prices VALUES ('MSFT', 300.0, '2020-01-01', NULL)")
    # GOOG: active 2020–2022, then expired (finite end date)
    conn.execute("INSERT INTO mkt.prices VALUES ('GOOG', 2800.0, '2020-01-01', '2022-01-01')")
    conn.close()


@pytest.fixture(scope="module")
def finders():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)

    temp_dir = tempfile.mkdtemp()
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    sys.path.insert(0, temp_dir)

    repo = _build_repository()
    mapping = load(_MAPPING_FILE, repo)
    generate(mapping, temp_dir)
    _seed_test_db()

    from prices.market.price_finder import PriceFinder  # type: ignore[import]

    yield {"Price": PriceFinder()}

    sys.path.remove(temp_dir)
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)
    try:
        conn = duckdb.connect(_DB_FILE)
        conn.execute("DROP SCHEMA IF EXISTS mkt CASCADE")
        conn.close()
    except Exception:
        pass


class TestNullEndMilestoningE2E:

    def test_null_end_rows_returned_as_active(self, finders):
        PriceFinder = finders["Price"]
        dt = datetime.datetime(2024, 6, 1, 12, 0, 0)
        result = PriceFinder.find_all(None, dt, [PriceFinder.symbol(), PriceFinder.price()]).to_pandas()
        assert set(result["Symbol"].tolist()) == {"AAPL", "MSFT"}

    def test_expired_row_excluded(self, finders):
        PriceFinder = finders["Price"]
        dt = datetime.datetime(2024, 6, 1, 12, 0, 0)
        result = PriceFinder.find_all(None, dt, [PriceFinder.symbol(), PriceFinder.price()]).to_pandas()
        assert "GOOG" not in result["Symbol"].tolist()

    def test_expired_row_visible_before_expiry(self, finders):
        PriceFinder = finders["Price"]
        dt = datetime.datetime(2021, 6, 1, 12, 0, 0)
        result = PriceFinder.find_all(None, dt, [PriceFinder.symbol(), PriceFinder.price()]).to_pandas()
        assert set(result["Symbol"].tolist()) == {"AAPL", "MSFT", "GOOG"}

    def test_null_end_row_visible_across_time(self, finders):
        PriceFinder = finders["Price"]
        for year in [2021, 2023, 2099]:
            dt = datetime.datetime(year, 1, 1, 0, 0, 0)
            result = PriceFinder.find_all(None, dt, [PriceFinder.symbol()]).to_pandas()
            assert "AAPL" in result["Symbol"].tolist(), f"AAPL should be active in {year}"
