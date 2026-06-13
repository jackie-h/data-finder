"""
Runs POSITION_DATE_FINDER_SPECS (date extract, arithmetic, diff) against the
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
from datafinder_examples import example_path

from datafinder_examples_tests.positions_specs import (
    POSITION_DATE_FINDER_SPECS,
    POSITIONS_DATE,
)

_MODS = ["position_finder", "position_finder_base"]


@pytest.fixture(scope="module")
def position_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    mapping = load(str(example_path(POSITION_DATE_FINDER_SPECS.mapping_file)))
    generate(mapping, tmp)

    conn = duckdb.connect("test.db")
    conn.execute("DROP TABLE IF EXISTS positions")
    conn.execute("CREATE TABLE positions (id INT, trade_date DATE, npv DOUBLE)")
    for r in POSITIONS_DATE:
        conn.execute("INSERT INTO positions VALUES (?, ?, ?)", r)
    conn.close()

    from position_finder import PositionFinder  # type: ignore[import]
    yield PositionFinder()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "expectation", POSITION_DATE_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_position_date(expectation, position_finder):
    expectation.run(position_finder, backend="duckdb")
