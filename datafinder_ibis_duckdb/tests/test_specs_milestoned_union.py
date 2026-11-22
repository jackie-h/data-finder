"""
Runs MILESTONED_UNION_ORDER_FINDER_SPECS (a milestoned step read through a union) against the ibis/DuckDB backend.
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

from datafinder_examples_tests.milestoned_union_specs import MILESTONED_UNION_ORDER_FINDER_SPECS

_MODS = ["order_finder", "order_finder_base", "product_finder", "product_finder_base",
         "productclassification_finder", "productclassification_finder_base"]


def _seed_db():
    conn = duckdb.connect("test.db")
    for table in ("OrderTable", "ProductTableWithBusinessSnapshotMilestoning", "ProductClassificationTableWithBusinessSnapshotMilestoning"):
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    load_duckdb(conn, "milestoned_union")
    conn.close()


@pytest.fixture(scope="module")
def order_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    generate(load(str(example_path(MILESTONED_UNION_ORDER_FINDER_SPECS.mapping_file))), tmp)
    _seed_db()

    from order_finder import OrderFinder  # pyright: ignore[reportMissingImports]
    yield OrderFinder()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize("expectation", MILESTONED_UNION_ORDER_FINDER_SPECS.expectations, ids=lambda e: e.name)
def test_milestoned_union(expectation, order_finder):
    expectation.run(order_finder, backend="duckdb")
