"""
Runs COMPANY_FINDER_SPECS (count, sort, group_by, limit, string ops, window
functions) against the ibis/DuckDB backend.
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

from datafinder_examples_tests.companies_specs import COMPANY_FINDER_SPECS

_MODS = ["company_finder"]


@pytest.fixture(scope="module")
def company_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    mapping = load(str(example_path(COMPANY_FINDER_SPECS.mapping_file)))
    generate(mapping, tmp)

    conn = duckdb.connect("test.db")
    conn.execute("DROP TABLE IF EXISTS companies")
    conn.execute("CREATE TABLE companies (id INT, name VARCHAR, category VARCHAR)")
    conn.execute(f"INSERT INTO companies SELECT * FROM read_csv_auto('{str(example_path('companies.csv'))}')")
    conn.close()

    from company_finder import CompanyFinder as CF  # type: ignore[import]
    yield CF()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "expectation", COMPANY_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_company(expectation, company_finder):
    expectation.run(company_finder, backend="duckdb")
