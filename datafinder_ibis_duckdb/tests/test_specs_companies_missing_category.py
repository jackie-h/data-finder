"""
Runs COMPANIES_MISSING_CATEGORY_SPECS (a value that can be missing) against the ibis/DuckDB backend. Uses a dataset
in which two companies have no category.
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

from datafinder_examples_tests.companies_missing_category_specs import (
    COMPANIES_MISSING_CATEGORY_SPECS,
)

_MODS = ["company_finder"]


@pytest.fixture(scope="module")
def company_missing_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    mapping = load(str(example_path(COMPANIES_MISSING_CATEGORY_SPECS.mapping_file)))
    generate(mapping, tmp)

    conn = duckdb.connect("test.db")
    conn.execute("DROP TABLE IF EXISTS companies")
    conn.execute("CREATE TABLE companies (id INT, name VARCHAR, category VARCHAR)")
    conn.execute(f"INSERT INTO companies SELECT * FROM read_csv_auto('{str(example_path('companies_missing_category.csv'))}')")
    conn.close()

    from company_finder import CompanyFinder as CF  # type: ignore[import]
    yield CF()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "expectation", COMPANIES_MISSING_CATEGORY_SPECS.expectations, ids=lambda e: e.name
)
def test_company_missing_category(expectation, company_missing_finder):
    expectation.run(company_missing_finder, backend="duckdb")
