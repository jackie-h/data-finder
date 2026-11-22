"""
Runs COMPANIES_STRING_SCALAR_SPECS (upper, lower, strip, length, reverse, etc.)
against the ibis/DuckDB backend.  Uses a different dataset from the main
companies spec — names with whitespace variations.
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

from datafinder_examples_tests.companies_string_scalar_specs import (
    COMPANIES_STRING_SCALAR_SPECS,
)

_MODS = ["company_finder"]


@pytest.fixture(scope="module")
def company_scalar_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)
    tmp = tempfile.mkdtemp()
    for m in _MODS:
        sys.modules.pop(m, None)
    sys.path.insert(0, tmp)

    mapping = load(str(example_path(COMPANIES_STRING_SCALAR_SPECS.mapping_file)))
    generate(mapping, tmp)

    conn = duckdb.connect("test.db")
    conn.execute("DROP TABLE IF EXISTS companies")
    load_duckdb(conn, "companies_scalar")
    conn.close()

    from company_finder import CompanyFinder as CF  # pyright: ignore[reportMissingImports]
    yield CF()

    if tmp in sys.path:
        sys.path.remove(tmp)
    for m in _MODS:
        sys.modules.pop(m, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "expectation", COMPANIES_STRING_SCALAR_SPECS.expectations, ids=lambda e: e.name
)
def test_company_scalar(expectation, company_scalar_finder):
    expectation.run(company_scalar_finder, backend="duckdb")
