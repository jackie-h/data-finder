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

_MAPPING_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "companies_mapping.md")
)

_FINDER_MODULES = ["company_finder"]

_COMPANIES = [
    (1, "Acme Corp",       "Technology"),
    (2, "Acme Industries", "Manufacturing"),
    (3, "Beta Corp",       "Technology"),
    (4, "Gamma LLC",       "Finance"),
    (5, "Delta Ltd",       "Finance"),
]


def _build_test_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP TABLE IF EXISTS companies")
    conn.execute("CREATE TABLE companies (id INT, name VARCHAR, category VARCHAR)")
    for row in _COMPANIES:
        conn.execute("INSERT INTO companies VALUES (?, ?, ?)", row)
    conn.close()


@pytest.fixture(scope="module")
def CompanyFinder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)

    temp_dir = tempfile.mkdtemp()
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    sys.path.insert(0, temp_dir)

    mapping = load(_MAPPING_FILE)
    generate(mapping, temp_dir)
    _build_test_db()

    from company_finder import CompanyFinder as CF
    yield CF

    sys.path.remove(temp_dir)
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestLimit:

    def test_limit_returns_correct_number_of_rows(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
        ).limit(3).to_pandas()
        assert len(result) == 3

    def test_limit_one_returns_single_row(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
        ).limit(1).to_pandas()
        assert len(result) == 1

    def test_no_limit_returns_all_rows(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
        ).to_pandas()
        assert len(result) == 5

    def test_limit_returns_same_finder_result(self, CompanyFinder):
        result = CompanyFinder.find_all([CompanyFinder.name()])
        chained = result.limit(3)
        assert chained is result

    def test_limit_with_order_by(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
        ).order_by(CompanyFinder.name().ascending()).limit(2).to_pandas()
        names = result["Name"].tolist()
        assert len(names) == 2
        assert names == sorted(names)

    def test_limit_with_filter(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.category().eq("Technology"),
        ).limit(1).to_pandas()
        assert len(result) == 1
