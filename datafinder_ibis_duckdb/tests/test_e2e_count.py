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


class TestCountAll:

    def test_count_all_returns_total_row_count(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.count()],
        ).to_pandas()
        assert result.iloc[0]["Count"] == 5

    def test_count_all_with_filter_counts_matching_rows(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.count()],
            CompanyFinder.category().eq("Technology"),
        ).to_pandas()
        assert result.iloc[0]["Count"] == 2

    def test_count_all_with_filter_no_matches_returns_zero(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.count()],
            CompanyFinder.category().eq("Nonexistent"),
        ).to_pandas()
        assert result.iloc[0]["Count"] == 0

    def test_count_all_with_contains_filter(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.count()],
            CompanyFinder.name().contains("Corp"),
        ).to_pandas()
        assert result.iloc[0]["Count"] == 2

    def test_count_all_finance_category(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.count()],
            CompanyFinder.category().eq("Finance"),
        ).to_pandas()
        assert result.iloc[0]["Count"] == 2

    def test_count_all_result_has_count_column(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.count()],
        ).to_pandas()
        assert "Count" in result.columns
        assert len(result) == 1


class TestAttributeCount:

    def test_count_name_returns_non_null_count(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().count()],
        ).to_pandas()
        assert result.iloc[0]["COUNT name"] == 5

    def test_count_id_returns_total(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.id_().count()],
        ).to_pandas()
        assert result.iloc[0]["COUNT id"] == 5

    def test_count_attribute_with_filter(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().count()],
            CompanyFinder.category().eq("Technology"),
        ).to_pandas()
        assert result.iloc[0]["COUNT name"] == 2

    def test_count_attribute_with_ne_filter(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().count()],
            CompanyFinder.category().ne("Technology"),
        ).to_pandas()
        assert result.iloc[0]["COUNT name"] == 3
