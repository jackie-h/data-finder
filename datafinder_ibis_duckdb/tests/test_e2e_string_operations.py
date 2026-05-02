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


class TestStringEq:

    def test_eq_returns_exact_match(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().eq("Acme Corp"),
        ).to_pandas()
        assert result["Name"].tolist() == ["Acme Corp"]

    def test_eq_returns_empty_when_no_match(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().eq("Nonexistent"),
        ).to_pandas()
        assert len(result) == 0


class TestStringNe:

    def test_ne_excludes_matching_row(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().ne("Acme Corp"),
        ).to_pandas()
        assert "Acme Corp" not in result["Name"].tolist()

    def test_ne_returns_all_other_rows(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().ne("Acme Corp"),
        ).to_pandas()
        assert len(result) == 4

    def test_ne_all_rows_when_value_absent(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().ne("Nonexistent"),
        ).to_pandas()
        assert len(result) == 5


class TestStringContains:

    def test_contains_matches_substring(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().contains("Corp"),
        ).to_pandas()
        assert set(result["Name"].tolist()) == {"Acme Corp", "Beta Corp"}

    def test_contains_returns_empty_when_no_match(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().contains("ZZZZZ"),
        ).to_pandas()
        assert len(result) == 0

    def test_contains_matches_multiple_with_common_substring(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().contains("Acme"),
        ).to_pandas()
        assert set(result["Name"].tolist()) == {"Acme Corp", "Acme Industries"}


class TestStringStartsWith:

    def test_starts_with_matches_prefix(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().starts_with("Acme"),
        ).to_pandas()
        assert set(result["Name"].tolist()) == {"Acme Corp", "Acme Industries"}

    def test_starts_with_returns_empty_when_no_match(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().starts_with("Zzz"),
        ).to_pandas()
        assert len(result) == 0

    def test_starts_with_single_result(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().starts_with("Beta"),
        ).to_pandas()
        assert result["Name"].tolist() == ["Beta Corp"]


class TestStringEndsWith:

    def test_ends_with_matches_suffix(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().ends_with("Corp"),
        ).to_pandas()
        assert set(result["Name"].tolist()) == {"Acme Corp", "Beta Corp"}

    def test_ends_with_returns_empty_when_no_match(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().ends_with("Zzz"),
        ).to_pandas()
        assert len(result) == 0

    def test_ends_with_single_result(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().ends_with("LLC"),
        ).to_pandas()
        assert result["Name"].tolist() == ["Gamma LLC"]

    def test_ends_with_ltd(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.name().ends_with("Ltd"),
        ).to_pandas()
        assert result["Name"].tolist() == ["Delta Ltd"]


class TestStringOperationsOnNonKeyColumn:

    def test_filter_on_category_contains(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name(), CompanyFinder.category()],
            CompanyFinder.category().contains("nology"),
        ).to_pandas()
        assert set(result["Name"].tolist()) == {"Acme Corp", "Beta Corp"}

    def test_filter_on_category_starts_with(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name(), CompanyFinder.category()],
            CompanyFinder.category().starts_with("Fin"),
        ).to_pandas()
        assert set(result["Name"].tolist()) == {"Gamma LLC", "Delta Ltd"}

    def test_filter_on_category_ne(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name(), CompanyFinder.category()],
            CompanyFinder.category().ne("Technology"),
        ).to_pandas()
        names = set(result["Name"].tolist())
        assert "Acme Corp" not in names
        assert "Beta Corp" not in names
        assert len(result) == 3
