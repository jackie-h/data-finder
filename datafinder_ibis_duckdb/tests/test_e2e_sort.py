import os
import shutil
import sys
import tempfile

import duckdb
import pytest

from datafinder import QueryRunnerBase, FinderResult
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


class TestFinderResultIsReturned:

    def test_find_all_returns_finder_result(self, CompanyFinder):
        result = CompanyFinder.find_all([CompanyFinder.name()])
        assert isinstance(result, FinderResult)

    def test_order_by_returns_same_finder_result(self, CompanyFinder):
        result = CompanyFinder.find_all([CompanyFinder.name()])
        chained = result.order_by(CompanyFinder.name().ascending())
        assert chained is result

    def test_find_all_without_order_by_still_works(self, CompanyFinder):
        df = CompanyFinder.find_all([CompanyFinder.name()]).to_pandas()
        assert len(df) == 5


class TestSortAscending:

    def test_sort_by_name_asc(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
        ).order_by(CompanyFinder.name().ascending()).to_pandas()
        names = result["Name"].tolist()
        assert names == sorted(names)

    def test_sort_by_id_asc(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.id_()],
        ).order_by(CompanyFinder.id_().ascending()).to_pandas()
        ids = result["Id"].tolist()
        assert ids == sorted(ids)

    def test_sort_by_category_asc_first_row(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name(), CompanyFinder.category()],
        ).order_by(CompanyFinder.category().ascending()).to_pandas()
        assert result.iloc[0]["Category"] == "Finance"


class TestSortDescending:

    def test_sort_by_name_desc(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
        ).order_by(CompanyFinder.name().descending()).to_pandas()
        names = result["Name"].tolist()
        assert names == sorted(names, reverse=True)

    def test_sort_by_id_desc(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.id_()],
        ).order_by(CompanyFinder.id_().descending()).to_pandas()
        ids = result["Id"].tolist()
        assert ids == sorted(ids, reverse=True)

    def test_sort_by_id_desc_first_row_is_highest(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.id_()],
        ).order_by(CompanyFinder.id_().descending()).to_pandas()
        assert result.iloc[0]["Id"] == 5

    def test_sort_by_category_desc_first_row(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name(), CompanyFinder.category()],
        ).order_by(CompanyFinder.category().descending()).to_pandas()
        assert result.iloc[0]["Category"] == "Technology"


class TestMultiColumnSort:

    def test_sort_by_category_asc_then_name_asc(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name(), CompanyFinder.category()],
        ).order_by(CompanyFinder.category().ascending(), CompanyFinder.name().ascending()).to_pandas()
        # Finance comes first (alphabetically before Manufacturing and Technology)
        assert result.iloc[0]["Category"] == "Finance"
        # Within Finance: Delta Ltd before Gamma LLC
        finance_rows = result[result["Category"] == "Finance"]
        assert finance_rows.iloc[0]["Name"] == "Delta Ltd"
        assert finance_rows.iloc[1]["Name"] == "Gamma LLC"

    def test_sort_by_category_asc_then_name_desc(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name(), CompanyFinder.category()],
        ).order_by(CompanyFinder.category().ascending(), CompanyFinder.name().descending()).to_pandas()
        # Within Finance (first category): Gamma LLC before Delta Ltd (desc)
        finance_rows = result[result["Category"] == "Finance"]
        assert finance_rows.iloc[0]["Name"] == "Gamma LLC"
        assert finance_rows.iloc[1]["Name"] == "Delta Ltd"


class TestSortWithFilter:

    def test_sort_filtered_results(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name(), CompanyFinder.category()],
            CompanyFinder.category().eq("Technology"),
        ).order_by(CompanyFinder.name().ascending()).to_pandas()
        assert len(result) == 2
        assert result.iloc[0]["Name"] == "Acme Corp"
        assert result.iloc[1]["Name"] == "Beta Corp"

    def test_sort_filtered_results_desc(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name(), CompanyFinder.category()],
            CompanyFinder.category().eq("Technology"),
        ).order_by(CompanyFinder.name().descending()).to_pandas()
        assert result.iloc[0]["Name"] == "Beta Corp"
        assert result.iloc[1]["Name"] == "Acme Corp"

    def test_sort_with_single_result(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name()],
            CompanyFinder.category().eq("Manufacturing"),
        ).order_by(CompanyFinder.name().ascending()).to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["Name"] == "Acme Industries"
