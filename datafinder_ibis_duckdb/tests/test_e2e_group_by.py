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


class TestFinderResultGroupBy:

    def test_group_by_returns_same_finder_result(self, CompanyFinder):
        result = CompanyFinder.find_all([CompanyFinder.category(), CompanyFinder.count()])
        chained = result.group_by(CompanyFinder.category())
        assert chained is result

    def test_group_by_without_group_by_returns_all_rows(self, CompanyFinder):
        result = CompanyFinder.find_all([CompanyFinder.name()]).to_pandas()
        assert len(result) == 5


class TestGroupByCountAll:

    def test_count_per_category(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.category(), CompanyFinder.count()],
        ).group_by(CompanyFinder.category()).to_pandas()
        counts = dict(zip(result["Category"], result["Count"]))
        assert counts["Technology"] == 2
        assert counts["Finance"] == 2
        assert counts["Manufacturing"] == 1

    def test_group_by_returns_one_row_per_group(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.category(), CompanyFinder.count()],
        ).group_by(CompanyFinder.category()).to_pandas()
        assert len(result) == 3

    def test_group_by_with_filter(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.category(), CompanyFinder.count()],
            CompanyFinder.category().ne("Manufacturing"),
        ).group_by(CompanyFinder.category()).to_pandas()
        assert len(result) == 2
        counts = dict(zip(result["Category"], result["Count"]))
        assert counts["Technology"] == 2
        assert counts["Finance"] == 2


class TestGroupByAttributeCount:

    def test_count_attribute_per_category(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.category(), CompanyFinder.name().count()],
        ).group_by(CompanyFinder.category()).to_pandas()
        counts = dict(zip(result["Category"], result["Name Count"]))
        assert counts["Technology"] == 2
        assert counts["Finance"] == 2
        assert counts["Manufacturing"] == 1


class TestGroupByWithOrderBy:

    def test_group_by_with_order_by(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.category(), CompanyFinder.count()],
        ).group_by(CompanyFinder.category()).order_by(
            CompanyFinder.category().ascending()
        ).to_pandas()
        categories = result["Category"].tolist()
        assert categories == sorted(categories)

    def test_group_by_with_order_by_desc(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.category(), CompanyFinder.count()],
        ).group_by(CompanyFinder.category()).order_by(
            CompanyFinder.category().descending()
        ).to_pandas()
        categories = result["Category"].tolist()
        assert categories == sorted(categories, reverse=True)
