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

    from company_finder import CompanyFinder as CF  # type: ignore[import]
    yield CF()

    sys.path.remove(temp_dir)
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestRank:

    def test_rank_method_min_returns_one_row_per_company(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.name(), CompanyFinder.id_().rank(method='min', order_by=[CompanyFinder.id_().ascending()])],
        ).to_pandas()
        assert len(df) == 5

    def test_rank_method_min_values_are_sequential(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.name(), CompanyFinder.id_().rank(method='min', order_by=[CompanyFinder.id_().ascending()])],
        ).order_by(CompanyFinder.id_().ascending()).to_pandas()
        assert df["Rank"].tolist() == [1, 2, 3, 4, 5]

    def test_rank_method_first_produces_unique_row_numbers(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.name(), CompanyFinder.id_().rank(method='first', order_by=[CompanyFinder.id_().ascending()])],
        ).to_pandas()
        assert sorted(df["Row Number"].tolist()) == [1, 2, 3, 4, 5]

    def test_rank_method_dense_with_partition(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.name(), CompanyFinder.category(),
             CompanyFinder.id_().rank(method='dense',
                                      partition_by=[CompanyFinder.category()],
                                      order_by=[CompanyFinder.id_().ascending()])],
        ).to_pandas()
        # Each category has at most 2 members so dense rank values should be 1 or 2
        assert set(df["Dense Rank"].tolist()).issubset({1, 2})

    def test_rank_pct_true_values_between_zero_and_one(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.name(),
             CompanyFinder.id_().rank(pct=True, order_by=[CompanyFinder.id_().ascending()])],
        ).to_pandas()
        assert all(0.0 <= v <= 1.0 for v in df["Percent Rank"].tolist())

    def test_rank_pct_method_max_values_between_zero_and_one(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.name(),
             CompanyFinder.id_().rank(pct=True, method='max', order_by=[CompanyFinder.id_().ascending()])],
        ).to_pandas()
        assert all(0.0 <= v <= 1.0 for v in df["Cume Dist"].tolist())


class TestShift:

    def test_shift_positive_returns_previous_id(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.id_(), CompanyFinder.id_().shift(1, order_by=[CompanyFinder.id_().ascending()])],
        ).order_by(CompanyFinder.id_().ascending()).to_pandas()
        # First row has no previous value → NULL; second row should have id 1
        assert df.iloc[1]["Lag Id"] == 1

    def test_shift_first_row_is_null_for_positive_periods(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.id_(), CompanyFinder.id_().shift(1, order_by=[CompanyFinder.id_().ascending()])],
        ).order_by(CompanyFinder.id_().ascending()).to_pandas()
        import pandas as pd
        assert pd.isna(df.iloc[0]["Lag Id"])

    def test_shift_negative_returns_next_id(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.id_(), CompanyFinder.id_().shift(-1, order_by=[CompanyFinder.id_().ascending()])],
        ).order_by(CompanyFinder.id_().ascending()).to_pandas()
        # First row should see the next id (2)
        assert df.iloc[0]["Lead Id"] == 2

    def test_shift_last_row_is_null_for_negative_periods(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.id_(), CompanyFinder.id_().shift(-1, order_by=[CompanyFinder.id_().ascending()])],
        ).order_by(CompanyFinder.id_().ascending()).to_pandas()
        import pandas as pd
        assert pd.isna(df.iloc[-1]["Lead Id"])


class TestQcut:

    def test_qcut_returns_bucket_per_row(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.name(), CompanyFinder.id_().qcut(2, order_by=[CompanyFinder.id_().ascending()])],
        ).to_pandas()
        assert len(df) == 5

    def test_qcut_bucket_values_within_range(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.name(), CompanyFinder.id_().qcut(2, order_by=[CompanyFinder.id_().ascending()])],
        ).to_pandas()
        assert set(df["Quantile"].tolist()).issubset({1, 2})


class TestFirstLast:

    def test_first_returns_lowest_id_per_category(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.name(), CompanyFinder.category(),
             CompanyFinder.id_().first(partition_by=[CompanyFinder.category()],
                                       order_by=[CompanyFinder.id_().ascending()])],
        ).to_pandas()
        tech = df[df["Category"] == "Technology"]
        # Acme Corp (id=1) and Beta Corp (id=3) — first in partition is 1
        assert all(tech["First Id"] == 1)

    def test_last_id_grows_with_frame(self, CompanyFinder):
        df = CompanyFinder.find_all(
            None, None,
            [CompanyFinder.name(), CompanyFinder.category(),
             CompanyFinder.id_().last(partition_by=[CompanyFinder.category()],
                                      order_by=[CompanyFinder.id_().ascending()])],
        ).to_pandas()
        tech = df[df["Category"] == "Technology"].sort_values("Name")
        # Default frame is UNBOUNDED PRECEDING TO CURRENT ROW, so last_value equals
        # the current row's id. The final row in the partition (Beta Corp, id=3) sees id=3.
        assert tech.iloc[-1]["Last Id"] == 3
