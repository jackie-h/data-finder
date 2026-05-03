import datetime
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
    os.path.join(os.path.dirname(__file__), "positions_mapping.md")
)

_FINDER_MODULES = ["position_finder"]

_POSITIONS = [
    (1, datetime.date(2024, 3, 15), 500.0),
    (2, datetime.date(2024, 7, 20), 800.0),
    (3, datetime.date(2024, 11, 5), 1200.0),
]


def _build_test_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP TABLE IF EXISTS positions")
    conn.execute("CREATE TABLE positions (id INT, trade_date DATE, npv DOUBLE)")
    for row in _POSITIONS:
        conn.execute("INSERT INTO positions VALUES (?, ?, ?)", row)
    conn.close()


@pytest.fixture(scope="module")
def PositionFinder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)

    temp_dir = tempfile.mkdtemp()
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    sys.path.insert(0, temp_dir)

    mapping = load(_MAPPING_FILE)
    generate(mapping, temp_dir)
    _build_test_db()

    from position_finder import PositionFinder as PF
    yield PF

    sys.path.remove(temp_dir)
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestDateExtract:

    def test_year_extracts_correct_value(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().year()],
            PositionFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Year Trade Date"] == 2024

    def test_month_extracts_correct_value(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().month()],
            PositionFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Month Trade Date"] == 3

    def test_day_extracts_correct_value(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().day()],
            PositionFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Day Trade Date"] == 15

    def test_quarter_extracts_correct_value(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().quarter()],
            PositionFinder.id_().eq(2),
        ).to_pandas()
        assert result.iloc[0]["Quarter Trade Date"] == 3

    def test_week_extracts_correctly(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().week()],
            PositionFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Week Trade Date"] == 11

    def test_multiple_extract_columns(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().year(), PositionFinder.trade_date().month()],
        ).to_pandas()
        assert "Year Trade Date" in result.columns
        assert "Month Trade Date" in result.columns
        assert len(result) == 3


class TestDateArithmetic:

    def test_add_days_shifts_date_forward(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().add_days(10)],
            PositionFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Add Days Trade Date"].date() == datetime.date(2024, 3, 25)

    def test_subtract_days_shifts_date_back(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().subtract_days(5)],
            PositionFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Subtract Days Trade Date"].date() == datetime.date(2024, 3, 10)

    def test_add_months_shifts_date_forward(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().add_months(1)],
            PositionFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Add Months Trade Date"].date() == datetime.date(2024, 4, 15)

    def test_add_years_shifts_date_forward(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().add_years(1)],
            PositionFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Add Years Trade Date"].date() == datetime.date(2025, 3, 15)

    def test_subtract_months(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().subtract_months(1)],
            PositionFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Subtract Months Trade Date"].date() == datetime.date(2024, 2, 15)


class TestDateDiff:

    def test_diff_days_returns_correct_count(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().diff_days(datetime.date(2024, 3, 25))],
            PositionFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Diff Days Trade Date"] == 10

    def test_diff_months_returns_correct_count(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().diff_months(datetime.date(2024, 6, 15))],
            PositionFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Diff Months Trade Date"] == 3

    def test_diff_years_returns_correct_count(self, PositionFinder):
        result = PositionFinder.find_all(
            [PositionFinder.trade_date().diff_years(datetime.date(2026, 3, 15))],
            PositionFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Diff Years Trade Date"] == 2
