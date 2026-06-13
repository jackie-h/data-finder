"""
Test specifications for date functions using positions_mapping.md.

    positions — id INT, trade_date DATE, npv DOUBLE
"""
import datetime

import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

POSITIONS_MAPPING = "positions_mapping.md"

POSITIONS_DATE = [
    (1, datetime.date(2024, 3, 15),  500.0),
    (2, datetime.date(2024, 7, 20),  800.0),
    (3, datetime.date(2024, 11, 5), 1200.0),
]

_D1 = datetime.date(2024, 3, 15)
_D2 = datetime.date(2024, 7, 20)

POSITION_DATE_FINDER_SPECS = FinderSpec(
    finder_name="PositionFinder",
    mapping_file=POSITIONS_MAPPING,
    expectations=[
        # --- Date extract ---
        TestExpectation(
            name="year_extract",
            query=lambda f: f.find_all(None, None, [f.trade_date().year()], f.id_().eq(1)),
            expected_columns=["Year Trade Date"],
            expected_result=np.array([[_D1.year]], dtype=object),
        ),
        TestExpectation(
            name="month_extract",
            query=lambda f: f.find_all(None, None, [f.trade_date().month()], f.id_().eq(1)),
            expected_columns=["Month Trade Date"],
            expected_result=np.array([[_D1.month]], dtype=object),
        ),
        TestExpectation(
            name="day_extract",
            query=lambda f: f.find_all(None, None, [f.trade_date().day()], f.id_().eq(1)),
            expected_columns=["Day Trade Date"],
            expected_result=np.array([[_D1.day]], dtype=object),
        ),
        TestExpectation(
            name="quarter_extract",
            query=lambda f: f.find_all(None, None, [f.trade_date().quarter()], f.id_().eq(2)),
            expected_columns=["Quarter Trade Date"],
            expected_result=np.array([[(_D2.month - 1) // 3 + 1]], dtype=object),
        ),
        TestExpectation(
            name="week_extract",
            query=lambda f: f.find_all(None, None, [f.trade_date().week()], f.id_().eq(1)),
            expected_columns=["Week Trade Date"],
            expected_result=np.array([[_D1.isocalendar().week]], dtype=object),
        ),
        # --- Date arithmetic ---
        # ibis/DuckDB returns date arithmetic results as datetime64[us], not datetime.date
        TestExpectation(
            name="add_days_ten",
            query=lambda f: f.find_all(None, None, [f.trade_date().add_days(10)], f.id_().eq(1)),
            expected_columns=["Add Days Trade Date"],
            expected_result=np.array([[np.datetime64("2024-03-25", "us")]], dtype=object),
        ),
        TestExpectation(
            name="subtract_days_five",
            query=lambda f: f.find_all(None, None, [f.trade_date().subtract_days(5)], f.id_().eq(1)),
            expected_columns=["Subtract Days Trade Date"],
            expected_result=np.array([[np.datetime64("2024-03-10", "us")]], dtype=object),
        ),
        TestExpectation(
            name="add_months_one",
            query=lambda f: f.find_all(None, None, [f.trade_date().add_months(1)], f.id_().eq(1)),
            expected_columns=["Add Months Trade Date"],
            expected_result=np.array([[np.datetime64("2024-04-15", "us")]], dtype=object),
        ),
        TestExpectation(
            name="add_years_one",
            query=lambda f: f.find_all(None, None, [f.trade_date().add_years(1)], f.id_().eq(1)),
            expected_columns=["Add Years Trade Date"],
            expected_result=np.array([[np.datetime64("2025-03-15", "us")]], dtype=object),
        ),
        # --- Date diff ---
        TestExpectation(
            name="diff_days",
            query=lambda f: f.find_all(
                None, None,
                [f.trade_date().diff_days(datetime.date(2024, 3, 25))],
                f.id_().eq(1),
            ),
            expected_columns=["Diff Days Trade Date"],
            expected_result=np.array([[10]], dtype=object),
        ),
        TestExpectation(
            name="diff_months",
            query=lambda f: f.find_all(
                None, None,
                [f.trade_date().diff_months(datetime.date(2024, 6, 15))],
                f.id_().eq(1),
            ),
            expected_columns=["Diff Months Trade Date"],
            expected_result=np.array([[3]], dtype=object),
        ),
        TestExpectation(
            name="diff_years",
            query=lambda f: f.find_all(
                None, None,
                [f.trade_date().diff_years(datetime.date(2026, 3, 15))],
                f.id_().eq(1),
            ),
            expected_columns=["Diff Years Trade Date"],
            expected_result=np.array([[2]], dtype=object),
        ),
    ],
)
