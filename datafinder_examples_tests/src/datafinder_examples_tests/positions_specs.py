"""
Test specifications for date functions using positions_mapping.md.

    positions — id INT, trade_date DATE, npv DOUBLE

Data rows:
    id=1, trade_date=2024-03-15, npv=500.0
    id=2, trade_date=2024-07-20, npv=800.0
    id=3, trade_date=2024-11-05, npv=1200.0
"""
import datetime

import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

POSITIONS_MAPPING = "positions_mapping.md"

_D1 = datetime.date(2024, 3, 15)
_D2 = datetime.date(2024, 7, 20)
_D3 = datetime.date(2024, 11, 5)

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
            name="subtract_months_one",
            query=lambda f: f.find_all(None, None, [f.trade_date().subtract_months(1)], f.id_().eq(1)),
            expected_columns=["Subtract Months Trade Date"],
            expected_result=np.array([[np.datetime64("2024-02-15", "us")]], dtype=object),
        ),
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
        # --- DateAttribute extra operations ---
        TestExpectation(
            name="day_of_week",
            query=lambda f: f.find_all(None, None, [f.trade_date().day_of_week()], f.id_().eq(1)),
            expected_columns=["Day Of Week Trade Date"],
            # 2024-03-15 is Friday; DuckDB DOW: 0=Sunday → Friday=5
            expected_result=np.array([[5]], dtype=object),
        ),
        TestExpectation(
            name="subtract_years_one",
            query=lambda f: f.find_all(None, None, [f.trade_date().subtract_years(1)], f.id_().eq(1)),
            expected_columns=["Subtract Years Trade Date"],
            expected_result=np.array([[np.datetime64("2023-03-15", "us")]], dtype=object),
        ),
        TestExpectation(
            name="add_timedelta_days",
            query=lambda f: f.find_all(None, None, [f.trade_date() + datetime.timedelta(days=5)], f.id_().eq(1)),
            expected_columns=["Add Days Trade Date"],
            expected_result=np.array([[np.datetime64("2024-03-20", "us")]], dtype=object),
        ),
        TestExpectation(
            name="sub_timedelta_days",
            query=lambda f: f.find_all(None, None, [f.trade_date() - datetime.timedelta(days=5)], f.id_().eq(1)),
            expected_columns=["Subtract Days Trade Date"],
            expected_result=np.array([[np.datetime64("2024-03-10", "us")]], dtype=object),
        ),
        # --- DateAttribute comparisons ---
        TestExpectation(
            name="trade_date_eq",
            query=lambda f: f.find_all(None, None, [f.id_()], f.trade_date().eq(_D1)),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="trade_date_gt",
            query=lambda f: f.find_all(None, None, [f.id_()], f.trade_date() > _D2).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[3]], dtype=object),
        ),
        TestExpectation(
            name="trade_date_lt",
            query=lambda f: f.find_all(None, None, [f.id_()], f.trade_date() < _D2),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="trade_date_ge",
            query=lambda f: f.find_all(None, None, [f.id_()], f.trade_date() >= _D2).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3]], dtype=object),
        ),
        TestExpectation(
            name="trade_date_le",
            query=lambda f: f.find_all(None, None, [f.id_()], f.trade_date() <= _D2).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2]], dtype=object),
        ),
        # --- DoubleAttribute comparisons ---
        TestExpectation(
            name="npv_eq",
            query=lambda f: f.find_all(None, None, [f.id_()], f.npv().eq(500.0)),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="npv_gt",
            query=lambda f: f.find_all(None, None, [f.id_()], f.npv() > 500.0).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3]], dtype=object),
        ),
        TestExpectation(
            name="npv_lt",
            query=lambda f: f.find_all(None, None, [f.id_()], f.npv() < 800.0),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="npv_ge",
            query=lambda f: f.find_all(None, None, [f.id_()], f.npv() >= 800.0).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3]], dtype=object),
        ),
        TestExpectation(
            name="npv_le",
            query=lambda f: f.find_all(None, None, [f.id_()], f.npv() <= 800.0).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2]], dtype=object),
        ),
        # --- NumericAttribute aggregates ---
        TestExpectation(
            name="npv_min",
            query=lambda f: f.find_all(None, None, [f.npv().min()]),
            expected_columns=["Min Npv"],
            expected_result=np.array([[500.0]], dtype=object),
        ),
        TestExpectation(
            name="npv_max",
            query=lambda f: f.find_all(None, None, [f.npv().max()]),
            expected_columns=["Max Npv"],
            expected_result=np.array([[1200.0]], dtype=object),
        ),
        TestExpectation(
            name="npv_average",
            query=lambda f: f.find_all(None, None, [f.npv().average()]),
            expected_columns=["Average Npv"],
            expected_result=np.array([[2500.0 / 3]], dtype=object),
        ),
        # --- DoubleAttribute.__eq__ dunder (vs .eq() method) ---
        TestExpectation(
            name="npv_eq_operator",
            query=lambda f: f.find_all(None, None, [f.id_()], f.npv() == 500.0),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        # --- IntegerAttribute comparisons ---
        TestExpectation(
            name="id_eq_operator",
            query=lambda f: f.find_all(None, None, [f.id_()], f.id_() == 1),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="id_gt",
            query=lambda f: f.find_all(None, None, [f.id_()], f.id_() > 1).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3]], dtype=object),
        ),
        TestExpectation(
            name="id_lt",
            query=lambda f: f.find_all(None, None, [f.id_()], f.id_() < 3),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2]], dtype=object),
        ),
        TestExpectation(
            name="id_ge",
            query=lambda f: f.find_all(None, None, [f.id_()], f.id_() >= 2).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3]], dtype=object),
        ),
        TestExpectation(
            name="id_le",
            query=lambda f: f.find_all(None, None, [f.id_()], f.id_() <= 2).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2]], dtype=object),
        ),
    ],
)
