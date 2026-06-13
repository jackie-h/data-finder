"""
Test specifications for numeric functions using finance_mapping.md.

Uses a different trade dataset from finance_specs.py — values chosen to exercise
abs, ceil, floor, sqrt, mod, power, and round.

    ref_data.account_master — ID INT, ACCT_NAME VARCHAR
    ref_data.price          — SYM VARCHAR, PRICE DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP
    trading.trades          — sym VARCHAR, price DOUBLE, is_settled BOOLEAN,
                              account_id INT, in_z TIMESTAMP, out_z TIMESTAMP
"""
import math

import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

FINANCE_MAPPING = "finance_mapping.md"

ACCOUNTS_NUMERIC = [(1, "Alpha Fund")]

PRICES_NUMERIC: list = []  # no price rows needed for these specs

TRADES_NUMERIC = [
    ("AAPL", -25.5, True,  1, "2020-01-01 00:00:00", "9999-12-31 23:59:59"),
    ("GOOG",  36.0, True,  1, "2020-01-01 00:00:00", "9999-12-31 23:59:59"),
    ("MSFT", 100.4, True,  1, "2020-01-01 00:00:00", "9999-12-31 23:59:59"),
    ("TSLA",   9.0, False, 1, "2020-01-01 00:00:00", "9999-12-31 23:59:59"),
]

_AT = "2030-01-01 00:00:00"

TRADE_NUMERIC_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="abs_negative_price",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().abs()], f.symbol().eq("AAPL")),
            expected_columns=["Symbol", "Abs Price"],
            expected_result=np.array([["AAPL", abs(-25.5)]], dtype=object),
        ),
        TestExpectation(
            name="ceil_price",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().ceil()], f.symbol().eq("GOOG")),
            expected_columns=["Symbol", "Ceil Price"],
            expected_result=np.array([["GOOG", math.ceil(36.0)]], dtype=object),
        ),
        TestExpectation(
            name="floor_price",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().floor()], f.symbol().eq("MSFT")),
            expected_columns=["Symbol", "Floor Price"],
            expected_result=np.array([["MSFT", math.floor(100.4)]], dtype=object),
        ),
        TestExpectation(
            name="sqrt_price",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().sqrt()], f.symbol().eq("TSLA")),
            expected_columns=["Symbol", "Sqrt Price"],
            expected_result=np.array([["TSLA", math.sqrt(9.0)]], dtype=object),
        ),
        TestExpectation(
            name="mod_price_by_10",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().mod(10)], f.symbol().eq("GOOG")),
            expected_columns=["Symbol", "Mod Price"],
            expected_result=np.array([["GOOG", 36.0 % 10]], dtype=object),
        ),
        TestExpectation(
            name="power_price_squared",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().power(2)], f.symbol().eq("TSLA")),
            expected_columns=["Symbol", "Power Price"],
            expected_result=np.array([["TSLA", 9.0 ** 2]], dtype=object),
        ),
        TestExpectation(
            name="round_price_no_decimals",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().round()], f.symbol().eq("MSFT")),
            expected_columns=["Symbol", "Round Price"],
            expected_result=np.array([["MSFT", round(100.4)]], dtype=object),
        ),
        TestExpectation(
            name="round_price_one_decimal",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().round(1)], f.symbol().eq("AAPL")),
            expected_columns=["Symbol", "Round Price"],
            expected_result=np.array([["AAPL", round(-25.5, 1)]], dtype=object),
        ),
    ],
)
