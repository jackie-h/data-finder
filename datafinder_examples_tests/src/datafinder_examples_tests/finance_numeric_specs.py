"""
Test specifications for numeric functions using finance_mapping.md.

Uses a different trade dataset from finance_specs.py — values chosen to exercise
numeric scalar functions and arithmetic.

    ref_data.account_master — ID INT, ACCT_NAME VARCHAR
    ref_data.price          — SYM VARCHAR, PRICE DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP
    trading.trades          — sym VARCHAR, price DOUBLE, is_settled BOOLEAN,
                              account_id INT, in_z TIMESTAMP, out_z TIMESTAMP
"""
import math

import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

FINANCE_MAPPING = "finance_mapping.md"

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
            name="trigonometric_and_exponential_functions",
            query=lambda f: f.find_all(None, _AT, [
                f.price().sin(), f.price().cos(), f.price().tan(),
                f.price().mod(1).asin(), f.price().mod(1).acos(),
                f.price().atan(), f.price().atan2(2), f.price().exp(), f.price().log(),
            ], f.symbol().eq("TSLA")),
            expected_columns=["Sin Price", "Cos Price", "Tan Price", "Asin Mod Price", "Acos Mod Price",
                              "Atan Price", "Atan2 Price", "Exp Price", "Log Price"],
            expected_result=np.array([[
                math.sin(9.0), math.cos(9.0), math.tan(9.0), math.asin(0.0), math.acos(0.0),
                math.atan(9.0), math.atan2(9.0, 2), math.exp(9.0), math.log(9.0),
            ]], dtype=object),
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
        # --- Operator overloads (separate from the explicit .mod() / .power() methods) ---
        TestExpectation(
            name="mod_operator",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price() % 10], f.symbol().eq("GOOG")),
            expected_columns=["Symbol", "Mod Price"],
            expected_result=np.array([["GOOG", 36.0 % 10]], dtype=object),
        ),
        TestExpectation(
            name="pow_operator",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price() ** 2], f.symbol().eq("TSLA")),
            expected_columns=["Symbol", "Power Price"],
            expected_result=np.array([["TSLA", 9.0 ** 2]], dtype=object),
        ),
    ],
)


# Numeric expressions combine with + - * / and a number or another numeric expression. GOOG's price is 36.0 and
# AAPL's is -25.5. The GraphQL finder cannot compute arithmetic.
TRADE_ARITHMETIC_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    skip_backends={"graphql": "The GraphQL finder has no arithmetic between columns"},
    expectations=[
        TestExpectation(
            name="multiply_by_a_number",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price() * 2], f.symbol().eq("GOOG")),
            expected_columns=["Symbol", "Expression"],
            expected_result=np.array([["GOOG", 72.0]], dtype=object),
            expected_sql={"duckdb": "t0.price * 2"},
        ),
        TestExpectation(
            name="add_a_number",
            query=lambda f: f.find_all(None, _AT, [f.price() + 1.5], f.symbol().eq("GOOG")),
            expected_columns=["Expression"],
            expected_result=np.array([[37.5]], dtype=object),
        ),
        TestExpectation(
            name="a_number_minus_a_column",
            query=lambda f: f.find_all(None, _AT, [100 - f.price()], f.symbol().eq("GOOG")),
            expected_columns=["Expression"],
            expected_result=np.array([[64.0]], dtype=object),
            expected_sql={"duckdb": "100 - t0.price"},
        ),
        TestExpectation(
            name="divide_by_a_number",
            query=lambda f: f.find_all(None, _AT, [f.price() / 4], f.symbol().eq("GOOG")),
            expected_columns=["Expression"],
            expected_result=np.array([[9.0]], dtype=object),
        ),
        TestExpectation(
            name="a_number_divided_by_a_column",
            query=lambda f: f.find_all(None, _AT, [72 / f.price()], f.symbol().eq("GOOG")),
            expected_columns=["Expression"],
            expected_result=np.array([[2.0]], dtype=object),
            expected_sql={"duckdb": "72 / t0.price"},
        ),
        TestExpectation(
            name="one_column_times_another",
            query=lambda f: f.find_all(None, _AT, [f.price() * f.account().id_()], f.symbol().eq("GOOG")),
            expected_columns=["Expression"],
            expected_result=np.array([[36.0]], dtype=object),
        ),
        TestExpectation(
            name="parentheses_where_the_operators_need_them",
            query=lambda f: f.find_all(None, _AT, [(f.price() + 1) * 2], f.symbol().eq("GOOG")),
            expected_columns=["Expression"],
            expected_result=np.array([[74.0]], dtype=object),
            expected_sql={"duckdb": "(t0.price + 1) * 2"},
        ),
        TestExpectation(
            name="no_parentheses_where_they_are_not_needed",
            query=lambda f: f.find_all(None, _AT, [f.price() + 1 * 2], f.symbol().eq("GOOG")),
            expected_columns=["Expression"],
            expected_result=np.array([[38.0]], dtype=object),
        ),
        TestExpectation(
            name="the_right_side_of_a_subtraction_is_parenthesised",
            query=lambda f: f.find_all(None, _AT, [f.price() - (f.price() - 2)], f.symbol().eq("GOOG")),
            expected_columns=["Expression"],
            expected_result=np.array([[2.0]], dtype=object),
            expected_sql={"duckdb": "t0.price - (t0.price - 2)"},
        ),
        TestExpectation(
            name="arithmetic_on_a_function_of_a_column",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().abs() * 2], f.symbol().eq("AAPL")),
            expected_columns=["Symbol", "Expression"],
            expected_result=np.array([["AAPL", 51.0]], dtype=object),
        ),
        TestExpectation(
            name="numeric_functions_chain",
            query=lambda f: f.find_all(None, _AT, [f.price().sqrt().floor()], f.symbol().eq("GOOG")),
            expected_columns=["Floor Sqrt Price"],
            expected_result=np.array([[6.0]], dtype=object),
            expected_sql={"duckdb": "FLOOR(SQRT(t0.price))"},
        ),
        TestExpectation(
            name="numeric_functions_chain_with_an_argument",
            query=lambda f: f.find_all(None, _AT, [f.price().sqrt().power(2)], f.symbol().eq("GOOG")),
            expected_columns=["Power Sqrt Price"],
            expected_result=np.array([[36.0]], dtype=object),
        ),
        TestExpectation(
            name="text_to_number_to_arithmetic",
            query=lambda f: f.find_all(None, _AT, [f.symbol().lower().length() + 1], f.symbol().eq("GOOG")),
            expected_columns=["Expression"],
            expected_result=np.array([[5]], dtype=object),
        ),
        TestExpectation(
            name="arithmetic_on_the_length_of_a_string",
            query=lambda f: f.find_all(None, _AT, [f.symbol().length() * 10], f.symbol().eq("GOOG")),
            expected_columns=["Expression"],
            expected_result=np.array([[40]], dtype=object),
        ),
    ],
)
