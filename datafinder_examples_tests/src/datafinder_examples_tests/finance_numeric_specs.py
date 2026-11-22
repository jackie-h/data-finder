"""
Test specifications for numeric functions using finance_mapping.md.

Uses a different trade dataset from finance_specs.py — values chosen to exercise
numeric scalar functions and arithmetic.

    ref_data.account_master — ID INT, ACCT_NAME VARCHAR
    ref_data.price          — SYM VARCHAR, PRICE DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP
    trading.trades          — sym VARCHAR, price DOUBLE, is_settled BOOLEAN,
                              account_id INT, in_z TIMESTAMP, out_z TIMESTAMP
"""


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
        ),
        TestExpectation(
            name="ceil_price",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().ceil()], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="floor_price",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().floor()], f.symbol().eq("MSFT")),
        ),
        TestExpectation(
            name="sqrt_price",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().sqrt()], f.symbol().eq("TSLA")),
        ),
        TestExpectation(
            name="trigonometric_and_exponential_functions",
            query=lambda f: f.find_all(None, _AT, [
                f.price().sin(), f.price().cos(), f.price().tan(),
                f.price().mod(1).asin(), f.price().mod(1).acos(),
                f.price().atan(), f.price().atan2(2), f.price().exp(), f.price().log(),
            ], f.symbol().eq("TSLA")),
        ),
        TestExpectation(
            name="mod_price_by_10",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().mod(10)], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="power_price_squared",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().power(2)], f.symbol().eq("TSLA")),
        ),
        TestExpectation(
            name="round_price_no_decimals",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().round()], f.symbol().eq("MSFT")),
        ),
        TestExpectation(
            name="round_price_one_decimal",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().round(1)], f.symbol().eq("AAPL")),
        ),
        # --- Operator overloads (separate from the explicit .mod() / .power() methods) ---
        TestExpectation(
            name="mod_operator",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price() % 10], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="pow_operator",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price() ** 2], f.symbol().eq("TSLA")),
        ),
    ],
)


# Numeric expressions combine with + - * / and a number or another numeric expression. GOOG's price is 36.0 and
# AAPL's is -25.5. The GraphQL finder cannot compute arithmetic.
TRADE_ARITHMETIC_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="multiply_by_a_number",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price() * 2], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="add_a_number",
            query=lambda f: f.find_all(None, _AT, [f.price() + 1.5], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="a_number_minus_a_column",
            query=lambda f: f.find_all(None, _AT, [100 - f.price()], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="divide_by_a_number",
            query=lambda f: f.find_all(None, _AT, [f.price() / 4], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="a_number_divided_by_a_column",
            query=lambda f: f.find_all(None, _AT, [72 / f.price()], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="one_column_times_another",
            query=lambda f: f.find_all(None, _AT, [f.price() * f.account().id_()], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="parentheses_where_the_operators_need_them",
            query=lambda f: f.find_all(None, _AT, [(f.price() + 1) * 2], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="no_parentheses_where_they_are_not_needed",
            query=lambda f: f.find_all(None, _AT, [f.price() + 1 * 2], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="the_right_side_of_a_subtraction_is_parenthesised",
            query=lambda f: f.find_all(None, _AT, [f.price() - (f.price() - 2)], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="arithmetic_on_a_function_of_a_column",
            query=lambda f: f.find_all(None, _AT, [f.symbol(), f.price().abs() * 2], f.symbol().eq("AAPL")),
        ),
        TestExpectation(
            name="numeric_functions_chain",
            query=lambda f: f.find_all(None, _AT, [f.price().sqrt().floor()], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="numeric_functions_chain_with_an_argument",
            query=lambda f: f.find_all(None, _AT, [f.price().sqrt().power(2)], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="text_to_number_to_arithmetic",
            query=lambda f: f.find_all(None, _AT, [f.symbol().lower().length() + 1], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="arithmetic_on_the_length_of_a_string",
            query=lambda f: f.find_all(None, _AT, [f.symbol().length() * 10], f.symbol().eq("GOOG")),
        ),
    ],
)
