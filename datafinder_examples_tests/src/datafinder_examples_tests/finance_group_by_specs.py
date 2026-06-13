"""
Test specifications for group_by aggregate queries using finance_mapping.md.

Uses a different dataset from finance_specs.py — multiple accounts and trades
to exercise group_by with average().

    ref_data.account_master — ID INT, ACCT_NAME VARCHAR
    ref_data.price          — SYM VARCHAR, PRICE DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP
    trading.trades          — sym VARCHAR, price DOUBLE, is_settled BOOLEAN,
                              account_id INT, in_z TIMESTAMP, out_z TIMESTAMP
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

FINANCE_MAPPING = "finance_mapping.md"

_AT = "2030-01-01 00:00:00"

TRADE_GROUP_BY_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="average_price_per_account",
            query=lambda f: f.find_all(
                None, _AT,
                [f.account().name(), f.price().average()],
            ).group_by(f.account().name()).order_by(f.account().name().ascending()),
            expected_columns=["Account Name", "Average Price"],
            expected_result=np.array(
                [["Alpha Fund", 150.0], ["Beta Fund", 350.0], ["Gamma Fund", 500.0]],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="average_price_settled_only",
            query=lambda f: f.find_all(
                None, _AT,
                [f.account().name(), f.price().average()],
                f.is_settled().is_true(),
            ).group_by(f.account().name()).order_by(f.account().name().ascending()),
            expected_columns=["Account Name", "Average Price"],
            expected_result=np.array(
                [["Alpha Fund", 150.0], ["Beta Fund", 300.0], ["Gamma Fund", 500.0]],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="group_count_per_account",
            query=lambda f: f.find_all(
                None, _AT, [f.account().name(), f.count()],
            ).group_by(f.account().name()).order_by(f.account().name().ascending()),
            expected_columns=["Account Name", "Count"],
            expected_result=np.array(
                [["Alpha Fund", 2], ["Beta Fund", 2], ["Gamma Fund", 1]],
                dtype=object,
            ),
        ),
    ],
)
