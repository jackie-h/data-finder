"""
Test specifications for the finance domain (finance_mapping.md).

Backends must seed their database with ACCOUNTS, PRICES, TRADES, and POSITIONS
before running these specs.  Column ordering matches the tables in finance_mapping.md:

    ref_data.account_master  — ID INT, ACCT_NAME VARCHAR
    ref_data.price           — SYM VARCHAR, PRICE DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP
    trading.trades           — sym VARCHAR, price DOUBLE, is_settled BOOLEAN,
                               account_id INT, in_z TIMESTAMP, out_z TIMESTAMP
    trading.contractualposition — DATE DATE, QUANTITY DOUBLE, NPV DOUBLE,
                                  in_z TIMESTAMP, out_z TIMESTAMP
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

FINANCE_MAPPING = "finance_mapping.md"

ACCOUNTS = [
    (1, "Acme Corp"),
]

PRICES = [
    ("AAPL", 150.0, "2020-01-01 00:00:00", "9999-12-31 23:59:59"),
    ("GOOG", 2800.0, "2020-01-01 00:00:00", "2022-01-01 00:00:00"),
]

TRADES = [
    ("AAPL", 84.11, True, 1, "2020-01-01 00:00:00", "9999-12-31 23:59:59"),
    ("GOOG", 200.0, False, 1, "2020-01-01 00:00:00", "2022-01-01 00:00:00"),
]

POSITIONS = [
    ("2023-01-15", 100.0, 500.0, "2023-01-15 00:00:00", "9999-12-31 23:59:59"),
    ("2023-01-15", 90.0, 450.0, "2023-01-10 00:00:00", "2023-01-15 00:00:00"),  # superseded
    ("2023-01-16", 200.0, 1000.0, "2023-01-16 00:00:00", "9999-12-31 23:59:59"),
]

_AT = "2023-12-01 12:00:00"

ACCOUNT_FINDER_SPECS = FinderSpec(
    finder_name="AccountFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="account_by_id",
            query=lambda f: f.find_all(None, None, [f.id_(), f.name()], f.id_().eq(1)),
            expected_columns=["Id", "Name"],
            expected_result=np.array([[1, "Acme Corp"]], dtype=object),
        ),
    ],
)

TRADE_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="aapl_with_account",
            query=lambda f: f.find_all(
                None, _AT,
                [f.account().name(), f.symbol(), f.price()],
                f.symbol().eq("AAPL"),
            ),
            expected_columns=["Account Name", "Symbol", "Price"],
            expected_result=np.array([["Acme Corp", "AAPL", 84.11]], dtype=object),
        ),
        TestExpectation(
            name="goog_expired",
            query=lambda f: f.find_all(None, _AT, [f.symbol()], f.symbol().eq("GOOG")),
            expected_columns=["Symbol"],
            expected_result=np.empty((0, 1), dtype=object),
        ),
        TestExpectation(
            name="price_sum_single_active_trade",
            query=lambda f: f.find_all(None, _AT, [f.price().sum()]),
            expected_columns=["Price"],
            expected_result=np.array([[84.11]], dtype=object),
        ),
        TestExpectation(
            name="both_trades_active_in_2021",
            query=lambda f: f.find_all(
                None, "2021-06-01 12:00:00", [f.symbol()]
            ).order_by(f.symbol().ascending()),
            expected_columns=["Symbol"],
            expected_result=np.array([["AAPL"], ["GOOG"]], dtype=object),
        ),
    ],
)

CONTRACTUAL_POSITION_FINDER_SPECS = FinderSpec(
    finder_name="ContractualPositionFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="position_jan15_current",
            query=lambda f: f.find_all("2023-01-15", _AT, [f.quantity(), f.npv()]),
            expected_columns=["Quantity", "Npv"],
            expected_result=np.array([[100.0, 500.0]], dtype=object),
        ),
        TestExpectation(
            name="position_jan16_current",
            query=lambda f: f.find_all("2023-01-16", _AT, [f.quantity(), f.npv()]),
            expected_columns=["Quantity", "Npv"],
            expected_result=np.array([[200.0, 1000.0]], dtype=object),
        ),
        TestExpectation(
            name="position_jan15_before_update",
            query=lambda f: f.find_all("2023-01-15", "2023-01-12 12:00:00", [f.quantity()]),
            expected_columns=["Quantity"],
            expected_result=np.array([[90.0]], dtype=object),
        ),
        TestExpectation(
            name="range_jan15_jan16",
            query=lambda f: f.find_for_date_range(
                "2023-01-15", "2023-01-16", _AT, [f.quantity()]
            ).order_by(f.quantity().ascending()),
            expected_columns=["Quantity"],
            expected_result=np.array([[100.0], [200.0]], dtype=object),
        ),
        TestExpectation(
            name="range_jan15_only",
            query=lambda f: f.find_for_date_range(
                "2023-01-15", "2023-01-15", _AT, [f.quantity()]
            ),
            expected_columns=["Quantity"],
            expected_result=np.array([[100.0]], dtype=object),
        ),
        TestExpectation(
            name="range_before_data",
            query=lambda f: f.find_for_date_range(
                "2023-01-01", "2023-01-14", _AT, [f.quantity()]
            ),
            expected_columns=["Quantity"],
            expected_result=np.empty((0, 1), dtype=object),
        ),
    ],
)
