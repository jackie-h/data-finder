"""
Test specifications for the embedded (join-less) mapping feature on finance_mapping.md's
Trade.account association: the `trades` table carries a denormalized `acct_name` column
mapped to the chained property `account.name` (see the trailing row in finance_mapping.md's
`trades` Table section).

Backends must seed trading.trades with an `acct_name` column (see
finance_trades_embedded.csv) distinct from ref_data.account_master.ACCT_NAME, so a
passing test proves which physical column was actually read:

    ref_data.account_master.ACCT_NAME = "Acme Corp"
    trading.trades.acct_name          = "Acme Corp (denorm)"
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

FINANCE_MAPPING = "finance_mapping.md"

_AT = "2023-12-01 12:00:00"

EMBEDDED_TRADE_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="embedded_only_elides_join",
            query=lambda f: f.find_all(
                None, _AT, [f.account().name(), f.symbol()], f.symbol().eq("AAPL"),
            ),
            expected_columns=["Account Name", "Symbol"],
            expected_result=np.array([["Acme Corp (denorm)", "AAPL"]], dtype=object),
            unexpected_sql={"duckdb": "JOIN"},
        ),
        TestExpectation(
            name="mixed_selection_forces_real_join",
            query=lambda f: f.find_all(
                None, _AT, [f.account().name(), f.account().id_(), f.symbol()], f.symbol().eq("AAPL"),
            ),
            expected_columns=["Account Name", "Account Id", "Symbol"],
            # Real join: Account Name now comes from ref_data.account_master, not the flat column.
            expected_result=np.array([["Acme Corp", 1, "AAPL"]], dtype=object),
            expected_sql={"duckdb": "JOIN"},
        ),
    ],
)
