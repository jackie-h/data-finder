"""
Test specifications for the embedded (join-less) mapping feature, using the dedicated
finance_mapping_embedded.md example (finance_mapping.md itself stays join-only). The `trades`
table carries denormalized columns for both a one-hop chain (`account.name`) and a two-hop
chain (`account.branch.city`).

Backends must seed trading.trades with `acct_name`/`branch_city` columns (see
finance_trades_embedded.csv) distinct from the real ref_data.account_master.ACCT_NAME and
ref_data.branch_master.CITY values, so a passing test proves which physical column was
actually read:

    ref_data.account_master.ACCT_NAME = "Acme Corp"        vs. trading.trades.acct_name   = "Acme Corp (denorm)"
    ref_data.branch_master.CITY       = "New York"         vs. trading.trades.branch_city = "NYC (denorm)"
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

FINANCE_MAPPING_EMBEDDED = "finance_mapping_embedded.md"

_AT = "2023-12-01 12:00:00"

EMBEDDED_TRADE_FINDER_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file=FINANCE_MAPPING_EMBEDDED,
    expectations=[
        TestExpectation(
            name="one_hop_embedded_only_elides_join",
            query=lambda f: f.find_all(
                None, _AT, [f.account().name(), f.symbol()], f.symbol().eq("AAPL"),
            ),
            expected_columns=["Account Name", "Symbol"],
            expected_result=np.array([["Acme Corp (denorm)", "AAPL"]], dtype=object),
            unexpected_sql={"duckdb": "JOIN"},
        ),
        TestExpectation(
            name="one_hop_mixed_selection_forces_real_join",
            query=lambda f: f.find_all(
                None, _AT, [f.account().name(), f.account().id_(), f.symbol()], f.symbol().eq("AAPL"),
            ),
            expected_columns=["Account Name", "Account Id", "Symbol"],
            # Real join: Account Name now comes from ref_data.account_master, not the flat column.
            expected_result=np.array([["Acme Corp", 1, "AAPL"]], dtype=object),
            expected_sql={"duckdb": "JOIN"},
        ),
        TestExpectation(
            name="two_hop_embedded_only_elides_both_joins",
            query=lambda f: f.find_all(
                None, _AT, [f.account().branch().city(), f.symbol()], f.symbol().eq("AAPL"),
            ),
            expected_columns=["Branch City", "Symbol"],
            expected_result=np.array([["NYC (denorm)", "AAPL"]], dtype=object),
            unexpected_sql={"duckdb": "JOIN"},
        ),
        TestExpectation(
            name="two_hop_mixed_selection_forces_both_real_joins",
            query=lambda f: f.find_all(
                None, _AT,
                [f.account().branch().city(), f.account().branch().id_(), f.symbol()],
                f.symbol().eq("AAPL"),
            ),
            expected_columns=["Branch City", "Branch Id", "Symbol"],
            # Real joins: trades -> account_master -> branch_master.
            expected_result=np.array([["New York", 10, "AAPL"]], dtype=object),
            expected_sql={"duckdb": "JOIN"},
        ),
    ],
)
