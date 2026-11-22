"""
Test specifications for NULL-ended milestoning (null_end_milestoning_mapping.md).

Rows with out_z IS NULL are treated as currently active (open-ended).

    mkt.prices — sym VARCHAR, price DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP
"""
import datetime


from datafinder_examples_tests.spec import FinderSpec, TestExpectation

NULL_END_MILESTONING_MAPPING = "null_end_milestoning_mapping.md"

_AT_2024 = datetime.datetime(2024, 6, 1, 12, 0, 0)
_AT_2021 = datetime.datetime(2021, 6, 1, 12, 0, 0)

NULL_END_PRICE_FINDER_SPECS = FinderSpec(
    finder_name="PriceFinder",
    mapping_file=NULL_END_MILESTONING_MAPPING,
    expectations=[
        TestExpectation(
            name="null_end_rows_active_at_2024",
            query=lambda f: f.find_all(None, _AT_2024, [f.symbol()]).order_by(f.symbol().ascending()),
        ),
        TestExpectation(
            name="expired_row_excluded_at_2024",
            query=lambda f: f.find_all(None, _AT_2024, [f.symbol()], f.symbol().eq("GOOG")),
        ),
        TestExpectation(
            name="all_three_active_before_expiry",
            query=lambda f: f.find_all(None, _AT_2021, [f.symbol()]).order_by(f.symbol().ascending()),
        ),
        TestExpectation(
            name="aapl_active_at_future_date",
            query=lambda f: f.find_all(
                None, datetime.datetime(2099, 1, 1),
                [f.symbol()], f.symbol().eq("AAPL"),
            ),
        ),
    ],
)
