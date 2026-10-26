"""
Test specifications for embedded properties that have no table behind them, using people_denormalised_mapping.md.
A person's row holds their home address, their firm and the firm's address, so reading them needs no join.
"""
import numpy as np

from datafinder import when
from datafinder_examples_tests.spec import FinderSpec, TestExpectation

PEOPLE_DENORMALISED_MAPPING = "people_denormalised_mapping.md"

_ALL = [
    ["Peter", "1 the street", "Firm X", "200 west"],
    ["John", "5 Park Ave", "Firm X", "200 west"],
    ["Fabrice", "7 Palo Alto", "Firm A", "3 somewhere"],
]

PERSON_DENORMALISED_FINDER_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file=PEOPLE_DENORMALISED_MAPPING,
    expectations=[
        TestExpectation(
            name="embedded_properties_are_read_from_the_row",
            query=lambda f: f.find_all(
                None, None, [f.first_name(), f.address().name(), f.firm().legal_name(), f.firm().address().name()],
            ),
            expected_columns=["FirstName", "Address Name", "Firm LegalName", "Firm Address Name"],
            expected_result=np.array(_ALL, dtype=object),
            unexpected_sql={"duckdb": "JOIN"},
        ),
        TestExpectation(
            name="filter_on_an_embedded_property",
            query=lambda f: f.find_all(
                None, None, [f.first_name(), f.firm().address().name()], f.firm().legal_name().eq("Firm A"),
            ),
            expected_columns=["FirstName", "Firm Address Name"],
            expected_result=np.array([["Fabrice", "3 somewhere"]], dtype=object),
            unexpected_sql={"duckdb": "JOIN"},
        ),
        TestExpectation(
            name="filter_on_a_nested_embedded_property",
            query=lambda f: f.find_all(
                None, None, [f.first_name()], f.firm().address().name().ends_with("west"),
            ),
            expected_columns=["FirstName"],
            expected_result=np.array([["Peter"], ["John"]], dtype=object),
        ),
        TestExpectation(
            name="group_by_an_embedded_property",
            query=lambda f: f.find_all(
                None, None,
                [f.firm().legal_name(), when(f.address().name().starts_with("1")).then(1).otherwise(0).sum()],
            ).group_by(f.firm().legal_name()).order_by(f.firm().legal_name().ascending()),
            expected_columns=["Firm LegalName", "Sum Expression"],
            expected_result=np.array([["Firm A", 0], ["Firm X", 1]], dtype=object),
        ),
    ],
)
