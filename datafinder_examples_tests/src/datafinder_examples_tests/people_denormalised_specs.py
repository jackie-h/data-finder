"""
Test specifications for embedded properties that have no table behind them, using people_denormalised_mapping.md.
A person's row holds their home address, their firm and the firm's address, so reading them needs no join.
"""

from datafinder import when
from datafinder_examples_tests.spec import FinderSpec, TestExpectation

PEOPLE_DENORMALISED_MAPPING = "people_denormalised_mapping.md"

PERSON_DENORMALISED_FINDER_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file=PEOPLE_DENORMALISED_MAPPING,
    expectations=[
        TestExpectation(
            name="embedded_properties_are_read_from_the_row",
            query=lambda f: f.find_all(
                [f.first_name(), f.address().name(), f.firm().legal_name(), f.firm().address().name()],
            ),
        ),
        TestExpectation(
            name="filter_on_an_embedded_property",
            query=lambda f: f.find_all(
                [f.first_name(), f.firm().address().name()], f.firm().legal_name().eq("Firm A"),
            ),
        ),
        TestExpectation(
            name="filter_on_a_nested_embedded_property",
            query=lambda f: f.find_all(
                [f.first_name()], f.firm().address().name().ends_with("west"),
            ),
        ),
        TestExpectation(
            name="group_by_an_embedded_property",
            query=lambda f: f.find_all(
                [f.firm().legal_name(), when(f.address().name().starts_with("1")).then(1).otherwise(0).sum()],
            ).group_by(f.firm().legal_name()).order_by(f.firm().legal_name().ascending()),
        ),
    ],
)
