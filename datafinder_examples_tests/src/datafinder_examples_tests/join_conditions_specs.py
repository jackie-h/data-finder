"""
Test specifications for a join with a condition besides its key columns (join_conditions_mapping.md): the condition is
ANDed into the ``ON`` of the left outer join, so a row it rejects leaves the joined columns NULL and drops no row.

    org.firms — id, legal_name, flag; org.persons — id, first_name, flag, firm_id; org.memberships — person_id, firm_id, active
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

JOIN_CONDITIONS_MAPPING = "join_conditions_mapping.md"
_NO_CONDITIONS = {"graphql": "The GraphQL finder reads no join conditions"}

JOIN_CONDITIONS_FIRM_FINDER_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file=JOIN_CONDITIONS_MAPPING,
    expectations=[
        TestExpectation(
            name="a_join_condition_of_is_null_and_is_not_null",
            skip_backends=_NO_CONDITIONS,
            query=lambda f: f.find_all([f.legal_name(), f.employees().first_name()])
                             .order_by(f.legal_name().ascending(), f.employees().first_name().ascending()),
            expected_columns=["LegalName", "Employees FirstName"],
            # Beta's flag is not NULL, so it has no employees; John (1) has a NULL flag, so he is not an employee of Alpha
            expected_result=np.array([
                ["Alpha", "Mary"], ["Alpha", "Peter"], ["Beta", None], ["Gamma", "Bob"], ["Gamma", "John"],
            ], dtype=object),
            expected_sql={"duckdb": "ON (t0.id = t1.firm_id AND t0.flag IS NULL AND t1.flag IS NOT NULL)"},
        ),
        TestExpectation(
            name="a_filter_on_a_property_reached_through_a_join_with_a_condition",
            skip_backends=_NO_CONDITIONS,
            query=lambda f: f.find_all([f.legal_name()], f.employees().first_name().eq("Bob")),
            expected_columns=["LegalName"],
            expected_result=np.array([["Gamma"]], dtype=object),
        ),
    ],
)

JOIN_CONDITIONS_PERSON_FINDER_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file=JOIN_CONDITIONS_MAPPING,
    expectations=[
        TestExpectation(
            name="the_same_condition_reads_the_same_from_the_other_end",
            skip_backends=_NO_CONDITIONS,
            query=lambda f: f.find_all([f.id_(), f.firm().legal_name()]).order_by(f.id_().ascending()),
            expected_columns=["Id", "Firm LegalName"],
            # Ann's firm (Beta) has a flag, John (2) has none himself, so neither has a firm
            expected_result=np.array([
                [1, "Alpha"], [2, None], [3, "Alpha"], [4, None], [5, "Gamma"], [6, "Gamma"],
            ], dtype=object),
            expected_sql={"duckdb": "ON (t0.firm_id = t1.id AND t1.flag IS NULL AND t0.flag IS NOT NULL)"},
        ),
        TestExpectation(
            name="a_condition_with_an_or_group_and_string_literals",
            skip_backends=_NO_CONDITIONS,
            query=lambda f: f.find_all([f.id_(), f.firm_if_peter_or_john().legal_name()]).order_by(f.id_().ascending()),
            expected_columns=["Id", "Firmifpeterorjohn LegalName"],
            # only a Peter or a John has the firm: Mary, Ann and Bob do not
            expected_result=np.array([
                [1, "Alpha"], [2, "Alpha"], [3, None], [4, None], [5, None], [6, "Gamma"],
            ], dtype=object),
            expected_sql={"duckdb": "(t0.first_name = 'Peter' OR t0.first_name = 'John')"},
        ),
        TestExpectation(
            name="a_condition_on_one_hop_of_a_property_mapped_through_another_table",
            skip_backends=_NO_CONDITIONS,
            query=lambda f: f.find_all([f.id_(), f.active_firms().legal_name()])
                             .order_by(f.id_().ascending(), f.active_firms().legal_name().ascending()),
            expected_columns=["Id", "Activefirms LegalName"],
            # Peter is a member of Alpha (active) and Beta (not): the condition is on the hop to the firm, so his membership
            # of Beta is still a row, with no firm. Ann is a member of Beta (active).
            expected_result=np.array([
                [1, "Alpha"], [1, None], [2, None], [3, None], [4, "Beta"], [5, None], [6, None],
            ], dtype=object),
            expected_sql={"duckdb": "ON (t0.id = t2.person_id) LEFT OUTER JOIN org.firms AS t1 ON (t2.firm_id = t1.id AND t2.active = 1)"},
        ),
    ],
)
