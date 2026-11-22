"""
Test specifications for a join with a condition besides its key columns (join_conditions_mapping.md): the condition is
ANDed into the ``ON`` of the left outer join, so a row it rejects leaves the joined columns NULL and drops no row.

    org.firms — id, legal_name, flag; org.persons — id, first_name, flag, firm_id; org.memberships — person_id, firm_id, active
"""

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

JOIN_CONDITIONS_MAPPING = "join_conditions_mapping.md"
JOIN_CONDITIONS_FIRM_FINDER_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file=JOIN_CONDITIONS_MAPPING,
    expectations=[
        TestExpectation(
            name="a_join_condition_of_is_null_and_is_not_null",
            # Beta's flag is not NULL, so it has no employees; John (1) has a NULL flag, so he is not an employee of Alpha
            query=lambda f: f.find_all([f.legal_name(), f.employees().first_name()])
                             .order_by(f.legal_name().ascending(), f.employees().first_name().ascending()),
        ),
        TestExpectation(
            name="a_filter_on_a_navigation_is_anded_with_the_condition_of_its_join",
            # Mary is an employee of Alpha; Beta has none, and Gamma has Bob and John but no Mary
            query=lambda f: f.find_all([f.legal_name(), f.employees().filter(lambda each: each.first_name().eq("Mary")).first_name()])
                             .order_by(f.legal_name().ascending()),
        ),
        TestExpectation(
            name="a_filter_on_a_property_reached_through_a_join_with_a_condition",
            query=lambda f: f.find_all([f.legal_name()], f.employees().first_name().eq("Bob")),
        ),
    ],
)

JOIN_CONDITIONS_PERSON_FINDER_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file=JOIN_CONDITIONS_MAPPING,
    expectations=[
        TestExpectation(
            name="the_same_condition_reads_the_same_from_the_other_end",
            # Ann's firm (Beta) has a flag, John (2) has none himself, so neither has a firm
            query=lambda f: f.find_all([f.id_(), f.firm().legal_name()]).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="a_condition_with_an_or_group_and_string_literals",
            # only a Peter or a John has the firm: Mary, Ann and Bob do not
            query=lambda f: f.find_all([f.id_(), f.firm_if_peter_or_john().legal_name()]).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="a_condition_on_one_hop_of_a_property_mapped_through_another_table",
            # Peter is a member of Alpha (active) and Beta (not): the condition is on the hop to the firm, so his membership
            # of Beta is still a row, with no firm. Ann is a member of Beta (active).
            query=lambda f: f.find_all([f.id_(), f.active_firms().legal_name()])
                             .order_by(f.id_().ascending(), f.active_firms().legal_name().ascending()),
        ),
    ],
)

JOIN_CONDITIONS_EMPLOYEE_FINDER_SPECS = FinderSpec(
    finder_name="EmployeeFinder",
    mapping_file=JOIN_CONDITIONS_MAPPING,
    expectations=[
        TestExpectation(
            name="a_condition_on_a_table_joined_to_itself_says_which_side_each_column_is_on",
            # Cy is not active, and neither is Di's manager Ed: either side's condition drops the manager, not the employee
            query=lambda f: f.find_all([f.id_(), f.manager().name()]).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="the_join_followed_from_the_other_end_swaps_the_sides",
            # an employee's reports are those that are active, under an active manager: Ann has Bob, Bob has Flo,
            # and Ed, who is not active, has none
            query=lambda f: f.find_all([f.id_(), f.reports().name()])
                             .order_by(f.id_().ascending(), f.reports().name().ascending()),
        ),
    ],
)
