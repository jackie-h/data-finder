"""
Test specifications for the simple org chart domain (orgchart_mapping.md).

    hr.employees — id INT, name VARCHAR, manager_id INT
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

ORGCHART_MAPPING = "orgchart_mapping.md"

ORGCHART_FINDER_SPECS = FinderSpec(
    finder_name="EmployeeFinder",
    mapping_file=ORGCHART_MAPPING,
    expectations=[
        TestExpectation(
            name="all_employees",
            query=lambda f: f.find_all(None, None, [f.id_(), f.name()]).order_by(f.id_().ascending()),
            expected_columns=["Id", "Name"],
            expected_result=np.array([[1, "Alice"], [2, "Bob"], [3, "Carol"], [4, "Dave"]], dtype=object),
        ),
        TestExpectation(
            name="manager_name_for_bob",
            query=lambda f: f.find_all(None, None, [f.name(), f.manager().name()], f.name().eq("Bob")),
            expected_columns=["Name", "Manager Name"],
            expected_result=np.array([["Bob", "Alice"]], dtype=object),
        ),
        TestExpectation(
            name="alias_names_a_column_reached_through_an_association",
            query=lambda f: f.find_all(
                None, None, [f.name().alias("Employee"), f.manager().name().alias("Boss")], f.name().eq("Bob")
            ),
            expected_columns=["Employee", "Boss"],
            expected_result=np.array([["Bob", "Alice"]], dtype=object),
        ),
        TestExpectation(
            name="coalesce_replaces_a_missing_manager",
            query=lambda f: f.find_all(
                None, None, [f.name(), f.manager().name().coalesce("nobody")]
            ).order_by(f.id_().ascending()),
            expected_columns=["Name", "Coalesce Manager Name"],
            expected_result=np.array(
                [["Alice", "nobody"], ["Bob", "Alice"], ["Carol", "Alice"], ["Dave", "Bob"]], dtype=object),
            expected_sql={"duckdb": "COALESCE(t1.name, 'nobody')"},
        ),
        TestExpectation(
            name="coalesce_takes_the_first_default_in_order",
            query=lambda f: f.find_all(
                None, None, [f.manager().name().coalesce("first", "second")], f.name().eq("Alice")
            ),
            expected_columns=["Coalesce Manager Name"],
            expected_result=np.array([["first"]], dtype=object),
        ),
        TestExpectation(
            name="coalesce_leaves_a_value_that_is_there",
            query=lambda f: f.find_all(
                None, None, [f.name().coalesce("nobody")], f.name().eq("Bob")
            ),
            expected_columns=["Coalesce Name"],
            expected_result=np.array([["Bob"]], dtype=object),
        ),
        TestExpectation(
            name="manager_name_for_dave",
            query=lambda f: f.find_all(None, None, [f.name(), f.manager().name()], f.name().eq("Dave")),
            expected_columns=["Name", "Manager Name"],
            expected_result=np.array([["Dave", "Bob"]], dtype=object),
        ),
        TestExpectation(
            name="filter_by_manager_alice",
            query=lambda f: f.find_all(
                None, None, [f.name()], f.manager().name().eq("Alice")
            ).order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array([["Bob"], ["Carol"]], dtype=object),
        ),
        TestExpectation(
            name="filter_by_manager_bob",
            query=lambda f: f.find_all(None, None, [f.name()], f.manager().name().eq("Bob")),
            expected_columns=["Name"],
            expected_result=np.array([["Dave"]], dtype=object),
        ),
    ],
)


# An aggregate over a to-many association is computed for each row of the root table, so each employee gets the
# aggregate of their own reports; employees with no reports get a count of 0.
ORGCHART_AGGREGATE_FINDER_SPECS = FinderSpec(
    finder_name="EmployeeFinder",
    mapping_file=ORGCHART_MAPPING,
    skip_backends={"graphql": "The GraphQL finder has no count() yet"},
    expectations=[
        TestExpectation(
            name="count_of_reports_for_each_employee",
            query=lambda f: f.find_all(
                None, None, [f.name(), f.employees().id_().count()]
            ).order_by(f.name().ascending()),
            expected_columns=["Name", "Employee Id Count"],
            expected_result=np.array([["Alice", 2], ["Bob", 1], ["Carol", 0], ["Dave", 0]], dtype=object),
            expected_sql={"duckdb": "GROUP BY t0.id, t0.name"},
        ),
        TestExpectation(
            name="count_of_reports_alone_is_still_for_each_employee",
            query=lambda f: f.find_all(
                None, None, [f.employees().id_().count()]
            ).order_by(f.id_().ascending()),
            expected_columns=["Employee Id Count"],
            expected_result=np.array([[2], [1], [0], [0]], dtype=object),
            expected_sql={"duckdb": "GROUP BY t0.id ORDER BY t0.id ASC"},
        ),
        TestExpectation(
            name="explicit_group_by_is_used_as_given",
            query=lambda f: f.find_all(
                None, None, [f.manager().name(), f.id_().count()]
            ).group_by(f.manager().name()).order_by(f.manager().name().ascending()),
            expected_columns=["Manager Name", "Id Count"],
            expected_result=np.array([["Alice", 2], ["Bob", 1], [None, 1]], dtype=object),
        ),
        TestExpectation(
            name="aggregate_over_a_to_one_association_groups_by_the_plain_columns",
            query=lambda f: f.find_all(
                None, None, [f.manager().name(), f.id_().count()]
            ).order_by(f.manager().name().ascending()),
            expected_columns=["Manager Name", "Id Count"],
            expected_result=np.array([["Alice", 2], ["Bob", 1], [None, 1]], dtype=object),
            expected_sql={"duckdb": "GROUP BY t1.name"},
        ),
    ],
)
