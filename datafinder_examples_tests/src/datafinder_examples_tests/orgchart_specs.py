"""
Test specifications for the simple org chart domain (orgchart_mapping.md).

    hr.employees — id INT, name VARCHAR, manager_id INT
"""
import numpy as np

from datafinder import if_
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
            name="a_condition_on_a_related_column_as_a_column",
            skip_backends={"graphql": "The GraphQL finder cannot compute a condition as a column"},
            query=lambda f: f.find_all(
                None, None, [f.name(), f.manager().name().is_none()]
            ).order_by(f.id_().ascending()),
            expected_columns=["Name", "Expression"],
            expected_result=np.array(
                [["Alice", True], ["Bob", False], ["Carol", False], ["Dave", False]], dtype=object),
        ),
        TestExpectation(
            name="a_comparison_with_a_missing_value_is_missing",
            skip_backends={"graphql": "The GraphQL finder cannot compute a condition as a column"},
            query=lambda f: f.find_all(
                None, None, [f.name(), f.manager().name().eq("Alice")]
            ).order_by(f.id_().ascending()),
            expected_columns=["Name", "Expression"],
            expected_result=np.array(
                [["Alice", None], ["Bob", True], ["Carol", True], ["Dave", False]], dtype=object),
        ),
        TestExpectation(
            name="if_on_a_related_column_with_a_fallback",
            skip_backends={"graphql": "The GraphQL finder cannot compute a conditional"},
            query=lambda f: f.find_all(
                None, None, [f.name(), if_(f.manager().name().is_none(), "top", f.manager().name())]
            ).order_by(f.id_().ascending()),
            expected_columns=["Name", "Expression"],
            expected_result=np.array(
                [["Alice", "top"], ["Bob", "Alice"], ["Carol", "Alice"], ["Dave", "Bob"]], dtype=object),
        ),
        TestExpectation(
            name="if_whose_condition_is_missing_takes_otherwise",
            skip_backends={"graphql": "The GraphQL finder cannot compute a conditional"},
            query=lambda f: f.find_all(
                None, None, [f.name(), if_(f.manager().name().eq("Alice"), "alice", "other")]
            ).order_by(f.id_().ascending()),
            expected_columns=["Name", "Expression"],
            expected_result=np.array(
                [["Alice", "other"], ["Bob", "alice"], ["Carol", "alice"], ["Dave", "other"]],
                dtype=object),
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
