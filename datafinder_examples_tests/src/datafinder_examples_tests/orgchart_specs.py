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
