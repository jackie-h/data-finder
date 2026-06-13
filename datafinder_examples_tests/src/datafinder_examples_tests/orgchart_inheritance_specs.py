"""
Test specifications for the org chart with inheritance (orgchart_inheritance_mapping.md).

Covers: inherited property queries, self-referential manager join, project join (forward),
reverse navigation (project → employee), join filters (kwargs), and multi-hop joins.

    hr.employees — emp_id INT, first_name VARCHAR, last_name VARCHAR,
                   email VARCHAR, department VARCHAR, manager_id INT
    hr.projects  — project_id INT, name VARCHAR, code VARCHAR, assignee_id INT
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

ORGCHART_INHERITANCE_MAPPING = "orgchart_inheritance_mapping.md"

EMPLOYEE_INHERITANCE_FINDER_SPECS = FinderSpec(
    finder_name="EmployeeFinder",
    mapping_file=ORGCHART_INHERITANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="all_employees_inherited_name",
            query=lambda f: f.find_all(
                None, None, [f.first_name(), f.last_name()]
            ).order_by(f.first_name().ascending()),
            expected_columns=["First Name", "Last Name"],
            expected_result=np.array(
                [["Alice", "Smith"], ["Bob", "Jones"], ["Carol", "White"], ["Dave", "Brown"]],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="filter_by_inherited_first_name",
            query=lambda f: f.find_all(
                None, None, [f.first_name(), f.department()],
                f.first_name().eq("Alice"),
            ),
            expected_columns=["First Name", "Department"],
            expected_result=np.array([["Alice", "Executive"]], dtype=object),
        ),
        TestExpectation(
            name="filter_by_inherited_email",
            query=lambda f: f.find_all(
                None, None, [f.first_name()],
                f.email().eq("bob@example.com"),
            ),
            expected_columns=["First Name"],
            expected_result=np.array([["Bob"]], dtype=object),
        ),
        TestExpectation(
            name="filter_by_department_engineering",
            query=lambda f: f.find_all(
                None, None, [f.first_name()],
                f.department().eq("Engineering"),
            ).order_by(f.first_name().ascending()),
            expected_columns=["First Name"],
            expected_result=np.array([["Bob"], ["Carol"]], dtype=object),
        ),
        TestExpectation(
            name="manager_name_for_bob",
            query=lambda f: f.find_all(
                None, None, [f.first_name(), f.manager().first_name()],
                f.first_name().eq("Bob"),
            ),
            expected_columns=["First Name", "Manager First Name"],
            expected_result=np.array([["Bob", "Alice"]], dtype=object),
        ),
        TestExpectation(
            name="employees_with_all_projects",
            query=lambda f: f.find_all(
                None, None, [f.first_name(), f.projects().name()],
            ).order_by(f.first_name().ascending(), f.projects().name().ascending()),
            expected_columns=["First Name", "Project Name"],
            expected_result=np.array(
                [
                    ["Alice", "Epsilon Research"],
                    ["Bob", "Alpha Initiative"],
                    ["Bob", "Beta Platform"],
                    ["Carol", "Gamma Tooling"],
                    ["Dave", "Delta Ops"],
                ],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="forward_join_filter_code_alpha",
            query=lambda f: f.find_all(
                None, None, [f.first_name(), f.projects(code="ALPHA").name()],
            ).order_by(f.first_name().ascending()),
            expected_columns=["First Name", "Project Name"],
            expected_result=np.array(
                [["Alice", None], ["Bob", "Alpha Initiative"], ["Carol", None], ["Dave", None]],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="multihop_manager_projects_for_dave",
            query=lambda f: f.find_all(
                None, None, [f.manager().projects().name()],
                f.first_name().eq("Dave"),
            ).order_by(f.manager().projects().name().ascending()),
            expected_columns=["Project Name"],
            expected_result=np.array([["Alpha Initiative"], ["Beta Platform"]], dtype=object),
        ),
        TestExpectation(
            name="filter_by_manager_project_code",
            query=lambda f: f.find_all(
                None, None, [f.first_name()],
                f.manager().projects().code().eq("ALPHA"),
            ),
            expected_columns=["First Name"],
            expected_result=np.array([["Dave"]], dtype=object),
        ),
        TestExpectation(
            name="filter_by_manager_name_select_project",
            query=lambda f: f.find_all(
                None, None, [f.manager().projects().name()],
                f.manager().first_name().eq("Alice"),
            ).order_by(f.manager().projects().name().ascending()),
            expected_columns=["Project Name"],
            expected_result=np.array([["Epsilon Research"], ["Epsilon Research"]], dtype=object),
        ),
        # --- Additional inheritance specs ---
        TestExpectation(
            name="filter_by_inherited_last_name",
            query=lambda f: f.find_all(
                None, None, [f.first_name()], f.last_name().eq("Jones"),
            ),
            expected_columns=["First Name"],
            expected_result=np.array([["Bob"]], dtype=object),
        ),
        TestExpectation(
            name="project_all_inherited_columns",
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.first_name(), f.last_name(), f.email(), f.department()],
                f.first_name().eq("Dave"),
            ),
            expected_columns=["Id", "First Name", "Last Name", "Email", "Department"],
            expected_result=np.array(
                [[4, "Dave", "Brown", "dave@example.com", "QA"]], dtype=object
            ),
        ),
        TestExpectation(
            name="manager_name_for_all",
            query=lambda f: f.find_all(
                None, None, [f.first_name(), f.manager().first_name()],
            ).order_by(f.first_name().ascending()),
            expected_columns=["First Name", "Manager First Name"],
            # Alice has no manager → None; Bob/Carol report to Alice; Dave reports to Bob
            expected_result=np.array(
                [["Alice", None], ["Bob", "Alice"], ["Carol", "Alice"], ["Dave", "Bob"]],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="filter_by_manager_alice",
            query=lambda f: f.find_all(
                None, None, [f.first_name()],
                f.manager().first_name().eq("Alice"),
            ).order_by(f.first_name().ascending()),
            expected_columns=["First Name"],
            expected_result=np.array([["Bob"], ["Carol"]], dtype=object),
        ),
        TestExpectation(
            name="manager_email_for_bob",
            query=lambda f: f.find_all(
                None, None, [f.first_name(), f.manager().email()],
                f.first_name().eq("Bob"),
            ),
            expected_columns=["First Name", "Manager Email"],
            expected_result=np.array([["Bob", "alice@example.com"]], dtype=object),
        ),
        TestExpectation(
            name="employee_projects_for_bob",
            query=lambda f: f.find_all(
                None, None, [f.projects().code()],
                f.first_name().eq("Bob"),
            ).order_by(f.projects().code().ascending()),
            expected_columns=["Project Code"],
            expected_result=np.array([["ALPHA"], ["BETA"]], dtype=object),
        ),
        TestExpectation(
            name="filter_employee_by_project_name",
            query=lambda f: f.find_all(
                None, None, [f.first_name()],
                f.projects().name().eq("Delta Ops"),
            ),
            expected_columns=["First Name"],
            expected_result=np.array([["Dave"]], dtype=object),
        ),
        # --- Additional join filter specs ---
        TestExpectation(
            name="forward_join_filter_name_gamma",
            query=lambda f: f.find_all(
                None, None, [f.first_name(), f.projects(name="Gamma Tooling").name()],
            ).order_by(f.first_name().ascending()),
            expected_columns=["First Name", "Project Name"],
            expected_result=np.array(
                [["Alice", None], ["Bob", None], ["Carol", "Gamma Tooling"], ["Dave", None]],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="forward_join_filter_multi_kwarg",
            query=lambda f: f.find_all(
                None, None,
                [f.first_name(), f.projects(name="Alpha Initiative", code="ALPHA").name()],
            ).order_by(f.first_name().ascending()),
            expected_columns=["First Name", "Project Name"],
            expected_result=np.array(
                [["Alice", None], ["Bob", "Alpha Initiative"], ["Carol", None], ["Dave", None]],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="forward_join_filter_nonexistent",
            query=lambda f: f.find_all(
                None, None, [f.first_name(), f.projects(code="NONEXISTENT").name()],
            ).order_by(f.first_name().ascending()),
            expected_columns=["First Name", "Project Name"],
            expected_result=np.array(
                [["Alice", None], ["Bob", None], ["Carol", None], ["Dave", None]],
                dtype=object,
            ),
        ),
    ],
)

PROJECT_FINDER_SPECS = FinderSpec(
    finder_name="ProjectFinder",
    mapping_file=ORGCHART_INHERITANCE_MAPPING,
    expectations=[
        TestExpectation(
            name="all_projects_with_assignee",
            query=lambda f: f.find_all(
                None, None, [f.name(), f.assignee().first_name()]
            ).order_by(f.name().ascending()),
            expected_columns=["Name", "Assignee First Name"],
            expected_result=np.array(
                [
                    ["Alpha Initiative", "Bob"],
                    ["Beta Platform", "Bob"],
                    ["Delta Ops", "Dave"],
                    ["Epsilon Research", "Alice"],
                    ["Gamma Tooling", "Carol"],
                ],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="filter_projects_by_assignee_engineering",
            query=lambda f: f.find_all(
                None, None, [f.name(), f.code()],
                f.assignee().department().eq("Engineering"),
            ).order_by(f.code().ascending()),
            expected_columns=["Name", "Code"],
            expected_result=np.array(
                [["Alpha Initiative", "ALPHA"], ["Beta Platform", "BETA"], ["Gamma Tooling", "GAMMA"]],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="filter_assignee_by_join_kwarg_dept",
            query=lambda f: f.find_all(
                None, None, [f.name(), f.assignee(department="Engineering").first_name()],
            ).order_by(f.name().ascending()),
            expected_columns=["Name", "Assignee First Name"],
            expected_result=np.array(
                [
                    ["Alpha Initiative", "Bob"],
                    ["Beta Platform", "Bob"],
                    ["Delta Ops", None],
                    ["Epsilon Research", None],
                    ["Gamma Tooling", "Carol"],
                ],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="reverse_join_filter_nonexistent",
            query=lambda f: f.find_all(
                None, None, [f.name(), f.assignee(department="UNKNOWN").first_name()],
            ).order_by(f.name().ascending()),
            expected_columns=["Name", "Assignee First Name"],
            expected_result=np.array(
                [
                    ["Alpha Initiative", None],
                    ["Beta Platform", None],
                    ["Delta Ops", None],
                    ["Epsilon Research", None],
                    ["Gamma Tooling", None],
                ],
                dtype=object,
            ),
        ),
    ],
)
