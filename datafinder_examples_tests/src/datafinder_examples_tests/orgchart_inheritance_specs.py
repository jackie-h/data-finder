"""
Test specifications for the org chart with inheritance (orgchart_inheritance_mapping.md).

Covers: inherited property queries, self-referential manager join, project join (forward),
reverse navigation (project → employee), join filters (kwargs), and multi-hop joins.

    hr.employees — emp_id INT, first_name VARCHAR, last_name VARCHAR,
                   email VARCHAR, department VARCHAR, manager_id INT
    hr.projects  — project_id INT, name VARCHAR, code VARCHAR, assignee_id INT
"""

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

ORGCHART_INHERITANCE_MAPPING = "orgchart_inheritance_mapping.md"

EMPLOYEE_INHERITANCE_FINDER_SPECS = FinderSpec(
    finder_name="EmployeeFinder",
    mapping_file=ORGCHART_INHERITANCE_MAPPING,
    expectations=[
        # Employee.department is optional (0..1) and first_name is required (1). An ordering or pattern
        # comparison on an optional value says it needs one: the guard changes no row, but it is in the SQL.
        TestExpectation(
            name="optional_property_pattern_comparison_is_guarded",
            query=lambda f: f.find_all(
                [f.first_name()], f.department().starts_with("Eng")
            ).order_by(f.first_name().ascending()),
        ),
        TestExpectation(
            name="optional_property_equality_is_not_guarded",
            query=lambda f: f.find_all([f.first_name()], f.department().eq("QA")),
        ),
        TestExpectation(
            name="required_property_comparison_is_not_guarded",
            query=lambda f: f.find_all(
                [f.first_name()], f.first_name().starts_with("C")
            ),
        ),
        TestExpectation(
            name="required_property_behind_an_optional_navigation_is_guarded",
            # Employee.manager is optional, so the manager's first name can be missing though it is a required property
            query=lambda f: f.find_all(
                [f.first_name()], f.manager().first_name().starts_with("B")
            ),
        ),
        TestExpectation(
            name="all_employees_inherited_name",
            query=lambda f: f.find_all(
                [f.first_name(), f.last_name()]
            ).order_by(f.first_name().ascending()),
        ),
        TestExpectation(
            name="filter_by_inherited_first_name",
            query=lambda f: f.find_all(
                [f.first_name(), f.department()],
                f.first_name().eq("Alice"),
            ),
        ),
        TestExpectation(
            name="filter_by_inherited_email",
            query=lambda f: f.find_all(
                [f.first_name()],
                f.email().eq("bob@example.com"),
            ),
        ),
        TestExpectation(
            name="filter_by_department_engineering",
            query=lambda f: f.find_all(
                [f.first_name()],
                f.department().eq("Engineering"),
            ).order_by(f.first_name().ascending()),
        ),
        TestExpectation(
            name="manager_name_for_bob",
            query=lambda f: f.find_all(
                [f.first_name(), f.manager().first_name()],
                f.first_name().eq("Bob"),
            ),
        ),
        TestExpectation(
            name="employees_with_all_projects",
            query=lambda f: f.find_all(
                [f.first_name(), f.projects().name()],
            ).order_by(f.first_name().ascending(), f.projects().name().ascending()),
        ),
        TestExpectation(
            name="forward_join_filter_code_alpha",
            query=lambda f: f.find_all(
                [f.first_name(), f.projects(code="ALPHA").name()],
            ).order_by(f.first_name().ascending()),
        ),
        TestExpectation(
            name="multihop_manager_projects_for_dave",
            query=lambda f: f.find_all(
                [f.manager().projects().name()],
                f.first_name().eq("Dave"),
            ).order_by(f.manager().projects().name().ascending()),
        ),
        TestExpectation(
            name="filter_by_manager_project_code",
            query=lambda f: f.find_all(
                [f.first_name()],
                f.manager().projects().code().eq("ALPHA"),
            ),
        ),
        TestExpectation(
            name="filter_by_manager_name_select_project",
            query=lambda f: f.find_all(
                [f.manager().projects().name()],
                f.manager().first_name().eq("Alice"),
            ).order_by(f.manager().projects().name().ascending()),
        ),
        # --- Additional inheritance specs ---
        TestExpectation(
            name="filter_by_inherited_last_name",
            query=lambda f: f.find_all(
                [f.first_name()], f.last_name().eq("Jones"),
            ),
        ),
        TestExpectation(
            name="project_all_inherited_columns",
            query=lambda f: f.find_all(
                [f.id_(), f.first_name(), f.last_name(), f.email(), f.department()],
                f.first_name().eq("Dave"),
            ),
        ),
        TestExpectation(
            name="manager_name_for_all",
            # Alice has no manager → None; Bob/Carol report to Alice; Dave reports to Bob
            query=lambda f: f.find_all(
                [f.first_name(), f.manager().first_name()],
            ).order_by(f.first_name().ascending()),
        ),
        TestExpectation(
            name="filter_by_manager_alice",
            query=lambda f: f.find_all(
                [f.first_name()],
                f.manager().first_name().eq("Alice"),
            ).order_by(f.first_name().ascending()),
        ),
        TestExpectation(
            name="manager_email_for_bob",
            query=lambda f: f.find_all(
                [f.first_name(), f.manager().email()],
                f.first_name().eq("Bob"),
            ),
        ),
        TestExpectation(
            name="employee_projects_for_bob",
            query=lambda f: f.find_all(
                [f.projects().code()],
                f.first_name().eq("Bob"),
            ).order_by(f.projects().code().ascending()),
        ),
        TestExpectation(
            name="filter_employee_by_project_name",
            query=lambda f: f.find_all(
                [f.first_name()],
                f.projects().name().eq("Delta Ops"),
            ),
        ),
        # --- Additional join filter specs ---
        TestExpectation(
            name="forward_join_filter_name_gamma",
            query=lambda f: f.find_all(
                [f.first_name(), f.projects(name="Gamma Tooling").name()],
            ).order_by(f.first_name().ascending()),
        ),
        TestExpectation(
            name="forward_join_filter_multi_kwarg",
            query=lambda f: f.find_all(
                [f.first_name(), f.projects(name="Alpha Initiative", code="ALPHA").name()],
            ).order_by(f.first_name().ascending()),
        ),
        TestExpectation(
            name="forward_join_filter_nonexistent",
            query=lambda f: f.find_all(
                [f.first_name(), f.projects(code="NONEXISTENT").name()],
            ).order_by(f.first_name().ascending()),
        ),
        # --- RelatedFinder.exists() / not_exists() ---
        TestExpectation(
            name="manager_exists_filter",
            # Alice has no manager; Bob/Carol/Dave all have managers
            query=lambda f: f.find_all(
                [f.first_name()],
                f.manager().exists(),
            ).order_by(f.first_name().ascending()),
        ),
        TestExpectation(
            name="manager_not_exists_filter",
            # Only Alice has no manager (manager_id IS NULL)
            query=lambda f: f.find_all(
                [f.first_name()],
                f.manager().not_exists(),
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
                [f.name(), f.assignee().first_name()]
            ).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="filter_projects_by_assignee_engineering",
            query=lambda f: f.find_all(
                [f.name(), f.code()],
                f.assignee().department().eq("Engineering"),
            ).order_by(f.code().ascending()),
        ),
        TestExpectation(
            name="filter_assignee_by_join_kwarg_dept",
            query=lambda f: f.find_all(
                [f.name(), f.assignee(department="Engineering").first_name()],
            ).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="reverse_join_filter_nonexistent",
            query=lambda f: f.find_all(
                [f.name(), f.assignee(department="UNKNOWN").first_name()],
            ).order_by(f.name().ascending()),
        ),
    ],
)
