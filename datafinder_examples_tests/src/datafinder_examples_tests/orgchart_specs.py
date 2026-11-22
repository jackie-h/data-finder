"""
Test specifications for the simple org chart domain (orgchart_mapping.md).

    hr.employees — id INT, name VARCHAR, manager_id INT
"""

from datafinder import constant, when
from datafinder.expressions import count_all
from datafinder_examples_tests.spec import FinderSpec, TestExpectation

ORGCHART_MAPPING = "orgchart_mapping.md"

ORGCHART_FINDER_SPECS = FinderSpec(
    finder_name="EmployeeFinder",
    mapping_file=ORGCHART_MAPPING,
    expectations=[
        TestExpectation(
            name="all_employees",
            query=lambda f: f.find_all([f.id_(), f.name()]).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="manager_name_for_bob",
            query=lambda f: f.find_all([f.name(), f.manager().name()], f.name().eq("Bob")),
        ),
        TestExpectation(
            name="a_step_keeps_the_name_a_column_was_given",
            query=lambda f: (lambda r: r.order_by(r["employeeName"].ascending()))(
                f.find_all([f.name().alias("employeeName")]).distinct()
                 .extend(size=lambda r: r.employeeName.length())
                 .filter(lambda r: r.size > 4)
                 .select("employeeName", "size")),
        ),
        TestExpectation(
            name="a_step_is_ordered_by_a_column_of_the_result_before_it",
            query=lambda f: (lambda r: r.order_by(r["employeeName"].descending()))(
                f.find_all([f.name().alias("employeeName")]).limit(3).filter(lambda r: r.employeeName != "Bob")),
        ),
        TestExpectation(
            name="the_manager_of_a_manager",
            query=lambda f: f.find_all([f.name(), f.manager().manager().name()], f.name().eq("Dave")),
        ),
        TestExpectation(
            name="a_result_is_ordered_by_a_column_that_is_a_constant",
            query=lambda f: (lambda r: r.order_by(r["greeting"].ascending(), r["name"].descending()))(
                f.find_all([f.name().alias("name"), constant("Hello").alias("greeting")])),
        ),
        TestExpectation(
            name="a_result_is_ordered_by_a_column_of_the_result_it_was_made_from",
            query=lambda f: (lambda r: r.distinct().order_by(r["employeeName"].descending()))(
                f.find_all([f.name().alias("employeeName")])),
        ),
        # --- a step over a result is part of the select the result is, as far as it can be ---
        TestExpectation(
            name="a_filter_of_a_result_is_in_its_select",
            query=lambda f: (lambda r: r.order_by(r["employeeName"].ascending()))(
                f.find_all([f.name().alias("employeeName")]).filter(lambda r: r.employeeName != "Bob")),
        ),
        TestExpectation(
            name="an_extend_and_a_filter_on_the_column_it_made_are_in_one_select",
            query=lambda f: (lambda r: r.order_by(r["employeeName"].ascending()))(
                f.find_all([f.name().alias("employeeName")])
                 .extend(size=lambda r: r.employeeName.length())
                 .filter(lambda r: r.size > 4, lambda r: r.employeeName != "Alice")),
        ),
        TestExpectation(
            name="a_filter_of_a_filter_is_one_where",
            query=lambda f: (lambda r: r.order_by(r["employeeName"].ascending()))(
                f.find_all([f.name().alias("employeeName")]).filter(lambda r: r.employeeName != "Bob").filter(lambda r: r.employeeName != "Dave")),
        ),
        TestExpectation(
            name="a_select_of_the_columns_of_a_result_is_in_its_select",
            query=lambda f: (lambda r: r.order_by(r["size"].ascending(), r["employeeName"].ascending()))(
                f.find_all([f.name().alias("employeeName"), f.id_().alias("id")])
                 .extend(size=lambda r: r.employeeName.length())
                 .select("size", "employeeName")),
        ),
        TestExpectation(
            name="a_column_renamed_by_extend_and_select_is_in_one_select",
            query=lambda f: (lambda r: r.order_by(r["First Name"].ascending()))(
                f.find_all([f.name().alias("firstName")])
                 .extend(**{"First Name": lambda r: r.firstName})
                 .select("First Name")),
        ),
        TestExpectation(
            name="an_aggregate_of_a_filtered_result_is_in_one_select",
            query=lambda f: (lambda r: r.order_by(r["small"].ascending()))(
                f.find_all([f.name().alias("employeeName"), f.id_().alias("id")])
                 .filter(lambda r: r.employeeName != "Dave")
                 .extend(small=lambda r: r.id < 3)
                 .group_by(lambda r: r.small)
                 .aggregate(total=lambda r: r.id.sum())),
        ),
        TestExpectation(
            name="an_aggregate_of_the_whole_result_is_in_one_select",
            query=lambda f: f.find_all([f.id_().alias("id")]).filter(lambda r: r.id > 1).aggregate(total=lambda r: r.id.sum(), n=lambda r: r.id.count()),
        ),
        TestExpectation(
            name="a_filter_on_an_aggregate_is_over_the_select_that_made_it",
            query=lambda f: f.find_all([f.id_().alias("id")]).aggregate(total=lambda r: r.id.sum()).filter(lambda r: r.total > 5),
        ),
        TestExpectation(
            name="distinct_after_an_order_is_in_the_select_that_orders",
            query=lambda f: (lambda r: r.order_by(r["employeeName"].descending()).distinct())(
                f.find_all([f.name().alias("employeeName")])),
        ),
        TestExpectation(
            name="a_column_that_is_a_condition_can_be_tested_in_a_list_in_the_select",
            query=lambda f: (lambda r: r.order_by(r["id"].ascending()))(
                f.find_all([f.id_().alias("id"), (f.id_() > 2).alias("big")]).extend(listed=lambda r: r.big.is_in([True]))),
        ),
        TestExpectation(
            name="a_filter_on_a_window_column_is_still_over_the_select_that_made_it",
            query=lambda f: (lambda r: r.order_by(r["employeeName"].ascending()))(
                f.find_all([f.name().alias("employeeName"), f.id_().alias("id")])
                 .extend(newest=lambda r: r.id.max().over())
                 .filter(lambda r: r.id == r.newest)),
        ),
        TestExpectation(
            name="fill_null_replaces_a_missing_manager",
            query=lambda f: f.find_all(
                [f.name(), f.manager().name().fill_null("nobody")]
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="concat_with_a_missing_part_is_missing",
            query=lambda f: f.find_all(
                [f.name() + " reports to " + f.manager().name()]
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="filter_on_a_function_of_a_related_column",
            query=lambda f: f.find_all(
                [f.name()], f.manager().name().upper().eq("ALICE")
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="a_condition_on_a_related_column_as_a_column",
            query=lambda f: f.find_all(
                [f.name(), f.manager().name().is_none()]
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="a_comparison_with_a_missing_value_is_missing",
            query=lambda f: f.find_all(
                [f.name(), f.manager().name().eq("Alice")]
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="alias_names_a_column_reached_through_an_association",
            query=lambda f: f.find_all(
                [f.name().alias("Employee"), f.manager().name().alias("Boss")], f.name().eq("Bob")
            ),
        ),
        TestExpectation(
            name="fill_null_leaves_a_value_that_is_there",
            query=lambda f: f.find_all(
                [f.name().fill_null("nobody")], f.name().eq("Bob")
            ),
        ),
        TestExpectation(
            name="if_on_a_related_column_with_a_fallback",
            query=lambda f: f.find_all(
                [f.name(), when(f.manager().name().is_none()).then("top").otherwise(f.manager().name())]
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="if_whose_condition_is_missing_takes_otherwise",
            query=lambda f: f.find_all(
                [f.name(), when(f.manager().name().eq("Alice")).then("alice").otherwise("other")]
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="if_as_a_filter_on_a_related_column_keeps_the_rows_with_no_related_row_out",
            query=lambda f: f.find_all(
                [f.name()], when(f.manager().name().eq("Alice")).then(True).otherwise(False)
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="if_as_a_filter_on_a_related_column_with_a_missing_row_taking_otherwise",
            query=lambda f: f.find_all(
                [f.name()], when(f.manager().name().eq("Alice")).then(False).otherwise(True)
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="manager_name_for_dave",
            query=lambda f: f.find_all([f.name(), f.manager().name()], f.name().eq("Dave")),
        ),
        TestExpectation(
            name="filter_by_manager_alice",
            query=lambda f: f.find_all(
                [f.name()], f.manager().name().eq("Alice")
            ).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="filter_by_manager_bob",
            query=lambda f: f.find_all([f.name()], f.manager().name().eq("Bob")),
        ),
    ],
)


# An aggregate over a to-many association is computed for each row of the root table, so each employee gets the
# aggregate of their own reports; employees with no reports get a count of 0.
ORGCHART_AGGREGATE_FINDER_SPECS = FinderSpec(
    finder_name="EmployeeFinder",
    mapping_file=ORGCHART_MAPPING,
    expectations=[
        TestExpectation(
            name="count_of_reports_for_each_employee",
            query=lambda f: f.find_all(
                [f.name(), f.employees().id_().count()]
            ).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="count_of_reports_alone_is_still_for_each_employee",
            query=lambda f: f.find_all(
                [f.employees().id_().count()]
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="explicit_group_by_is_used_as_given",
            # the employee with no manager is first: a missing value is the smallest, so it comes first when ascending
            query=lambda f: f.find_all(
                [f.manager().name(), f.id_().count()]
            ).group_by(f.manager().name()).order_by(f.manager().name().ascending()),
        ),
        TestExpectation(
            name="aggregate_over_a_to_one_association_groups_by_the_plain_columns",
            # the employee with no manager is first: a missing value is the smallest, so it comes first when ascending
            query=lambda f: f.find_all(
                [f.manager().name(), f.id_().count()]
            ).order_by(f.manager().name().ascending()),
        ),
    ],
)


# exists() and not_exists() on a to-many association ask whether a row exists, so a manager with several reports is
# one row, and counts once. Alice has two reports (Bob and Carol), Bob has one (Dave); Carol and Dave have none. The
# reports are rows of the same table, so the sub-select has an alias of its own.
ORGCHART_EXISTS_FINDER_SPECS = FinderSpec(
    finder_name="EmployeeFinder",
    mapping_file=ORGCHART_MAPPING,
    expectations=[
        TestExpectation(
            name="exists_to_many_is_one_row_however_many_rows_match",
            query=lambda f: f.find_all([f.name()], f.employees().exists()).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="exists_to_many_counts_a_row_once",
            query=lambda f: f.find_all([count_all()], f.employees().exists()),
        ),
        TestExpectation(
            name="not_exists_to_many_is_a_row_with_no_match",
            query=lambda f: f.find_all([f.name()], f.employees().not_exists()).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="exists_and_not_exists_on_one_step_cover_every_row_once",
            query=lambda f: f.find_all(
                [f.name()], f.employees().exists().or_(f.employees().not_exists())
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="exists_to_many_through_a_to_one_step_is_one_row",
            # Bob, Carol and Dave each have a manager with reports. Alice has two, so a join would repeat Bob and Carol.
            query=lambda f: f.find_all([f.name()], f.manager().employees().exists())
            .order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="reading_a_value_through_the_step_keeps_a_row_for_each_match",
            # A value read through the step is a row for each report, and exists() then holds for a report in the result
            query=lambda f: f.find_all([f.name(), f.employees().name()], f.employees().exists())
            .order_by(f.id_().ascending(), f.employees().name().ascending()),
        ),
    ],
)
