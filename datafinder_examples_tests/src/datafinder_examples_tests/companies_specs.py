"""
Test specifications for the companies domain (companies_mapping.md).

Backends must seed their database with COMPANIES before running these specs.

    companies — id INT, name VARCHAR, category VARCHAR
"""
import numpy as np
import pandas as pd

from datafinder import constant


from datafinder import when
from datafinder_examples_tests.spec import FinderSpec, TestExpectation

COMPANIES_MAPPING = "companies_mapping.md"

COMPANY_FINDER_SPECS = FinderSpec(
    finder_name="CompanyFinder",
    mapping_file=COMPANIES_MAPPING,
    expectations=[
        # --- count_distinct: the different values ---
        TestExpectation(
            name="count_distinct_of_the_whole_result",
            query=lambda f: f.find_all(None, None, [f.category().count_distinct()]),
            expected_columns=["Category Count Distinct"],
            expected_result=np.array([[3]], dtype=object),
            expected_sql={"duckdb": "COUNT(DISTINCT t0.category)"},
        ),
        TestExpectation(
            name="count_distinct_is_less_than_count_where_values_repeat",
            skip_backends={"graphql": "The GraphQL finder has no count() yet"},
            query=lambda f: f.find_all(None, None, [f.category().count(), f.category().count_distinct()]),
            expected_columns=["Category Count", "Category Count Distinct"],
            expected_result=np.array([[5, 3]], dtype=object),
        ),
        TestExpectation(
            name="count_distinct_per_group",
            query=lambda f: f.find_all(
                None, None, [f.category(), f.name().count_distinct()]
            ).group_by(f.category()).order_by(f.category().ascending()),
            expected_columns=["Category", "Name Count Distinct"],
            expected_result=np.array([["Finance", 2], ["Manufacturing", 1], ["Technology", 2]], dtype=object),
        ),
        TestExpectation(
            name="count_distinct_with_a_filter",
            query=lambda f: f.find_all(None, None, [f.category().count_distinct()], f.name().starts_with("Acme")),
            expected_columns=["Category Count Distinct"],
            expected_result=np.array([[2]], dtype=object),
        ),
        # --- join: the values of a group joined into one text ---
        TestExpectation(
            name="join_per_group_in_an_order",
            skip_backends={"graphql": "The GraphQL engine cannot order the values it joins"},
            query=lambda f: f.find_all(
                None, None, [f.category(), f.name().join(", ", order_by=[f.name().ascending()])]
            ).group_by(f.category()).order_by(f.category().ascending()),
            expected_columns=["Category", "Join Name"],
            expected_result=np.array(
                [
                    ["Finance", "Delta Ltd, Gamma LLC"],
                    ["Manufacturing", "Acme Industries"],
                    ["Technology", "Acme Corp, Beta Corp"],
                ],
                dtype=object,
            ),
            expected_sql={"duckdb": "STRING_AGG(t0.name, ', ' ORDER BY t0.name ASC)"},
        ),
        TestExpectation(
            name="join_descending",
            skip_backends={"graphql": "The GraphQL engine cannot order the values it joins"},
            query=lambda f: f.find_all(
                None, None, [f.category(), f.name().join("|", order_by=[f.name().descending()])]
            ).group_by(f.category()).order_by(f.category().ascending()),
            expected_columns=["Category", "Join Name"],
            expected_result=np.array(
                [
                    ["Finance", "Gamma LLC|Delta Ltd"],
                    ["Manufacturing", "Acme Industries"],
                    ["Technology", "Beta Corp|Acme Corp"],
                ],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="join_of_the_whole_result",
            skip_backends={"graphql": "The GraphQL engine cannot order the values it joins"},
            query=lambda f: f.find_all(
                None, None, [f.category().join("/", order_by=[f.id_().ascending()])]
            ),
            expected_columns=["Join Category"],
            expected_result=np.array([["Technology/Manufacturing/Technology/Finance/Finance"]], dtype=object),
        ),
        TestExpectation(
            name="join_without_an_order_joins_every_value",
            query=lambda f: f.find_all(
                None, None, [f.category(), f.name().join(",")], f.category().eq("Manufacturing")
            ).group_by(f.category()),
            expected_columns=["Category", "Join Name"],
            expected_result=np.array([["Manufacturing", "Acme Industries"]], dtype=object),
            expected_sql={"duckdb": "STRING_AGG(t0.name, ',')"},
        ),
        # --- Joining text with + ---
        TestExpectation(
            name="concat_columns_and_text",
            skip_backends={"graphql": "The GraphQL finder cannot join text"},
            query=lambda f: f.find_all(
                None, None, [f.name() + " (" + f.category() + ")"]
            ).order_by(f.name().ascending()).limit(2),
            expected_columns=["Expression"],
            expected_result=np.array([["Acme Corp (Technology)"], ["Acme Industries (Manufacturing)"]], dtype=object),
            expected_sql={"duckdb": "t0.name || ' (' || t0.category || ')'"},
        ),
        TestExpectation(
            name="concat_text_before_a_column",
            skip_backends={"graphql": "The GraphQL finder cannot join text"},
            query=lambda f: f.find_all(
                None, None, ["Company: " + f.name()]
            ).order_by(f.name().ascending()).limit(1),
            expected_columns=["Expression"],
            expected_result=np.array([["Company: Acme Corp"]], dtype=object),
        ),
        TestExpectation(
            name="concat_a_function_of_a_column",
            skip_backends={"graphql": "The GraphQL finder cannot join text"},
            query=lambda f: f.find_all(
                None, None, [f.name().upper() + "/" + f.category().lower()]
            ).order_by(f.name().ascending()).limit(1),
            expected_columns=["Expression"],
            expected_result=np.array([["ACME CORP/technology"]], dtype=object),
        ),
        # --- Functions chain: the result of one has the functions of its type ---
        TestExpectation(
            name="text_functions_chain",
            skip_backends={"graphql": "The GraphQL finder cannot chain functions"},
            query=lambda f: f.find_all(
                None, None, [f.name().upper().length()]
            ).order_by(f.name().ascending()).limit(2),
            expected_columns=["Length Upper Name"],
            expected_result=np.array([[9], [15]], dtype=object),
            expected_sql={"duckdb": "LENGTH(UPPER(t0.name))"},
        ),
        TestExpectation(
            name="a_text_function_of_a_text_function",
            skip_backends={"graphql": "The GraphQL finder cannot chain functions"},
            query=lambda f: f.find_all(
                None, None, [f.name().upper().replace("ACME", "X")]
            ).order_by(f.name().ascending()).limit(1),
            expected_columns=["Replace Upper Name"],
            expected_result=np.array([["X CORP"]], dtype=object),
        ),
        TestExpectation(
            name="slice_the_result_of_a_function",
            skip_backends={"graphql": "The GraphQL finder cannot chain functions"},
            query=lambda f: f.find_all(
                None, None, [f.name().lower()[:4]]
            ).order_by(f.name().ascending()).limit(1),
            expected_columns=["Left Lower Name"],
            expected_result=np.array([["acme"]], dtype=object),
        ),
        # --- Filters on expressions: a function, arithmetic or a join of text, compared like a column ---
        TestExpectation(
            name="filter_on_a_function_of_a_column",
            skip_backends={"graphql": "A filter on an expression cannot be pushed to the server"},
            query=lambda f: f.find_all(None, None, [f.name()], f.name().upper().eq("ACME CORP")),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Corp"]], dtype=object),
            expected_sql={"duckdb": "UPPER(t0.name) = 'ACME CORP'"},
        ),
        TestExpectation(
            name="filter_on_the_length_of_a_column",
            skip_backends={"graphql": "A filter on an expression cannot be pushed to the server"},
            query=lambda f: f.find_all(None, None, [f.name()], f.name().length() > 10).order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Industries"]], dtype=object),
        ),
        TestExpectation(
            name="filter_on_arithmetic",
            skip_backends={"graphql": "A filter on an expression cannot be pushed to the server"},
            query=lambda f: f.find_all(None, None, [f.id_()], f.id_() * 2 >= 8).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[4], [5]], dtype=object),
            expected_sql={"duckdb": "t0.id * 2 >= 8"},
        ),
        TestExpectation(
            name="filter_on_joined_text",
            skip_backends={"graphql": "A filter on an expression cannot be pushed to the server"},
            query=lambda f: f.find_all(None, None, [f.id_()], f.name() + "/" + f.category() == "Beta Corp/Technology"),
            expected_columns=["Id"],
            expected_result=np.array([[3]], dtype=object),
        ),
        TestExpectation(
            name="filter_comparing_two_expressions",
            skip_backends={"graphql": "A filter on an expression cannot be pushed to the server"},
            query=lambda f: f.find_all(None, None, [f.id_()], f.name().length() > f.category().length() + 1)
            .order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [4], [5]], dtype=object),
        ),
        TestExpectation(
            name="filter_on_an_expression_with_another_filter",
            skip_backends={"graphql": "A filter on an expression cannot be pushed to the server"},
            query=lambda f: f.find_all(
                None, None, [f.id_()],
                f.name().upper().eq("DELTA LTD").or_(f.id_() * 1 == 1),
            ).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [5]], dtype=object),
        ),
        TestExpectation(
            name="not_equal_on_an_expression",
            skip_backends={"graphql": "A filter on an expression cannot be pushed to the server"},
            query=lambda f: f.find_all(None, None, [f.id_()], f.name().lower().ne("acme corp"))
            .order_by(f.id_().ascending()).limit(2),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3]], dtype=object),
        ),
        # --- Sorting by an aggregate ---
        TestExpectation(
            name="sort_by_an_aggregate_descending_then_a_column",
            skip_backends={"graphql": "The GraphQL finder has no count() yet"},
            query=lambda f: f.find_all(
                None, None, [f.category(), f.id_().count()]
            ).group_by(f.category()).order_by(f.id_().count().descending(), f.category().ascending()),
            expected_columns=["Category", "Id Count"],
            expected_result=np.array([["Finance", 2], ["Technology", 2], ["Manufacturing", 1]], dtype=object),
            expected_sql={"duckdb": "ORDER BY COUNT(t0.id) DESC, t0.category ASC"},
        ),
        TestExpectation(
            name="sort_by_an_aggregate_ascending",
            skip_backends={"graphql": "The GraphQL finder has no count() yet"},
            query=lambda f: f.find_all(
                None, None, [f.category(), f.id_().count()]
            ).group_by(f.category()).order_by(f.id_().count().ascending(), f.category().descending()),
            expected_columns=["Category", "Id Count"],
            expected_result=np.array([["Manufacturing", 1], ["Technology", 2], ["Finance", 2]], dtype=object),
        ),
        # --- A condition as a column: true or false for each row ---
        TestExpectation(
            name="a_comparison_as_a_column",
            skip_backends={"graphql": "The GraphQL finder cannot compute a condition as a column"},
            query=lambda f: f.find_all(
                None, None, [f.name(), f.category().eq("Technology")]
            ).order_by(f.id_().ascending()).limit(3),
            expected_columns=["Name", "Expression"],
            expected_result=np.array(
                [["Acme Corp", True], ["Acme Industries", False], ["Beta Corp", True]], dtype=object),
            expected_sql={"duckdb": "t0.category = 'Technology' AS \"Expression\""},
        ),
        TestExpectation(
            name="a_combined_condition_as_a_column",
            skip_backends={"graphql": "The GraphQL finder cannot compute a condition as a column"},
            query=lambda f: f.find_all(
                None, None, [f.id_(), f.category().eq("Finance").or_(f.name().starts_with("Acme"))]
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Expression"],
            expected_result=np.array([[1, True], [2, True], [3, False], [4, True], [5, True]], dtype=object),
        ),
        TestExpectation(
            name="a_null_check_as_a_column",
            skip_backends={"graphql": "The GraphQL finder cannot compute a condition as a column"},
            query=lambda f: f.find_all(None, None, [f.name().is_none()]).order_by(f.id_().ascending()).limit(2),
            expected_columns=["Expression"],
            expected_result=np.array([[False], [False]], dtype=object),
        ),
        TestExpectation(
            name="a_membership_test_as_a_column",
            skip_backends={"graphql": "The GraphQL finder cannot compute a condition as a column"},
            query=lambda f: f.find_all(None, None, [f.id_().is_in([1, 3])]).order_by(f.id_().ascending()).limit(3),
            expected_columns=["Expression"],
            expected_result=np.array([[True], [False], [True]], dtype=object),
        ),
        TestExpectation(
            name="a_condition_as_a_column_next_to_a_filter",
            skip_backends={"graphql": "The GraphQL finder cannot compute a condition as a column"},
            query=lambda f: f.find_all(
                None, None, [f.name(), f.id_() > 3], f.category().eq("Finance")
            ).order_by(f.id_().ascending()),
            expected_columns=["Name", "Expression"],
            expected_result=np.array([["Gamma LLC", True], ["Delta Ltd", True]], dtype=object),
        ),
        # --- Aliases: a column can be given the name it has in the result ---
        TestExpectation(
            name="alias_names_the_columns",
            query=lambda f: f.find_all(
                None, None, [f.name().alias("Company"), f.category().alias("Sector")]
            ).order_by(f.name().ascending()).limit(2),
            expected_columns=["Company", "Sector"],
            expected_result=np.array([["Acme Corp", "Technology"], ["Acme Industries", "Manufacturing"]], dtype=object),
            expected_sql={"duckdb": 't0.name AS "Company"'},
        ),
        TestExpectation(
            name="alias_names_the_same_column_twice",
            query=lambda f: f.find_all(
                None, None, [f.name().alias("A"), f.name().alias("B")]
            ).order_by(f.name().ascending()).limit(1),
            expected_columns=["A", "B"],
            expected_result=np.array([["Acme Corp", "Acme Corp"]], dtype=object),
        ),
        TestExpectation(
            name="alias_again_replaces_the_name",
            query=lambda f: f.find_all(
                None, None, [f.name().alias("First").alias("Second")]
            ).order_by(f.name().ascending()).limit(1),
            expected_columns=["Second"],
            expected_result=np.array([["Acme Corp"]], dtype=object),
        ),
        TestExpectation(
            name="alias_names_an_aggregate",
            skip_backends={"graphql": "The GraphQL finder has no count() yet"},
            query=lambda f: f.find_all(None, None, [f.id_().count().alias("Companies")]),
            expected_columns=["Companies"],
            expected_result=np.array([[5]], dtype=object),
        ),
        TestExpectation(
            name="alias_names_a_function_of_a_column",
            skip_backends={"graphql": "The GraphQL finder has no scalar functions"},
            query=lambda f: f.find_all(
                None, None, [f.name().upper().alias("Loud")]
            ).order_by(f.name().ascending()).limit(1),
            expected_columns=["Loud"],
            expected_result=np.array([["ACME CORP"]], dtype=object),
        ),
        # --- Constants: a fixed value as a column ---
        TestExpectation(
            name="constant_string_column",
            query=lambda f: f.find_all(
                None, None, [f.name(), constant("Hello").alias("Greeting")]
            ).order_by(f.name().ascending()).limit(2),
            expected_columns=["Name", "Greeting"],
            expected_result=np.array([["Acme Corp", "Hello"], ["Acme Industries", "Hello"]], dtype=object),
            expected_sql={"duckdb": "'Hello' AS \"Greeting\""},
        ),
        TestExpectation(
            name="constant_number_and_boolean_columns",
            query=lambda f: f.find_all(
                None, None, [f.name(), constant(7).alias("Seven"), constant(1.5).alias("Ratio"), constant(True).alias("Flag")]
            ).order_by(f.name().ascending()).limit(1),
            expected_columns=["Name", "Seven", "Ratio", "Flag"],
            expected_result=np.array([["Acme Corp", 7, 1.5, True]], dtype=object),
        ),
        TestExpectation(
            name="constant_column_is_named_constant_by_default",
            query=lambda f: f.find_all(None, None, [f.name(), constant(1)]).order_by(f.name().ascending()).limit(1),
            expected_columns=["Name", "Constant"],
            expected_result=np.array([["Acme Corp", 1]], dtype=object),
        ),
        TestExpectation(
            name="constant_column_with_a_filter_and_no_matches",
            query=lambda f: f.find_all(None, None, [f.name(), constant("x").alias("X")], f.name().eq("Nobody")),
            expected_columns=["Name", "X"],
            expected_result=np.empty((0, 2), dtype=object),
        ),
        # --- Distinct: each row once ---
        TestExpectation(
            name="distinct_removes_repeated_rows",
            query=lambda f: f.find_all(None, None, [f.category()]).distinct().order_by(f.category().ascending()),
            expected_columns=["Category"],
            expected_result=np.array([["Finance"], ["Manufacturing"], ["Technology"]], dtype=object),
            expected_sql={"duckdb": "SELECT DISTINCT"},
        ),
        TestExpectation(
            name="without_distinct_rows_repeat",
            query=lambda f: f.find_all(None, None, [f.category()]).order_by(f.category().ascending()),
            expected_columns=["Category"],
            expected_result=np.array(
                [["Finance"], ["Finance"], ["Manufacturing"], ["Technology"], ["Technology"]], dtype=object),
            unexpected_sql={"duckdb": "DISTINCT"},
        ),
        TestExpectation(
            name="distinct_keeps_rows_that_differ_in_any_column",
            query=lambda f: f.find_all(None, None, [f.category(), f.name()]).distinct()
            .order_by(f.category().ascending(), f.name().ascending()).limit(3),
            expected_columns=["Category", "Name"],
            expected_result=np.array(
                [["Finance", "Delta Ltd"], ["Finance", "Gamma LLC"], ["Manufacturing", "Acme Industries"]], dtype=object),
        ),
        TestExpectation(
            name="distinct_with_a_filter",
            query=lambda f: f.find_all(None, None, [f.category()], f.name().starts_with("Acme")).distinct()
            .order_by(f.category().ascending()),
            expected_columns=["Category"],
            expected_result=np.array([["Manufacturing"], ["Technology"]], dtype=object),
        ),
        TestExpectation(
            name="distinct_applies_before_the_limit",
            query=lambda f: f.find_all(None, None, [f.category()]).distinct()
            .order_by(f.category().ascending()).limit(2),
            expected_columns=["Category"],
            expected_result=np.array([["Finance"], ["Manufacturing"]], dtype=object),
        ),
        # --- Offset and slice: skip rows, counting from 0, in the order given ---
        TestExpectation(
            name="offset_skips_the_first_rows",
            query=lambda f: f.find_all(None, None, [f.name()]).order_by(f.name().ascending()).offset(2),
            expected_columns=["Name"],
            expected_result=np.array([["Beta Corp"], ["Delta Ltd"], ["Gamma LLC"]], dtype=object),
            expected_sql={"duckdb": "OFFSET 2"},
        ),
        TestExpectation(
            name="offset_then_limit_is_a_page",
            query=lambda f: f.find_all(None, None, [f.name()]).order_by(f.name().ascending()).offset(1).limit(2),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Industries"], ["Beta Corp"]], dtype=object),
            expected_sql={"duckdb": "LIMIT 2 OFFSET 1"},
        ),
        TestExpectation(
            name="slice_is_start_up_to_stop",
            query=lambda f: f.find_all(None, None, [f.name()]).order_by(f.name().ascending()).slice(1, 4),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Industries"], ["Beta Corp"], ["Delta Ltd"]], dtype=object),
        ),
        TestExpectation(
            name="offset_past_the_end_is_empty",
            query=lambda f: f.find_all(None, None, [f.name()]).order_by(f.name().ascending()).offset(10),
            expected_columns=["Name"],
            expected_result=np.empty((0, 1), dtype=object),
        ),
        TestExpectation(
            name="offset_zero_changes_nothing",
            query=lambda f: f.find_all(None, None, [f.id_()]).order_by(f.id_().ascending()).offset(0).limit(2),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2]], dtype=object),
        ),
        TestExpectation(
            name="offset_applies_after_distinct",
            query=lambda f: f.find_all(None, None, [f.category()]).distinct()
            .order_by(f.category().ascending()).offset(1).limit(1),
            expected_columns=["Category"],
            expected_result=np.array([["Manufacturing"]], dtype=object),
        ),
        # --- when / then / otherwise: a value that depends on a condition ---
        TestExpectation(
            name="if_chooses_between_two_values",
            skip_backends={"graphql": "The GraphQL finder cannot compute a conditional"},
            query=lambda f: f.find_all(
                None, None, [f.name(), when(f.category().eq("Technology")).then("tech").otherwise("other")]
            ).order_by(f.id_().ascending()).limit(3),
            expected_columns=["Name", "Expression"],
            expected_result=np.array(
                [["Acme Corp", "tech"], ["Acme Industries", "other"], ["Beta Corp", "tech"]], dtype=object),
            expected_sql={"duckdb": "CASE WHEN t0.category = 'Technology' THEN 'tech' ELSE 'other' END"},
        ),
        TestExpectation(
            name="if_nests_for_several_cases",
            skip_backends={"graphql": "The GraphQL finder cannot compute a conditional"},
            query=lambda f: f.find_all(
                None, None, [when(f.id_() < 2).then("low").when(f.id_() < 4).then("mid").otherwise("high")]
            ).order_by(f.id_().ascending()),
            expected_columns=["Expression"],
            expected_result=np.array([["low"], ["mid"], ["mid"], ["high"], ["high"]], dtype=object),
        ),
        TestExpectation(
            name="if_with_numbers_as_the_values",
            skip_backends={"graphql": "The GraphQL finder cannot compute a conditional"},
            query=lambda f: f.find_all(
                None, None, [when(f.name().starts_with("Acme")).then(1).otherwise(0)]
            ).order_by(f.id_().ascending()),
            expected_columns=["Expression"],
            expected_result=np.array([[1], [1], [0], [0], [0]], dtype=object),
        ),
        TestExpectation(
            name="if_with_columns_as_the_values",
            skip_backends={"graphql": "The GraphQL finder cannot compute a conditional"},
            query=lambda f: f.find_all(
                None, None, [when(f.id_() > 3).then(f.name()).otherwise(f.category())]
            ).order_by(f.id_().ascending()),
            expected_columns=["Expression"],
            expected_result=np.array(
                [["Technology"], ["Manufacturing"], ["Technology"], ["Gamma LLC"], ["Delta Ltd"]], dtype=object),
        ),
        TestExpectation(
            name="if_without_otherwise_is_missing_where_the_condition_fails",
            skip_backends={"graphql": "The GraphQL finder cannot compute a conditional"},
            query=lambda f: f.find_all(
                None, None, [when(f.category().eq("Finance")).then("F")]
            ).order_by(f.id_().ascending()),
            expected_columns=["Expression"],
            expected_result=np.array([[None], [None], [None], ["F"], ["F"]], dtype=object),
        ),
        TestExpectation(
            name="if_with_a_combined_condition",
            skip_backends={"graphql": "The GraphQL finder cannot compute a conditional"},
            query=lambda f: f.find_all(
                None, None, [when(f.category().eq("Technology").or_(f.id_().eq(5))).then("yes").otherwise("no")]
            ).order_by(f.id_().ascending()),
            expected_columns=["Expression"],
            expected_result=np.array([["yes"], ["no"], ["yes"], ["no"], ["yes"]], dtype=object),
        ),
        # --- Count ---
        TestExpectation(
            name="count_all",
            skip_backends={"graphql": "The GraphQL finder has no count() yet"},
            query=lambda f: f.find_all(None, None, [f.count()]),
            expected_columns=["Count"],
            expected_result=np.array([[5]], dtype=object),
        ),
        TestExpectation(
            name="count_technology",
            skip_backends={"graphql": "The GraphQL finder has no count() yet"},
            query=lambda f: f.find_all(None, None, [f.count()], f.category().eq("Technology")),
            expected_columns=["Count"],
            expected_result=np.array([[2]], dtype=object),
        ),
        TestExpectation(
            name="count_no_matches",
            skip_backends={"graphql": "The GraphQL finder has no count() yet"},
            query=lambda f: f.find_all(None, None, [f.count()], f.category().eq("Nonexistent")),
            expected_columns=["Count"],
            expected_result=np.array([[0]], dtype=object),
        ),
        TestExpectation(
            name="count_name_attribute",
            query=lambda f: f.find_all(None, None, [f.name().count()]),
            expected_columns=["Name Count"],
            expected_result=np.array([[5]], dtype=object),
        ),
        TestExpectation(
            name="group_by_category_count",
            skip_backends={"graphql": "The GraphQL finder has no count() yet"},
            query=lambda f: f.find_all(
                None, None, [f.category(), f.count()]
            ).group_by(f.category()).order_by(f.category().ascending()),
            expected_columns=["Category", "Count"],
            expected_result=np.array(
                [["Finance", 2], ["Manufacturing", 1], ["Technology", 2]],
                dtype=object,
            ),
        ),
        # --- Sort ---
        TestExpectation(
            name="sort_by_name_asc",
            query=lambda f: f.find_all(
                None, None, [f.name()]
            ).order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array(
                [["Acme Corp"], ["Acme Industries"], ["Beta Corp"], ["Delta Ltd"], ["Gamma LLC"]],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="sort_by_id_desc",
            query=lambda f: f.find_all(
                None, None, [f.id_()]
            ).order_by(f.id_().descending()),
            expected_columns=["Id"],
            expected_result=np.array([[5], [4], [3], [2], [1]], dtype=object),
        ),
        # order_by calls accumulate: the latest call takes priority and earlier ones break its ties.
        TestExpectation(
            name="chained_order_by_makes_the_later_sort_primary",
            query=lambda f: f.find_all(
                None, None, [f.name(), f.category()]
            ).order_by(f.name().ascending()).order_by(f.category().ascending()),
            expected_columns=["Name", "Category"],
            expected_result=np.array(
                [
                    ["Delta Ltd", "Finance"],
                    ["Gamma LLC", "Finance"],
                    ["Acme Industries", "Manufacturing"],
                    ["Acme Corp", "Technology"],
                    ["Beta Corp", "Technology"],
                ],
                dtype=object,
            ),
            expected_sql={"duckdb": "ORDER BY t0.category ASC, t0.name ASC"},
        ),
        TestExpectation(
            name="chained_order_by_replaces_the_direction_of_a_column_sorted_again",
            query=lambda f: f.find_all(
                None, None, [f.name()]
            ).order_by(f.name().ascending()).order_by(f.name().descending()),
            expected_columns=["Name"],
            expected_result=np.array(
                [["Gamma LLC"], ["Delta Ltd"], ["Beta Corp"], ["Acme Industries"], ["Acme Corp"]], dtype=object),
            expected_sql={"duckdb": "ORDER BY t0.name DESC"},
            unexpected_sql={"duckdb": "ASC"},
        ),
        TestExpectation(
            name="chained_order_by_of_three_calls",
            query=lambda f: f.find_all(
                None, None, [f.id_(), f.name(), f.category()]
            ).order_by(f.id_().ascending()).order_by(f.name().descending()).order_by(f.category().ascending()),
            expected_columns=["Id", "Name", "Category"],
            expected_result=np.array(
                [
                    [4, "Gamma LLC", "Finance"],
                    [5, "Delta Ltd", "Finance"],
                    [2, "Acme Industries", "Manufacturing"],
                    [3, "Beta Corp", "Technology"],
                    [1, "Acme Corp", "Technology"],
                ],
                dtype=object,
            ),
            expected_sql={"duckdb": "ORDER BY t0.category ASC, t0.name DESC, t0.id ASC"},
        ),
        TestExpectation(
            name="sort_by_category_asc_name_desc",
            query=lambda f: f.find_all(
                None, None, [f.name(), f.category()]
            ).order_by(f.category().ascending(), f.name().descending()),
            expected_columns=["Name", "Category"],
            expected_result=np.array(
                [
                    ["Gamma LLC", "Finance"],
                    ["Delta Ltd", "Finance"],
                    ["Acme Industries", "Manufacturing"],
                    ["Beta Corp", "Technology"],
                    ["Acme Corp", "Technology"],
                ],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="sort_filter_technology_asc",
            query=lambda f: f.find_all(
                None, None, [f.name()],
                f.category().eq("Technology"),
            ).order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Corp"], ["Beta Corp"]], dtype=object),
        ),
        # --- Limit ---
        TestExpectation(
            name="limit_two_by_name",
            query=lambda f: f.find_all(
                None, None, [f.name()]
            ).order_by(f.name().ascending()).limit(2),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Corp"], ["Acme Industries"]], dtype=object),
        ),
        # --- String operations ---
        TestExpectation(
            name="string_eq_exact_match",
            query=lambda f: f.find_all(None, None, [f.name()], f.name().eq("Acme Corp")),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Corp"]], dtype=object),
        ),
        TestExpectation(
            name="string_eq_operator",
            query=lambda f: f.find_all(None, None, [f.name()], f.name() == "Acme Corp"),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Corp"]], dtype=object),
        ),
        TestExpectation(
            name="string_ne_excludes_row",
            skip_backends={"graphql": "The GraphQL finder has no count() yet"},
            query=lambda f: f.find_all(
                None, None, [f.count()], f.name().ne("Acme Corp")
            ),
            expected_columns=["Count"],
            expected_result=np.array([[4]], dtype=object),
        ),
        TestExpectation(
            name="string_contains_corp",
            query=lambda f: f.find_all(
                None, None, [f.name()], f.name().contains("Corp")
            ).order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Corp"], ["Beta Corp"]], dtype=object),
        ),
        TestExpectation(
            name="string_contains_acme",
            query=lambda f: f.find_all(
                None, None, [f.name()], f.name().contains("Acme")
            ).order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Corp"], ["Acme Industries"]], dtype=object),
        ),
        TestExpectation(
            name="string_starts_with_acme",
            query=lambda f: f.find_all(
                None, None, [f.name()], f.name().starts_with("Acme")
            ).order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Corp"], ["Acme Industries"]], dtype=object),
        ),
        TestExpectation(
            name="string_ends_with_corp",
            query=lambda f: f.find_all(
                None, None, [f.name()], f.name().ends_with("Corp")
            ).order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Corp"], ["Beta Corp"]], dtype=object),
        ),
        TestExpectation(
            name="string_ends_with_llc",
            query=lambda f: f.find_all(None, None, [f.name()], f.name().ends_with("LLC")),
            expected_columns=["Name"],
            expected_result=np.array([["Gamma LLC"]], dtype=object),
        ),
        TestExpectation(
            name="string_is_in",
            query=lambda f: f.find_all(
                None, None, [f.name()], f.name().is_in(["Acme Corp", "Gamma LLC"])
            ).order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Corp"], ["Gamma LLC"]], dtype=object),
        ),
        TestExpectation(
            name="string_is_not_in",
            query=lambda f: f.find_all(
                None, None, [f.name()], f.name().is_not_in(["Acme Corp", "Acme Industries", "Gamma LLC"])
            ).order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array([["Beta Corp"], ["Delta Ltd"]], dtype=object),
        ),
        TestExpectation(
            name="string_category_starts_with_fin",
            query=lambda f: f.find_all(
                None, None, [f.name()], f.category().starts_with("Fin")
            ).order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array([["Delta Ltd"], ["Gamma LLC"]], dtype=object),
        ),
        TestExpectation(
            name="string_category_ne_technology",
            skip_backends={"graphql": "The GraphQL finder has no count() yet"},
            query=lambda f: f.find_all(
                None, None, [f.count()], f.category().ne("Technology")
            ),
            expected_columns=["Count"],
            expected_result=np.array([[3]], dtype=object),
        ),
        # --- Window functions ---
        TestExpectation(
            name="rank_min_by_id",
            skip_backends={"graphql": "Window functions are not supported by the GraphQL engine"},
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.id_().rank(method="min", order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Rank"],
            expected_result=np.array([[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]], dtype=object),
        ),
        TestExpectation(
            name="rank_dense_with_partition_by_category",
            skip_backends={"graphql": "Window functions are not supported by the GraphQL engine"},
            query=lambda f: f.find_all(
                None, None,
                [f.id_(),
                 f.id_().rank(method="dense",
                              partition_by=[f.category()],
                              order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Dense Rank"],
            # Finance: id=4→rank1, id=5→rank2; Manufacturing: id=2→rank1; Technology: id=1→rank1, id=3→rank2
            expected_result=np.array([[1, 1], [2, 1], [3, 2], [4, 1], [5, 2]], dtype=object),
        ),
        TestExpectation(
            name="shift_lag_second_row",
            skip_backends={"graphql": "Window functions are not supported by the GraphQL engine"},
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.id_().shift(1, order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Lag Id"],
            # Row 1 has no predecessor → pd.NA; rows 2-5 have the previous id, still as int
            # (Arrow-backed nullable Int64 dtype, not pandas' classic float64-on-null upcast).
            expected_result=np.array(
                [[1, pd.NA], [2, 1], [3, 2], [4, 3], [5, 4]], dtype=object
            ),
        ),
        TestExpectation(
            name="shift_lead_last_row",
            skip_backends={"graphql": "Window functions are not supported by the GraphQL engine"},
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.id_().shift(-1, order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Lead Id"],
            # Rows 1-4 see the next id; row 5 has no successor → pd.NA (see above).
            expected_result=np.array(
                [[1, 2], [2, 3], [3, 4], [4, 5], [5, pd.NA]], dtype=object
            ),
        ),
        TestExpectation(
            name="qcut_two_buckets",
            skip_backends={"graphql": "Window functions are not supported by the GraphQL engine"},
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.id_().qcut(2, order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Quantile"],
            # 5 rows into 2 buckets: ntile(2) gives [1,1,1,2,2] (ceil(5/2)=3 in bucket 1)
            expected_result=np.array([[1, 1], [2, 1], [3, 1], [4, 2], [5, 2]], dtype=object),
        ),
        TestExpectation(
            name="first_id_per_category",
            skip_backends={"graphql": "Window functions are not supported by the GraphQL engine"},
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.category(),
                 f.id_().first(partition_by=[f.category()], order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Category", "First Id"],
            expected_result=np.array([
                [1, "Technology", 1],
                [2, "Manufacturing", 2],
                [3, "Technology", 1],
                [4, "Finance", 4],
                [5, "Finance", 4],
            ], dtype=object),
        ),
        TestExpectation(
            name="last_id_per_category",
            skip_backends={"graphql": "Window functions are not supported by the GraphQL engine"},
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.category(),
                 f.id_().last(partition_by=[f.category()], order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Category", "Last Id"],
            # LAST_VALUE uses default frame (ROWS UNBOUNDED PRECEDING → CURRENT ROW),
            # so each row sees the last value up to itself within its partition.
            expected_result=np.array([
                [1, "Technology", 1],
                [2, "Manufacturing", 2],
                [3, "Technology", 3],
                [4, "Finance", 4],
                [5, "Finance", 5],
            ], dtype=object),
        ),
        TestExpectation(
            name="rank_method_first",
            skip_backends={"graphql": "Window functions are not supported by the GraphQL engine"},
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.id_().rank(method="first", order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Row Number"],
            expected_result=np.array([[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]], dtype=object),
        ),
        TestExpectation(
            name="rank_pct",
            skip_backends={"graphql": "Window functions are not supported by the GraphQL engine"},
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.id_().rank(pct=True, order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Percent Rank"],
            # percent_rank() = (rank - 1) / (N - 1) for N=5
            expected_result=np.array(
                [[1, 0.0], [2, 0.25], [3, 0.5], [4, 0.75], [5, 1.0]], dtype=float
            ),
        ),
        TestExpectation(
            name="rank_pct_method_max",
            skip_backends={"graphql": "Window functions are not supported by the GraphQL engine"},
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.id_().rank(pct=True, method="max", order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Cume Dist"],
            # cume_dist() = rank_max / N for N=5
            expected_result=np.array(
                [[1, 0.2], [2, 0.4], [3, 0.6], [4, 0.8], [5, 1.0]], dtype=float
            ),
        ),
        # --- Additional count variants ---
        TestExpectation(
            name="count_with_contains_filter",
            skip_backends={"graphql": "The GraphQL finder has no count() yet"},
            query=lambda f: f.find_all(None, None, [f.count()], f.name().contains("Corp")),
            expected_columns=["Count"],
            expected_result=np.array([[2]], dtype=object),
        ),
        TestExpectation(
            name="count_finance_category",
            skip_backends={"graphql": "The GraphQL finder has no count() yet"},
            query=lambda f: f.find_all(None, None, [f.count()], f.category().eq("Finance")),
            expected_columns=["Count"],
            expected_result=np.array([[2]], dtype=object),
        ),
        TestExpectation(
            name="count_id_attribute",
            query=lambda f: f.find_all(None, None, [f.id_().count()]),
            expected_columns=["Id Count"],
            expected_result=np.array([[5]], dtype=object),
        ),
        TestExpectation(
            name="count_name_technology_filter",
            query=lambda f: f.find_all(None, None, [f.name().count()], f.category().eq("Technology")),
            expected_columns=["Name Count"],
            expected_result=np.array([[2]], dtype=object),
        ),
        TestExpectation(
            name="count_name_ne_technology",
            query=lambda f: f.find_all(None, None, [f.name().count()], f.category().ne("Technology")),
            expected_columns=["Name Count"],
            expected_result=np.array([[3]], dtype=object),
        ),
        # --- Additional sort variants ---
        TestExpectation(
            name="sort_by_id_asc",
            query=lambda f: f.find_all(None, None, [f.id_()]).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2], [3], [4], [5]], dtype=object),
        ),
        TestExpectation(
            name="sort_by_name_desc",
            query=lambda f: f.find_all(None, None, [f.name()]).order_by(f.name().descending()),
            expected_columns=["Name"],
            expected_result=np.array(
                [["Gamma LLC"], ["Delta Ltd"], ["Beta Corp"], ["Acme Industries"], ["Acme Corp"]],
                dtype=object,
            ),
        ),
        TestExpectation(
            name="sort_by_category_asc_name_asc",
            query=lambda f: f.find_all(
                None, None, [f.name(), f.category()]
            ).order_by(f.category().ascending(), f.name().ascending()),
            expected_columns=["Name", "Category"],
            expected_result=np.array(
                [
                    ["Delta Ltd", "Finance"],
                    ["Gamma LLC", "Finance"],
                    ["Acme Industries", "Manufacturing"],
                    ["Acme Corp", "Technology"],
                    ["Beta Corp", "Technology"],
                ],
                dtype=object,
            ),
        ),
        # --- Additional limit variants ---
        TestExpectation(
            name="limit_one",
            query=lambda f: f.find_all(
                None, None, [f.name()]
            ).order_by(f.name().ascending()).limit(1),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Corp"]], dtype=object),
        ),
        TestExpectation(
            name="limit_three",
            query=lambda f: f.find_all(
                None, None, [f.name()]
            ).order_by(f.name().ascending()).limit(3),
            expected_columns=["Name"],
            expected_result=np.array(
                [["Acme Corp"], ["Acme Industries"], ["Beta Corp"]], dtype=object
            ),
        ),
        TestExpectation(
            name="limit_with_filter",
            query=lambda f: f.find_all(
                None, None, [f.name()], f.category().eq("Technology")
            ).order_by(f.name().ascending()).limit(1),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Corp"]], dtype=object),
        ),
    ],
)
