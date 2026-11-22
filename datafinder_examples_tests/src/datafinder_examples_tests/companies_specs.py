"""
Test specifications for the companies domain (companies_mapping.md).

Backends must seed their database with COMPANIES before running these specs.

    companies — id INT, name VARCHAR, category VARCHAR, embedding VECTOR(3)
"""
import csv
import json

import numpy as np

from datafinder import constant, count_all, when
from datafinder_examples import example_path
from datafinder_examples_tests.spec import FinderSpec, TestExpectation

COMPANIES_MAPPING = "companies_mapping.md"

# The companies and their embeddings as the data file has them, to compute the distances that the vector specs expect with numpy.
with open(example_path("companies.csv"), newline="") as _file:
    _COMPANIES = [(int(row["id"]), row["name"], row["category"], np.array(json.loads(row["embedding"])) if row["embedding"] else None)
                  for row in csv.DictReader(_file)]

_QUERY = [1.0, 0.2, 0.0]
def _nearest_in_each_category(f):
    """The nearest company of each category: a row number within the category by distance, and a filter on it (a window cannot be tested in WHERE)."""
    distance = f.embedding().cosine_distance(_QUERY)
    ranked = f.find_all([f.category().alias("category"), f.name().alias("name"), distance.round(4).alias("distance"),
                         f.id_().rank(method="first", partition_by=[f.category()], order_by=[distance.ascending(nulls="last")]).alias("rank")])
    nearest = ranked.filter(lambda r: r.rank <= 1).select("category", "name", "distance")
    return nearest.order_by(nearest["category"].ascending())


def _ascending(result, *columns):
    """The result ordered by the named columns, as a concatenated result names them."""
    return result.order_by(*(result[column].ascending() for column in columns))


def _descending(result, *columns):
    return result.order_by(*(result[column].descending() for column in columns))


COMPANY_FINDER_SPECS = FinderSpec(
    finder_name="CompanyFinder",
    mapping_file=COMPANIES_MAPPING,
    expectations=[
        # --- count_distinct: the different values ---
        TestExpectation(
            name="count_distinct_of_the_whole_result",
            query=lambda f: f.find_all([f.category().count_distinct()]),
        ),
        TestExpectation(
            name="count_distinct_is_less_than_count_where_values_repeat",
            query=lambda f: f.find_all([f.category().count(), f.category().count_distinct()]),
        ),
        TestExpectation(
            name="count_distinct_per_group",
            query=lambda f: f.find_all(
                [f.category(), f.name().count_distinct()]
            ).group_by(f.category()).order_by(f.category().ascending()),
        ),
        TestExpectation(
            name="count_distinct_with_a_filter",
            query=lambda f: f.find_all([f.category().count_distinct()], f.name().starts_with("Acme")),
        ),
        # --- join: the values of a group joined into one text ---
        TestExpectation(
            name="join_per_group_in_an_order",
            query=lambda f: f.find_all(
                [f.category(), f.name().join(", ", order_by=[f.name().ascending()])]
            ).group_by(f.category()).order_by(f.category().ascending()),
        ),
        TestExpectation(
            name="join_descending",
            query=lambda f: f.find_all(
                [f.category(), f.name().join("|", order_by=[f.name().descending()])]
            ).group_by(f.category()).order_by(f.category().ascending()),
        ),
        TestExpectation(
            name="join_of_the_whole_result",
            query=lambda f: f.find_all(
                [f.category().join("/", order_by=[f.id_().ascending()])]
            ),
        ),
        TestExpectation(
            name="join_without_an_order_joins_every_value",
            query=lambda f: f.find_all(
                [f.category(), f.name().join(",")], f.category().eq("Manufacturing")
            ).group_by(f.category()),
        ),
        # --- count_all (COUNT(*)), standard deviation, variance, and min / max of text ---
        TestExpectation(
            name="count_all_counts_rows_per_group",
            query=lambda f: f.find_all([f.category(), count_all()]).group_by(f.category())
            .order_by(f.category().ascending()),
        ),
        TestExpectation(
            name="population_standard_deviation",
            query=lambda f: f.find_all([f.id_().std(ddof=0)], f.id_().is_in([1, 3])),
        ),
        TestExpectation(
            name="sample_variance",
            query=lambda f: f.find_all([f.id_().var()], f.id_().is_in([1, 3])),
        ),
        TestExpectation(
            name="population_variance",
            query=lambda f: f.find_all([f.id_().var(ddof=0)], f.id_().is_in([1, 3])),
        ),
        TestExpectation(
            name="smallest_and_largest_text",
            query=lambda f: f.find_all([f.name().min(), f.name().max()]),
        ),
        # --- Having: keep the groups that satisfy a condition ---
        TestExpectation(
            name="having_a_count",
            query=lambda f: f.find_all([f.category(), count_all()]).group_by(f.category())
            .having(count_all() > 1).order_by(f.category().ascending()),
        ),
        TestExpectation(
            name="having_a_sum_and_a_grouping_column",
            query=lambda f: f.find_all([f.category(), f.id_().sum()]).group_by(f.category())
            .having(f.id_().sum() > 3, f.category().ne("Finance")).order_by(f.category().ascending()),
        ),
        TestExpectation(
            name="having_on_joined_text",
            query=lambda f: f.find_all([f.category(), f.name().join(",")]).group_by(f.category())
            .having(f.name().join(",").contains("Gamma")),
        ),
        TestExpectation(
            name="two_counts_compared_in_a_select_are_compared_with_a_plain_equals",
            # a count is never missing, so there is no IS NULL alternative to a plain =
            query=lambda f: f.find_all([f.category(), (f.name().count_distinct() == f.name().count()).alias("all distinct")])
            .group_by(f.category()).order_by(f.category().ascending()),
        ),
        # --- String tests on an expression ---
        TestExpectation(
            name="starts_with_on_a_function_of_a_column",
            query=lambda f: f.find_all([f.name()], f.name().upper().starts_with("ACME"))
            .order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="negate_a_string_test_with_a_tilde",
            query=lambda f: f.find_all([f.name()], ~f.name().starts_with("Acme")).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="matches_a_regular_expression_anywhere_in_the_text",
            query=lambda f: f.find_all([f.name()], f.name().matches("^(Acme|Beta) .*p$")).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="negate_a_regular_expression_match",
            query=lambda f: f.find_all([f.name()], ~f.name().matches("Corp|Ltd")).order_by(f.name().ascending()),
        ),
        # --- Aggregates of an expression ---
        TestExpectation(
            name="sum_of_arithmetic",
            query=lambda f: f.find_all([(f.id_() * 2).sum()]),
        ),
        TestExpectation(
            name="sum_of_a_constant_counts_the_rows",
            query=lambda f: f.find_all([constant(1).sum()]),
        ),
        TestExpectation(
            name="sum_of_a_conditional_per_group",
            query=lambda f: f.find_all(
                [f.category(), when(f.category().eq("Technology")).then(5.0).otherwise(1.0).sum().alias("Profit")]
            ).group_by(f.category()).order_by(f.category().ascending()),
        ),
        TestExpectation(
            name="all_and_any_of_a_condition_per_group",
            query=lambda f: f.find_all(
                [f.category(), (f.id_() > 0).all().alias("All"), (f.id_() > 3).any().alias("Any")]
            ).group_by(f.category()).order_by(f.category().ascending()),
        ),
        # --- Joining text with + ---
        TestExpectation(
            name="concat_columns_and_text",
            query=lambda f: f.find_all(
                [f.name() + " (" + f.category() + ")"]
            ).order_by(f.name().ascending()).limit(2),
        ),
        TestExpectation(
            name="concat_text_before_a_column",
            query=lambda f: f.find_all(
                ["Company: " + f.name()]
            ).order_by(f.name().ascending()).limit(1),
        ),
        TestExpectation(
            name="concat_a_function_of_a_column",
            query=lambda f: f.find_all(
                [f.name().upper() + "/" + f.category().lower()]
            ).order_by(f.name().ascending()).limit(1),
        ),
        # --- Functions chain: the result of one has the functions of its type ---
        TestExpectation(
            name="text_functions_chain",
            query=lambda f: f.find_all(
                [f.name().upper().length()]
            ).order_by(f.name().ascending()).limit(2),
        ),
        TestExpectation(
            name="a_text_function_of_a_text_function",
            query=lambda f: f.find_all(
                [f.name().upper().replace("ACME", "X")]
            ).order_by(f.name().ascending()).limit(1),
        ),
        TestExpectation(
            name="slice_the_result_of_a_function",
            query=lambda f: f.find_all(
                [f.name().lower()[:4]]
            ).order_by(f.name().ascending()).limit(1),
        ),
        # --- Filters on expressions: a function, arithmetic or a join of text, compared like a column ---
        TestExpectation(
            name="filter_on_a_function_of_a_column",
            query=lambda f: f.find_all([f.name()], f.name().upper().eq("ACME CORP")),
        ),
        TestExpectation(
            name="filter_on_the_length_of_a_column",
            query=lambda f: f.find_all([f.name()], f.name().length() > 10).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="filter_on_arithmetic",
            query=lambda f: f.find_all([f.id_()], f.id_() * 2 >= 8).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="filter_on_joined_text",
            query=lambda f: f.find_all([f.id_()], f.name() + "/" + f.category() == "Beta Corp/Technology"),
        ),
        TestExpectation(
            name="filter_comparing_two_expressions",
            query=lambda f: f.find_all([f.id_()], f.name().length() > f.category().length() + 1)
            .order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="filter_on_an_expression_with_another_filter",
            query=lambda f: f.find_all(
                [f.id_()],
                f.name().upper().eq("DELTA LTD").or_(f.id_() * 1 == 1),
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="not_equal_on_an_expression",
            query=lambda f: f.find_all([f.id_()], f.name().lower().ne("acme corp"))
            .order_by(f.id_().ascending()).limit(2),
        ),
        # --- Sorting by an aggregate ---
        TestExpectation(
            name="sort_by_an_aggregate_descending_then_a_column",
            query=lambda f: f.find_all(
                [f.category(), f.id_().count()]
            ).group_by(f.category()).order_by(f.id_().count().descending(), f.category().ascending()),
        ),
        TestExpectation(
            name="sort_by_an_aggregate_ascending",
            query=lambda f: f.find_all(
                [f.category(), f.id_().count()]
            ).group_by(f.category()).order_by(f.id_().count().ascending(), f.category().descending()),
        ),
        # --- A condition as a column: true or false for each row ---
        TestExpectation(
            name="a_comparison_as_a_column",
            query=lambda f: f.find_all(
                [f.name(), f.category().eq("Technology")]
            ).order_by(f.id_().ascending()).limit(3),
        ),
        TestExpectation(
            name="a_combined_condition_as_a_column",
            query=lambda f: f.find_all(
                [f.id_(), f.category().eq("Finance").or_(f.name().starts_with("Acme"))]
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="a_null_check_as_a_column",
            query=lambda f: f.find_all([f.name().is_none()]).order_by(f.id_().ascending()).limit(2),
        ),
        TestExpectation(
            name="a_membership_test_as_a_column",
            query=lambda f: f.find_all([f.id_().is_in([1, 3])]).order_by(f.id_().ascending()).limit(3),
        ),
        TestExpectation(
            name="a_condition_as_a_column_next_to_a_filter",
            query=lambda f: f.find_all(
                [f.name(), f.id_() > 3], f.category().eq("Finance")
            ).order_by(f.id_().ascending()),
        ),
        # --- Aliases: a column can be given the name it has in the result ---
        TestExpectation(
            name="alias_names_the_columns",
            query=lambda f: f.find_all(
                [f.name().alias("Company"), f.category().alias("Sector")]
            ).order_by(f.name().ascending()).limit(2),
        ),
        TestExpectation(
            name="alias_names_the_same_column_twice",
            query=lambda f: f.find_all(
                [f.name().alias("A"), f.name().alias("B")]
            ).order_by(f.name().ascending()).limit(1),
        ),
        TestExpectation(
            name="alias_again_replaces_the_name",
            query=lambda f: f.find_all(
                [f.name().alias("First").alias("Second")]
            ).order_by(f.name().ascending()).limit(1),
        ),
        TestExpectation(
            name="alias_names_an_aggregate",
            query=lambda f: f.find_all([f.id_().count().alias("Companies")]),
        ),
        TestExpectation(
            name="alias_names_a_function_of_a_column",
            query=lambda f: f.find_all(
                [f.name().upper().alias("Loud")]
            ).order_by(f.name().ascending()).limit(1),
        ),
        # --- Constants: a fixed value as a column ---
        TestExpectation(
            name="constant_string_column",
            query=lambda f: f.find_all(
                [f.name(), constant("Hello").alias("Greeting")]
            ).order_by(f.name().ascending()).limit(2),
        ),
        TestExpectation(
            name="constant_number_and_boolean_columns",
            query=lambda f: f.find_all(
                [f.name(), constant(7).alias("Seven"), constant(1.5).alias("Ratio"), constant(True).alias("Flag")]
            ).order_by(f.name().ascending()).limit(1),
        ),
        TestExpectation(
            name="constant_column_is_named_constant_by_default",
            query=lambda f: f.find_all([f.name(), constant(1)]).order_by(f.name().ascending()).limit(1),
        ),
        TestExpectation(
            name="constant_column_with_a_filter_and_no_matches",
            query=lambda f: f.find_all([f.name(), constant("x").alias("X")], f.name().eq("Nobody")),
        ),
        # --- Distinct: each row once ---
        TestExpectation(
            name="distinct_removes_repeated_rows",
            query=lambda f: f.find_all([f.category()]).distinct().order_by(f.category().ascending()),
        ),
        TestExpectation(
            name="without_distinct_rows_repeat",
            query=lambda f: f.find_all([f.category()]).order_by(f.category().ascending()),
        ),
        TestExpectation(
            name="distinct_keeps_rows_that_differ_in_any_column",
            query=lambda f: f.find_all([f.category(), f.name()]).distinct()
            .order_by(f.category().ascending(), f.name().ascending()).limit(3),
        ),
        TestExpectation(
            name="distinct_with_a_filter",
            query=lambda f: f.find_all([f.category()], f.name().starts_with("Acme")).distinct()
            .order_by(f.category().ascending()),
        ),
        TestExpectation(
            name="distinct_applies_before_the_limit",
            query=lambda f: f.find_all([f.category()]).distinct()
            .order_by(f.category().ascending()).limit(2),
        ),
        # --- Offset and slice: skip rows, counting from 0, in the order given ---
        TestExpectation(
            name="offset_skips_the_first_rows",
            query=lambda f: f.find_all([f.name()]).order_by(f.name().ascending()).offset(2),
        ),
        TestExpectation(
            name="offset_then_limit_is_a_page",
            query=lambda f: f.find_all([f.name()]).order_by(f.name().ascending()).offset(1).limit(2),
        ),
        TestExpectation(
            name="slice_is_start_up_to_stop",
            query=lambda f: f.find_all([f.name()]).order_by(f.name().ascending()).slice(1, 4),
        ),
        TestExpectation(
            name="offset_past_the_end_is_empty",
            query=lambda f: f.find_all([f.name()]).order_by(f.name().ascending()).offset(10),
        ),
        TestExpectation(
            name="offset_zero_changes_nothing",
            query=lambda f: f.find_all([f.id_()]).order_by(f.id_().ascending()).offset(0).limit(2),
        ),
        TestExpectation(
            name="offset_applies_after_distinct",
            query=lambda f: f.find_all([f.category()]).distinct()
            .order_by(f.category().ascending()).offset(1).limit(1),
        ),
        # --- when / then / otherwise: a value that depends on a condition ---
        TestExpectation(
            name="if_chooses_between_two_values",
            query=lambda f: f.find_all(
                [f.name(), when(f.category().eq("Technology")).then("tech").otherwise("other")]
            ).order_by(f.id_().ascending()).limit(3),
        ),
        TestExpectation(
            name="if_nests_for_several_cases",
            query=lambda f: f.find_all(
                [when(f.id_() < 2).then("low").when(f.id_() < 4).then("mid").otherwise("high")]
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="if_with_numbers_as_the_values",
            query=lambda f: f.find_all(
                [when(f.name().starts_with("Acme")).then(1).otherwise(0)]
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="if_with_columns_as_the_values",
            query=lambda f: f.find_all(
                [when(f.id_() > 3).then(f.name()).otherwise(f.category())]
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="if_without_otherwise_is_missing_where_the_condition_fails",
            query=lambda f: f.find_all(
                [when(f.category().eq("Finance")).then("F")]
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="if_with_a_combined_condition",
            query=lambda f: f.find_all(
                [when(f.category().eq("Technology").or_(f.id_().eq(5))).then("yes").otherwise("no")]
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="if_as_a_filter_keeps_the_rows_where_it_is_true",
            query=lambda f: f.find_all(
                [f.name()], when(f.category().eq("Technology")).then(True).otherwise(False)
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="if_as_a_filter_with_the_values_the_other_way_round",
            query=lambda f: f.find_all(
                [f.name()], when(f.category().eq("Technology")).then(False).otherwise(True)
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="if_as_a_filter_without_otherwise_drops_the_rows_where_the_condition_fails",
            query=lambda f: f.find_all(
                [f.name()], when(f.category().eq("Finance")).then(True)
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="if_as_a_filter_on_a_condition_over_a_column_of_the_row",
            query=lambda f: f.find_all(
                [f.name()], when(f.name().starts_with("Acme")).then(f.id_() > 1).otherwise(f.id_() > 3)
            ).order_by(f.id_().ascending()),
        ),
        # --- Count ---
        TestExpectation(
            name="count_all",
            query=lambda f: f.find_all([f.count()]),
        ),
        TestExpectation(
            name="count_technology",
            query=lambda f: f.find_all([f.count()], f.category().eq("Technology")),
        ),
        TestExpectation(
            name="count_no_matches",
            query=lambda f: f.find_all([f.count()], f.category().eq("Nonexistent")),
        ),
        TestExpectation(
            name="count_name_attribute",
            query=lambda f: f.find_all([f.name().count()]),
        ),
        TestExpectation(
            name="group_by_category_count",
            query=lambda f: f.find_all(
                [f.category(), f.count()]
            ).group_by(f.category()).order_by(f.category().ascending()),
        ),
        # --- Sort ---
        TestExpectation(
            name="sort_by_name_asc",
            query=lambda f: f.find_all(
                [f.name()]
            ).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="sort_by_id_desc",
            query=lambda f: f.find_all(
                [f.id_()]
            ).order_by(f.id_().descending()),
        ),
        # order_by calls accumulate: the latest call takes priority and earlier ones break its ties.
        TestExpectation(
            name="chained_order_by_makes_the_later_sort_primary",
            query=lambda f: f.find_all(
                [f.name(), f.category()]
            ).order_by(f.name().ascending()).order_by(f.category().ascending()),
        ),
        TestExpectation(
            name="chained_order_by_replaces_the_direction_of_a_column_sorted_again",
            query=lambda f: f.find_all(
                [f.name()]
            ).order_by(f.name().ascending()).order_by(f.name().descending()),
        ),
        TestExpectation(
            name="chained_order_by_of_three_calls",
            query=lambda f: f.find_all(
                [f.id_(), f.name(), f.category()]
            ).order_by(f.id_().ascending()).order_by(f.name().descending()).order_by(f.category().ascending()),
        ),
        TestExpectation(
            name="sort_by_category_asc_name_desc",
            query=lambda f: f.find_all(
                [f.name(), f.category()]
            ).order_by(f.category().ascending(), f.name().descending()),
        ),
        TestExpectation(
            name="sort_filter_technology_asc",
            query=lambda f: f.find_all(
                [f.name()],
                f.category().eq("Technology"),
            ).order_by(f.name().ascending()),
        ),
        # --- Limit ---
        TestExpectation(
            name="limit_two_by_name",
            query=lambda f: f.find_all(
                [f.name()]
            ).order_by(f.name().ascending()).limit(2),
        ),
        # --- String operations ---
        TestExpectation(
            name="string_eq_exact_match",
            query=lambda f: f.find_all([f.name()], f.name().eq("Acme Corp")),
        ),
        TestExpectation(
            name="string_eq_operator",
            query=lambda f: f.find_all([f.name()], f.name() == "Acme Corp"),
        ),
        TestExpectation(
            name="string_ne_excludes_row",
            query=lambda f: f.find_all(
                [f.count()], f.name().ne("Acme Corp")
            ),
        ),
        TestExpectation(
            name="string_contains_corp",
            query=lambda f: f.find_all(
                [f.name()], f.name().contains("Corp")
            ).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="string_contains_acme",
            query=lambda f: f.find_all(
                [f.name()], f.name().contains("Acme")
            ).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="string_starts_with_acme",
            query=lambda f: f.find_all(
                [f.name()], f.name().starts_with("Acme")
            ).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="string_ends_with_corp",
            query=lambda f: f.find_all(
                [f.name()], f.name().ends_with("Corp")
            ).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="string_ends_with_llc",
            query=lambda f: f.find_all([f.name()], f.name().ends_with("LLC")),
        ),
        TestExpectation(
            name="string_is_in",
            query=lambda f: f.find_all(
                [f.name()], f.name().is_in(["Acme Corp", "Gamma LLC"])
            ).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="string_is_not_in",
            query=lambda f: f.find_all(
                [f.name()], f.name().is_not_in(["Acme Corp", "Acme Industries", "Gamma LLC"])
            ).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="string_category_starts_with_fin",
            query=lambda f: f.find_all(
                [f.name()], f.category().starts_with("Fin")
            ).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="string_category_ne_technology",
            query=lambda f: f.find_all(
                [f.count()], f.category().ne("Technology")
            ),
        ),
        # --- Window functions ---
        TestExpectation(
            name="rank_min_by_id",
            query=lambda f: f.find_all(
                [f.id_(), f.id_().rank(method="min", order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="rank_dense_with_partition_by_category",
            # Finance: id=4→rank1, id=5→rank2; Manufacturing: id=2→rank1; Technology: id=1→rank1, id=3→rank2
            query=lambda f: f.find_all(
                [f.id_(),
                 f.id_().rank(method="dense",
                              partition_by=[f.category()],
                              order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="shift_lag_second_row",
            # Row 1 has no predecessor → pd.NA; rows 2-5 have the previous id, still as int
            # (Arrow-backed nullable Int64 dtype, not pandas' classic float64-on-null upcast).
            query=lambda f: f.find_all(
                [f.id_(), f.id_().shift(1, order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="shift_lead_last_row",
            # Rows 1-4 see the next id; row 5 has no successor → pd.NA (see above).
            query=lambda f: f.find_all(
                [f.id_(), f.id_().shift(-1, order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="integer_power_is_an_integer",
            query=lambda f: f.find_all([f.id_(), f.id_().power(2)]).order_by(f.id_().ascending()),
        ),
        # --- concatenate: UNION and UNION ALL of two results with the same columns, a result like any other ---
        TestExpectation(
            name="concatenate_removes_the_duplicate_rows",
            # ids 1, 2 are Technology and Manufacturing; ids 3, 4, 5 are Technology, Finance, Finance
            query=lambda f: _ascending(
                f.find_all([f.category()], f.id_() < 3).concatenate(f.find_all([f.category()], f.id_() > 2)), "category"),
        ),
        TestExpectation(
            name="concatenate_all_keeps_the_duplicate_rows",
            query=lambda f: _ascending(
                f.find_all([f.category()], f.id_() < 3).concatenate(f.find_all([f.category()], f.id_() > 2), all=True),
                "category"),
        ),
        TestExpectation(
            name="concatenate_more_than_two_results",
            query=lambda f: _descending(
                f.find_all([f.id_()], f.id_().eq(1)).concatenate(f.find_all([f.id_()], f.id_().eq(2)), all=True)
                .concatenate(f.find_all([f.id_()], f.id_().eq(3)), all=True), "id"),
        ),
        TestExpectation(
            name="concatenate_more_than_two_results_with_named_columns",
            # one chain of unions, not a select over each union
            query=lambda f: _descending(
                f.find_all([f.id_().alias("theKey")], f.id_().eq(1)).concatenate(f.find_all([f.id_().alias("theKey")], f.id_().eq(2)), all=True)
                .concatenate(f.find_all([f.id_().alias("theKey")], f.id_().eq(3)), all=True), "theKey"),
        ),
        TestExpectation(
            name="concatenate_then_offset_and_limit",
            query=lambda f: _ascending(
                f.find_all([f.id_()], f.id_() < 3).concatenate(f.find_all([f.id_()], f.id_() >= 3)), "id").offset(1).limit(3),
        ),
        TestExpectation(
            name="concatenate_then_take_then_sort",
            # the three highest ids, then those three in ascending order (not the three lowest)
            query=lambda f: (lambda top: top.order_by(top["id"].ascending()))(_descending(
                f.find_all([f.id_()], f.id_() < 3).concatenate(f.find_all([f.id_()], f.id_() >= 3)), "id").limit(3)),
        ),
        TestExpectation(
            name="concatenate_then_take_then_drop",
            # the first four ids, without the first of them (not four rows after the first)
            query=lambda f: _ascending(
                f.find_all([f.id_()], f.id_() < 3).concatenate(f.find_all([f.id_()], f.id_() >= 3)), "id").limit(4).offset(1),
        ),
        TestExpectation(
            name="concatenate_then_take_then_slice",
            query=lambda f: _ascending(
                f.find_all([f.id_()], f.id_() < 3).concatenate(f.find_all([f.id_()], f.id_() >= 3)), "id").limit(4).slice(1, 3),
        ),
        TestExpectation(
            name="concatenate_then_drop_then_take_and_drop_again",
            query=lambda f: _ascending(
                f.find_all([f.id_()], f.id_() < 3).concatenate(f.find_all([f.id_()], f.id_() >= 3)), "id").offset(1).limit(3).offset(1),
        ),
        TestExpectation(
            name="concatenate_a_result_with_its_own_order_and_limit",
            # the two highest ids, 5 and 4, then 1
            query=lambda f: _ascending(
                f.find_all([f.id_()]).order_by(f.id_().descending()).limit(2)
                .concatenate(f.find_all([f.id_()], f.id_().eq(1)), all=True), "id"),
        ),
        TestExpectation(
            name="aggregate_over_a_window_keeps_every_row",
            # the highest id of the row's category: Technology 1, 3; Manufacturing 2; Finance 4, 5. No row is grouped away.
            query=lambda f: f.find_all(
                [f.id_(), f.id_().max().over(partition_by=[f.category()])],
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="qcut_two_buckets",
            # 5 rows into 2 buckets: ntile(2) gives [1,1,1,2,2] (ceil(5/2)=3 in bucket 1)
            query=lambda f: f.find_all(
                [f.id_(), f.id_().qcut(2, order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="first_id_per_category",
            query=lambda f: f.find_all(
                [f.id_(), f.category(),
                 f.id_().first(partition_by=[f.category()], order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="last_id_per_category",
            # LAST_VALUE uses default frame (ROWS UNBOUNDED PRECEDING → CURRENT ROW),
            # so each row sees the last value up to itself within its partition.
            query=lambda f: f.find_all(
                [f.id_(), f.category(),
                 f.id_().last(partition_by=[f.category()], order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="rank_method_first",
            query=lambda f: f.find_all(
                [f.id_(), f.id_().rank(method="first", order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="rank_pct",
            # percent_rank() = (rank - 1) / (N - 1) for N=5
            query=lambda f: f.find_all(
                [f.id_(), f.id_().rank(pct=True, order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="rank_pct_method_max",
            # cume_dist() = rank_max / N for N=5
            query=lambda f: f.find_all(
                [f.id_(), f.id_().rank(pct=True, method="max", order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
        ),
        # --- Additional count variants ---
        TestExpectation(
            name="count_with_contains_filter",
            query=lambda f: f.find_all([f.count()], f.name().contains("Corp")),
        ),
        TestExpectation(
            name="count_finance_category",
            query=lambda f: f.find_all([f.count()], f.category().eq("Finance")),
        ),
        TestExpectation(
            name="count_id_attribute",
            query=lambda f: f.find_all([f.id_().count()]),
        ),
        TestExpectation(
            name="count_name_technology_filter",
            query=lambda f: f.find_all([f.name().count()], f.category().eq("Technology")),
        ),
        TestExpectation(
            name="count_name_ne_technology",
            query=lambda f: f.find_all([f.name().count()], f.category().ne("Technology")),
        ),
        # --- Additional sort variants ---
        TestExpectation(
            name="sort_by_id_asc",
            query=lambda f: f.find_all([f.id_()]).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="sort_by_name_desc",
            query=lambda f: f.find_all([f.name()]).order_by(f.name().descending()),
        ),
        TestExpectation(
            name="sort_by_category_asc_name_asc",
            query=lambda f: f.find_all(
                [f.name(), f.category()]
            ).order_by(f.category().ascending(), f.name().ascending()),
        ),
        # --- vector search: the distance of an embedding to a vector, and the nearest rows ---
        TestExpectation(
            name="cosine_distance_of_each_company",
            query=lambda f: f.find_all([f.id_(), f.embedding().cosine_distance(_QUERY).round(4)]).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="l2_distance_of_each_company",
            query=lambda f: f.find_all([f.id_(), f.embedding().l2_distance(_QUERY).round(4)]).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="inner_product_of_each_company",
            query=lambda f: f.find_all([f.id_(), f.embedding().inner_product(_QUERY).round(4)]).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="nearest_by_cosine_distance_is_the_closest_first",
            query=lambda f: f.find_all([f.name()]).nearest(f.embedding().cosine_distance(_QUERY), 2),
        ),
        TestExpectation(
            name="nearest_by_l2_distance_is_the_closest_first",
            query=lambda f: f.find_all([f.name()]).nearest(f.embedding().l2_distance(_QUERY), 3),
        ),
        TestExpectation(
            name="nearest_by_inner_product_is_the_largest_first",
            query=lambda f: f.find_all([f.name()]).nearest(f.embedding().inner_product(_QUERY), 2),
        ),
        TestExpectation(
            name="nearest_of_the_rows_a_filter_leaves",
            query=lambda f: f.find_all([f.name()], f.category().eq("Technology")).nearest(f.embedding().cosine_distance(_QUERY), 1),
        ),
        TestExpectation(
            name="nearest_puts_a_company_with_no_embedding_last",
            query=lambda f: f.find_all([f.name()]).nearest(f.embedding().cosine_distance(_QUERY), 5),
        ),
        TestExpectation(
            name="nearest_with_its_distance_selected",
            query=lambda f: f.find_all([f.name(), f.embedding().cosine_distance(_QUERY).round(4)]).nearest(f.embedding().cosine_distance(_QUERY), 2),
        ),
        TestExpectation(
            name="nearest_in_each_category_is_a_rank_within_the_category",
            query=lambda f: _nearest_in_each_category(f),
        ),
        TestExpectation(
            name="a_distance_is_compared_like_a_number",
            query=lambda f: f.find_all([f.name()], f.embedding().cosine_distance(_QUERY) < 0.3).order_by(f.name().ascending()),
        ),
        # --- Additional limit variants ---
        TestExpectation(
            name="limit_one",
            query=lambda f: f.find_all(
                [f.name()]
            ).order_by(f.name().ascending()).limit(1),
        ),
        TestExpectation(
            name="limit_three",
            query=lambda f: f.find_all(
                [f.name()]
            ).order_by(f.name().ascending()).limit(3),
        ),
        TestExpectation(
            name="limit_with_filter",
            query=lambda f: f.find_all(
                [f.name()], f.category().eq("Technology")
            ).order_by(f.name().ascending()).limit(1),
        ),
    ],
)
