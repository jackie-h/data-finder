"""
Test specifications for a value that can be missing, using companies_mapping.md.

Uses a dataset in which two companies have no category, which Company.category (multiplicity 0..1) allows. A
missing value is different from any value, so ``!=`` against a value matches it, and ``==``
does not. The ``OR IS NULL`` is written for any column, a required property and a primary key included.

A sort puts a missing value where Legend's database does, as the smallest value: first when ascending and last when descending.
``nulls="first"`` or ``nulls="last"`` on the sort says otherwise, so the same rows come in the same order on every engine.

    companies — id INT, name VARCHAR, category VARCHAR (NULL for ids 2 and 4)
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

COMPANIES_MISSING_CATEGORY_SPECS = FinderSpec(
    finder_name="CompanyFinder",
    mapping_file="companies_mapping.md",
    expectations=[
        TestExpectation(
            name="not_equal_matches_a_missing_value",
            query=lambda f: f.find_all([f.id_()], f.category() != "Finance").order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2], [4]], dtype=object),
            expected_sql={"duckdb": "<> 'Finance' OR "},
        ),
        TestExpectation(
            name="ne_matches_a_missing_value",
            query=lambda f: f.find_all([f.id_()], f.category().ne("Technology")).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3], [4]], dtype=object),
        ),
        TestExpectation(
            name="equal_does_not_match_a_missing_value",
            query=lambda f: f.find_all([f.id_()], f.category().eq("Finance")).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[3]], dtype=object),
        ),
        TestExpectation(
            name="not_equal_with_another_condition",
            query=lambda f: f.find_all([f.id_()], f.category().ne("Finance").and_(f.id_() < 3))
            .order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2]], dtype=object),
            expected_sql={"duckdb": "(t0.category <> 'Finance' OR t0.category IS NULL) AND"},
        ),
        TestExpectation(
            name="not_equal_on_a_required_property_also_has_the_null_alternative",
            query=lambda f: f.find_all([f.id_()], f.name() != "Acme Corp").order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3], [4]], dtype=object),
            expected_sql={"duckdb": "<> 'Acme Corp' OR "},
        ),
        TestExpectation(
            name="not_equal_on_a_primary_key_also_has_the_null_alternative",
            query=lambda f: f.find_all([f.id_()], f.id_() != 1).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3], [4]], dtype=object),
            expected_sql={"duckdb": "<> 1 OR "},
        ),
        TestExpectation(
            name="a_missing_value_sorts_first_when_ascending",
            query=lambda f: f.find_all([f.id_()]).order_by(f.category().ascending(), f.id_().ascending()),
            expected_columns=["Id"],
            # 2 and 4 have no category, then Finance (3) and Technology (1)
            expected_result=np.array([[2], [4], [3], [1]], dtype=object),
            expected_sql={"duckdb": "ORDER BY t0.category ASC NULLS FIRST, t0.id ASC"},
        ),
        TestExpectation(
            name="a_missing_value_sorts_last_when_descending",
            query=lambda f: f.find_all([f.id_()]).order_by(f.category().descending(), f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [3], [2], [4]], dtype=object),
            expected_sql={"duckdb": "ORDER BY t0.category DESC NULLS LAST, t0.id ASC"},
        ),
        TestExpectation(
            name="an_ascending_sort_can_put_a_missing_value_last",
            query=lambda f: f.find_all([f.id_()]).order_by(f.category().ascending(nulls="last"), f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[3], [1], [2], [4]], dtype=object),
            expected_sql={"duckdb": "ORDER BY t0.category ASC NULLS LAST, t0.id ASC"},
        ),
        TestExpectation(
            name="a_descending_sort_can_put_a_missing_value_first",
            query=lambda f: f.find_all([f.id_()]).order_by(f.category().descending(nulls="first"), f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [4], [1], [3]], dtype=object),
            expected_sql={"duckdb": "ORDER BY t0.category DESC NULLS FIRST, t0.id ASC"},
        ),
        TestExpectation(
            name="a_sort_on_a_column_that_cannot_be_missing_says_nothing_of_a_missing_value",
            query=lambda f: f.find_all([f.id_()]).order_by(f.id_().descending()),
            expected_columns=["Id"],
            expected_result=np.array([[4], [3], [2], [1]], dtype=object),
            expected_sql={"duckdb": "ORDER BY t0.id DESC"},
            unexpected_sql={"duckdb": "NULLS"},
        ),
        TestExpectation(
            name="a_sort_by_something_that_is_not_a_column_says_where_a_missing_value_goes",
            query=lambda f: f.find_all([f.id_(), f.category().alias("category")]).order_by(f.category().alias("category").ascending(), f.id_().ascending()),
            expected_columns=["Id", "category"],
            expected_result=np.array([[2, None], [4, None], [3, "Finance"], [1, "Technology"]], dtype=object),
        ),
    ],
)
