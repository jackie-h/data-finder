"""
Test specifications for a value that can be missing, using companies_mapping.md.

Uses a dataset in which two companies have no category, which Company.category (multiplicity 0..1) allows. A
missing value is different from any value, so ``!=`` against a value matches it, and ``==``
does not. The ``OR IS NULL`` is written for any column, a required property and a primary key included.

A sort puts a missing value where Legend's database does, as the smallest value: first when ascending and last when descending.
``nulls="first"`` or ``nulls="last"`` on the sort says otherwise. The SQL says where a missing value goes for every sort, so the same
rows come in the same order on every engine.

    companies — id INT, name VARCHAR, category VARCHAR (NULL for ids 2 and 4; id 5 has no name either)
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
            expected_result=np.array([[1], [2], [4], [5]], dtype=object),
            expected_sql={"duckdb": "<> 'Finance' OR "},
        ),
        TestExpectation(
            name="ne_matches_a_missing_value",
            query=lambda f: f.find_all([f.id_()], f.category().ne("Technology")).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3], [4], [5]], dtype=object),
        ),
        TestExpectation(
            name="not_equal_on_a_function_matches_a_missing_value",
            query=lambda f: f.find_all([f.id_()], f.category().upper() != "FINANCE").order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2], [4], [5]], dtype=object),
            expected_sql={"duckdb": "UPPER(t0.category) <> 'FINANCE' OR UPPER(t0.category) IS NULL"},
        ),
        TestExpectation(
            name="not_equal_on_an_expression_matches_a_missing_value",
            query=lambda f: f.find_all([f.id_()], f.category().length() != 7).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [2], [4], [5]], dtype=object),
            expected_sql={"duckdb": "LENGTH(t0.category) <> 7 OR LENGTH(t0.category) IS NULL"},
        ),
        TestExpectation(
            name="not_equal_on_a_column_of_a_result_matches_a_missing_value",
            query=lambda f: f.find_all([f.id_(), f.category().alias("category")])
            .filter(lambda r: r.category != "Finance").order_by(f.id_().ascending()),
            expected_columns=["Id", "category"],
            expected_result=np.array([[1, "Technology"], [2, None], [4, None], [5, None]], dtype=object),
        ),
        TestExpectation(
            name="not_equal_between_two_columns_matches_a_missing_value",
            query=lambda f: f.find_all([f.id_()], f.category() != f.name()).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            # company 5 has neither, and two missing values are not different
            expected_result=np.array([[1], [2], [3], [4]], dtype=object),
            expected_sql={"duckdb": "t0.category <> t0.name OR (t0.category IS NULL AND t0.name IS NOT NULL) OR (t0.category IS NOT NULL AND t0.name IS NULL)"},
        ),
        TestExpectation(
            name="not_equal_in_a_having_matches_a_missing_value",
            query=lambda f: f.find_all([f.category(), f.id_().sum().alias("total")]).group_by(f.category())
            .having(f.category().ne("Finance")).order_by(f.id_().sum().ascending()),
            expected_columns=["Category", "total"],
            expected_result=np.array([["Technology", 1], [None, 11]], dtype=object),
            expected_sql={"duckdb": "HAVING t0.category <> 'Finance' OR t0.category IS NULL"},
        ),
        TestExpectation(
            name="equal_between_two_columns_matches_two_missing_values",
            query=lambda f: f.find_all([f.id_()], f.category() == f.name()).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            # company 5 has neither a name nor a category; a missing value is not equal to a value
            expected_result=np.array([[5]], dtype=object),
            expected_sql={"duckdb": "t0.category = t0.name OR (t0.category IS NULL AND t0.name IS NULL)"},
        ),
        TestExpectation(
            name="less_than_does_not_match_a_missing_value",
            query=lambda f: f.find_all([f.id_()], f.category().length() < 8).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[3]], dtype=object),
        ),
        TestExpectation(
            name="less_than_or_equal_does_not_match_a_missing_value",
            query=lambda f: f.find_all([f.id_()], f.category().length() <= 10).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [3]], dtype=object),
        ),
        TestExpectation(
            name="greater_than_does_not_match_a_missing_value",
            query=lambda f: f.find_all([f.id_()], f.category().length() > 7).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1]], dtype=object),
        ),
        TestExpectation(
            name="greater_than_or_equal_does_not_match_a_missing_value",
            query=lambda f: f.find_all([f.id_()], f.category().length() >= 7).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [3]], dtype=object),
        ),
        TestExpectation(
            name="ordering_two_expressions_does_not_match_a_missing_value",
            query=lambda f: f.find_all([f.id_()], f.category().length() < f.name().length()).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            # Technology is 10 long and Acme Corp 9; Finance is 7 and Gamma LLC 9; the others have a missing side
            expected_result=np.array([[3]], dtype=object),
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
            expected_result=np.array([[2], [3], [4], [5]], dtype=object),
            expected_sql={"duckdb": "<> 'Acme Corp' OR "},
        ),
        TestExpectation(
            name="not_equal_on_a_primary_key_also_has_the_null_alternative",
            query=lambda f: f.find_all([f.id_()], f.id_() != 1).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3], [4], [5]], dtype=object),
            expected_sql={"duckdb": "<> 1 OR "},
        ),
        TestExpectation(
            name="a_missing_value_sorts_first_when_ascending",
            query=lambda f: f.find_all([f.id_()]).order_by(f.category().ascending(), f.id_().ascending()),
            expected_columns=["Id"],
            # 2 and 4 have no category, then Finance (3) and Technology (1)
            expected_result=np.array([[2], [4], [5], [3], [1]], dtype=object),
            expected_sql={"duckdb": "ORDER BY t0.category ASC NULLS FIRST, t0.id ASC NULLS FIRST"},
        ),
        TestExpectation(
            name="a_missing_value_sorts_last_when_descending",
            query=lambda f: f.find_all([f.id_()]).order_by(f.category().descending(), f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [3], [2], [4], [5]], dtype=object),
            expected_sql={"duckdb": "ORDER BY t0.category DESC NULLS LAST, t0.id ASC NULLS FIRST"},
        ),
        TestExpectation(
            name="an_ascending_sort_can_put_a_missing_value_last",
            query=lambda f: f.find_all([f.id_()]).order_by(f.category().ascending(nulls="last"), f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[3], [1], [2], [4], [5]], dtype=object),
            expected_sql={"duckdb": "ORDER BY t0.category ASC NULLS LAST, t0.id ASC NULLS FIRST"},
        ),
        TestExpectation(
            name="a_descending_sort_can_put_a_missing_value_first",
            query=lambda f: f.find_all([f.id_()]).order_by(f.category().descending(nulls="first"), f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [4], [5], [1], [3]], dtype=object),
            expected_sql={"duckdb": "ORDER BY t0.category DESC NULLS FIRST, t0.id ASC NULLS FIRST"},
        ),
        TestExpectation(
            name="a_sort_says_where_a_missing_value_goes_whatever_the_column",
            query=lambda f: f.find_all([f.id_()]).order_by(f.id_().descending()),
            expected_columns=["Id"],
            expected_result=np.array([[5], [4], [3], [2], [1]], dtype=object),
            expected_sql={"duckdb": "ORDER BY t0.id DESC NULLS LAST"},
        ),
        TestExpectation(
            name="a_sort_by_something_that_is_not_a_column_says_where_a_missing_value_goes",
            query=lambda f: f.find_all([f.id_(), f.category().alias("category")]).order_by(f.category().alias("category").ascending(), f.id_().ascending()),
            expected_columns=["Id", "category"],
            expected_result=np.array([[2, None], [4, None], [5, None], [3, "Finance"], [1, "Technology"]], dtype=object),
        ),
    ],
)
