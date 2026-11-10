"""
Test specifications for a value that can be missing, using companies_mapping.md.

Uses a dataset in which two companies have no category, which Company.category (multiplicity 0..1) allows. As in
Legend, a missing value is different from any value, so ``!=`` matches it, and ``==`` does not.

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
            name="not_equal_on_a_required_value_is_a_plain_comparison",
            query=lambda f: f.find_all([f.id_()], f.name() != "Acme Corp").order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[2], [3], [4]], dtype=object),
        ),
    ],
)
