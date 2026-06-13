"""
Test specifications for the companies domain (companies_mapping.md).

Backends must seed their database with COMPANIES before running these specs.

    companies — id INT, name VARCHAR, category VARCHAR
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

COMPANIES_MAPPING = "companies_mapping.md"

COMPANIES = [
    (1, "Acme Corp", "Technology"),
    (2, "Acme Industries", "Manufacturing"),
    (3, "Beta Corp", "Technology"),
    (4, "Gamma LLC", "Finance"),
    (5, "Delta Ltd", "Finance"),
]

COMPANY_FINDER_SPECS = FinderSpec(
    finder_name="CompanyFinder",
    mapping_file=COMPANIES_MAPPING,
    expectations=[
        # --- Count ---
        TestExpectation(
            name="count_all",
            query=lambda f: f.find_all(None, None, [f.count()]),
            expected_columns=["Count"],
            expected_result=np.array([[5]], dtype=object),
        ),
        TestExpectation(
            name="count_technology",
            query=lambda f: f.find_all(None, None, [f.count()], f.category().eq("Technology")),
            expected_columns=["Count"],
            expected_result=np.array([[2]], dtype=object),
        ),
        TestExpectation(
            name="count_no_matches",
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
            name="string_ne_excludes_row",
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
            name="string_category_starts_with_fin",
            query=lambda f: f.find_all(
                None, None, [f.name()], f.category().starts_with("Fin")
            ).order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array([["Delta Ltd"], ["Gamma LLC"]], dtype=object),
        ),
        TestExpectation(
            name="string_category_ne_technology",
            query=lambda f: f.find_all(
                None, None, [f.count()], f.category().ne("Technology")
            ),
            expected_columns=["Count"],
            expected_result=np.array([[3]], dtype=object),
        ),
        # --- Window functions ---
        TestExpectation(
            name="rank_min_by_id",
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.id_().rank(method="min", order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Rank"],
            expected_result=np.array([[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]], dtype=object),
        ),
        TestExpectation(
            name="rank_dense_with_partition_by_category",
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
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.id_().shift(1, order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Lag Id"],
            # Row 1 has no predecessor → NaN; rows 2-5 have the previous id.
            # dtype=float because pandas/DuckDB encodes NaN-containing int cols as float64.
            expected_result=np.array(
                [[1, np.nan], [2, 1], [3, 2], [4, 3], [5, 4]], dtype=float
            ),
        ),
        TestExpectation(
            name="shift_lead_last_row",
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.id_().shift(-1, order_by=[f.id_().ascending()])],
            ).order_by(f.id_().ascending()),
            expected_columns=["Id", "Lead Id"],
            # Rows 1-4 see the next id; row 5 has no successor → NaN.
            # dtype=float because pandas/DuckDB encodes NaN-containing int cols as float64.
            expected_result=np.array(
                [[1, 2], [2, 3], [3, 4], [4, 5], [5, np.nan]], dtype=float
            ),
        ),
        TestExpectation(
            name="qcut_two_buckets",
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
    ],
)
