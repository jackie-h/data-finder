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
        TestExpectation(
            name="count_all",
            query=lambda f: f.find_all(None, None, [f.count()]),
            expected_columns=["Count"],
            expected_result=np.array([[5]], dtype=object),
        ),
        TestExpectation(
            name="count_technology",
            query=lambda f: f.find_all(
                None, None, [f.count()], f.category().eq("Technology")
            ),
            expected_columns=["Count"],
            expected_result=np.array([[2]], dtype=object),
        ),
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
        TestExpectation(
            name="limit_two_by_name",
            query=lambda f: f.find_all(
                None, None, [f.name()]
            ).order_by(f.name().ascending()).limit(2),
            expected_columns=["Name"],
            expected_result=np.array([["Acme Corp"], ["Acme Industries"]], dtype=object),
        ),
    ],
)
