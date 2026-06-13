"""
Test specifications for diamond inheritance (diamond_mapping.md).

Diamond hierarchy: Record extends Trackable & Versioned, both extend Auditable.
All fields are stored in a single flat table.

    records.items — item_id INT, created_at VARCHAR, updated_at VARCHAR,
                    version INT, record_name VARCHAR
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

DIAMOND_MAPPING = "diamond_mapping.md"

DIAMOND_ITEMS = [
    (1, "2024-01-01", "2024-06-01", 3, "Alpha"),
    (2, "2024-02-01", "2024-07-01", 1, "Beta"),
    (3, "2024-03-01", "2024-03-01", 2, "Gamma"),
]

DIAMOND_FINDER_SPECS = FinderSpec(
    finder_name="RecordFinder",
    mapping_file=DIAMOND_MAPPING,
    expectations=[
        TestExpectation(
            name="all_records",
            query=lambda f: f.find_all(None, None, [f.id_(), f.record_name()]).order_by(f.id_().ascending()),
            expected_columns=["Id", "Record Name"],
            expected_result=np.array([[1, "Alpha"], [2, "Beta"], [3, "Gamma"]], dtype=object),
        ),
        TestExpectation(
            name="filter_by_auditable_id",
            query=lambda f: f.find_all(None, None, [f.record_name()], f.id_().eq(2)),
            expected_columns=["Record Name"],
            expected_result=np.array([["Beta"]], dtype=object),
        ),
        TestExpectation(
            name="filter_by_created_at",
            query=lambda f: f.find_all(None, None, [f.record_name()], f.created_at().eq("2024-03-01")),
            expected_columns=["Record Name"],
            expected_result=np.array([["Gamma"]], dtype=object),
        ),
        TestExpectation(
            name="filter_by_updated_at",
            query=lambda f: f.find_all(None, None, [f.record_name()], f.updated_at().eq("2024-06-01")),
            expected_columns=["Record Name"],
            expected_result=np.array([["Alpha"]], dtype=object),
        ),
        TestExpectation(
            name="filter_by_version",
            query=lambda f: f.find_all(
                None, None, [f.record_name(), f.version()], f.version().eq(1)
            ),
            expected_columns=["Record Name", "Version"],
            expected_result=np.array([["Beta", 1]], dtype=object),
        ),
        TestExpectation(
            name="full_hierarchy_projection",
            query=lambda f: f.find_all(
                None, None,
                [f.id_(), f.created_at(), f.updated_at(), f.version(), f.record_name()],
                f.record_name().eq("Alpha"),
            ),
            expected_columns=["Id", "Created At", "Updated At", "Version", "Record Name"],
            expected_result=np.array([[1, "2024-01-01", "2024-06-01", 3, "Alpha"]], dtype=object),
        ),
    ],
)
