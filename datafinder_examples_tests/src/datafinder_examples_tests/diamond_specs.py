"""
Test specifications for diamond inheritance (diamond_mapping.md).

Diamond hierarchy: Record extends Trackable & Versioned, both extend Auditable.
All fields are stored in a single flat table.

    records.items — item_id INT, created_at VARCHAR, updated_at VARCHAR,
                    version INT, record_name VARCHAR
"""

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

DIAMOND_MAPPING = "diamond_mapping.md"

DIAMOND_FINDER_SPECS = FinderSpec(
    finder_name="RecordFinder",
    mapping_file=DIAMOND_MAPPING,
    expectations=[
        TestExpectation(
            name="all_records",
            query=lambda f: f.find_all([f.id_(), f.record_name()]).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="filter_by_auditable_id",
            query=lambda f: f.find_all([f.record_name()], f.id_().eq(2)),
        ),
        TestExpectation(
            name="filter_by_created_at",
            query=lambda f: f.find_all([f.record_name()], f.created_at().eq("2024-03-01")),
        ),
        TestExpectation(
            name="filter_by_updated_at",
            query=lambda f: f.find_all([f.record_name()], f.updated_at().eq("2024-06-01")),
        ),
        TestExpectation(
            name="filter_by_version",
            query=lambda f: f.find_all(
                [f.record_name(), f.version()], f.version().eq(1)
            ),
        ),
        TestExpectation(
            name="full_hierarchy_projection",
            query=lambda f: f.find_all(
                [f.id_(), f.created_at(), f.updated_at(), f.version(), f.record_name()],
                f.record_name().eq("Alpha"),
            ),
        ),
    ],
)
