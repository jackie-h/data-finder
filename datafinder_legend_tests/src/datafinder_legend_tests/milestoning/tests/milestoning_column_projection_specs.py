"""
Ported from legend-engine core_relational ``milestoning/tests/testMilestoningColumnProjection.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "milestoning/tests/testMilestoningColumnProjection.pure"


MILESTONING_COLUMN_PROJECTION_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testMilestoningColumnProjectionForRoot", f"{_SRC}::testMilestoningColumnProjectionForRoot",
                    'select "root".id as "id", "root".from_z as "from", "root".thru_z as "thru" from ProductTable as "root"',
                    'projecting the milestoning columns themselves'),
        unsupported("testMilestoningColumnProjectionForEmbedded", f"{_SRC}::testMilestoningColumnProjectionForEmbedded",
                    'select "root".type as "type", "root".from_z as "from", "root".thru_z as "thru" from ProductTable as "root"',
                    'projecting the milestoning columns themselves'),
        unsupported("testMilestoningColumnProjectionForInlineEmbedded", f"{_SRC}::testMilestoningColumnProjectionForInlineEmbedded",
                    'select "root".exchange as "name", "root".from_z as "from", "root".thru_z as "thru" from ProductTable as "root"',
                    'projecting the milestoning columns themselves'),
        unsupported("testMilestoningColumnProjectionWithNonMilestonedTable", f"{_SRC}::testMilestoningColumnProjectionWithNonMilestonedTable",
                    'select "root".id as "id", null as "from", null as "thru" from ProductTableNoMilestoning as "root"',
                    'projecting the milestoning columns themselves'),
    ],
)
