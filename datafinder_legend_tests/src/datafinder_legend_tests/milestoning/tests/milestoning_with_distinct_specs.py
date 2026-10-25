"""
Ported from legend-engine core_relational ``milestoning/tests/testMilestoningWithDistinct.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "milestoning/tests/testMilestoningWithDistinct.pure"


MILESTONING_WITH_DISTINCT_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testDistinctUsageAtRootOfMilestonedQuery", f"{_SRC}::testDistinctUsageAtRootOfMilestonedQuery",
                    'select "root".id as "id", "root".name as "name", "root".type as "type" from (select distinct "producttable_1".id as id, "producttable_1".name as name, "producttable_1".type as type, \'2015-08-16\' as "k_businessDate" from ProductTable as "producttable_1" where "producttable_1".from_z <= \'2015-08-16\' and "producttable_1".thru_z > \'2015-08-16\') as "root"',
                    'a mapping that the port does not load'),
    ],
)
