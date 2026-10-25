"""
Ported from legend-engine core_relational ``tests/mapping/filter/testFilterMappingTree.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/filter/testFilterMappingTree.pure"


FILTER_MAPPING_TREE_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testFilterMappingWithProjection", f"{_SRC}::testFilterMappingWithProjection",
                    'select "root".name as "name" from orgTable as "root" where "root".filterVal <= 4',
                    'a class mapping filter (~filter) applied through a tree of joins'),
        unsupported("testFilterMappingWithProjectionOverlapp", f"{_SRC}::testFilterMappingWithProjectionOverlapp",
                    'select "root".name as "name", "orgtable_1".name as "p_name", "orgtable_2".name as "p_p_name" from orgTable as "root" left outer join orgTable as "orgtable_1" on ("root".parentId = "orgtable_1".id and "orgtable_1".filterVal <= 4) left outer join (select "orgtable_3".id as id, "orgtable_3".name as name from orgTable as "orgtable_3" where "orgtable_3".filterVal <= 4) as "orgtable_2" on ("orgtable_1".parentId = "orgtable_2".id) where "root".filterVal <= 4',
                    'a class mapping filter (~filter) applied through a tree of joins'),
        unsupported("testFilterMappingWithProjectionAndJoin", f"{_SRC}::testFilterMappingWithProjectionAndJoin",
                    'select "root".name as "name" from orgTable as "root" left outer join otherTable as "othertable_0" on ("root".id = "othertable_0".orgTableId) where "othertable_0".filterVal <= 4',
                    'a class mapping filter (~filter) applied through a tree of joins'),
    ],
)
