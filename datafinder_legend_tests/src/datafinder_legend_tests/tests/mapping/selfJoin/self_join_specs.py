"""
Ported from legend-engine core_relational ``tests/mapping/selfJoin/selfJoin.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/selfJoin/selfJoin.pure"


SELF_JOIN_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testSelfJoinPropertyMappingOverlap", f"{_SRC}::testSelfJoinPropertyMappingOverlap",
                    'select "root".name as "name", "orgtable_1".name as "p_name", "orgtable_2".name as "p_p_name" from orgTable as "root" left outer join orgTable as "orgtable_1" on ("root".parentId = "orgtable_1".id) left outer join orgTable as "orgtable_2" on ("orgtable_1".parentId = "orgtable_2".id)',
                    'a self join with a filter in the class mapping'),
        unsupported("testSelfJoinPropertyMappingWithDynaFunction", f"{_SRC}::testSelfJoinPropertyMappingWithDynaFunction",
                    'select "root".name as "orgName", "orgtable_3".name as "3rd parent", "orgtable_4".name as "4th parent", "orgtable_4".name is null and "orgtable_3".name is null as "dyna operation" from orgTable as "root" left outer join orgTable as "orgtable_1" on ("root".parentId = "orgtable_1".id) left outer join orgTable as "orgtable_2" on ("orgtable_1".parentId = "orgtable_2".id) left outer join orgTable as "orgtable_3" on ("orgtable_2".parentId = "orgtable_3".id) left outer join orgTable as "orgtable_4" on ("orgtable_3".parentId = "orgtable_4".id)',
                    'a self join with a filter in the class mapping'),
    ],
)
