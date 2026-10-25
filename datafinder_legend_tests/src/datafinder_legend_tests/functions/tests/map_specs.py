"""
Ported from legend-engine core_relational ``functions/tests/testMap.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/testMap.pure"


MAP_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testSequenceMapWithConfusingSetImplementation", f"{_SRC}::testSequenceMapWithConfusingSetImplementation",
                    'select "root".name as "name", \'ok\' as "p_name", "orgtable_1".name as "p_p_name" from orgTable as "root" left outer join orgTable as "orgtable_1" on ("root".parentId = "orgtable_1".id and "orgtable_1".filterVal <= 4) where "root".filterVal <= 4',
                    'a mapping that the port does not load'),
    ],
)
