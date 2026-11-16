"""
Ported from legend-engine core_relational ``tests/mapping/union/testUnionWithPartialForeignKeyUsage.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/union/testUnionWithPartialForeignKeyUsage.pure"


UNION_WITH_PARTIAL_FOREIGN_KEY_USAGE_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testUnionWithPartialForeignKeyUsage1", f"{_SRC}::testUnionWithPartialForeignKeyUsage1",
                    '',
                    'Legend marks this test ToFix: it asserts an empty SQL'),
        unsupported("testUnionWithPartialForeignKeyUsage2", f"{_SRC}::testUnionWithPartialForeignKeyUsage2",
                    '',
                    'Legend marks this test ToFix: it asserts an empty SQL'),
    ],
)
