"""
Ported from legend-engine core_relational ``milestoning/tests/testLatestDateMilestoning.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "milestoning/tests/testLatestDateMilestoning.pure"


LATEST_DATE_MILESTONING_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testLatestIgnoredForNonMilestonedMappedBiTemporalClassesWithProject", f"{_SRC}::testLatestIgnoredForNonMilestonedMappedBiTemporalClassesWithProject",
                    'select "producttablenomilestoning_0".type as "productType" from OrderTable as "root" left outer join ProductTableNoMilestoning as "producttablenomilestoning_0" on ("root".prodFk = "producttablenomilestoning_0".id)',
                    'a mapping that the port does not load'),
    ],
)
