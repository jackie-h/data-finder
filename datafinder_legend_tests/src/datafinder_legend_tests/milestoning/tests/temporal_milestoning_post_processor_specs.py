"""
Ported from legend-engine core_relational ``milestoning/tests/testTemporalMilestoningPostProcessor.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "milestoning/tests/testTemporalMilestoningPostProcessor.pure"


TEMPORAL_MILESTONING_POST_PROCESSOR_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testLatestTemporalMilestoningPostProcessor", f"{_SRC}::testLatestTemporalMilestoningPostProcessor",
                    'select "root".NAME as "name", "synonymtable_0".NAME as "synonym" from productSchema.productTable as "root" left outer join productSchema.synonymTable as "synonymtable_0" on ("synonymtable_0".PRODID = "root".ID and "synonymtable_0".thru_z = \'9999-12-31 00:00:00.000\') where "root".thru_z = \'9999-12-31 00:00:00.000\'',
                    'a mapping that the port does not load'),
    ],
)
