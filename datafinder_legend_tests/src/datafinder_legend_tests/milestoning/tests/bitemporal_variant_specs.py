"""
Ported from legend-engine core_relational ``milestoning/tests/testBiTemporalDateMilestoning.pure`` and
``testLatestDateMilestoning.pure``: bitemporal types mapped to tables that are milestoned less than the types are.

The class is bitemporal in Legend, so a query can give its properties both dates. Against a table with no milestoning
columns they are ignored and nothing filters it; against a table with a business range only, the processing date is.
``%latest`` is not ported: its two tests are written without the date, which the table ignores.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported

NONE_MAPPING = "milestoning/tests/bitemporal_none_mapping.md"
SINGLE_MAPPING = "milestoning/tests/bitemporal_single_mapping.md"

BITEMPORAL_ON_NO_MILESTONING_SPECS = FinderSpec(
    finder_name="OrderFinder",
    mapping_file=NONE_MAPPING,
    expectations=[
        ported("testMultipleBiTemporalPropertyUsageInProjectWithMilestoningInfinityNotSpecifiedInDB", "milestoning/tests/testBiTemporalDateMilestoning.pure::testMultipleBiTemporalPropertyUsageInProjectWithMilestoningInfinityNotSpecifiedInDB",
               'select "productclassificationtablenomilestoning_0".type as "type" from OrderTable as "root" left outer join ProductTableNoMilestoning as "producttablenomilestoning_0" on ("root".prodFk = "producttablenomilestoning_0".id) left outer join ProductClassificationTableNoMilestoning as "productclassificationtablenomilestoning_0" on ("producttablenomilestoning_0".type = "productclassificationtablenomilestoning_0".type)',
               lambda f: f.find_all([f.bi_temporal_product().bi_temporal_classification().type_()])),
    ],
)

BITEMPORAL_ON_SINGLE_TEMPORAL_SPECS = FinderSpec(
    finder_name="BiTemporalProductFinder",
    mapping_file=SINGLE_MAPPING,
    expectations=[
        ported("testBiTemporalQueryMappedToSingleTemporalStore", "milestoning/tests/testBiTemporalDateMilestoning.pure::testBiTemporalQueryMappedToSingleTemporalStore",
               'select "root".name as "productName", "productclassificationtable_0".type as "classificationType" from ProductTable as "root" left outer join ProductClassificationTable as "productclassificationtable_0" on ("root".type = "productclassificationtable_0".type and ("productclassificationtable_0".from_z <= \'2017-06-10\' and "productclassificationtable_0".thru_z > \'2017-06-10\')) where "root".from_z <= \'2017-06-10\' and "root".thru_z > \'2017-06-10\'',
               lambda f: f.find_all("2017-06-10", "2017-06-11", [f.name(), f.bi_temporal_classification().type_()])),
    ],
)
