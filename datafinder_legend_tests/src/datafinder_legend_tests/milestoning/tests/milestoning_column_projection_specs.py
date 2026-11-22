"""
Ported from legend-engine core_relational ``milestoning/tests/testMilestoningColumnProjection.pure``.

Legend's ``milestoning.from`` and ``milestoning.thru`` are the finder's ``business_from()`` and ``business_thru()``, which read the
columns, and ``allVersions()`` is a query with no business date (``find_all(None, None, ...)``). The milestoning of an object embedded in
a product's row is the row's own columns, so it is read from the product. The root runs on milestoning/tests/business_date_mapping.md,
the embedded one on milestoning/tests/embedded_system_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "milestoning/tests/testMilestoningColumnProjection.pure"


_SIMPLE = "tests/simple_mapping.md"

MILESTONING_COLUMN_PROJECTION_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file=_SIMPLE,
    expectations=[
        unsupported("testMilestoningColumnProjectionWithNonMilestonedTable", f"{_SRC}::testMilestoningColumnProjectionWithNonMilestonedTable",
                    'select "root".id as "id", null as "from", null as "thru" from ProductTableNoMilestoning as "root"',
                    'projecting the milestoning columns of a class that is not milestoned: it has no business_from() or business_thru(), and Legend gives NULL for them'),
    ],
)

MILESTONING_COLUMN_PROJECTION_ROOT_SPECS = FinderSpec(
    finder_name="ProductFinder",
    mapping_file="milestoning/tests/business_date_mapping.md",
    expectations=[
        ported("testMilestoningColumnProjectionForRoot", f"{_SRC}::testMilestoningColumnProjectionForRoot",
               'select "root".id as "id", "root".from_z as "from", "root".thru_z as "thru" from ProductTable as "root"',
               lambda f: f.find_all(None, None, [f.id_().alias("id"), f.business_from().alias("from"), f.business_thru().alias("thru")])),
    ],
)

MILESTONING_COLUMN_PROJECTION_EMBEDDED_SPECS = FinderSpec(
    finder_name="ProductFinder",
    mapping_file="milestoning/tests/embedded_system_mapping.md",
    expectations=[
        ported("testMilestoningColumnProjectionForEmbedded", f"{_SRC}::testMilestoningColumnProjectionForEmbedded",
               'select "root".type as "type", "root".from_z as "from", "root".thru_z as "thru" from ProductTable as "root"',
               lambda f: f.find_all(None, None, [f.classification().type_().alias("type"), f.business_from().alias("from"), f.business_thru().alias("thru")])),
        ported("testMilestoningColumnProjectionForInlineEmbedded", f"{_SRC}::testMilestoningColumnProjectionForInlineEmbedded",
               'select "root".exchange as "name", "root".from_z as "from", "root".thru_z as "thru" from ProductTable as "root"',
               lambda f: f.find_all(None, None, [f.exchange().name().alias("name"), f.business_from().alias("from"), f.business_thru().alias("thru")])),
    ],
)
