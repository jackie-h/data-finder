"""
Ported from legend-engine core_relational ``milestoning/tests/testBiTemporalDateMilestoning.pure``.

Legend's ``X.all(processingDate, businessDate)`` is ``find_all(business_date, processing_datetime, ...)`` here, and
a date propagates to every milestoned table the query joins to. Legend's per-property dates
(``classification(%date)``) and non-milestoned roots reaching a milestoned property have no equivalent yet.
Mapping: milestoning/tests/milestoning_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "milestoning/tests/testBiTemporalDateMilestoning.pure"

BITEMPORAL_PRODUCT_SPECS = FinderSpec(
    finder_name="BiTemporalProductFinder",
    mapping_file="milestoning/tests/milestoning_mapping.md",
    expectations=[
        ported("testBiTemporalToBiTemporalDatePropagationForAll",
               f"{_SRC}::testBiTemporalToBiTemporalDatePropagationForAll",
               'select "bitemporalproductclassificationtable_0".type as "type" from BiTemporalProductTable as "root" '
               'left outer join BiTemporalProductClassificationTable as "bitemporalproductclassificationtable_0" '
               'on ("root".type = "bitemporalproductclassificationtable_0".type and '
               '("bitemporalproductclassificationtable_0".in_z <= \'2017-06-11\' and '
               '"bitemporalproductclassificationtable_0".out_z > \'2017-06-11\' and '
               '"bitemporalproductclassificationtable_0".from_z <= \'2017-06-10\' and '
               '"bitemporalproductclassificationtable_0".thru_z > \'2017-06-10\')) '
               'where "root".in_z <= \'2017-06-11\' and "root".out_z > \'2017-06-11\' and '
               '"root".from_z <= \'2017-06-10\' and "root".thru_z > \'2017-06-10\'',
               lambda f: f.find_all("2017-06-10", "2017-06-11", [f.bi_temporal_classification().type_()])),
        ported("testPopulationOfMilestonedThisBiTemporalDatesInProject",
               f"{_SRC}::testPopulationOfMilestonedThisBiTemporalDatesInProject",
               'select "root".id as "id", "bitemporalproductclassificationtable_0".type as "classificationType" '
               'from BiTemporalProductTable as "root" '
               'left outer join BiTemporalProductClassificationTable as "bitemporalproductclassificationtable_0" '
               'on ("root".type = "bitemporalproductclassificationtable_0".type and '
               '("bitemporalproductclassificationtable_0".in_z <= \'9999-12-31\' and '
               '"bitemporalproductclassificationtable_0".out_z > \'9999-12-31\' and '
               '"bitemporalproductclassificationtable_0".from_z <= \'2015-10-16\' and '
               '"bitemporalproductclassificationtable_0".thru_z > \'2015-10-16\')) '
               'where "root".in_z <= \'9999-12-31\' and "root".out_z > \'9999-12-31\' and '
               '"root".from_z <= \'2015-10-16\' and "root".thru_z > \'2015-10-16\'',
               lambda f: f.find_all("2015-10-16", "9999-12-31",
                                    [f.id_(), f.bi_temporal_classification().type_()])),
        unsupported("testBiTemporalToBiTemporalSingleDatePropagationForAll",
                    f"{_SRC}::testBiTemporalToBiTemporalSingleDatePropagationForAll",
                    'select "bitemporalproductclassificationtable_0".type as "type" from BiTemporalProductTable as "root" '
                    'left outer join BiTemporalProductClassificationTable as "bitemporalproductclassificationtable_0" '
                    'on ("root".type = "bitemporalproductclassificationtable_0".type and '
                    '("bitemporalproductclassificationtable_0".in_z <= \'2017-06-11\' and '
                    '"bitemporalproductclassificationtable_0".out_z > \'2017-06-11\' and '
                    '"bitemporalproductclassificationtable_0".from_z <= \'2017-06-10\' and '
                    '"bitemporalproductclassificationtable_0".thru_z > \'2017-06-10\')) '
                    'where "root".in_z <= \'2017-06-11\' and "root".out_z > \'2017-06-11\' and '
                    '"root".from_z <= \'2017-06-10\' and "root".thru_z > \'2017-06-10\'',
                    "a milestoned property cannot be given its own date (classification(%2017-6-12))"),
    ],
)
