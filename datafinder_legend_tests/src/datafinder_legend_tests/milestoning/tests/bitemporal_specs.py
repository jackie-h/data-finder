"""
Ported from legend-engine core_relational ``milestoning/tests/testBiTemporalDateMilestoning.pure``.

Legend's ``X.all(processingDate, businessDate)`` is ``find_all(business_date, processing_datetime, ...)`` here, and
a date propagates to every milestoned table the query joins to. Legend's per-property dates
(``classification(%date)``) and non-milestoned roots reaching a milestoned property have no equivalent yet.
Mapping: milestoning/tests/milestoning_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported, BACKEND

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
        ported("testBiTemporalToBiTemporalSingleDatePropagationForAll",
               f"{_SRC}::testBiTemporalToBiTemporalSingleDatePropagationForAll",
               'select "bitemporalproductclassificationtable_0".type as "type" from BiTemporalProductTable as "root" '
               'left outer join BiTemporalProductClassificationTable as "bitemporalproductclassificationtable_0" '
               'on ("root".type = "bitemporalproductclassificationtable_0".type and '
               '("bitemporalproductclassificationtable_0".in_z <= \'2017-06-11\' and '
               '"bitemporalproductclassificationtable_0".out_z > \'2017-06-11\' and '
               '"bitemporalproductclassificationtable_0".from_z <= \'2017-06-12\' and '
               '"bitemporalproductclassificationtable_0".thru_z > \'2017-06-12\')) '
               'where "root".in_z <= \'2017-06-11\' and "root".out_z > \'2017-06-11\' and '
               '"root".from_z <= \'2017-06-10\' and "root".thru_z > \'2017-06-10\'',
               lambda f: f.find_all("2017-06-10", "2017-06-11",
                                    [f.bi_temporal_classification(business_date="2017-06-12").type_()])),
    ],
)


BI_TEMPORAL_DATE_MILESTONING_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testBiTemporalPropertyUsageInProject", f"{_SRC}::testBiTemporalPropertyUsageInProject",
                    'select "bitemporalproducttable_0".id as "prodId" from OrderTable as "root" left outer join BiTemporalProductTable as "bitemporalproducttable_0" on ("root".prodFk = "bitemporalproducttable_0".id and ("bitemporalproducttable_0".in_z <= \'2017-06-10\' and "bitemporalproducttable_0".out_z > \'2017-06-10\' and "bitemporalproducttable_0".from_z <= \'2017-06-09\' and "bitemporalproducttable_0".thru_z > \'2017-06-09\'))',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testBiTemporalPropertyUsageInProjectPath", f"{_SRC}::testBiTemporalPropertyUsageInProjectPath",
                    'select "bitemporalproducttable_0".id as "id" from OrderTable as "root" left outer join BiTemporalProductTable as "bitemporalproducttable_0" on ("root".prodFk = "bitemporalproducttable_0".id and ("bitemporalproducttable_0".in_z <= \'2017-06-10\' and "bitemporalproducttable_0".out_z > \'2017-06-10\' and "bitemporalproducttable_0".from_z <= \'2017-06-09\' and "bitemporalproducttable_0".thru_z > \'2017-06-09\'))',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testMultipleBiTemporalPropertyUsageInProject", f"{_SRC}::testMultipleBiTemporalPropertyUsageInProject",
                    'select "bitemporalproductclassificationtable_0".type as "type" from OrderTable as "root" left outer join BiTemporalProductTable as "bitemporalproducttable_0" on ("root".prodFk = "bitemporalproducttable_0".id and ("bitemporalproducttable_0".in_z <= \'2017-06-10\' and "bitemporalproducttable_0".out_z > \'2017-06-10\' and "bitemporalproducttable_0".from_z <= \'2017-06-09\' and "bitemporalproducttable_0".thru_z > \'2017-06-09\')) left outer join (select "bitemporalproductclassificationtable_1".type as type from BiTemporalProductClassificationTable as "bitemporalproductclassificationtable_1" where ("bitemporalproductclassificationtable_1".in_z <= \'2017-06-11\' and "bitemporalproductclassificationtable_1".out_z > \'2017-06-11\' and "bitemporalproductclassificationtable_1".from_z <= \'2017-06-10\' and "bitemporalproductclassificationtable_1".thru_z > \'2017-06-10\')) as "bitemporalproductclassificationtable_0" on ("bitemporalproducttable_0".type = "bitemporalproductclassificationtable_0".type)',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testMultipleBiTemporalPropertyUsageInProjectWithMilestoningInfinityNotSpecifiedInDB", f"{_SRC}::testMultipleBiTemporalPropertyUsageInProjectWithMilestoningInfinityNotSpecifiedInDB",
                    'select "productclassificationtablenomilestoning_0".type as "type" from OrderTable as "root" left outer join ProductTableNoMilestoning as "producttablenomilestoning_0" on ("root".prodFk = "producttablenomilestoning_0".id) left outer join ProductClassificationTableNoMilestoning as "productclassificationtablenomilestoning_0" on ("producttablenomilestoning_0".type = "productclassificationtablenomilestoning_0".type)',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testMultipleBiTemporalPropertyUsageInProjectWithMilestoningInfinitySpecifiedInDB", f"{_SRC}::testMultipleBiTemporalPropertyUsageInProjectWithMilestoningInfinitySpecifiedInDB",
                    'select "bitemporalproductclassificationtablewithlatest_0".type as "type" from OrderTable as "root" left outer join BiTemporalProductTableWithLatest as "bitemporalproducttablewithlatest_0" on ("root".prodFk = "bitemporalproducttablewithlatest_0".id and ("bitemporalproducttablewithlatest_0".out_z = \'9999-12-31 00:00:00.0000\' and "bitemporalproducttablewithlatest_0".thru_z = \'9999-12-31\')) left outer join (select "bitemporalproductclassificationtablewithlatest_1".type as type from BiTemporalProductClassificationTableWithLatest as "bitemporalproductclassificationtablewithlatest_1" where ("bitemporalproductclassificationtablewithlatest_1".out_z = \'9999-12-31 00:00:00.0000\' and "bitemporalproductclassificationtablewithlatest_1".thru_z = \'9999-12-31 00:00:00.0000\')) as "bitemporalproductclassificationtablewithlatest_0" on ("bitemporalproducttablewithlatest_0".type = "bitemporalproductclassificationtablewithlatest_0".type)',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testMultipleBiTemporalPropertyUsageInProjectPathWithMilestoningInfinitySpecifiedInDBPlusLatestPropagation", f"{_SRC}::testMultipleBiTemporalPropertyUsageInProjectPathWithMilestoningInfinitySpecifiedInDBPlusLatestPropagation",
                    'select "bitemporalproductclassificationtablewithlatest_0".type as "type" from OrderTable as "root" left outer join BiTemporalProductTableWithLatest as "bitemporalproducttablewithlatest_0" on ("root".prodFk = "bitemporalproducttablewithlatest_0".id and ("bitemporalproducttablewithlatest_0".out_z = \'9999-12-31 00:00:00.0000\' and "bitemporalproducttablewithlatest_0".thru_z = \'9999-12-31\')) left outer join (select "bitemporalproductclassificationtablewithlatest_1".type as type from BiTemporalProductClassificationTableWithLatest as "bitemporalproductclassificationtablewithlatest_1" where ("bitemporalproductclassificationtablewithlatest_1".out_z = \'9999-12-31 00:00:00.0000\' and "bitemporalproductclassificationtablewithlatest_1".thru_z = \'9999-12-31 00:00:00.0000\')) as "bitemporalproductclassificationtablewithlatest_0" on ("bitemporalproducttablewithlatest_0".type = "bitemporalproductclassificationtablewithlatest_0".type)',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testMultipleBiTemporalPropertyUsageInProjectWithMilestoningInfinitySpecifiedInDBPlusLatestPropagation", f"{_SRC}::testMultipleBiTemporalPropertyUsageInProjectWithMilestoningInfinitySpecifiedInDBPlusLatestPropagation",
                    'select "bitemporalproductclassificationtablewithlatest_0".type as "type" from OrderTable as "root" left outer join BiTemporalProductTableWithLatest as "bitemporalproducttablewithlatest_0" on ("root".prodFk = "bitemporalproducttablewithlatest_0".id and ("bitemporalproducttablewithlatest_0".out_z = \'9999-12-31 00:00:00.0000\' and "bitemporalproducttablewithlatest_0".thru_z = \'9999-12-31\')) left outer join (select "bitemporalproductclassificationtablewithlatest_1".type as type from BiTemporalProductClassificationTableWithLatest as "bitemporalproductclassificationtablewithlatest_1" where ("bitemporalproductclassificationtablewithlatest_1".out_z = \'9999-12-31 00:00:00.0000\' and "bitemporalproductclassificationtablewithlatest_1".thru_z = \'9999-12-31 00:00:00.0000\')) as "bitemporalproductclassificationtablewithlatest_0" on ("bitemporalproducttablewithlatest_0".type = "bitemporalproductclassificationtablewithlatest_0".type)',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testPopulationOfMilestonedThisBiTemporalDatesInProjectAgainstNonMilestonedStore", f"{_SRC}::testPopulationOfMilestonedThisBiTemporalDatesInProjectAgainstNonMilestonedStore",
                    'select "root".id as "id", "productclassificationtablenomilestoning_0".type as "classificationType" from ProductTableNoMilestoning as "root" left outer join ProductClassificationTableNoMilestoning as "productclassificationtablenomilestoning_0" on ("root".type = "productclassificationtablenomilestoning_0".type)',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testBiTemporalQueryMappedToSingleTemporalStore", f"{_SRC}::testBiTemporalQueryMappedToSingleTemporalStore",
                    'select "root".name as "productName", "productclassificationtable_0".type as "classificationType" from ProductTable as "root" left outer join ProductClassificationTable as "productclassificationtable_0" on ("root".type = "productclassificationtable_0".type and ("productclassificationtable_0".from_z <= \'2017-06-10\' and "productclassificationtable_0".thru_z > \'2017-06-10\')) where "root".from_z <= \'2017-06-10\' and "root".thru_z > \'2017-06-10\'',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testBiTemporalToBiTemporalDatePropagation", f"{_SRC}::testBiTemporalToBiTemporalDatePropagation",
                    'select "bitemporalproductclassificationtable_0".type as "type" from OrderTable as "root" left outer join BiTemporalProductTable as "bitemporalproducttable_0" on ("root".prodFk = "bitemporalproducttable_0".id and ("bitemporalproducttable_0".in_z <= \'2017-06-11\' and "bitemporalproducttable_0".out_z > \'2017-06-11\' and "bitemporalproducttable_0".from_z <= \'2017-06-10\' and "bitemporalproducttable_0".thru_z > \'2017-06-10\')) left outer join (select "bitemporalproductclassificationtable_1".type as type from BiTemporalProductClassificationTable as "bitemporalproductclassificationtable_1" where ("bitemporalproductclassificationtable_1".in_z <= \'2017-06-11\' and "bitemporalproductclassificationtable_1".out_z > \'2017-06-11\' and "bitemporalproductclassificationtable_1".from_z <= \'2017-06-10\' and "bitemporalproductclassificationtable_1".thru_z > \'2017-06-10\')) as "bitemporalproductclassificationtable_0" on ("bitemporalproducttable_0".type = "bitemporalproductclassificationtable_0".type)',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testBiTemporalToSingleSingleTemporalDatePropagation", f"{_SRC}::testBiTemporalToSingleSingleTemporalDatePropagation",
                    'select "productclassificationtable_0".type as "type" from BiTemporalProductTable as "root" left outer join ProductClassificationTable as "productclassificationtable_0" on ("root".type = "productclassificationtable_0".type and "productclassificationtable_0".from_z <= \'2017-06-10\' and "productclassificationtable_0".thru_z > \'2017-06-10\') where "root".in_z <= \'2017-06-11\' and "root".out_z > \'2017-06-11\' and "root".from_z <= \'2017-06-10\' and "root".thru_z > \'2017-06-10\'',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testBusinessToBiTemporalDatePropagation", f"{_SRC}::testBusinessToBiTemporalDatePropagation",
                    'select "root".name as "name", "bitemporalproductclassificationtable_0".type as "type" from ProductTable as "root" left outer join BiTemporalProductClassificationTable as "bitemporalproductclassificationtable_0" on ("root".type = "bitemporalproductclassificationtable_0".type and ("bitemporalproductclassificationtable_0".in_z <= \'9999-12-30\' and "bitemporalproductclassificationtable_0".out_z > \'9999-12-30\' and "bitemporalproductclassificationtable_0".from_z <= \'9999-12-29\' and "bitemporalproductclassificationtable_0".thru_z > \'9999-12-29\')) where "root".from_z <= \'9999-12-29\' and "root".thru_z > \'9999-12-29\'',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testProcessingToBiTemporalDatePropagation", f"{_SRC}::testProcessingToBiTemporalDatePropagation",
                    'select "root".kerberos as "traderName", "bitemporallocationtable_0".PLACE as "traderLocation" from TraderTable as "root" left outer join BiTemporalLocationTable as "bitemporallocationtable_0" on ("root".kerberos = "bitemporallocationtable_0".kerberos and ("bitemporallocationtable_0".in_z <= \'2015-10-16\' and "bitemporallocationtable_0".out_z > \'2015-10-16\' and "bitemporallocationtable_0".from_z <= \'9999-12-28\' and "bitemporallocationtable_0".thru_z > \'9999-12-28\')) where "root".in_z <= \'2015-10-16\' and "root".out_z > \'2015-10-16\'',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testBiTemporalToBiTemporalProjectWithMilestoningInfinitySpecifiedInDB", f"{_SRC}::testBiTemporalToBiTemporalProjectWithMilestoningInfinitySpecifiedInDB",
                    'select "bitemporalproductclassificationtablewithlatest_0".type as "type" from OrderTable as "root" left outer join BiTemporalProductTableWithLatest as "bitemporalproducttablewithlatest_0" on ("root".prodFk = "bitemporalproducttablewithlatest_0".id and ("bitemporalproducttablewithlatest_0".out_z = \'9999-12-31 00:00:00.0000\' and "bitemporalproducttablewithlatest_0".thru_z = \'9999-12-31\')) left outer join (select "bitemporalproductclassificationtablewithlatest_1".type as type from BiTemporalProductClassificationTableWithLatest as "bitemporalproductclassificationtablewithlatest_1" where ("bitemporalproductclassificationtablewithlatest_1".out_z = \'9999-12-31 00:00:00.0000\' and "bitemporalproductclassificationtablewithlatest_1".thru_z = \'9999-12-31 00:00:00.0000\')) as "bitemporalproductclassificationtablewithlatest_0" on ("bitemporalproducttablewithlatest_0".type = "bitemporalproductclassificationtablewithlatest_0".type)',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testBiTemporalToBiTemporalProcessingDateProjectWithMilestoningInfinitySpecifiedInDB", f"{_SRC}::testBiTemporalToBiTemporalProcessingDateProjectWithMilestoningInfinitySpecifiedInDB",
                    'select "bitemporalproductclassificationtablewithlatest_0".type as "type" from OrderTable as "root" left outer join BiTemporalProductTableWithLatest as "bitemporalproducttablewithlatest_0" on ("root".prodFk = "bitemporalproducttablewithlatest_0".id and ("bitemporalproducttablewithlatest_0".out_z = \'9999-12-31 00:00:00.0000\' and "bitemporalproducttablewithlatest_0".thru_z = \'9999-12-31\')) left outer join (select "bitemporalproductclassificationtablewithlatest_1".type as type from BiTemporalProductClassificationTableWithLatest as "bitemporalproductclassificationtablewithlatest_1" where ("bitemporalproductclassificationtablewithlatest_1".out_z = \'9999-12-31 00:00:00.0000\' and "bitemporalproductclassificationtablewithlatest_1".from_z <= \'2017-06-10\' and "bitemporalproductclassificationtablewithlatest_1".thru_z > \'2017-06-10\')) as "bitemporalproductclassificationtablewithlatest_0" on ("bitemporalproducttablewithlatest_0".type = "bitemporalproductclassificationtablewithlatest_0".type)',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
        unsupported("testBiTemporalDateInjectionFromVarReference", f"{_SRC}::testBiTemporalDateInjectionFromVarReference",
                    'select "bitemporalproductclassificationtable_0".type as "type" from OrderTable as "root" left outer join BiTemporalProductTable as "bitemporalproducttable_0" on ("root".prodFk = "bitemporalproducttable_0".id and "bitemporalproducttable_0".in_z <= "root".orderDate and "bitemporalproducttable_0".out_z > "root".orderDate and "bitemporalproducttable_0".from_z <= "root".orderDate and "bitemporalproducttable_0".thru_z > "root".orderDate) left outer join (select "bitemporalproductclassificationtable_1".type as type, "bitemporalproductclassificationtable_1".in_z as in_z, "bitemporalproductclassificationtable_1".out_z as out_z, "bitemporalproductclassificationtable_1".from_z as from_z, "bitemporalproductclassificationtable_1".thru_z as thru_z from BiTemporalProductClassificationTable as "bitemporalproductclassificationtable_1") as "bitemporalproductclassificationtable_0" on ("bitemporalproductclassificationtable_0".in_z <= "root".orderDate and "bitemporalproductclassificationtable_0".out_z > "root".orderDate and "bitemporalproductclassificationtable_0".from_z <= "root".orderDate and "bitemporalproductclassificationtable_0".thru_z > "root".orderDate and "bitemporalproducttable_0".type = "bitemporalproductclassificationtable_0".type)',
                    'bitemporal milestoning variants on mappings not loaded in the port (latest, single temporal, none)'),
    ],
)
