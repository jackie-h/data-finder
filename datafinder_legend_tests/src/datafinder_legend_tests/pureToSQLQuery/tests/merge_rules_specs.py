"""
Ported from legend-engine core_relational ``pureToSQLQuery/tests/testMergeRules.pure``.

Rules for merging the joins of a filter and a projection.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported, BACKEND, JOIN_OPERAND_ORDER

_SRC = "pureToSQLQuery/tests/testMergeRules.pure"


MERGE_RULES_SYNONYM_SPECS = FinderSpec(
    finder_name="SynonymFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testToOneJoinTreeNodesForFilterAndProjectMerge", f"{_SRC}::testToOneJoinTreeNodesForFilterAndProjectMerge",
               'select "root".NAME as "synonymName", "producttable_0".NAME as "productName" from productSchema.synonymTable as "root" left outer join productSchema.productTable as "producttable_0" on ("root".PRODID = "producttable_0".ID) where "producttable_0".NAME = \'Firm C\'',
               lambda f: f.find_all(None, None, [f.name(), f.product().name()], f.product().name().eq("Firm C")),
               known_gaps={BACKEND: JOIN_OPERAND_ORDER}),
    ],
)


MERGE_RULES_FIRM_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testZeroToOneJoinTreeNodesForFilterAndProjectMerge", f"{_SRC}::testZeroToOneJoinTreeNodesForFilterAndProjectMerge",
               'select "root".LEGALNAME as "legalName", "addresstable_0".NAME as "firmAddress" from firmTable as "root" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "root".ADDRESSID) where "addresstable_0".NAME = \'Tokyo\'',
               lambda f: f.find_all(None, None, [f.legal_name(), f.address().name()], f.address().name().eq("Tokyo"))),
    ],
)


MERGE_RULES_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testToManyJoinTreeNodesForFilterAndProjectDoNotMerge", f"{_SRC}::testToManyJoinTreeNodesForFilterAndProjectDoNotMerge",
                    'select "root".LEGALNAME as "legalName", "persontable_2".LASTNAME as "lastName" from firmTable as "root" left outer join (select distinct "persontable_1".FIRMID from personTable as "persontable_1" where "persontable_1".LASTNAME = \'Smith\') as "persontable_0" on ("root".ID = "persontable_0".FIRMID) left outer join personTable as "persontable_2" on ("root".ID = "persontable_2".FIRMID) where "persontable_0".FIRMID is not null',
                    'milestoned or to-many join merging'),
        unsupported("testToManyJoinTreeNodesForInvalidUsageOfFilterAndProjectDoMergeGivingWrongResults", f"{_SRC}::testToManyJoinTreeNodesForInvalidUsageOfFilterAndProjectDoMergeGivingWrongResults",
                    'select "root".LEGALNAME as "legalName", "personTable_d#4_d_m2".LASTNAME as "lastName" from firmTable as "root" left outer join (select distinct "personTable_d#4".FIRMID from personTable as "personTable_d#4" where "personTable_d#4".LASTNAME = \'Smith\') as "personTable_d#4_ecq_d#2_m1" on ("root".ID = "personTable_d#4_ecq_d#2_m1".FIRMID) left outer join personTable as "personTable_d#4_d_m2" on ("root".ID = "personTable_d#4_d_m2".FIRMID) where "personTable_d#4_ecq_d#2_m1".FIRMID is not null',
                    'milestoned or to-many join merging'),
        unsupported("testToOneJoinTreeNodesForMilestoningFilterAndProjectMerge", f"{_SRC}::testToOneJoinTreeNodesForMilestoningFilterAndProjectMerge",
                    'select "root".name as "productName", "productclassificationtable_0".type as "productClassificationType" from ProductTable as "root" left outer join ProductClassificationTable as "productclassificationtable_0" on ("root".type = "productclassificationtable_0".type and "productclassificationtable_0".from_z <= \'2015-10-16\' and "productclassificationtable_0".thru_z > \'2015-10-16\') where "productclassificationtable_0".type = \'STOCK\' and "root".from_z <= \'2015-10-16\' and "root".thru_z > \'2015-10-16\'',
                    'milestoned or to-many join merging'),
        unsupported("testToManyJoinTreeNodesForMilestoningFilterAndProjectDoNotMerge", f"{_SRC}::testToManyJoinTreeNodesForMilestoningFilterAndProjectDoNotMerge",
                    'select "root".name as "productName", "productsynonymtable_2".synonym as "productSynonymsName" from ProductTable as "root" left outer join (select distinct "productsynonymtable_1".name from ProductSynonymTable as "productsynonymtable_1" where "productsynonymtable_1".from_z <= \'2015-10-16\' and "productsynonymtable_1".thru_z > \'2015-10-16\' and "productsynonymtable_1".synonym = \'GS-Mod-S1\') as "productsynonymtable_0" on ("root".name = "productsynonymtable_0".name) left outer join ProductSynonymTable as "productsynonymtable_2" on ("root".name = "productsynonymtable_2".name and "productsynonymtable_2".from_z <= \'2015-10-16\' and "productsynonymtable_2".thru_z > \'2015-10-16\') where "productsynonymtable_0".name is not null and "root".from_z <= \'2015-10-16\' and "root".thru_z > \'2015-10-16\'',
                    'milestoned or to-many join merging'),
    ],
)
