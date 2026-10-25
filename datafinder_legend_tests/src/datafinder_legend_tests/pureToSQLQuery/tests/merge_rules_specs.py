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
