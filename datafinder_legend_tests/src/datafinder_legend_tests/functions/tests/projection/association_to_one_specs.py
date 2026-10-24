"""
Ported from legend-engine core_relational ``functions/tests/projection/testAssociationToOne.pure``.

Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, BACKEND, JOIN_OPERAND_ORDER

_SRC = "functions/tests/projection/testAssociationToOne.pure"

ASSOCIATION_TO_ONE_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testAssociationToOne", f"{_SRC}::testAssociationToOne",
               'select "root".FIRSTNAME as "firstName", "firmtable_0".LEGALNAME as "legalName" from personTable as "root" '
               'left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID)',
               lambda f: f.find_all(None, None, [f.first_name(), f.firm().legal_name()]),
               known_gaps={BACKEND: JOIN_OPERAND_ORDER}),
        ported("testAssociationToOneWithQuery", f"{_SRC}::testAssociationToOneWithQuery",
               'select "root".FIRSTNAME as "firstName", "firmtable_0".LEGALNAME as "legalName" from personTable as "root" '
               'left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) '
               "where \"firmtable_0\".LEGALNAME = 'Firm X'",
               lambda f: f.find_all(None, None, [f.first_name(), f.firm().legal_name()],
                                    f.firm().legal_name().eq("Firm X")),
               known_gaps={BACKEND: JOIN_OPERAND_ORDER}),
        ported("testTwoAssociationsToOneBranchWithQuery", f"{_SRC}::testTwoAssociationsToOneBranchWithQuery",
               'select "root".LASTNAME as "lastName", "firmtable_0".LEGALNAME as "legalName", "addresstable_0".NAME as "name" '
               'from personTable as "root" '
               'left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) '
               'left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "root".ADDRESSID) '
               "where \"firmtable_0\".LEGALNAME = 'Firm X' and \"addresstable_0\".NAME = 'Hoboken'",
               lambda f: f.find_all(None, None, [f.last_name(), f.firm().legal_name(), f.address().name()],
                                    f.firm().legal_name().eq("Firm X").and_op(f.address().name().eq("Hoboken"))),
               known_gaps={BACKEND: JOIN_OPERAND_ORDER}),
    ],
)
