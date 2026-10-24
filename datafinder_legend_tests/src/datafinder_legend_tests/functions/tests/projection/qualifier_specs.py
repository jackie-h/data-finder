"""
Ported from legend-engine core_relational ``functions/tests/projection/testQualifier.pure``.

Qualified (derived) properties.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/projection/testQualifier.pure"

QUALIFIER_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testSimpleDerived", f"{_SRC}::testSimpleDerived",
                    'select "root".FIRSTNAME as "firstName", concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "name" from personTable as "root"',
                    'no qualified (derived) properties'),
    ],
)

QUALIFIER_FIRM_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testSimpleDerivedThroughAssociation", f"{_SRC}::testSimpleDerivedThroughAssociation",
                    'select "root".LEGALNAME as "legalName", concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME) as "name" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID)',
                    'no qualified (derived) properties'),
    ],
)
