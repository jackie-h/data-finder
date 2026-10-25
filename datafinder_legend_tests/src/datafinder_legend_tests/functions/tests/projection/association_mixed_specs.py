"""
Ported from legend-engine core_relational ``functions/tests/projection/testAssociationMixed.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/projection/testAssociationMixed.pure"


ASSOCIATION_MIXED_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testAssociationMixedAdvanced", f"{_SRC}::testAssociationMixedAdvanced",
                    'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "name", "root".FIRSTNAME as "firstName", "firmtable_0".LEGALNAME as "legalName" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) left outer join (select distinct "persontable_2".FIRMID from personTable as "persontable_2" where concat("persontable_2".FIRSTNAME, \' \', "persontable_2".LASTNAME) = \'Peter Smith\' and "persontable_2".LASTNAME = \'Smith\') as "persontable_1" on ("firmtable_0".ID = "persontable_1".FIRMID) where "persontable_1".FIRMID is not null and "firmtable_0".LEGALNAME = \'Firm X\'',
                    'a mapping that the port does not load'),
    ],
)
