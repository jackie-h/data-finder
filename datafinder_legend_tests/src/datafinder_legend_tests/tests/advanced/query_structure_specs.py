"""
Ported from legend-engine core_relational ``tests/advanced/testQueryStructure.pure``.

Query structure with nested filters.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/advanced/testQueryStructure.pure"

QUERY_STRUCTURE_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testQualifierWithLineAndOrWithInLine", f"{_SRC}::testQualifierWithLineAndOrWithInLine",
                    'select "root".LEGALNAME as "name1", "persontable_0".FIRSTNAME as "name2", "persontable_0".LASTNAME as "name3" from firmTable as "root" left outer join (select "persontable_1".FIRMID as FIRMID, "persontable_1".FIRSTNAME as FIRSTNAME, "persontable_1".LASTNAME as LASTNAME from personTable as "persontable_1" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "persontable_1".ADDRESSID) where "persontable_1".LASTNAME = \'Smith\' or "addresstable_0".NAME = \'New York\') as "persontable_0" on ("root".ID = "persontable_0".FIRMID) where "root".LEGALNAME = \'Firm X\'',
                    'no filter inside a navigation'),
    ],
)
