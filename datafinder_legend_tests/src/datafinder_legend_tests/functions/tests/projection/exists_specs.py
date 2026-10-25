"""
Ported from legend-engine core_relational ``functions/tests/projection/testExists.pure``.

``exists`` over an association. Only the test on the simple mapping is here; the others use their own mappings.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import unsupported

_SRC = "functions/tests/projection/testExists.pure"

EXISTS_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
    ],
)


PERSON_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testSimpleExists", f"{_SRC}::testSimpleExists",
                    'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "a", exists (select 1 from (select "addresstable_0".ID as ID, null as ADDRESSID, "addresstable_0".NAME as "addressTableNAME" from addressTable as "addresstable_0" union all select "firmtable_0".ID as ID, "firmtable_0".ADDRESSID as ADDRESSID, "addresstable_1".NAME as "addressTableNAME" from firmTable as "firmtable_0" left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "firmtable_0".ADDRESSID)) as "unionalias_0" where ("unionalias_0".ID = "root".FIRMID or "unionalias_0".ID = "root".ADDRESSID) and "unionalias_0"."addressTableNAME" = \'Hoboken\') as "prop" from personTable as "root"',
                    'exists over an association that is mapped as a union'),
    ],
)
