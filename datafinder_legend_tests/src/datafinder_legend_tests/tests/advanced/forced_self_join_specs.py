"""
Ported from legend-engine core_relational ``tests/advanced/testForcedSelfJoin.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/advanced/testForcedSelfJoin.pure"


FORCED_SELF_JOIN_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("isolationTest", f"{_SRC}::isolationTest",
                    'select "root".legalName as "firmname", concat("persontable_0".firstName, \' \', "persontable_0".lastName) as "employeeProductName", "persontable_1".name as "testCol" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID) left outer join (select "persontable_2".ID as ID, "organizationtable_1".name as name from personTable as "persontable_2" left outer join organizationTable as "organizationtable_0" on ("organizationtable_0".orgId = "persontable_2".orgId) left outer join organizationTable as "organizationtable_1" on ("organizationtable_0".orgId = "organizationtable_1".parentId) left outer join productTable as "producttable_0" on ("producttable_0".orgId = "organizationtable_1".orgId) left outer join productTable as "producttable_1" on ("persontable_2".ID = "producttable_1".ownerId) where "producttable_0".name = "producttable_1".name or ("producttable_0".name is null and "producttable_1".name is null)) as "persontable_1" on ("persontable_0".ID = "persontable_1".ID)',
                    'a forced self join'),
    ],
)
