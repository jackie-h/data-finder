"""
Ported from legend-engine core_relational ``tests/mapping/merge/testMerge.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/merge/testMerge.pure"


MERGE_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testDynafunctionMerge", f"{_SRC}::testDynafunctionMerge",
                    'select "root".lastName as "Employee Name", concat(\'Ayco Certified: \', case when \'Ayco\' = "organizationset_0".name and (\'30 Hudson\' = "addressset_0".name or \'200 W\' = "addressset_1".name or \'200 W\' = "addressset_0".name or \'30 Hudson\' = "addressset_1".name) then \'Yes\' else \'No\' end) as "Ayco Certified Americas?" from personSet as "root" left outer join organizationSet as "organizationset_0" on ("organizationset_0".name = "root".orgName) left outer join FirmSet1 as "firmset1_0" on ("organizationset_0".parentId = "firmset1_0".ID) left outer join addressSet as "addressset_0" on ("firmset1_0".LegalName = "addressset_0".ownerName) left outer join FirmSet2 as "firmset2_0" on ("organizationset_0".parentId = "firmset2_0".ID) left outer join addressSet as "addressset_1" on ("firmset2_0".LegalName = "addressset_1".ownerName)',
                    'a mapping that the port does not load'),
    ],
)
