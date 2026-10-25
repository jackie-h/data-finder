"""
Ported from legend-engine core_relational ``tests/mapping/union/testSpecialUnionMapping.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/union/testSpecialUnionMapping.pure"


SPECIAL_UNION_MAPPING_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testProject", f"{_SRC}::testProject",
                    'select "unionalias_0"."lastName" as "lastName" from (select "root".lastName_s1 as "lastName" from PersonSet1 as "root" union all select "root".lastName_s2 as "lastName" from PersonSet2 as "root") as "unionalias_0"',
                    'union class mappings (Operation_SetImplementation)'),
        unsupported("testFilteredProject", f"{_SRC}::testFilteredProject",
                    'select "unionalias_0"."lastName" as "lastName" from (select "root".lastName_s1 as "lastName" from PersonSet1 as "root" left outer join Firm as "firm_0" on ("root".FirmID = "firm_0".ID) where "firm_0".name = \'Firm X\' union all select "root".lastName_s2 as "lastName" from PersonSet2 as "root" left outer join Firm as "firm_0" on ("root".FirmID = "firm_0".ID) where "firm_0".name = \'Firm X\') as "unionalias_0"',
                    'union class mappings (Operation_SetImplementation)'),
        unsupported("testFilteredProjectWithFrom", f"{_SRC}::testFilteredProjectWithFrom",
                    'select "unionalias_0"."lastName" as "lastName" from (select "root".lastName_s1 as "lastName" from PersonSet1 as "root" left outer join Firm as "firm_0" on ("root".FirmID = "firm_0".ID) where "firm_0".name = \'Firm X\' union all select "root".lastName_s2 as "lastName" from PersonSet2 as "root" left outer join Firm as "firm_0" on ("root".FirmID = "firm_0".ID) where "firm_0".name = \'Firm X\') as "unionalias_0"',
                    'union class mappings (Operation_SetImplementation)'),
        unsupported("testMultiFilterProject", f"{_SRC}::testMultiFilterProject",
                    'select "unionalias_0"."lastName" as "lastName" from (select "root".lastName_s1 as "lastName" from PersonSet1 as "root" left outer join Firm as "firm_0" on ("root".FirmID = "firm_0".ID) where ("root".lastName_s1 like \'A%\' or "root".lastName_s1 like \'T%\') and "firm_0".name = \'Firm X\' union all select "root".lastName_s2 as "lastName" from PersonSet2 as "root" left outer join Firm as "firm_0" on ("root".FirmID = "firm_0".ID) where ("root".lastName_s2 like \'A%\' or "root".lastName_s2 like \'T%\') and "firm_0".name = \'Firm X\') as "unionalias_0"',
                    'union class mappings (Operation_SetImplementation)'),
        unsupported("testProjectWithPostTdsOperations", f"{_SRC}::testProjectWithPostTdsOperations",
                    'select "unionalias_0"."Len" as "Len", count("lastName") as "Count" from (select distinct "unionalias_1"."lastName" as "lastName", char_length("unionalias_1"."lastName") as "Len" from (select "root".lastName_s1 as "lastName" from PersonSet1 as "root" union all select "root".lastName_s2 as "lastName" from PersonSet2 as "root") as "unionalias_1") as "unionalias_0" group by "Len"',
                    'union class mappings (Operation_SetImplementation)'),
        unsupported("testFilteredProjectWithPostTdsOperations", f"{_SRC}::testFilteredProjectWithPostTdsOperations",
                    'select "unionalias_0"."Len" as "Len", count("lastName") as "Count" from (select distinct "unionalias_1"."lastName" as "lastName", char_length("unionalias_1"."lastName") as "Len" from (select "root".lastName_s1 as "lastName" from PersonSet1 as "root" left outer join Firm as "firm_0" on ("root".FirmID = "firm_0".ID) where "firm_0".name = \'Firm X\' union all select "root".lastName_s2 as "lastName" from PersonSet2 as "root" left outer join Firm as "firm_0" on ("root".FirmID = "firm_0".ID) where "firm_0".name = \'Firm X\') as "unionalias_1") as "unionalias_0" group by "Len"',
                    'union class mappings (Operation_SetImplementation)'),
        unsupported("testGroupBy", f"{_SRC}::testGroupBy",
                    'select "firm_0".name as "Firm", count("unionBase"."PersonSet1lastName_s1_PersonSet2lastName_s2") as "EmployeeCount" from (select "root".ID as "pk_0_0", null as "pk_0_1", "root".FirmID as FirmID_0, null as FirmID_1, "root".lastName_s1 as "PersonSet1lastName_s1_PersonSet2lastName_s2" from PersonSet1 as "root" union all select null as "pk_0_0", "root".ID as "pk_0_1", null as FirmID_0, "root".FirmID as FirmID_1, "root".lastName_s2 as "PersonSet1lastName_s1_PersonSet2lastName_s2" from PersonSet2 as "root") as "unionBase" left outer join Firm as "firm_0" on ("unionBase".FirmID_0 = "firm_0".ID or "unionBase".FirmID_1 = "firm_0".ID) group by "Firm"',
                    'union class mappings (Operation_SetImplementation)'),
    ],
)
