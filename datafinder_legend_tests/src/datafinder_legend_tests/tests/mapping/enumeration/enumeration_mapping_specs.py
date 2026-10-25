"""
Ported from legend-engine core_relational ``tests/mapping/enumeration/testEnumerationMapping.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/enumeration/testEnumerationMapping.pure"


ENUMERATION_MAPPING_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testProjectWithIfWhereEnumEqualsClassProp", f"{_SRC}::testProjectWithIfWhereEnumEqualsClassProp",
                    'select case when "root".TYPE = \'CUSIP\' then "root".NAME else "root".TYPE end as "ress" from productSchema.synonymTable as "root"',
                    'enumeration mappings'),
        unsupported("testTdsProjectWithSingleStringEnumEqualityComparison", f"{_SRC}::testTdsProjectWithSingleStringEnumEqualityComparison",
                    'select case when "root".type = \'FTE\' then \'yes\' else \'no\' end as "Type" from employeeTable as "root"',
                    'enumeration mappings'),
        unsupported("testTdsProjectWithMultiStringEnumEqualityComparison", f"{_SRC}::testTdsProjectWithMultiStringEnumEqualityComparison",
                    'select case when "root".type in (\'FTC\', \'FTO\') then \'yes\' else \'no\' end as "Type" from employeeTable as "root"',
                    'enumeration mappings'),
        unsupported("testTdsProjectWithEnumInequalityComparison", f"{_SRC}::testTdsProjectWithEnumInequalityComparison",
                    'select case when not "root".type in (\'FTC\', \'FTO\') or "root".type is null then \'yes\' else \'no\' end as "Type" from employeeTable as "root"',
                    'enumeration mappings'),
        unsupported("testTdsProjectWithEnumToStringEqualityComparison", f"{_SRC}::testTdsProjectWithEnumToStringEqualityComparison",
                    'select case when "root".type = \'FTE\' then \'yes\' else \'no\' end as "Type" from employeeTable as "root"',
                    'enumeration mappings'),
        unsupported("testTdsProjectWithEnumsInInClause", f"{_SRC}::testTdsProjectWithEnumsInInClause",
                    'select case when "root".type in (\'FTC\', \'FTO\', \'FTE\') then \'yes\' else \'no\' end as "Type" from employeeTable as "root"',
                    'enumeration mappings'),
    ],
)
