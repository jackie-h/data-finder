"""
Ported from legend-engine core_relational ``tests/mapping/distinct/testDistinct.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/distinct/testDistinct.pure"


DISTINCT_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testDistinctMappingSimpleProjectSelectAll", f"{_SRC}::testDistinctMappingSimpleProjectSelectAll",
                    'select "root".IF_NAME as "name", "root".IF_CODE as "code" from (select distinct "account_info_1".IF_CODE as IF_CODE, "account_info_1".IF_NAME as IF_NAME from ACCOUNT_INFO as "account_info_1") as "root" order by "code" asc',
                    'a class mapping with ~distinct'),
        unsupported("testDistinctMappingSimpleProjectSelectOneOfTheDistinctProperties", f"{_SRC}::testDistinctMappingSimpleProjectSelectOneOfTheDistinctProperties",
                    'select "root".IF_NAME as "name" from (select distinct "account_info_1".IF_CODE as IF_CODE, "account_info_1".IF_NAME as IF_NAME from ACCOUNT_INFO as "account_info_1") as "root" order by "name" asc',
                    'a class mapping with ~distinct'),
        unsupported("testDistinctMappingSimpleProjectDistinct", f"{_SRC}::testDistinctMappingSimpleProjectDistinct",
                    'select distinct "root".IF_NAME as "name" from (select distinct "account_info_1".IF_CODE as IF_CODE, "account_info_1".IF_NAME as IF_NAME from ACCOUNT_INFO as "account_info_1") as "root"',
                    'a class mapping with ~distinct'),
        unsupported("testDistinctMappingWithFilterSelectOneProperty", f"{_SRC}::testDistinctMappingWithFilterSelectOneProperty",
                    'select "root".IF_NAME as "name" from (select distinct "account_info_1".IF_CODE as IF_CODE, "account_info_1".IF_NAME as IF_NAME from ACCOUNT_INFO as "account_info_1" where "account_info_1".IF_TYPE = \'Type 1\') as "root" order by "name" asc',
                    'a class mapping with ~distinct'),
        unsupported("testDistinctMappingWithJoinProject", f"{_SRC}::testDistinctMappingWithJoinProject",
                    'select "if_other_info_1".IF_OTHER_NAME as "IfName" from (select distinct "account_info_1".IF_CODE as IF_CODE, "account_info_1".IF_NAME as IF_NAME, "if_other_info_0".IF_OTHER_NAME as IF_OTHER_NAME from ACCOUNT_INFO as "account_info_1" left outer join IF_OTHER_INFO as "if_other_info_0" on ("account_info_1".IF_CODE = "if_other_info_0".IF_CODE)) as "root" left outer join IF_OTHER_INFO as "if_other_info_1" on ("root".IF_CODE = "if_other_info_1".IF_CODE)',
                    'a class mapping with ~distinct'),
        unsupported("testProjectDistinctMappingWithDistinctInJoin", f"{_SRC}::testProjectDistinctMappingWithDistinctInJoin",
                    'select "root".IF_OTHER_NAME as "name", "account_info_0".IF_NAME as "incomeFunction" from IF_OTHER_INFO as "root" left outer join (select distinct "root".IF_CODE, "root".IF_NAME, "if_other_info_1".IF_OTHER_NAME from ACCOUNT_INFO as "root" left outer join IF_OTHER_INFO as "if_other_info_1" on ("root".IF_CODE = "if_other_info_1".IF_CODE)) as "account_info_0" on ("account_info_0".IF_CODE = "root".IF_CODE)',
                    'a class mapping with ~distinct'),
        unsupported("testProjectDistinctMappingWithDistinctInJoinWithDup", f"{_SRC}::testProjectDistinctMappingWithDistinctInJoinWithDup",
                    'select "root".IF_OTHER_NAME as "name", "account_info_0".IF_NAME as "incomeFunction" from IF_OTHER_INFO as "root" left outer join (select distinct "root".IF_NAME, "if_other_info_1".IF_OTHER_NAME, "root".IF_CODE from ACCOUNT_INFO as "root" left outer join IF_OTHER_INFO as "if_other_info_1" on ("root".IF_CODE = "if_other_info_1".IF_CODE and "root".IF_CODE is not null)) as "account_info_0" on ("account_info_0".IF_CODE = "root".IF_CODE and "account_info_0".IF_CODE is not null)',
                    'a class mapping with ~distinct'),
        unsupported("testDistinctMappingWithDistinctInJoinWithFilter", f"{_SRC}::testDistinctMappingWithDistinctInJoinWithFilter",
                    'select "root".IF_OTHER_NAME as "name", "account_info_0".IF_NAME as "incomeFunction" from IF_OTHER_INFO as "root" left outer join (select distinct "root".IF_CODE, "root".IF_NAME, "if_other_info_1".IF_OTHER_NAME from ACCOUNT_INFO as "root" left outer join IF_OTHER_INFO as "if_other_info_1" on ("root".IF_CODE = "if_other_info_1".IF_CODE)) as "account_info_0" on ("account_info_0".IF_CODE = "root".IF_CODE) where "root".IF_OTHER_NAME = \'IfName2\'',
                    'a class mapping with ~distinct'),
        unsupported("testDistinctMappingWithDistinctInJoinWithFilterOnJoin", f"{_SRC}::testDistinctMappingWithDistinctInJoinWithFilterOnJoin",
                    'select "root".IF_OTHER_NAME as "name", "account_info_0".IF_NAME as "incomeFunction" from IF_OTHER_INFO as "root" left outer join (select distinct "root".IF_CODE, "root".IF_NAME, "if_other_info_1".IF_OTHER_NAME from ACCOUNT_INFO as "root" left outer join IF_OTHER_INFO as "if_other_info_1" on ("root".IF_CODE = "if_other_info_1".IF_CODE)) as "account_info_0" on ("account_info_0".IF_CODE = "root".IF_CODE) where exists (select 1 from (select distinct "root".IF_CODE, "root".IF_NAME, "if_other_info_2".IF_OTHER_NAME from ACCOUNT_INFO as "root" left outer join IF_OTHER_INFO as "if_other_info_2" on ("root".IF_CODE = "if_other_info_2".IF_CODE)) as "account_info_2" where "account_info_2".IF_CODE = "root".IF_CODE and "account_info_2".IF_NAME = \'IF 2\')',
                    'a class mapping with ~distinct'),
        unsupported("testDistinctMappingWithCaseStatement", f"{_SRC}::testDistinctMappingWithCaseStatement",
                    'select "root"."name" as "name", case when "if_other_info_0".IF_OTHER_NAME is null then \'Unknown\' else "if_other_info_0".IF_OTHER_NAME end as "IfName" from (select distinct "account_info_1".IF_CODE as IF_CODE, case when "account_info_1".IF_NAME is null then \'C\' else "account_info_1".IF_NAME end as "name" from ACCOUNT_INFO as "account_info_1") as "root" left outer join IF_OTHER_INFO as "if_other_info_0" on ("root".IF_CODE = "if_other_info_0".IF_CODE) order by "name" asc, "IfName" asc',
                    'a class mapping with ~distinct'),
    ],
)
