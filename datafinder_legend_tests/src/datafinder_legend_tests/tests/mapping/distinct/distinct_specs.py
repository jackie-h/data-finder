"""
Ported from legend-engine core_relational ``tests/mapping/distinct/testDistinct.pure``.

A class mapped with ``~distinct`` is a ``Query`` section here (``SELECT DISTINCT``). Legend writes the distinct into the root table when
the whole class is fetched, and into a sub select when properties are projected; the finder always writes the sub select, so the ones
that fetch the whole class, and the ones that join to or from a distinct mapping, are recorded as unsupported.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import BACKEND, ROWS, DISTINCT_QUERY_FILTER_ON_JOIN, DISTINCT_QUERY_JOIN_INSIDE, QUERY_EXPRESSION_IN_DERIVED_TABLE, ported, unsupported

_SRC = "tests/mapping/distinct/testDistinct.pure"


DISTINCT_SPECS = FinderSpec(
    finder_name="IncomeFunctionFinder",
    mapping_file="tests/mapping/distinct/distinct_mapping.md",
    expectations=[
        unsupported("testDistinctMappingSelectAll", f"{_SRC}::testDistinctMappingSelectAll",
                    'select distinct "root".IF_CODE as "code", "root".IF_NAME as "name" from ACCOUNT_INFO as "root"',
                    'the whole class fetched through a distinct mapping, which Legend writes without a sub select'),
        unsupported("testDistinctMappingSelectAllWithFilter", f"{_SRC}::testDistinctMappingSelectAllWithFilter",
                    'select distinct "root".IF_CODE as "code", "root".IF_NAME as "name" from ACCOUNT_INFO as "root" where "root".IF_CODE = 1002',
                    'the whole class fetched through a distinct mapping, which Legend writes without a sub select'),
        ported("testDistinctMappingSimpleProjectSelectAll", f"{_SRC}::testDistinctMappingSimpleProjectSelectAll",
               'select "root".IF_NAME as "name", "root".IF_CODE as "code" from (select distinct "account_info_1".IF_CODE as IF_CODE, "account_info_1".IF_NAME as IF_NAME from ACCOUNT_INFO as "account_info_1") as "root" order by "code" asc',
               lambda f: f.find_all([f.name(), f.code()]).order_by(f.code().ascending())),
        ported("testDistinctMappingSimpleProjectSelectOneOfTheDistinctProperties", f"{_SRC}::testDistinctMappingSimpleProjectSelectOneOfTheDistinctProperties",
               'select "root".IF_NAME as "name" from (select distinct "account_info_1".IF_CODE as IF_CODE, "account_info_1".IF_NAME as IF_NAME from ACCOUNT_INFO as "account_info_1") as "root" order by "name" asc',
               lambda f: f.find_all([f.name()]).order_by(f.name().ascending())),
        ported("testDistinctMappingSimpleProjectDistinct", f"{_SRC}::testDistinctMappingSimpleProjectDistinct",
               'select distinct "root".IF_NAME as "name" from (select distinct "account_info_1".IF_CODE as IF_CODE, "account_info_1".IF_NAME as IF_NAME from ACCOUNT_INFO as "account_info_1") as "root"',
               lambda f: f.find_all([f.name()]).distinct()),
    ],
)

DISTINCT_FILTER_SPECS = FinderSpec(
    finder_name="IncomeFunctionFinder",
    mapping_file="tests/mapping/distinct/distinct_filter_mapping.md",
    expectations=[
        unsupported("testDistinctMappingWithFilterSelectAll", f"{_SRC}::testDistinctMappingWithFilterSelectAll",
                    'select distinct "root".IF_CODE as "code", "root".IF_NAME as "name" from ACCOUNT_INFO as "root" where "root".IF_TYPE = \'Type 1\'',
                    'the whole class fetched through a distinct mapping, which Legend writes without a sub select'),
        ported("testDistinctMappingWithFilterSelectOneProperty", f"{_SRC}::testDistinctMappingWithFilterSelectOneProperty",
               'select "root".IF_NAME as "name" from (select distinct "account_info_1".IF_CODE as IF_CODE, "account_info_1".IF_NAME as IF_NAME from ACCOUNT_INFO as "account_info_1" where "account_info_1".IF_TYPE = \'Type 1\') as "root" order by "name" asc',
               lambda f: f.find_all([f.name()]).order_by(f.name().ascending())),
    ],
)

DISTINCT_JOIN_SPECS = FinderSpec(
    finder_name="ClassificationFinder",
    mapping_file="tests/mapping/distinct/distinct_join_mapping.md",
    expectations=[
        ported("testProjectDistinctMappingWithDistinctInJoin", f"{_SRC}::testProjectDistinctMappingWithDistinctInJoin",
               'select "root".IF_OTHER_NAME as "name", "account_info_0".IF_NAME as "incomeFunction" from IF_OTHER_INFO as "root" left outer join (select distinct "root".IF_CODE, "root".IF_NAME, "if_other_info_1".IF_OTHER_NAME from ACCOUNT_INFO as "root" left outer join IF_OTHER_INFO as "if_other_info_1" on ("root".IF_CODE = "if_other_info_1".IF_CODE)) as "account_info_0" on ("account_info_0".IF_CODE = "root".IF_CODE)',
               lambda f: f.find_all([f.name(), f.income_functions().name().alias("incomeFunction")])),
        ported("testDistinctMappingWithDistinctInJoinWithFilter", f"{_SRC}::testDistinctMappingWithDistinctInJoinWithFilter",
               'select "root".IF_OTHER_NAME as "name", "account_info_0".IF_NAME as "incomeFunction" from IF_OTHER_INFO as "root" left outer join (select distinct "root".IF_CODE, "root".IF_NAME, "if_other_info_1".IF_OTHER_NAME from ACCOUNT_INFO as "root" left outer join IF_OTHER_INFO as "if_other_info_1" on ("root".IF_CODE = "if_other_info_1".IF_CODE)) as "account_info_0" on ("account_info_0".IF_CODE = "root".IF_CODE) where "root".IF_OTHER_NAME = \'IfName2\'',
               lambda f: f.find_all([f.name(), f.income_functions().name().alias("incomeFunction")], f.name().eq("IfName2"))),
        ported("testDistinctMappingWithDistinctInJoinWithFilterOnJoin", f"{_SRC}::testDistinctMappingWithDistinctInJoinWithFilterOnJoin",
               'select "root".IF_OTHER_NAME as "name", "account_info_0".IF_NAME as "incomeFunction" from IF_OTHER_INFO as "root" left outer join (select distinct "root".IF_CODE, "root".IF_NAME, "if_other_info_1".IF_OTHER_NAME from ACCOUNT_INFO as "root" left outer join IF_OTHER_INFO as "if_other_info_1" on ("root".IF_CODE = "if_other_info_1".IF_CODE)) as "account_info_0" on ("account_info_0".IF_CODE = "root".IF_CODE) where exists (select 1 from (select distinct "root".IF_CODE, "root".IF_NAME, "if_other_info_2".IF_OTHER_NAME from ACCOUNT_INFO as "root" left outer join IF_OTHER_INFO as "if_other_info_2" on ("root".IF_CODE = "if_other_info_2".IF_CODE)) as "account_info_2" where "account_info_2".IF_CODE = "root".IF_CODE and "account_info_2".IF_NAME = \'IF 2\')',
               lambda f: f.find_all([f.name(), f.income_functions().name().alias("incomeFunction")], f.income_functions().name().eq("IF 2")),
               known_gaps={BACKEND: DISTINCT_QUERY_FILTER_ON_JOIN}),
    ],
)

DISTINCT_JOIN_INCOME_FUNCTION_SPECS = FinderSpec(
    finder_name="IncomeFunctionFinder",
    mapping_file="tests/mapping/distinct/distinct_join_mapping.md",
    expectations=[
        ported("testDistinctMappingWithJoinProject", f"{_SRC}::testDistinctMappingWithJoinProject",
               'select "if_other_info_1".IF_OTHER_NAME as "IfName" from (select distinct "account_info_1".IF_CODE as IF_CODE, "account_info_1".IF_NAME as IF_NAME, "if_other_info_0".IF_OTHER_NAME as IF_OTHER_NAME from ACCOUNT_INFO as "account_info_1" left outer join IF_OTHER_INFO as "if_other_info_0" on ("account_info_1".IF_CODE = "if_other_info_0".IF_CODE)) as "root" left outer join IF_OTHER_INFO as "if_other_info_1" on ("root".IF_CODE = "if_other_info_1".IF_CODE)',
               lambda f: f.find_all([f.if_name()]),
               known_gaps={BACKEND: DISTINCT_QUERY_JOIN_INSIDE}),
    ],
)

DISTINCT_DENORM_SPECS = FinderSpec(
    finder_name="FirmAccountFinder",
    mapping_file="tests/mapping/distinct/distinct_denorm_mapping.md",
    expectations=[
        ported("testDistinctMappingWithFullDenormSelfJoins", f"{_SRC}::testDistinctMappingWithFullDenormSelfJoins",
               'select "root".ACCT_NAME as "accountName", "account_info_1".IF_NAME as "IncomeFunction", "account_info_2".DESKHEAD_NAME as "DeskHead" from ACCOUNT_INFO as "root" left outer join (select distinct "root".IF_CODE, "root".IF_NAME, "root".DESKHEAD_ID from ACCOUNT_INFO as "root" where "root".IF_TYPE = \'Type 1\') as "account_info_1" on ("root".IF_CODE = "account_info_1".IF_CODE) left outer join (select distinct "root".DESKHEAD_NAME, "root".DESKHEAD_ID from ACCOUNT_INFO as "root") as "account_info_2" on ("account_info_1".DESKHEAD_ID = "account_info_2".DESKHEAD_ID)',
               lambda f: f.find_all([f.name().alias("accountName"), f.income_function().name().alias("IncomeFunction"), f.income_function().desk_head().name().alias("DeskHead")])),
        unsupported("testDistinctMappingWithFullDenormSelfJoinsWithFilterOnJoin", f"{_SRC}::testDistinctMappingWithFullDenormSelfJoinsWithFilterOnJoin",
                    'select "root".id as "pk_0", "root".ACC_NUM as "number", "root".ACCT_NAME as "name" from ACCOUNT_INFO as "root" left outer join (select distinct "root".IF_CODE, "root".IF_NAME, "root".DESKHEAD_ID from ACCOUNT_INFO as "root" where "root".IF_TYPE = \'Type 1\') as "account_info_1" on ("root".IF_CODE = "account_info_1".IF_CODE) where "account_info_1".IF_NAME = \'IF 2\'',
                    'the whole class fetched'),
        unsupported("testDistinctMappingWithFullDenormSelfJoinsWithTwoFiltersOnJoin", f"{_SRC}::testDistinctMappingWithFullDenormSelfJoinsWithTwoFiltersOnJoin",
                    'select "root".id as "pk_0", "root".ACC_NUM as "number", "root".ACCT_NAME as "name" from ACCOUNT_INFO as "root" left outer join (select distinct "root".IF_CODE, "root".IF_NAME, "root".DESKHEAD_ID from ACCOUNT_INFO as "root" where "root".IF_TYPE = \'Type 1\') as "account_info_1" on ("root".IF_CODE = "account_info_1".IF_CODE) where "account_info_1".IF_NAME = \'IF 2\' and "account_info_1".IF_CODE = 1002',
                    'the whole class fetched'),
    ],
)

DISTINCT_CASE_SPECS = FinderSpec(
    finder_name="IncomeFunctionFinder",
    mapping_file="tests/mapping/distinct/distinct_case_mapping.md",
    expectations=[
        ported("testDistinctMappingWithCaseStatement", f"{_SRC}::testDistinctMappingWithCaseStatement",
               'select "root"."name" as "name", case when "if_other_info_0".IF_OTHER_NAME is null then \'Unknown\' else "if_other_info_0".IF_OTHER_NAME end as "IfName" from (select distinct "account_info_1".IF_CODE as IF_CODE, case when "account_info_1".IF_NAME is null then \'C\' else "account_info_1".IF_NAME end as "name" from ACCOUNT_INFO as "account_info_1") as "root" left outer join IF_OTHER_INFO as "if_other_info_0" on ("root".IF_CODE = "if_other_info_0".IF_CODE) order by "name" asc, "IfName" asc',
               lambda f: f.find_all([f.name(), f.classification().name().alias("IfName")]).order_by(f.name().ascending(), f.classification().name().ascending()),
               known_gaps={BACKEND: QUERY_EXPRESSION_IN_DERIVED_TABLE, ROWS: QUERY_EXPRESSION_IN_DERIVED_TABLE}),
    ],
)

DISTINCT_UNSUPPORTED_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testProjectDistinctMappingWithDistinctInJoinWithDup", f"{_SRC}::testProjectDistinctMappingWithDistinctInJoinWithDup",
                    'select "root".IF_OTHER_NAME as "name", "account_info_0".IF_NAME as "incomeFunction" from IF_OTHER_INFO as "root" left outer join (select distinct "root".IF_NAME, "if_other_info_1".IF_OTHER_NAME, "root".IF_CODE from ACCOUNT_INFO as "root" left outer join IF_OTHER_INFO as "if_other_info_1" on ("root".IF_CODE = "if_other_info_1".IF_CODE and "root".IF_CODE is not null)) as "account_info_0" on ("account_info_0".IF_CODE = "root".IF_CODE and "account_info_0".IF_CODE is not null)',
                    'a distinct mapping joined to, or from, other classes'),
    ],
)
