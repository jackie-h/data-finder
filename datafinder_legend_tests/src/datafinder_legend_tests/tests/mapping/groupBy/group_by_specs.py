"""
Ported from legend-engine core_relational ``tests/mapping/groupBy/testGroupBy.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/groupBy/testGroupBy.pure"


GROUP_BY_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testGroupByMappingWithFilterOnAggregateWithProject", f"{_SRC}::testGroupByMappingWithFilterOnAggregateWithProject",
                    'select "root".GSN as "gsn", "root"."quantity" as "quantity" from (select "trade_1".ACC_NUM as ACC_NUM, "trade_1".GSN as GSN, sum("trade_1".QTY) as "quantity", "trade_1".PRODUCT_ID as PRODUCT_ID from TRADE as "trade_1" group by "trade_1".ACC_NUM, "trade_1".PRODUCT_ID, "trade_1".GSN) as "root" where ("root"."quantity" > 100 and "root"."quantity" < 1000) or "root".GSN = \'EA4GNY\'',
                    'a class mapping with ~groupBy'),
        unsupported("testGroupByMappingProject", f"{_SRC}::testGroupByMappingProject",
                    'select "root".GSN as "gsn", sum("root".QTY) as "quantity" from TRADE as "root" group by "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN order by "gsn" asc',
                    'a class mapping with ~groupBy'),
        unsupported("testGroupByMappingProjectWithJoin", f"{_SRC}::testGroupByMappingProjectWithJoin",
                    'select "root".GSN as "gsn", "product_0".DESCRIPTION as "description" from TRADE as "root" left outer join PRODUCT as "product_0" on ("root".PRODUCT_ID = "product_0".ID) group by "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN order by "gsn" asc',
                    'a class mapping with ~groupBy'),
        unsupported("testGroupByMappingProjectWithJoinAndTableFilter", f"{_SRC}::testGroupByMappingProjectWithJoinAndTableFilter",
                    'select "root".GSN as "gsn", "product_0".DESCRIPTION as "description" from TRADE as "root" left outer join PRODUCT as "product_0" on ("root".PRODUCT_ID = "product_0".ID and "product_0".ID = 1) where "root".PRODUCT_ID = 1 group by "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN',
                    'a class mapping with ~groupBy'),
        unsupported("testGroupByMappingProjectWithGroupByInJoin", f"{_SRC}::testGroupByMappingProjectWithGroupByInJoin",
                    'select "trade_0".GSN as "gsn", "root".DESCRIPTION as "description" from PRODUCT as "root" left outer join (select "root".ACC_NUM, "root".GSN, sum("root".QTY) as "quantity", "root".PRODUCT_ID from TRADE as "root" group by "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN) as "trade_0" on ("trade_0".PRODUCT_ID = "root".ID)',
                    'a class mapping with ~groupBy'),
        unsupported("testGroupByMappingProjectWithMultipleGroupBys", f"{_SRC}::testGroupByMappingProjectWithMultipleGroupBys",
                    'select "root".GSN as "gsn", "product_exploded_0".DESCRIPTION as "description" from TRADE as "root" left outer join (select "root".DESCRIPTION, "root".PRODUCT_ID from PRODUCT_EXPLODED as "root" where "root".ACTIVE = 1 group by "root".PRODUCT_ID, "root".DESCRIPTION) as "product_exploded_0" on ("root".PRODUCT_ID = "product_exploded_0".PRODUCT_ID and "product_exploded_0".PRODUCT_ID = 1) where "root".PRODUCT_ID = 1 group by "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN',
                    'a class mapping with ~groupBy'),
    ],
)
