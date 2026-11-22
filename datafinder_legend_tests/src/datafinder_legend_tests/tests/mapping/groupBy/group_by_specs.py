"""
Ported from legend-engine core_relational ``tests/mapping/groupBy/testGroupBy.pure``.

A class mapped with ``~groupBy`` is a ``Query`` section here (``GROUP BY``, with the aggregate as a column). Legend writes the group by into
the select of the table; the finder reads the query as a derived table, so its SQL has one more level of select, with the same rows.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import BACKEND, GROUP_BY_QUERY_SUBSELECT, ROWS, ported

_SRC = "tests/mapping/groupBy/testGroupBy.pure"

GROUP_BY_MAPPING_SPECS = FinderSpec(
    finder_name="PositionFinder",
    mapping_file="tests/mapping/groupBy/group_by_mapping.md",
    expectations=[
        ported("testGroupByMapping", f"{_SRC}::testGroupByMapping",
               'select "root".ACC_NUM as "acctNum", "root".GSN as "gsn", sum("root".QTY) as "quantity" from TRADE as "root" group by "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN',
               lambda f: f.find_all([f.acct_num(), f.gsn(), f.quantity()]),
               known_gaps={BACKEND: GROUP_BY_QUERY_SUBSELECT}),
        ported("testGroupByMappingWithFilter", f"{_SRC}::testGroupByMappingWithFilter",
               'select "root".ACC_NUM as "acctNum", "root".GSN as "gsn", sum("root".QTY) as "quantity" from TRADE as "root" where "root".GSN = \'YU2EF5\' group by "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN',
               lambda f: f.find_all([f.acct_num(), f.gsn(), f.quantity()], f.gsn().eq("YU2EF5")),
               known_gaps={BACKEND: GROUP_BY_QUERY_SUBSELECT}),
        ported("testGroupByMappingWithFilterOnAggregate", f"{_SRC}::testGroupByMappingWithFilterOnAggregate",
               'select "root".ACC_NUM as "acctNum", "root".GSN as "gsn", "root"."quantity" as "quantity", "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN from (select "trade_1".ACC_NUM as ACC_NUM, "trade_1".GSN as GSN, sum("trade_1".QTY) as "quantity", "trade_1".PRODUCT_ID as PRODUCT_ID from TRADE as "trade_1" group by "trade_1".ACC_NUM, "trade_1".PRODUCT_ID, "trade_1".GSN) as "root" where "root"."quantity" > 0',
               lambda f: f.find_all([f.acct_num(), f.gsn(), f.quantity()], f.quantity() > 0),
               known_gaps={BACKEND: GROUP_BY_QUERY_SUBSELECT}),
        ported("testGroupByMappingWithFilterOnAggregateWithJoin", f"{_SRC}::testGroupByMappingWithFilterOnAggregateWithJoin",
               'select "root".ACC_NUM as "acctNum", "root".GSN as "gsn", "root"."quantity" as "quantity", "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN from (select "trade_1".ACC_NUM as ACC_NUM, "trade_1".GSN as GSN, sum("trade_1".QTY) as "quantity", "trade_1".PRODUCT_ID as PRODUCT_ID from TRADE as "trade_1" group by "trade_1".ACC_NUM, "trade_1".PRODUCT_ID, "trade_1".GSN) as "root" left outer join PRODUCT as "product_0" on ("root".PRODUCT_ID = "product_0".ID) where "root"."quantity" > 8 and "product_0".DESCRIPTION = \'Gold\'',
               lambda f: f.find_all([f.acct_num(), f.gsn(), f.quantity()], (f.quantity() > 8).and_(f.product().description().eq("Gold"))),
               known_gaps={BACKEND: GROUP_BY_QUERY_SUBSELECT}),
        ported("testGroupByMappingWithFilterOnAggregateWithProject", f"{_SRC}::testGroupByMappingWithFilterOnAggregateWithProject",
               'select "root".GSN as "gsn", "root"."quantity" as "quantity" from (select "trade_1".ACC_NUM as ACC_NUM, "trade_1".GSN as GSN, sum("trade_1".QTY) as "quantity", "trade_1".PRODUCT_ID as PRODUCT_ID from TRADE as "trade_1" group by "trade_1".ACC_NUM, "trade_1".PRODUCT_ID, "trade_1".GSN) as "root" where ("root"."quantity" > 100 and "root"."quantity" < 1000) or "root".GSN = \'EA4GNY\'',
               lambda f: f.find_all([f.gsn(), f.quantity()], ((f.quantity() > 100).and_(f.quantity() < 1000)).or_(f.gsn().eq("EA4GNY"))),
               known_gaps={BACKEND: GROUP_BY_QUERY_SUBSELECT}),
        ported("testGroupByMappingProject", f"{_SRC}::testGroupByMappingProject",
               'select "root".GSN as "gsn", sum("root".QTY) as "quantity" from TRADE as "root" group by "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN order by "gsn" asc',
               lambda f: f.find_all([f.gsn(), f.quantity()]).order_by(f.gsn().ascending()),
               known_gaps={BACKEND: GROUP_BY_QUERY_SUBSELECT}),
        ported("testGroupByMappingProjectWithJoin", f"{_SRC}::testGroupByMappingProjectWithJoin",
               'select "root".GSN as "gsn", "product_0".DESCRIPTION as "description" from TRADE as "root" left outer join PRODUCT as "product_0" on ("root".PRODUCT_ID = "product_0".ID) group by "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN order by "gsn" asc',
               lambda f: f.find_all([f.gsn(), f.product().description()]).order_by(f.gsn().ascending()),
               known_gaps={BACKEND: GROUP_BY_QUERY_SUBSELECT}),
    ],
)

GROUP_BY_PRODUCT_SPECS = FinderSpec(
    finder_name="ProductFinder",
    mapping_file="tests/mapping/groupBy/group_by_mapping.md",
    expectations=[
        ported("testGroupByMappingProjectWithGroupByInJoin", f"{_SRC}::testGroupByMappingProjectWithGroupByInJoin",
               'select "trade_0".GSN as "gsn", "root".DESCRIPTION as "description" from PRODUCT as "root" left outer join (select "root".ACC_NUM, "root".GSN, sum("root".QTY) as "quantity", "root".PRODUCT_ID from TRADE as "root" group by "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN) as "trade_0" on ("trade_0".PRODUCT_ID = "root".ID)',
               lambda f: f.find_all([f.positions().gsn(), f.description()]),
               known_gaps={BACKEND: GROUP_BY_QUERY_SUBSELECT}),
    ],
)


GROUP_BY_FILTER_SPECS = FinderSpec(
    finder_name="PositionFinder",
    mapping_file="tests/mapping/groupBy/group_by_filter_mapping.md",
    expectations=[
        ported("testGroupByMappingProjectWithJoinAndTableFilter", f"{_SRC}::testGroupByMappingProjectWithJoinAndTableFilter",
               'select "root".GSN as "gsn", "product_0".DESCRIPTION as "description" from TRADE as "root" left outer join PRODUCT as "product_0" on ("root".PRODUCT_ID = "product_0".ID and "product_0".ID = 1) where "root".PRODUCT_ID = 1 group by "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN',
               lambda f: f.find_all([f.gsn(), f.product().description()]),
               known_gaps={BACKEND: GROUP_BY_QUERY_SUBSELECT}),
    ],
)


GROUP_BY_TWO_SPECS = FinderSpec(
    finder_name="PositionFinder",
    mapping_file="tests/mapping/groupBy/group_by_two_mapping.md",
    expectations=[
        ported("testGroupByMappingProjectWithMultipleGroupBys", f"{_SRC}::testGroupByMappingProjectWithMultipleGroupBys",
               'select "root".GSN as "gsn", "product_exploded_0".DESCRIPTION as "description" from TRADE as "root" left outer join (select "root".DESCRIPTION, "root".PRODUCT_ID from PRODUCT_EXPLODED as "root" where "root".ACTIVE = 1 group by "root".PRODUCT_ID, "root".DESCRIPTION) as "product_exploded_0" on ("root".PRODUCT_ID = "product_exploded_0".PRODUCT_ID and "product_exploded_0".PRODUCT_ID = 1) where "root".PRODUCT_ID = 1 group by "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN',
               lambda f: f.find_all([f.gsn(), f.product().description()]),
               known_gaps={BACKEND: GROUP_BY_QUERY_SUBSELECT}),
    ],
)

