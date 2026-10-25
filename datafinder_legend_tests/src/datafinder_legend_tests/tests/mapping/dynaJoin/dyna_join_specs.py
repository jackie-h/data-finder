"""
Ported from legend-engine core_relational ``tests/mapping/dynaJoin/testDynaJoin.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/dynaJoin/testDynaJoin.pure"


DYNA_JOIN_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testFilterOnMultiLevelJoinWithNonAggregateFunction", f"{_SRC}::testFilterOnMultiLevelJoinWithNonAggregateFunction",
                    'select "root".eventType as "eventType", concat("addresstable_0".NAME, "addresstable_0".STREET) as "traderAddress" from tradeEventTable as "root" left outer join personTable as "persontable_0" on ("root".person_id = "persontable_0".ID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "persontable_0".ADDRESSID)',
                    'a join defined with a function'),
        unsupported("testFilterOnJoinWithAggregateFunctionWithProject", f"{_SRC}::testFilterOnJoinWithAggregateFunctionWithProject",
                    'select "root".ID as "id", "tradeeventviewmaxtradeeventdate_0".maxTradeEventDate as "latestEventDate" from tradeTable as "root" left outer join (select "root".trade_id as trade_id, max("root".eventDate) as maxTradeEventDate from tradeEventTable as "root" where "root".trade_id in (1, 6) group by "root".trade_id) as "tradeeventviewmaxtradeeventdate_0" on ("root".ID = "tradeeventviewmaxtradeeventdate_0".trade_id and "tradeeventviewmaxtradeeventdate_0".trade_id in (1, 6)) where "root".ID in (1, 6)',
                    'a join defined with a function'),
    ],
)
