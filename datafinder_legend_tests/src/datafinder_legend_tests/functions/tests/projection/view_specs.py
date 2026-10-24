"""
Ported from legend-engine core_relational ``functions/tests/projection/testView.pure``.

Properties mapped through database views.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/projection/testView.pure"

VIEW_SPECS = FinderSpec(
    finder_name="AccountFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testJoinToView", f"{_SRC}::testJoinToView",
                    'select "root".name as "accountName", "ordertable_0".ID as "tradeId", "orderpnlview_0".pnl as "pnl" from accountTable as "root" left outer join orderTable as "ordertable_0" on ("ordertable_0".accountID = "root".ID) left outer join (select distinct "root".ORDER_ID as ORDER_ID, "root".pnl as pnl, "accounttable_1".ID as accountId, "salespersontable_0".NAME as supportContact, "salespersontable_0".PERSON_ID as supportContactId from orderPnlTable as "root" left outer join orderTable as "ordertable_1" on ("root".ORDER_ID = "ordertable_1".ID) left outer join accountTable as "accounttable_1" on ("ordertable_1".accountID = "accounttable_1".ID) left outer join salesPersonTable as "salespersontable_0" on ("ordertable_1".accountID = "salespersontable_0".ACCOUNT_ID) where "root".pnl > 0) as "orderpnlview_0" on ("orderpnlview_0".ORDER_ID = "ordertable_0".ID)',
                    'no mapping through database views'),
        unsupported("testJoinThroughView", f"{_SRC}::testJoinThroughView",
                    'select "root".name as "accountName", "ordertable_0".ID as "tradeId", concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME) as "pnl" from accountTable as "root" left outer join orderTable as "ordertable_0" on ("ordertable_0".accountID = "root".ID) left outer join (select distinct "root".ORDER_ID as ORDER_ID, "root".pnl as pnl, "accounttable_1".ID as accountId, "salespersontable_0".NAME as supportContact, "salespersontable_0".PERSON_ID as supportContactId from orderPnlTable as "root" left outer join orderTable as "ordertable_1" on ("root".ORDER_ID = "ordertable_1".ID) left outer join accountTable as "accounttable_1" on ("ordertable_1".accountID = "accounttable_1".ID) left outer join salesPersonTable as "salespersontable_0" on ("ordertable_1".accountID = "salespersontable_0".ACCOUNT_ID) where "root".pnl > 0) as "orderpnlview_0" on ("orderpnlview_0".ORDER_ID = "ordertable_0".ID) left outer join personTable as "persontable_0" on ("orderpnlview_0".supportContactId = "persontable_0".ID)',
                    'no mapping through database views'),
    ],
)
