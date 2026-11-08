"""The finder query for each benchmark case of tpch_bih_cases.py (a case holds the SQL of tpch_bih_definitions.py and the expected rows).

An entry is a ``FinderQuery``, the finder columns it returns and the query, which is given the finders ``q`` and the values of the
case's parameters ``p``; or ``Unsupported`` with the reason the finders cannot ask the query as the report does. The
columns of the case that are compared, and in what order, are the case's ``select``: a query returns those columns.
"""
from __future__ import annotations

import datetime
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable

from datafinder.conditional import when
from datafinder.expressions import constant, count_all
from datafinder_examples_tests.tpch_bih_data import CURRENT_SYS


@dataclass(frozen=True)
class FinderQuery:
    columns: list[str]
    query: Callable[[SimpleNamespace, SimpleNamespace], Any]


@dataclass(frozen=True)
class Unsupported:
    reason: str


_NOW = CURRENT_SYS

_PART_NAVIGATION = (
    "The part key is read through the PART navigation, which joins PART as of the date: a supply whose part is not "
    "available at that application time, as currently recorded (a delay moves the start of availability), is lost, where "
    "the benchmark's SQL uses ps_partkey directly")
_FOREIGN_KEY = (
    "o_custkey is a foreign key, which the finders do not return as a value: the order's customer is a navigation that joins "
    "CUSTOMER, and that repeats an order for each version of its customer")
_TWO_AGGREGATES = (
    "Compares two independently aggregated time-travel subqueries (each at its own time), which a finder query, one select, "
    "cannot express")
_JOINED_AGGREGATES = (
    "Joins two grouped time-travel subqueries on the customer and orders by the difference of their aggregates, which a finder "
    "query cannot express")
_TEMPORAL_AGGREGATION = (
    "Temporal aggregation groups by the points where the data changed (an interval join on the period boundaries) and "
    "aggregates over those groups: the finders aggregate over the versions a date selects")
_SCALAR_SUBQUERY = "A scalar subquery, max(...) of the versions before this time, which the finders cannot express"


# ---------------------------------------------------------------------------------------------------------------------
# B.1
# ---------------------------------------------------------------------------------------------------------------------

def _receivable(o, day):
    """'[APP_TIME]' BETWEEN receivable_time_start AND receivable_time_end (BETWEEN includes both ends)"""
    return (o.receivable_time_begin() <= day).and_(o.receivable_time_end() >= day)


B1 = {
    "T1": FinderQuery(["avg_supplycost", "count"], lambda q, p: q.partsupp.find_all(
        p.APP_TIME, p.SYS_TIME, [q.partsupp.supply_cost().average().alias("avg_supplycost"), count_all().alias("count")])),

    "T2": FinderQuery(["revenue", "count"], lambda q, p: q.order.find_all(
        p.APP_TIME, p.SYS_TIME, [q.order.total_price().sum().alias("revenue"), count_all().alias("count")],
        q.order.order_status().eq("O"))),

    "T3_sys": Unsupported(_TWO_AGGREGATES),
    "T3_app": Unsupported(_TWO_AGGREGATES),
    "T4_sys": Unsupported(_JOINED_AGGREGATES),
    "T4_app": Unsupported(_JOINED_AGGREGATES),

    "T5": Unsupported(_FOREIGN_KEY),

    "T6_sys": FinderQuery(["Customer Customer Key", "supp_revenue"], lambda q, p: q.order.find_all(
        None, p.SYS_TIME, [q.order.customer().customer_key(), q.order.total_price().average().alias("supp_revenue")],
        q.order.order_status().eq("O"),
    ).group_by(q.order.customer().customer_key()).order_by(q.order.customer().customer_key().ascending())),

    "T6_app": FinderQuery(["Customer Customer Key", "supp_revenue"], lambda q, p: q.order.find_all(
        p.APP_TIME, None, [q.order.customer().customer_key(), q.order.total_price().average().alias("supp_revenue")],
        q.order.order_status().eq("O"),
    ).group_by(q.order.customer().customer_key()).order_by(q.order.customer().customer_key().ascending())),

    "T7": Unsupported(_PART_NAVIGATION),

    "T8": FinderQuery(["count"], lambda q, p: q.order.find_all(
        p.APP_TIME, p.SYS_TIME, [count_all().alias("count")], _receivable(q.order, p.APP_TIME))),

    "T9": FinderQuery(["count"], lambda q, p: q.order.find_all(
        None, p.SYS_TIME, [count_all().alias("count")], _receivable(q.order, p.APP_TIME))),
}


# ---------------------------------------------------------------------------------------------------------------------
# B.2
# ---------------------------------------------------------------------------------------------------------------------

def _revenue(l):
    return l.extended_price() * (constant(1) - l.discount())


B2 = {
    "H1": FinderQuery(
        ["l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "avg_qty", "avg_price",
         "avg_disc", "count_order"],
        lambda q, p: q.lineitem.find_all(
            p.APP_TIME, p.SYS_TIME,
            [q.lineitem.return_flag().alias("l_returnflag"), q.lineitem.line_status().alias("l_linestatus"),
             q.lineitem.quantity().sum().alias("sum_qty"), q.lineitem.extended_price().sum().alias("sum_base_price"),
             _revenue(q.lineitem).sum().alias("sum_disc_price"),
             (_revenue(q.lineitem) * (constant(1) + q.lineitem.tax())).sum().alias("sum_charge"),
             q.lineitem.quantity().average().alias("avg_qty"), q.lineitem.extended_price().average().alias("avg_price"),
             q.lineitem.discount().average().alias("avg_disc"), count_all().alias("count_order")],
            q.lineitem.ship_date() <= datetime.date(1998, 11, 1),
        ).group_by(q.lineitem.return_flag(), q.lineitem.line_status())
        .order_by(q.lineitem.return_flag().ascending(), q.lineitem.line_status().ascending())),
}


# ---------------------------------------------------------------------------------------------------------------------
# The TPC-H queries (B.2)
# ---------------------------------------------------------------------------------------------------------------------

def _q19_branch(f, brand: str, containers: list[str], quantity: float, max_size: int):
    return (f.part().brand().eq(brand).and_(f.part().container().is_in(containers))
            .and_(f.quantity() >= quantity).and_(f.quantity() <= quantity + 10)
            .and_(f.part().size() >= 1).and_(f.part().size() <= max_size)
            .and_(f.ship_mode().is_in(["AIR", "AIR REG"])).and_(f.ship_instruct().eq("DELIVER IN PERSON")))



def _order_priority_checking(lines, p):
    order = lines.order()
    return lines.find_all(
        p.APP_TIME, p.SYS_TIME,
        [order.order_priority().alias("o_orderpriority"), order.order_key().count_distinct().alias("order_count")],
        (lines.commit_date() < lines.receipt_date()).and_(order.order_date() >= p.DATE_FROM)
        .and_(order.order_date() < p.DATE_TO),
    ).group_by(order.order_priority()).order_by(order.order_priority().ascending())


def _parts_supplier_relationship(ps, p):
    part, supplier = ps.part(), ps.supplier()
    return ps.find_all(
        p.APP_TIME, p.SYS_TIME,
        [part.brand().alias("p_brand"), part.type_().alias("p_type"), part.size().alias("p_size"),
         supplier.supplier_key().count_distinct().alias("supplier_cnt")],
        part.brand().ne("Brand#45").and_(part.type_().not_like("MEDIUM POLISHED%"))
        .and_(part.size().is_in([49, 14, 23, 45, 19, 3, 36, 9]))
        .and_(supplier.comment().not_like("%Customer%Complaints%")),
    ).group_by(part.brand(), part.type_(), part.size()).order_by(
        supplier.supplier_key().count_distinct().descending(), part.brand().ascending(), part.type_().ascending(),
        part.size().ascending())


TPCH = {
    "Q2": Unsupported("A correlated scalar subquery: ps_supplycost = (select min(ps_supplycost) ... for the same part)"),

    "Q3": FinderQuery(["l_orderkey", "revenue", "o_orderdate", "o_shippriority"], lambda q, p: q.lineitem.find_all(
        p.APP_TIME, p.SYS_TIME,
        [q.lineitem.order().order_key().alias("l_orderkey"), _revenue(q.lineitem).sum().alias("revenue"),
         q.lineitem.order().order_date().alias("o_orderdate"), q.lineitem.order().ship_priority().alias("o_shippriority")],
        q.lineitem.order().customer().mkt_segment().eq(p.SEGMENT)
        .and_(q.lineitem.order().order_date() < p.DATE).and_(q.lineitem.ship_date() > p.DATE),
    ).group_by(q.lineitem.order().order_key(), q.lineitem.order().order_date(), q.lineitem.order().ship_priority())
        .order_by(_revenue(q.lineitem).sum().descending(), q.lineitem.order().order_date().ascending()).limit(10)),

    # EXISTS (a late line) is a count of the distinct orders of the late lines: the lines are the rows
    "Q4": FinderQuery(["o_orderpriority", "order_count"], lambda q, p: _order_priority_checking(q.lineitem, p)),

    "Q5": FinderQuery(["n_name", "revenue"], lambda q, p: q.lineitem.find_all(
        p.APP_TIME, p.SYS_TIME,
        [q.lineitem.order().customer().nation().name().alias("n_name"), _revenue(q.lineitem).sum().alias("revenue")],
        q.lineitem.order().customer().nation().region().name().eq(p.REGION)
        .and_(q.lineitem.order().order_date() >= p.DATE_FROM).and_(q.lineitem.order().order_date() < p.DATE_TO)
        .and_(q.lineitem.order().customer().nation().name().eq(q.lineitem.supplier().nation().name())),
    ).group_by(q.lineitem.order().customer().nation().name()).order_by(_revenue(q.lineitem).sum().descending())),

    "Q6": FinderQuery(["revenue"], lambda q, p: q.lineitem.find_all(
        p.APP_TIME, p.SYS_TIME, [(q.lineitem.extended_price() * q.lineitem.discount()).sum().alias("revenue")],
        (q.lineitem.ship_date() >= p.DATE_FROM).and_(q.lineitem.ship_date() < p.DATE_TO)
        .and_(q.lineitem.discount() >= p.DISCOUNT_LOW).and_(q.lineitem.discount() <= p.DISCOUNT_HIGH)
        .and_(q.lineitem.quantity() < p.QUANTITY))),

    "Q7": FinderQuery(["supp_nation", "cust_nation", "l_year", "revenue"], lambda q, p: q.lineitem.find_all(
        p.APP_TIME, p.SYS_TIME,
        [q.lineitem.supplier().nation().name().alias("supp_nation"),
         q.lineitem.order().customer().nation().name().alias("cust_nation"),
         q.lineitem.ship_date().year().alias("l_year"), _revenue(q.lineitem).sum().alias("revenue")],
        q.lineitem.supplier().nation().name().eq(p.NATION1).and_(q.lineitem.order().customer().nation().name().eq(p.NATION2))
        .or_(q.lineitem.supplier().nation().name().eq(p.NATION2)
             .and_(q.lineitem.order().customer().nation().name().eq(p.NATION1)))
        .and_(q.lineitem.ship_date() >= p.DATE_FROM).and_(q.lineitem.ship_date() <= p.DATE_TO),
    ).order_by(q.lineitem.supplier().nation().name().ascending(), q.lineitem.order().customer().nation().name().ascending(),
               q.lineitem.ship_date().year().ascending())),

    "Q8": FinderQuery(["o_year", "mkt_share"], lambda q, p: q.lineitem.find_all(
        p.APP_TIME, p.SYS_TIME,
        [q.lineitem.order().order_date().year().alias("o_year"),
         (when(q.lineitem.supplier().nation().name().eq(p.NATION)).then(_revenue(q.lineitem)).otherwise(0).sum()
          / _revenue(q.lineitem).sum()).alias("mkt_share")],
        q.lineitem.order().customer().nation().region().name().eq(p.REGION)
        .and_(q.lineitem.order().order_date() >= p.DATE_FROM).and_(q.lineitem.order().order_date() <= p.DATE_TO)
        .and_(q.lineitem.part().type_().eq(p.TYPE)),
    ).group_by(q.lineitem.order().order_date().year()).order_by(q.lineitem.order().order_date().year().ascending())),

    "Q9": Unsupported(
        "Joins PARTSUPP on a composite key: ps_partkey = l_partkey and ps_suppkey = l_suppkey. An association joins on one column"),

    "Q10": FinderQuery(
        ["c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment"],
        lambda q, p: q.customer.find_all(
            p.APP_TIME, p.SYS_TIME,
            [q.customer.customer_key().alias("c_custkey"), q.customer.name().alias("c_name"),
             (q.customer.orders().line_items().extended_price() * (constant(1) - q.customer.orders().line_items().discount()))
             .sum().alias("revenue"),
             q.customer.acct_bal().alias("c_acctbal"), q.customer.nation().name().alias("n_name"),
             q.customer.address().alias("c_address"), q.customer.phone().alias("c_phone"),
             q.customer.comment().alias("c_comment")],
            (q.customer.orders().order_date() >= p.DATE_FROM).and_(q.customer.orders().order_date() < p.DATE_TO)
            .and_(q.customer.orders().line_items().return_flag().eq("R")),
        ).group_by(q.customer.customer_key(), q.customer.name(), q.customer.acct_bal(), q.customer.phone(),
                   q.customer.nation().name(), q.customer.address(), q.customer.comment())
        .order_by((q.customer.orders().line_items().extended_price()
                   * (constant(1) - q.customer.orders().line_items().discount())).sum().descending()).limit(20)),

    "Q11": Unsupported("A scalar subquery in HAVING: sum(ps_supplycost * ps_availqty) > (select sum(...) * FRACTION ...)"),

    "Q12": FinderQuery(["l_shipmode", "high_line_count", "low_line_count"], lambda q, p: q.lineitem.find_all(
        p.APP_TIME, p.SYS_TIME,
        [q.lineitem.ship_mode().alias("l_shipmode"),
         when(q.lineitem.order().order_priority().eq("1-URGENT").or_(q.lineitem.order().order_priority().eq("2-HIGH")))
         .then(1).otherwise(0).sum().alias("high_line_count"),
         when(q.lineitem.order().order_priority().ne("1-URGENT").and_(q.lineitem.order().order_priority().ne("2-HIGH")))
         .then(1).otherwise(0).sum().alias("low_line_count")],
        q.lineitem.ship_mode().is_in(["MAIL", "SHIP"]).and_(q.lineitem.commit_date() < q.lineitem.receipt_date())
        .and_(q.lineitem.ship_date() < q.lineitem.commit_date())
        .and_(q.lineitem.receipt_date() >= p.DATE_FROM).and_(q.lineitem.receipt_date() < p.DATE_TO)
        .and_(q.lineitem.order().exists()),
    ).order_by(q.lineitem.ship_mode().ascending())),

    "Q13": Unsupported(
        "A derived table with a LEFT OUTER JOIN whose ON clause has a condition (o_comment not like '%[WORD1]%[WORD2]%'), "
        "grouped again by the count it produced"),

    "Q14": FinderQuery(["promo_revenue"], lambda q, p: q.lineitem.find_all(
        p.APP_TIME, p.SYS_TIME,
        [((constant(100.0) * when(q.lineitem.part().type_().starts_with("PROMO")).then(_revenue(q.lineitem)).otherwise(0).sum())
          / _revenue(q.lineitem).sum()).alias("promo_revenue")],
        (q.lineitem.ship_date() >= p.DATE_FROM).and_(q.lineitem.ship_date() < p.DATE_TO).and_(q.lineitem.part().exists()))),

    "Q15": Unsupported("A derived table of revenue by supplier, and total_revenue = (select max(total_revenue) from it)"),
    # NOT IN (the suppliers with complaints) is the supplier's own comment not being a complaint: the supplier is a navigation
    "Q16": FinderQuery(["p_brand", "p_type", "p_size", "supplier_cnt"], lambda q, p: _parts_supplier_relationship(q.partsupp, p)),
    "Q17": Unsupported("A correlated scalar subquery: l_quantity < (select 0.2 * avg(l_quantity) ... for the same part)"),
    "Q18": Unsupported("o_orderkey in (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > [QUANTITY]): "
                       "an IN subquery with GROUP BY and HAVING"),

    "Q19": FinderQuery(["revenue"], lambda q, p: q.lineitem.find_all(
        p.APP_TIME, p.SYS_TIME, [_revenue(q.lineitem).sum().alias("revenue")],
        _q19_branch(q.lineitem, "Brand#12", ["SM CASE", "SM BOX", "SM PACK", "SM PKG"], 1.0, 5)
        .or_(_q19_branch(q.lineitem, "Brand#23", ["MED BAG", "MED BOX", "MED PKG", "MED PACK"], 10.0, 10))
        .or_(_q19_branch(q.lineitem, "Brand#34", ["LG CASE", "LG BOX", "LG PACK", "LG PKG"], 20.0, 15)))),

    "Q20": Unsupported("Nested IN subqueries and a correlated scalar subquery: ps_availqty > (select 0.5 * sum(l_quantity) ...)"),
    "Q21": Unsupported(
        "EXISTS and NOT EXISTS with conditions on the other lines of the order (l2.l_suppkey <> l1.l_suppkey, "
        "l3.l_receiptdate > l3.l_commitdate): exists() takes no condition"),
    "Q22": Unsupported("substring(c_phone from 1 for 2) in a list, a scalar subquery average, and NOT EXISTS"),
}

# ---------------------------------------------------------------------------------------------------------------------
# B.3
# ---------------------------------------------------------------------------------------------------------------------

_FOREVER_START = "0001-01-01"
_FOREVER_END = "9999-12-30"
_ORDER_COLUMNS = ["Order Key", "Order Status", "Total Price", "Order Date", "Order Priority", "Ship Priority"]
_CUSTOMER_COLUMNS = ["Customer Key", "Name", "Address", "Phone", "Acct Bal"]


def _order(o):
    return [o.order_key(), o.order_status(), o.total_price(), o.order_date(), o.order_priority(), o.ship_priority()]


def _customer(c):
    return [c.customer_key(), c.name(), c.address(), c.phone(), c.acct_bal()]


B3 = {
    "K1_sys": FinderQuery([*_ORDER_COLUMNS, "Sys Time Begin"], lambda q, p: q.order.find_all(
        p.APP_TIME, None, [*_order(q.order), q.order.sys_time_begin()], q.order.order_key().eq(p.ORDER_KEY),
    ).order_by(q.order.sys_time_begin().ascending())),

    "K1_app": FinderQuery([*_ORDER_COLUMNS, "Active Time Begin"], lambda q, p: q.order.find_for_date_range(
        _FOREVER_START, _FOREVER_END, _NOW, [*_order(q.order), q.order.active_time_begin()],
        q.order.order_key().eq(p.ORDER_KEY),
    ).order_by(q.order.active_time_begin().ascending())),

    "K2_sys": FinderQuery([*_CUSTOMER_COLUMNS, "Sys Time Begin"], lambda q, p: q.customer.find_all(
        p.APP_TIME, None, [*_customer(q.customer), q.customer.sys_time_begin()],
        q.customer.customer_key().eq(p.CUSTOMER_KEY).and_(q.customer.sys_time_begin() < p.SYS_TIME_END)
        .and_(q.customer.sys_time_end() > p.SYS_TIME_BEGIN),
    ).order_by(q.customer.sys_time_begin().ascending())),

    "K2_app": FinderQuery([*_CUSTOMER_COLUMNS, "Visible Time Begin"], lambda q, p: q.customer.find_for_date_range(
        p.APP_TIME_BEGIN, p.APP_TIME_END, _NOW, [*_customer(q.customer), q.customer.visible_time_begin()],
        q.customer.customer_key().eq(p.CUSTOMER_KEY),
    ).order_by(q.customer.visible_time_begin().ascending())),

    "K3_sys": FinderQuery(["min_balance"], lambda q, p: q.customer.find_all(
        p.APP_TIME, None, [q.customer.acct_bal().min().alias("min_balance")],
        q.customer.customer_key().eq(p.CUSTOMER_KEY).and_(q.customer.sys_time_begin() < p.SYS_TIME_END)
        .and_(q.customer.sys_time_end() > p.SYS_TIME_BEGIN))),

    "K3_app": FinderQuery(["min_balance"], lambda q, p: q.customer.find_for_date_range(
        p.APP_TIME_BEGIN, p.APP_TIME_END, _NOW, [q.customer.acct_bal().min().alias("min_balance")],
        q.customer.customer_key().eq(p.CUSTOMER_KEY))),

    "K4_sys": FinderQuery([*_CUSTOMER_COLUMNS, "Sys Time Begin", "Sys Time End"], lambda q, p: q.customer.find_all(
        p.APP_TIME, None, [*_customer(q.customer), q.customer.sys_time_begin(), q.customer.sys_time_end()],
        q.customer.customer_key().eq(p.CUSTOMER_KEY),
    ).order_by(q.customer.sys_time_begin().ascending()).limit(p.HSIZE)),

    "K4_app": FinderQuery([*_CUSTOMER_COLUMNS, "Visible Time Begin", "Visible Time End"],
                          lambda q, p: q.customer.find_for_date_range(
        _FOREVER_START, _FOREVER_END, _NOW,
        [*_customer(q.customer), q.customer.visible_time_begin(), q.customer.visible_time_end()],
        q.customer.customer_key().eq(p.CUST_KEY),
    ).order_by(q.customer.visible_time_begin().ascending()).limit(p.HSIZE)),

    "K5_sys": Unsupported(_SCALAR_SUBQUERY),
    "K5_app": Unsupported(_SCALAR_SUBQUERY),

    "K6_sys": FinderQuery(["Customer Key", "Acct Bal", "Sys Time Begin"], lambda q, p: q.customer.find_all(
        p.APP_TIME, None, [q.customer.customer_key(), q.customer.acct_bal(), q.customer.sys_time_begin()],
        q.customer.acct_bal() > p.CUST_BAL,
    ).order_by(q.customer.customer_key().ascending(), q.customer.acct_bal().ascending(),
               q.customer.sys_time_begin().ascending())),

    "K6_app": FinderQuery(["Customer Key", "Acct Bal", "Visible Time Begin"], lambda q, p: q.customer.find_for_date_range(
        _FOREVER_START, _FOREVER_END, _NOW,
        [q.customer.customer_key(), q.customer.acct_bal(), q.customer.visible_time_begin()],
        q.customer.acct_bal() > p.CUST_BAL,
    ).order_by(q.customer.customer_key().ascending(), q.customer.acct_bal().ascending(),
               q.customer.visible_time_begin().ascending())),
}


# ---------------------------------------------------------------------------------------------------------------------
# B.4
# ---------------------------------------------------------------------------------------------------------------------

B4 = {
    "R1_sys": Unsupported(
        "Joins two grouped system-time subqueries (the latest version before and the first after the move) and compares their "
        "bounds, with a scalar subquery for the nation"),
    "R1_app": Unsupported(
        "Joins two grouped application-time subqueries (the latest version before and the first after the move) and compares "
        "their bounds, with a scalar subquery for the nation"),
    "R2_sys": Unsupported("Joins two grouped subqueries, the earliest system time of each order status, and averages the "
                          "difference of two timestamps"),
    "R2_app": Unsupported("Joins two grouped subqueries, the earliest application time of each order status, and averages the "
                          "difference of two dates"),
    "R3a": Unsupported(_TEMPORAL_AGGREGATION),
    "R3b": Unsupported(_TEMPORAL_AGGREGATION),
    "R4": Unsupported(
        "Needs every version of a key compared with the others (the extreme stock levels and when they occurred), as nested "
        "aggregates over versions of the same table"),
    "R5": Unsupported(
        "COUNT DISTINCT is supported, but the finder joins a related temporal row at the root version's time: it cannot pair "
        "every customer version with every order version whose system periods overlap"),
    "R6": Unsupported(
        "A temporal aggregation (a row for each point where the data changed, from an interval join on the period boundaries) "
        "over a join of two temporal tables, which the finders cannot do"),

    "R7": FinderQuery(
        ["Part Part Key", "Supplier Supplier Key", "Supply Cost", "Validity Time Begin", "Validity Time End",
         "Same Part Validity Time Begin", "Same Part Validity Time End", "Same Part Supply Cost"],
        lambda q, p: q.partsupp.find_all(
            None, _NOW,
            [q.partsupp.part().part_key(), q.partsupp.supplier().supplier_key(), q.partsupp.supply_cost(),
             q.partsupp.validity_time_begin(), q.partsupp.validity_time_end(),
             q.partsupp.same_part().validity_time_begin(), q.partsupp.same_part().validity_time_end(),
             q.partsupp.same_part().supply_cost()],
            q.partsupp.supplier().supplier_key().eq(q.partsupp.same_part().supplier().supplier_key())
            .and_(q.partsupp.same_part().validity_time_end() == q.partsupp.validity_time_begin())
            .and_(q.partsupp.supply_cost() > q.partsupp.same_part().supply_cost() * 1.075),
        ).order_by(q.partsupp.validity_time_end().ascending(), q.partsupp.same_part().validity_time_end().descending())),
}

# ---------------------------------------------------------------------------------------------------------------------
# B.5: ps1 is the supply of the given part, ps2 the supply of the other part that is returned. The finder's root is ps2, and
# ps1 is reached through the supplier, which gives it the dates of the root unless it is asked for at others.
# ---------------------------------------------------------------------------------------------------------------------

def _other_parts(f, p, app, sys, application=False, system=None, anchor_app=None):
    """The part and the periods of ps2, DISTINCT. ``application``: ps1.VALIDITY_TIME_START <= ps2.VALIDITY_TIME_END AND ps1.VALIDITY_TIME_END >=
    ps2.VALIDITY_TIME_START. ``system``: "overlap" is ps1.sys_time_start < ps2.sys_time_END AND ps2.sys_time_start <
    ps1.sys_time_END; "starts_within" is ps1.sys_time_start <= ps2.sys_time_start AND ps2.sys_time_start < ps1.sys_time_END."""
    ps1 = f.supplier().part_supps() if anchor_app is None else f.supplier().part_supps(
        business_date=anchor_app, processing_time=sys)
    condition = ps1.part().part_key().eq(p.PART_KEY).and_(f.part().part_key().ne(p.PART_KEY))
    if application:
        condition = condition.and_(ps1.validity_time_begin() <= f.validity_time_end()) \
            .and_(ps1.validity_time_end() >= f.validity_time_begin())
    if system == "overlap":
        condition = condition.and_(ps1.sys_time_begin() < f.sys_time_end()).and_(f.sys_time_begin() < ps1.sys_time_end())
    elif system == "starts_within":
        condition = condition.and_(ps1.sys_time_begin() <= f.sys_time_begin()).and_(f.sys_time_begin() < ps1.sys_time_end())
    columns = [f.part().part_key(), f.validity_time_begin(), f.validity_time_end(), f.sys_time_begin(), f.sys_time_end()]
    return f.find_all(app, sys, columns, condition).distinct().order_by(*[c.ascending() for c in columns])


_PART_COLUMN = ["Part Part Key", "Validity Time Begin", "Validity Time End", "Sys Time Begin", "Sys Time End"]

B5 = {
    "B5_1": Unsupported(_PART_NAVIGATION),
    "B5_2": Unsupported(_PART_NAVIGATION),
    "B5_3": FinderQuery(_PART_COLUMN, lambda q, p: _other_parts(q.partsupp, p, None, _NOW, application=True)),
    "B5_4": FinderQuery(_PART_COLUMN, lambda q, p: _other_parts(q.partsupp, p, p.APP_TIME, None, system="overlap")),
    "B5_5": FinderQuery(_PART_COLUMN, lambda q, p: _other_parts(q.partsupp, p, None, None, application=True, system="overlap")),
    "B5_6": FinderQuery(_PART_COLUMN, lambda q, p: _other_parts(q.partsupp, p, None, _NOW)),
    "B5_7": FinderQuery(_PART_COLUMN, lambda q, p: _other_parts(q.partsupp, p, None, p.SYS_TIME)),
    "B5_8": FinderQuery(_PART_COLUMN, lambda q, p: _other_parts(q.partsupp, p, None, None, system="overlap")),
    "B5_9": FinderQuery(_PART_COLUMN, lambda q, p: _other_parts(q.partsupp, p, p.APP_TIME, None, system="starts_within")),
    "B5_10": FinderQuery(_PART_COLUMN, lambda q, p: _other_parts(q.partsupp, p, None, None, application=True, system="starts_within")),
    "B5_11": FinderQuery(_PART_COLUMN, lambda q, p: _other_parts(q.partsupp, p, None, None, system="starts_within")),
}

QUERIES: dict[str, FinderQuery | Unsupported] = {**B1, **B2, **B3, **B4, **B5, **TPCH}
