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

def _open_revenue(o, app, sys, name):
    """SELECT SUM(o_totalprice) AS name FROM orders ... WHERE o_orderstatus = 'O'"""
    return o.find_all(app, sys, [o.total_price().sum().alias(name)], o.order_status().eq("O"))


def _revenue_per_customer(o, app, sys, status, name):
    return o.find_all(app, sys, [o.total_price().sum().alias(name), o.customer().customer_key().alias(name + "_custkey")],
                      o.order_status().eq(status)).group_by(o.customer().customer_key())


def _revenue_increase(before, after):
    """``SELECT revenue2 - revenue1 AS inc_revenue, o_custkey ... WHERE a.o_custkey = b.o_custkey ORDER BY inc_revenue DESC
    FETCH FIRST 10``: two grouped relations joined on the customer, then a column worked out from both."""
    both = before.join(after, on=before["revenue1_custkey"] == after["revenue2_custkey"])
    extended = both.extend(inc_revenue=both["revenue2"] - both["revenue1"])
    result = extended.select("inc_revenue", extended["revenue1_custkey"].alias("o_custkey"))
    return result.order_by(result["inc_revenue"].descending()).limit(10)


def _receivable(o, day):
    """'[APP_TIME]' BETWEEN receivable_time_start AND receivable_time_end (BETWEEN includes both ends)"""
    return (o.receivable_time_begin() <= day).and_(o.receivable_time_end() >= day)


B1 = {
    "T1": FinderQuery(["avg_supplycost", "count"], lambda q, p: q.partsupp.find_all(
        p.APP_TIME, p.SYS_TIME, [q.partsupp.supply_cost().average().alias("avg_supplycost"), count_all().alias("count")])),

    "T2": FinderQuery(["revenue", "count"], lambda q, p: q.order.find_all(
        p.APP_TIME, p.SYS_TIME, [q.order.total_price().sum().alias("revenue"), count_all().alias("count")],
        q.order.order_status().eq("O"))),


    # two queries, each its own relation, and the join of them
    "T3_sys": FinderQuery(["old_revenue", "new_revenue"], lambda q, p: _open_revenue(
        q.order, p.APP_TIME, p.SYS_TIME1, "old_revenue").join(_open_revenue(q.order, p.APP_TIME, p.SYS_TIME2, "new_revenue"))),
    "T3_app": FinderQuery(["old_revenue", "new_revenue"], lambda q, p: _open_revenue(
        q.order, p.APP_TIME1, _NOW, "old_revenue").join(_open_revenue(q.order, p.APP_TIME2, _NOW, "new_revenue"))),
    "T4_sys": FinderQuery(["inc_revenue", "o_custkey"], lambda q, p: _revenue_increase(
        _revenue_per_customer(q.order, p.APP_TIME, p.SYS_TIME1, "O", "revenue1"),
        _revenue_per_customer(q.order, p.APP_TIME, p.SYS_TIME2, "F", "revenue2"))),
    "T4_app": FinderQuery(["inc_revenue", "o_custkey"], lambda q, p: _revenue_increase(
        _revenue_per_customer(q.order, p.APP_TIME1, _NOW, "O", "revenue1"),
        _revenue_per_customer(q.order, p.APP_TIME2, _NOW, "O", "revenue2"))),

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
        part.brand().ne("Brand#45").and_(~part.type_().starts_with("MEDIUM POLISHED"))
        .and_(part.size().is_in([49, 14, 23, 45, 19, 3, 36, 9]))
        .and_(~supplier.comment().matches("Customer.*Complaints")),
    ).group_by(part.brand(), part.type_(), part.size()).order_by(
        supplier.supplier_key().count_distinct().descending(), part.brand().ascending(), part.type_().ascending(),
        part.size().ascending())


def _important_stock(ps, p):
    value = (ps.supply_cost() * ps.avail_qty()).sum()
    parts = ps.find_all(
        p.APP_TIME, p.SYS_TIME, [ps.part().part_key().alias("ps_partkey"), value.alias("value")],
        ps.supplier().nation().name().eq(p.NATION)).group_by(ps.part().part_key())
    with_total = parts.extend(total=parts["value"].sum().over())
    important = with_total.filter(with_total["value"] > with_total["total"] * p.FRACTION).select("ps_partkey", "value")
    return important.order_by(important["value"].descending())


def _top_supplier(q, p):
    lines = q.lineitem
    revenue = lines.find_all(
        p.APP_TIME, p.SYS_TIME, [lines.supplier().supplier_key().alias("supplier_no"), _revenue(lines).sum().alias("total_revenue")],
        (lines.ship_date() >= p.DATE_FROM).and_(lines.ship_date() < p.DATE_TO)).group_by(lines.supplier().supplier_key())
    with_best = revenue.extend(best=revenue["total_revenue"].max().over())
    top = with_best.filter(with_best["total_revenue"] == with_best["best"]).select("supplier_no", "total_revenue")
    suppliers = q.supplier.find_all(
        None, p.SYS_TIME, [q.supplier.supplier_key().alias("s_suppkey"), q.supplier.name().alias("s_name"),
                           q.supplier.address().alias("s_address"), q.supplier.phone().alias("s_phone")])
    result = suppliers.join(top, on=suppliers["s_suppkey"] == top["supplier_no"]).select(
        "s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
    return result.order_by(result["s_suppkey"].ascending())


def _product_type_profit_measure(q, p):
    lines, ps = q.lineitem, q.partsupp
    sold = lines.find_all(
        p.APP_TIME, p.SYS_TIME,
        [lines.supplier().nation().name().alias("nation"), lines.order().order_date().year().alias("o_year"),
         lines.part().part_key().alias("l_partkey"), lines.supplier().supplier_key().alias("l_suppkey"),
         lines.extended_price().alias("price"), lines.discount().alias("discount"), lines.quantity().alias("quantity")],
        lines.part().name().contains("green"))
    cost = ps.find_all(p.APP_TIME, p.SYS_TIME, [ps.part().part_key().alias("ps_partkey"),
                                                 ps.supplier().supplier_key().alias("ps_suppkey"),
                                                 ps.supply_cost().alias("supplycost")])
    joined = sold.join(cost, on=(sold["l_partkey"] == cost["ps_partkey"]).and_(sold["l_suppkey"] == cost["ps_suppkey"]))
    profit = joined.extend(amount=joined["price"] * (constant(1) - joined["discount"]) - joined["supplycost"] * joined["quantity"])
    result = profit.select(profit["nation"], profit["o_year"], profit["amount"].sum().alias("sum_profit")) \
        .group_by(profit["nation"], profit["o_year"])
    return result.order_by(result["nation"].ascending(), result["o_year"].descending())


def _potential_part_promotion(q, p):
    lines, ps, parts, supplier = q.lineitem, q.partsupp, q.part, q.supplier
    forest = parts.find_all(p.APP_TIME, p.SYS_TIME, [parts.part_key().alias("forest_part")],
                            parts.name().starts_with(p.COLOR))
    shipped = lines.find_all(
        p.APP_TIME, p.SYS_TIME,
        [lines.part().part_key().alias("l_partkey"), lines.supplier().supplier_key().alias("l_suppkey"),
         lines.quantity().sum().alias("shipped")],
        (lines.ship_date() >= p.DATE_FROM).and_(lines.ship_date() < p.DATE_TO),
    ).group_by(lines.part().part_key(), lines.supplier().supplier_key())
    supplies = ps.find_all(p.APP_TIME, p.SYS_TIME, [ps.part().part_key().alias("ps_partkey"),
                                                     ps.supplier().supplier_key().alias("ps_suppkey"),
                                                     ps.avail_qty().alias("available")])
    in_forest = supplies.join(forest, on=supplies["ps_partkey"] == forest["forest_part"])
    with_shipped = in_forest.join(shipped, on=(in_forest["ps_partkey"] == shipped["l_partkey"])
                                  .and_(in_forest["ps_suppkey"] == shipped["l_suppkey"]))
    enough = with_shipped.filter(with_shipped["available"] > with_shipped["shipped"] * 0.5).select("ps_suppkey").distinct()
    suppliers = supplier.find_all(
        None, p.SYS_TIME,
        [supplier.supplier_key().alias("s_suppkey"), supplier.name().alias("s_name"), supplier.address().alias("s_address")],
        supplier.nation().name().eq(p.NATION))
    result = suppliers.join(enough, on=suppliers["s_suppkey"] == enough["ps_suppkey"]).select("s_name", "s_address")
    return result.order_by(result["s_name"].ascending())


def _suppliers_who_kept_orders_waiting(q, p):
    lines, orders = q.lineitem, q.order
    l1 = lines.find_all(
        p.APP_TIME, p.SYS_TIME,
        [lines.order().order_key().alias("l1_order"), lines.line_number().alias("l1_line"),
         lines.supplier().supplier_key().alias("l1_supplier"), lines.supplier().name().alias("s_name")],
        (lines.receipt_date() > lines.commit_date()).and_(lines.order().order_status().eq("F")).and_(lines.supplier().nation().name().eq(p.NATION)))
    others = lines.find_all(p.APP_TIME, p.SYS_TIME, [lines.order().order_key().alias("l2_order"),
                                                      lines.supplier().supplier_key().alias("l2_supplier")])
    late_others = lines.find_all(
        p.APP_TIME, p.SYS_TIME,
        [lines.order().order_key().alias("l3_order"), lines.supplier().supplier_key().alias("l3_supplier")],
        lines.receipt_date() > lines.commit_date())
    with_other = l1.join(others, on=(l1["l1_order"] == others["l2_order"]).and_(l1["l1_supplier"].ne(others["l2_supplier"])))
    waiting = with_other.select("l1_order", "l1_line", "l1_supplier", "s_name").distinct()
    without_late = waiting.join(
        late_others, on=(waiting["l1_order"] == late_others["l3_order"]).and_(waiting["l1_supplier"].ne(late_others["l3_supplier"])),
        left=True)
    kept = without_late.filter(without_late["l3_order"].is_none())
    result = kept.select(kept["s_name"], count_all().alias("numwait")).group_by(kept["s_name"])
    return result.order_by(result["numwait"].descending(), result["s_name"].ascending()).limit(100)


def _large_volume_customer(lines, p):
    order = lines.order()
    totals = lines.find_all(p.APP_TIME, p.SYS_TIME, [order.order_key().alias("big_order"),
                                                    lines.quantity().sum().alias("big_quantity")]).group_by(order.order_key())
    big = totals.filter(totals["big_quantity"] > p.QUANTITY)
    rows = lines.find_all(
        p.APP_TIME, p.SYS_TIME,
        [order.customer().name().alias("c_name"), order.customer().customer_key().alias("c_custkey"),
         order.order_key().alias("o_orderkey"), order.order_date().alias("o_orderdate"),
         order.total_price().alias("o_totalprice"), lines.quantity().sum().alias("sum_quantity")],
    ).group_by(order.customer().name(), order.customer().customer_key(), order.order_key(), order.order_date(),
               order.total_price())
    joined = rows.join(big, on=rows["o_orderkey"] == big["big_order"])
    result = joined.select(joined["c_name"], joined["c_custkey"], joined["o_orderkey"], joined["o_orderdate"],
                           joined["o_totalprice"], joined["sum_quantity"].alias("sum(l_quantity)"))
    return result.order_by(result["o_totalprice"].descending(), result["o_orderdate"].ascending()).limit(100)


def _customer_distribution(q, p):
    customers = q.customer.find_all(p.APP_TIME, p.SYS_TIME, [q.customer.customer_key().alias("c_custkey")])
    orders = q.order.find_all(
        p.APP_TIME, p.SYS_TIME, [q.order.order_key().alias("o_orderkey"), q.order.customer().customer_key().alias("o_custkey")],
        ~q.order.comment().matches("special.*requests"))
    joined = customers.join(orders, on=customers["c_custkey"] == orders["o_custkey"], left=True)
    counts = joined.select(joined["c_custkey"], joined["o_orderkey"].count().alias("c_count")).group_by(joined["c_custkey"])
    distribution = counts.select(counts["c_count"], count_all().alias("custdist")).group_by(counts["c_count"])
    return distribution.order_by(distribution["custdist"].descending(), distribution["c_count"].descending())


def _minimum_cost_supplier(ps, p):
    supplier = ps.supplier()
    rows = ps.find_all(
        p.APP_TIME, p.SYS_TIME,
        [supplier.acct_bal().alias("s_acctbal"), supplier.name().alias("s_name"), supplier.nation().name().alias("n_name"),
         ps.part().part_key().alias("p_partkey"), ps.part().mfgr().alias("p_mfgr"), supplier.address().alias("s_address"),
         supplier.phone().alias("s_phone"), supplier.comment().alias("s_comment"), ps.supply_cost().alias("cost")],
        ps.part().size().eq(p.SIZE).and_(ps.part().type_().ends_with(p.TYPE.removeprefix("%")))
        .and_(supplier.nation().region().name().eq(p.REGION)))
    with_cheapest = rows.extend(cheapest=rows["cost"].min().over(partition_by=[rows["p_partkey"]]))
    cheapest = with_cheapest.filter(with_cheapest["cost"] == with_cheapest["cheapest"]).select(
        "s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
    return cheapest.order_by(cheapest["s_acctbal"].descending(), cheapest["n_name"].ascending(),
                             cheapest["s_name"].ascending(), cheapest["p_partkey"].ascending()).limit(100)


def _small_quantity_order_revenue(lines, p):
    rows = lines.find_all(
        p.APP_TIME, p.SYS_TIME,
        [lines.part().part_key().alias("partkey"), lines.quantity().alias("quantity"),
         lines.extended_price().alias("price")],
        lines.part().brand().eq(p.BRAND).and_(lines.part().container().eq(p.CONTAINER)))
    with_average = rows.extend(average=rows["quantity"].average().over(partition_by=[rows["partkey"]]))
    small = with_average.filter(with_average["quantity"] < with_average["average"] * 0.2)
    return small.select((small["price"].sum() / 7.0).alias("avg_yearly"))


_Q22_CODES = ["13", "31", "23", "29", "30", "18", "17"]


def _global_sales_opportunity(c, p):
    code = c.phone().substring(0, 2)  # the first two characters: a start counts from 0
    average = c.find_all(p.APP_TIME, p.SYS_TIME, [c.acct_bal().average().alias("average_balance")],
                         code.is_in(_Q22_CODES).and_(c.acct_bal() > 0.0))
    customers = c.find_all(p.APP_TIME, p.SYS_TIME, [code.alias("cntrycode"), c.acct_bal().alias("c_acctbal")],
                           code.is_in(_Q22_CODES).and_(c.orders().not_exists()))
    both = customers.join(average)
    above = both.filter(both["c_acctbal"] > both["average_balance"]).select("cntrycode", "c_acctbal")
    result = above.select(above["cntrycode"], count_all().alias("numcust"), above["c_acctbal"].sum().alias("totacctbal")) \
        .group_by(above["cntrycode"])
    return result.order_by(result["cntrycode"].ascending())


TPCH = {
    # the cheapest supply of a part among the suppliers in the region is a window over those supplies, partitioned by part
    "Q2": FinderQuery(
        ["s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment"],
        lambda q, p: _minimum_cost_supplier(q.partsupp, p)),

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

    # the composite key join is a join of two relations on both columns
    "Q9": FinderQuery(["nation", "o_year", "sum_profit"], lambda q, p: _product_type_profit_measure(q, p)),

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

    # HAVING sum(...) > (SELECT sum(...) * FRACTION ...): the total over all the groups is a window over the grouped rows
    "Q11": FinderQuery(["ps_partkey", "value"], lambda q, p: _important_stock(q.partsupp, p)),

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

    # a left outer join, counted for each customer, and the customers counted for each count
    "Q13": FinderQuery(["c_count", "custdist"], lambda q, p: _customer_distribution(q, p)),

    "Q14": FinderQuery(["promo_revenue"], lambda q, p: q.lineitem.find_all(
        p.APP_TIME, p.SYS_TIME,
        [((constant(100.0) * when(q.lineitem.part().type_().starts_with("PROMO")).then(_revenue(q.lineitem)).otherwise(0).sum())
          / _revenue(q.lineitem).sum()).alias("promo_revenue")],
        (q.lineitem.ship_date() >= p.DATE_FROM).and_(q.lineitem.ship_date() < p.DATE_TO).and_(q.lineitem.part().exists()))),

    "Q15": FinderQuery(["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"], lambda q, p: _top_supplier(q, p)),
    # NOT IN (the suppliers with complaints) is the supplier's own comment not being a complaint: the supplier is a navigation
    "Q16": FinderQuery(["p_brand", "p_type", "p_size", "supplier_cnt"], lambda q, p: _parts_supplier_relationship(q.partsupp, p)),
    "Q17": FinderQuery(["avg_yearly"], lambda q, p: _small_quantity_order_revenue(q.lineitem, p)),
    # IN (... GROUP BY ... HAVING ...) is a join to the grouped relation, filtered
    "Q18": FinderQuery(["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice", "sum(l_quantity)"],
                       lambda q, p: _large_volume_customer(q.lineitem, p)),

    "Q19": FinderQuery(["revenue"], lambda q, p: q.lineitem.find_all(
        p.APP_TIME, p.SYS_TIME, [_revenue(q.lineitem).sum().alias("revenue")],
        _q19_branch(q.lineitem, "Brand#12", ["SM CASE", "SM BOX", "SM PACK", "SM PKG"], 1.0, 5)
        .or_(_q19_branch(q.lineitem, "Brand#23", ["MED BAG", "MED BOX", "MED PKG", "MED PACK"], 10.0, 10))
        .or_(_q19_branch(q.lineitem, "Brand#34", ["LG CASE", "LG BOX", "LG PACK", "LG PKG"], 20.0, 15)))),

    # the average is a relation of one row, joined to every customer: the scalar subquery
    # IN subqueries are joins, and the correlated sum is a grouped relation joined on the part and the supplier
    "Q20": FinderQuery(["s_name", "s_address"], lambda q, p: _potential_part_promotion(q, p)),
    # EXISTS is an inner join to the other lines, NOT EXISTS a left outer join that kept the lines that found none
    "Q21": FinderQuery(["s_name", "numwait"], lambda q, p: _suppliers_who_kept_orders_waiting(q, p)),

    "Q22": FinderQuery(["cntrycode", "numcust", "totacctbal"], lambda q, p: _global_sales_opportunity(q.customer, p)),
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


def _latest_before(rows, columns):
    """The rows whose ``end`` is the greatest of all the rows: ``WHERE end = (SELECT max(end) ...)``."""
    with_latest = rows.extend(latest=rows["end"].max().over())
    return with_latest.filter(with_latest["end"] == with_latest["latest"]).select(*columns)


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

    # the scalar subquery max(...) is a window over the rows it ranges over, and the row at that maximum is the one kept
    "K5_sys": FinderQuery(["c_custkey", "c_acctbal", "sys_time_begin"], lambda q, p: _latest_before(
        q.customer.find_all(
            p.APP_TIME, None,
            [q.customer.customer_key().alias("c_custkey"), q.customer.acct_bal().alias("c_acctbal"),
             q.customer.sys_time_begin().alias("sys_time_begin"), q.customer.sys_time_end().alias("end")],
            q.customer.customer_key().eq(p.CUSTOMER_KEY).and_(q.customer.sys_time_end() <= p.SYS_TIME)),
        ["c_custkey", "c_acctbal", "sys_time_begin"])),

    "K5_app": FinderQuery(["c_custkey", "c_acctbal", "visible_time_begin"], lambda q, p: _latest_before(
        q.customer.find_all(
            None, _NOW,
            [q.customer.customer_key().alias("c_custkey"), q.customer.acct_bal().alias("c_acctbal"),
             q.customer.visible_time_begin().alias("visible_time_begin"), q.customer.visible_time_end().alias("end")],
            q.customer.customer_key().eq(p.CUSTOMER_KEY).and_(q.customer.visible_time_end() <= p.APP_TIME)),
        ["c_custkey", "c_acctbal", "visible_time_begin"])),

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

def _moved_to(c, p, app_range, end, start, as_time):
    """Customers who moved to a nation and still live there: the latest end of their versions in another nation and the
    earliest start of their versions in it, as grouped relations joined on the customer."""
    def versions(name, in_nation, time, aggregate):
        columns = [aggregate(time).alias(name), c.customer_key().alias(name + "_custkey"), c.name().alias(name + "_name"),
                   c.nation().nation_key().alias(name + "_nation")]
        condition = c.nation().name().eq(p.NATION) if in_nation else c.nation().name().ne(p.NATION)
        found = c.find_all(None, None, columns, condition) if app_range is None \
            else c.find_for_date_range(app_range[0], app_range[1], _NOW, columns, condition)
        return found.group_by(c.customer_key(), c.name(), c.nation().nation_key())

    before = versions("time_until", False, end(c), lambda column: column.max())
    now = versions("time_from", True, start(c), lambda column: column.min())
    both = before.join(now, on=before["time_until_custkey"] == now["time_from_custkey"])
    low, high = as_time(p.SYS_TIME_BEGIN), as_time(p.SYS_TIME_END)
    moved = both.filter(both["time_until"] < low, both["time_from"] >= low, both["time_from"] <= high)
    return moved.select(moved["time_from_name"].alias("c_name"))


def _in_system_range(table, p):
    """FOR SYSTEM_TIME FROM [SYS_TIME_BEGIN] TO [SYS_TIME_END]: the versions whose period overlaps the range"""
    return (table.sys_time_begin() < p.SYS_TIME_END).and_(table.sys_time_end() > p.SYS_TIME_BEGIN)


def _time_slider(o, p, value):
    columns = [o.sys_time_begin().alias("ord_start"), o.sys_time_end().alias("ord_end")]
    condition = _in_system_range(o, p)
    if value:
        columns.append(o.total_price().alias("ord_price"))
        in_force = o.find_all(None, None, columns, condition.and_(o.order_status().eq("O")))
    else:
        in_force = o.find_all(None, None, columns, condition)
    starts = o.find_all(None, None, [o.sys_time_begin().alias("event_time")], condition)
    ends = o.find_all(None, None, [o.sys_time_end().alias("event_time")], condition)
    times = starts.concatenate(ends)
    joined = in_force.join(times, on=(in_force["ord_start"] <= times["event_time"]).and_(times["event_time"] < in_force["ord_end"]))
    aggregate = joined["ord_price"].sum() if value else count_all()
    totals = joined.select(joined["event_time"], aggregate.alias("total")).group_by(joined["event_time"])
    return totals.select(totals["total"].max().alias("max_total"))


def _temporal_join_count(q, p):
    customers, orders = q.customer, q.order
    cust = customers.find_all(
        None, None, [customers.customer_key().alias("c_custkey"), customers.sys_time_begin().alias("cust_start"),
                     customers.sys_time_end().alias("cust_end")],
        _in_system_range(customers, p).and_(customers.acct_bal() < 5000.0))
    ord_ = orders.find_all(
        None, None, [orders.order_key().alias("o_orderkey"), orders.customer().customer_key().alias("o_custkey"),
                     orders.sys_time_begin().alias("ord_start"), orders.sys_time_end().alias("ord_end")],
        _in_system_range(orders, p).and_(orders.order_status().eq("O")).and_(orders.total_price() > 10.0))
    joined = cust.join(ord_, on=(cust["c_custkey"] == ord_["o_custkey"]).and_(cust["cust_end"] > ord_["ord_start"])
                       .and_(ord_["ord_end"] > cust["cust_start"]))
    return joined.select(joined["o_orderkey"].count_distinct().alias("count_distinct_orders"))


def _temporal_aggregation_over_a_join(q, p):
    customers, orders = q.customer, q.order
    cust = customers.find_all(
        None, None, [customers.customer_key().alias("c_custkey"), customers.sys_time_begin().alias("cust_start"),
                     customers.sys_time_end().alias("cust_end")],
        _in_system_range(customers, p).and_(customers.acct_bal() < 100.0))
    ord_ = orders.find_all(
        None, None, [orders.customer().customer_key().alias("o_custkey"), orders.total_price().alias("orderprice"),
                     orders.sys_time_begin().alias("ord_start"), orders.sys_time_end().alias("ord_end")],
        _in_system_range(orders, p).and_(orders.order_status().eq("O")))
    joined = cust.join(ord_, on=(cust["c_custkey"] == ord_["o_custkey"]).and_(cust["cust_end"] > ord_["ord_start"])
                       .and_(ord_["ord_end"] > cust["cust_start"]))
    both = joined.extend(
        time_start=when(joined["cust_start"] > joined["ord_start"]).then(joined["cust_start"]).otherwise(joined["ord_start"]),
        time_end=when(joined["cust_end"] < joined["ord_end"]).then(joined["cust_end"]).otherwise(joined["ord_end"]))
    high_risk = both.select("orderprice", "time_start", "time_end")
    times = high_risk.select(high_risk["time_start"].alias("event_time")).concatenate(
        high_risk.select(high_risk["time_end"].alias("event_time")))
    in_force = both.select("orderprice", "time_start", "time_end")
    joined_times = in_force.join(times, on=(in_force["time_start"] <= times["event_time"])
                                 .and_(times["event_time"] < in_force["time_end"]))
    totals = joined_times.select(joined_times["event_time"], joined_times["orderprice"].sum().alias("total")) \
        .group_by(joined_times["event_time"])
    return totals.select(totals["total"].max().alias("aggr_result"))


def _mean_time_to_ship(o, app, sys, start, difference):
    """The mean time from the first version of an order that is open to the first that is filled: two grouped relations, one
    for each status, joined on the order, and the average of the difference of their times."""
    def first(status, name):
        return o.find_all(app, sys, [start(o).min().alias(name), o.order_key().alias(name + "_order")],
                          o.order_status().eq(status)).group_by(o.order_key())
    opened, shipped = first("O", "order_time"), first("F", "ship_time")
    both = opened.join(shipped, on=opened["order_time_order"] == shipped["ship_time_order"])
    taken = both.extend(taken=getattr(both["order_time"], difference)(both["ship_time"]))
    return taken.select(taken["taken"].average().alias("avg_seconds" if difference == "diff_seconds" else "avg_days"))


B4 = {
    # the customers' versions before and after the move, each a grouped relation, joined on the customer
    "R1_sys": FinderQuery(["c_name"], lambda q, p: _moved_to(
        q.customer, p, None, lambda c: c.sys_time_end(), lambda c: c.sys_time_begin(), lambda t: t)),
    "R1_app": FinderQuery(["c_name"], lambda q, p: _moved_to(
        q.customer, p, ("0001-01-01", "9999-12-30"), lambda c: c.visible_time_end(), lambda c: c.visible_time_begin(),
        lambda t: t.date())),
    "R2_sys": FinderQuery(["avg_seconds"], lambda q, p: _mean_time_to_ship(
        q.order, None, None, lambda o: o.sys_time_begin(), "diff_seconds")),
    "R2_app": FinderQuery(["avg_days"], lambda q, p: _mean_time_to_ship(
        q.order, None, _NOW, lambda o: o.active_time_begin(), "diff_days")),
    # the points where the data changed are the union of every start and end, and the orders in force at each are joined to them
    "R3a": FinderQuery(["max_total"], lambda q, p: _time_slider(q.order, p, value=False)),
    "R3b": FinderQuery(["max_total"], lambda q, p: _time_slider(q.order, p, value=True)),
    "R5": FinderQuery(["count_distinct_orders"], lambda q, p: _temporal_join_count(q, p)),
    # the chain is written (_temporal_aggregation_over_a_join), but the order's customer is read through a navigation, which joins
    # CUSTOMER and repeats an order for each of its versions, so the sums are too large until the key is read from the order
    "R6": Unsupported(_FOREIGN_KEY),

    "R4": Unsupported(
        "Needs every version of a key compared with the others (the extreme stock levels and when they occurred), as nested "
        "aggregates over versions of the same table"),
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
