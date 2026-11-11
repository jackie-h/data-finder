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

from datafinder import exists
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
    "Pairs two one-row aggregates, each at its own time, as a cross join: join has the ANSI join types inner, left, right and "
    "full, and a cross join has no condition")


# ---------------------------------------------------------------------------------------------------------------------
# B.1
# ---------------------------------------------------------------------------------------------------------------------

def _receivable(o, day):
    """'[APP_TIME]' BETWEEN receivable_time_start AND receivable_time_end (BETWEEN includes both ends)"""
    return (o.receivable_time_begin() <= day).and_(o.receivable_time_end() >= day)


def _revenue_increase(orders, first, second):
    """Two grouped queries, each of the open or filled orders of a customer at its own time, joined on the customer: the
    difference of the two revenues, the greatest first."""
    def revenue_of_customers(when, number):
        application, system, status = when
        return orders.find_all(
            application, system,
            [orders.customer().customer_key().alias("o_custkey" if number == 1 else "o_custkey2"),
             orders.total_price().sum().alias(f"revenue{number}")],
            orders.order_status().eq(status)).group_by(orders.customer().customer_key())

    both = revenue_of_customers(first, 1).join(revenue_of_customers(second, 2), lambda a, b: a.o_custkey == b.o_custkey2,
                                                how="inner")
    increase = both.extend(inc_revenue=lambda r: r.revenue2 - r.revenue1).select("inc_revenue", "o_custkey")
    return increase.order_by(increase["inc_revenue"].descending()).limit(10)


B1 = {
    "T1": FinderQuery(["avg_supplycost", "count"], lambda q, p: q.partsupp.find_all(
        p.APP_TIME, p.SYS_TIME, [q.partsupp.supply_cost().average().alias("avg_supplycost"), count_all().alias("count")])),

    "T2": FinderQuery(["revenue", "count"], lambda q, p: q.order.find_all(
        p.APP_TIME, p.SYS_TIME, [q.order.total_price().sum().alias("revenue"), count_all().alias("count")],
        q.order.order_status().eq("O"))),

    "T3_sys": Unsupported(_TWO_AGGREGATES),
    "T3_app": Unsupported(_TWO_AGGREGATES),
    "T4_sys": FinderQuery(["inc_revenue", "o_custkey"], lambda q, p: _revenue_increase(
        q.order, (p.APP_TIME, p.SYS_TIME1, "O"), (p.APP_TIME, p.SYS_TIME2, "F"))),
    "T4_app": FinderQuery(["inc_revenue", "o_custkey"], lambda q, p: _revenue_increase(
        q.order, (p.APP_TIME1, _NOW, "O"), (p.APP_TIME2, _NOW, "O"))),

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
    """HAVING sum(ps_supplycost * ps_availqty) > (SELECT sum(...) * FRACTION ...): the total over all the groups is a window
    over the grouped rows, which a filter then compares each group's value with."""
    value = (ps.supply_cost() * ps.avail_qty()).sum()
    parts = ps.find_all(
        p.APP_TIME, p.SYS_TIME, [ps.part().part_key().alias("ps_partkey"), value.alias("value")],
        ps.supplier().nation().name().eq(p.NATION)).group_by(ps.part().part_key())
    with_total = parts.extend(total=lambda r: r.value.sum().over())
    important = with_total.filter(lambda r: r.value > r.total * p.FRACTION).select("ps_partkey", "value")
    return important.order_by(important["value"].descending())


def _minimum_cost_supplier(ps, p):
    """ps_supplycost = (SELECT min(ps_supplycost) ... for the same part): the cheapest supply of a part among the suppliers in
    the region is a window over those supplies, partitioned by part."""
    supplier = ps.supplier()
    rows = ps.find_all(
        p.APP_TIME, p.SYS_TIME,
        [supplier.acct_bal().alias("s_acctbal"), supplier.name().alias("s_name"), supplier.nation().name().alias("n_name"),
         ps.part().part_key().alias("p_partkey"), ps.part().mfgr().alias("p_mfgr"), supplier.address().alias("s_address"),
         supplier.phone().alias("s_phone"), supplier.comment().alias("s_comment"), ps.supply_cost().alias("cost")],
        ps.part().size().eq(p.SIZE).and_(ps.part().type_().ends_with(p.TYPE.removeprefix("%")))
        .and_(supplier.nation().region().name().eq(p.REGION)))
    with_cheapest = rows.extend(cheapest=lambda r: r.cost.min().over(partition_by=[r.p_partkey]))
    cheapest = with_cheapest.filter(lambda r: r.cost == r.cheapest).select(
        "s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
    return cheapest.order_by(cheapest["s_acctbal"].descending(), cheapest["n_name"].ascending(),
                             cheapest["s_name"].ascending(), cheapest["p_partkey"].ascending()).limit(100)


def _product_type_profit(lines, supplies, p):
    """PARTSUPP is joined on a composite key, ps_partkey = l_partkey and ps_suppkey = l_suppkey: the lines and the supplies are
    two queries joined on both."""
    sold = lines.find_all(
        p.APP_TIME, p.SYS_TIME,
        [lines.part().part_key().alias("l_partkey"), lines.supplier().supplier_key().alias("l_suppkey"),
         lines.supplier().nation().name().alias("nation"), lines.order().order_date().year().alias("o_year"),
         _revenue(lines).alias("revenue"), lines.quantity().alias("quantity")],
        lines.part().name().contains("green"))
    supplied = supplies.find_all(
        p.APP_TIME, p.SYS_TIME,
        [supplies.part().part_key().alias("ps_partkey"), supplies.supplier().supplier_key().alias("ps_suppkey"),
         supplies.supply_cost().alias("supplycost")])
    profit = sold.join(supplied, lambda l, ps: (l.l_partkey == ps.ps_partkey) & (l.l_suppkey == ps.ps_suppkey), how="inner") \
        .extend(amount=lambda r: r.revenue - r.supplycost * r.quantity)
    by_nation = profit.group_by(lambda r: (r.nation, r.o_year)).aggregate(sum_profit=lambda r: r.amount.sum())
    return by_nation.order_by(by_nation["nation"].ascending(), by_nation["o_year"].descending())


def _customer_distribution(q, p):
    """A LEFT OUTER JOIN of the customers to their orders that are not special requests (the condition in ON is a filter of the
    orders), the orders counted for each customer, and the customers counted for each count."""
    customers = q.customer.find_all(p.APP_TIME, p.SYS_TIME, [q.customer.customer_key().alias("c_custkey")])
    orders = q.order.find_all(
        p.APP_TIME, p.SYS_TIME,
        [q.order.customer().customer_key().alias("o_custkey"), q.order.order_key().alias("o_orderkey")],
        ~q.order.comment().matches("special.*requests"))
    per_customer = customers.join(orders, lambda c, o: c.c_custkey == o.o_custkey, how="left") \
        .group_by(lambda r: r.c_custkey).aggregate(c_count=lambda r: r.o_orderkey.count())
    distribution = per_customer.group_by(lambda r: r.c_count).aggregate(custdist=lambda r: r.c_custkey.count())
    return distribution.order_by(distribution["custdist"].descending(), distribution["c_count"].descending())


def _top_supplier(q, p):
    """total_revenue = (SELECT max(total_revenue) FROM the same revenue by supplier): the revenue joined to its own maximum."""
    lines = q.lineitem
    revenue = lines.find_all(
        p.APP_TIME, p.SYS_TIME,
        [lines.supplier().supplier_key().alias("supplier_no"), _revenue(lines).sum().alias("total_revenue")],
        (lines.ship_date() >= datetime.date(1996, 1, 1)).and_(lines.ship_date() < datetime.date(1996, 4, 1)),
    ).group_by(lines.supplier().supplier_key())
    highest = revenue.aggregate(highest=lambda r: r.total_revenue.max())
    top = revenue.join(highest, lambda r, h: r.total_revenue == h.highest, how="inner")
    suppliers = q.supplier.find_all(
        None, p.SYS_TIME, [q.supplier.supplier_key().alias("s_suppkey"), q.supplier.name().alias("s_name"),
                           q.supplier.address().alias("s_address"), q.supplier.phone().alias("s_phone")])
    result = suppliers.join(top, lambda s, t: s.s_suppkey == t.supplier_no, how="inner") \
        .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
    return result.order_by(result["s_suppkey"].ascending())


def _large_volume_customer(lines, p):
    """o_orderkey IN (SELECT l_orderkey ... GROUP BY l_orderkey HAVING sum(l_quantity) > [QUANTITY]): the orders whose lines add
    up to more, joined to the customer's orders with their lines."""
    order = lines.order()
    large = lines.find_all(
        p.APP_TIME, p.SYS_TIME, [order.order_key().alias("l_orderkey"), lines.quantity().sum().alias("quantity")],
    ).group_by(order.order_key()).filter(lambda r: r.quantity > p.QUANTITY)
    rows = lines.find_all(
        p.APP_TIME, p.SYS_TIME,
        [order.customer().name().alias("c_name"), order.customer().customer_key().alias("c_custkey"),
         order.order_key().alias("o_orderkey"), order.order_date().alias("o_orderdate"),
         order.total_price().alias("o_totalprice"), lines.quantity().sum().alias("sum_quantity")],
    ).group_by(order.customer().name(), order.customer().customer_key(), order.order_key(), order.order_date(),
               order.total_price())
    result = rows.join(large, lambda r, l: r.o_orderkey == l.l_orderkey, how="inner") \
        .select("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice", "sum_quantity")
    return result.order_by(result["o_totalprice"].descending(), result["o_orderdate"].ascending()).limit(100)


def _potential_part_promotion(q, p):
    """s_suppkey IN (SELECT ps_suppkey ... ps_availqty > (SELECT 0.5 * sum(l_quantity) ... for the same part and supplier)): the
    supplies of the parts joined to the quantity sold of each, those with more than half of it in stock, and the suppliers that
    have one of them."""
    supplies, lines = q.partsupp, q.lineitem
    sold = lines.find_all(
        p.APP_TIME, p.SYS_TIME,
        [lines.part().part_key().alias("l_partkey"), lines.supplier().supplier_key().alias("l_suppkey"),
         lines.quantity().sum().alias("sold")],
        (lines.ship_date() >= datetime.date(1994, 1, 1)).and_(lines.ship_date() < datetime.date(1995, 1, 1)),
    ).group_by(lines.part().part_key(), lines.supplier().supplier_key())
    in_stock = supplies.find_all(
        p.APP_TIME, p.SYS_TIME,
        [supplies.part().part_key().alias("ps_partkey"), supplies.supplier().supplier_key().alias("ps_suppkey"),
         supplies.avail_qty().alias("avail")],
        supplies.part().name().starts_with("forest"))
    plenty = in_stock.join(sold, lambda s, l: (s.ps_partkey == l.l_partkey) & (s.ps_suppkey == l.l_suppkey), how="inner") \
        .filter(lambda r: r.avail > r.sold * 0.5)
    suppliers = q.supplier.find_all(
        None, p.SYS_TIME,
        [q.supplier.name().alias("s_name"), q.supplier.address().alias("s_address"), q.supplier.supplier_key().alias("s_suppkey")],
        q.supplier.nation().name().eq(p.NATION))
    kept = suppliers.filter(lambda r: exists(plenty, lambda x: x.ps_suppkey == r.s_suppkey)).select("s_name", "s_address")
    return kept.order_by(kept["s_name"].ascending())


def _suppliers_who_kept_orders_waiting(lines, p):
    """EXISTS another supplier's line of the order, and NOT EXISTS another supplier's late line of it: the late lines of finished
    orders are asked of the other lines of their order."""
    def rows(*columns, condition=None):
        return lines.find_all(p.APP_TIME, p.SYS_TIME, [*columns]) if condition is None \
            else lines.find_all(p.APP_TIME, p.SYS_TIME, [*columns], condition)

    waiting = rows(
        lines.supplier().name().alias("s_name"), lines.order().order_key().alias("l_orderkey"),
        lines.supplier().supplier_key().alias("l_suppkey"),
        condition=lines.order().order_status().eq("F").and_(lines.receipt_date() > lines.commit_date())
        .and_(lines.supplier().nation().name().eq(p.NATION)))
    every = rows(lines.order().order_key().alias("o_orderkey"), lines.supplier().supplier_key().alias("o_suppkey"))
    late = rows(lines.order().order_key().alias("o_orderkey"), lines.supplier().supplier_key().alias("o_suppkey"),
                condition=lines.receipt_date() > lines.commit_date())
    kept = waiting.filter(
        lambda r: exists(every, lambda o: (o.o_orderkey == r.l_orderkey) & (o.o_suppkey != r.l_suppkey))
        & ~exists(late, lambda o: (o.o_orderkey == r.l_orderkey) & (o.o_suppkey != r.l_suppkey)))
    waits = kept.group_by(lambda r: r.s_name).aggregate(numwait=lambda r: r.l_orderkey.count())
    return waits.order_by(waits["numwait"].descending(), waits["s_name"].ascending()).limit(100)


def _global_sales_opportunity(q, p):
    """c_acctbal > (SELECT avg(c_acctbal) ... where positive) is a window over the customers of those countries, and NOT EXISTS
    an order of the customer is a condition on the other result."""
    country = q.customer.phone().substring(0, 2)
    customers = q.customer.find_all(
        p.APP_TIME, p.SYS_TIME,
        [country.alias("cntrycode"), q.customer.customer_key().alias("c_custkey"), q.customer.acct_bal().alias("c_acctbal")],
        country.is_in(["13", "31", "23", "29", "30", "18", "17"]))
    orders = q.order.find_all(p.APP_TIME, p.SYS_TIME, [q.order.customer().customer_key().alias("o_custkey")])
    # the average of the positive balances, as their sum over their count
    with_average = customers.extend(
        positive_sum=lambda r: when(r.c_acctbal > 0.0).then(r.c_acctbal).otherwise(0.0).sum().over(),
        positive_count=lambda r: when(r.c_acctbal > 0.0).then(1).otherwise(0).sum().over(),
    ).extend(average=lambda r: r.positive_sum / r.positive_count)
    wanted = with_average.filter(
        lambda r: (r.c_acctbal > r.average) & ~exists(orders, lambda o: o.o_custkey == r.c_custkey))
    by_country = wanted.group_by(lambda r: r.cntrycode).aggregate(
        numcust=lambda r: r.c_custkey.count(), totacctbal=lambda r: r.c_acctbal.sum())
    return by_country.order_by(by_country["cntrycode"].ascending())


def _small_quantity_order_revenue(lines, p):
    """l_quantity < (SELECT 0.2 * avg(l_quantity) ... for the same part): the average is a window over the lines of the part,
    a filter keeps the small orders, and the revenue is an aggregate of those rows."""
    rows = lines.find_all(
        p.APP_TIME, p.SYS_TIME,
        [lines.part().part_key().alias("partkey"), lines.quantity().alias("quantity"), lines.extended_price().alias("price")],
        lines.part().brand().eq(p.BRAND).and_(lines.part().container().eq(p.CONTAINER)))
    with_average = rows.extend(average=lambda r: r.quantity.average().over(partition_by=[r.partkey]))
    small = with_average.filter(lambda r: r.quantity < r.average * 0.2)
    return small.aggregate(avg_yearly=lambda r: r.price.sum() / 7.0)


TPCH = {
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

    "Q9": FinderQuery(["nation", "o_year", "sum_profit"], lambda q, p: _product_type_profit(q.lineitem, q.partsupp, p)),

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
    "Q18": FinderQuery(["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice", "sum_quantity"],
                       lambda q, p: _large_volume_customer(q.lineitem, p)),

    "Q19": FinderQuery(["revenue"], lambda q, p: q.lineitem.find_all(
        p.APP_TIME, p.SYS_TIME, [_revenue(q.lineitem).sum().alias("revenue")],
        _q19_branch(q.lineitem, "Brand#12", ["SM CASE", "SM BOX", "SM PACK", "SM PKG"], 1.0, 5)
        .or_(_q19_branch(q.lineitem, "Brand#23", ["MED BAG", "MED BOX", "MED PKG", "MED PACK"], 10.0, 10))
        .or_(_q19_branch(q.lineitem, "Brand#34", ["LG CASE", "LG BOX", "LG PACK", "LG PKG"], 20.0, 15)))),

    "Q20": FinderQuery(["s_name", "s_address"], lambda q, p: _potential_part_promotion(q, p)),
    "Q21": FinderQuery(["s_name", "numwait"], lambda q, p: _suppliers_who_kept_orders_waiting(q.lineitem, p)),
    "Q22": FinderQuery(["cntrycode", "numcust", "totacctbal"], lambda q, p: _global_sales_opportunity(q, p)),
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
    """The rows whose ``end`` is the greatest of all the rows: ``WHERE end = (SELECT max(end) ...)``, the scalar subquery as a
    window over the rows it ranges over, and the row at that maximum is the one kept."""
    with_latest = rows.extend(latest=lambda r: r.end.max().over())
    return with_latest.filter(lambda r: r.end == r.latest).select(*columns)


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

def _state_transition(customers, p, history, begin, end):
    """Two grouped queries over the history of the customers, joined on the customer: the customers that were last somewhere else
    before a time, and first in the nation after it. The nation is read through the customer's navigation."""
    nation = customers.nation()
    prv = history(
        [customers.customer_key().alias("prv_custkey"), end.max().alias("time_until")],
        nation.name() != p.NATION,
    ).group_by(customers.customer_key(), customers.name(), nation.nation_key())
    now = history(
        [customers.customer_key().alias("now_custkey"), customers.name().alias("c_name"), begin.min().alias("time_from")],
        nation.name().eq(p.NATION),
    ).group_by(customers.customer_key(), customers.name(), nation.nation_key())
    moved = prv.join(now, lambda a, b: a.prv_custkey == b.now_custkey, how="inner")
    return moved.filter(
        lambda r: (r.time_until < p.SYS_TIME_BEGIN) & (r.time_from >= p.SYS_TIME_BEGIN) & (r.time_from <= p.SYS_TIME_END)
    ).select("c_name")


def _average_time_to_state(orders, history, begin, between, name):
    """Two grouped queries over the history of the orders, the earliest time of each status, joined on the order: the average
    time from one to the other."""
    def first(status, key, name):
        return history([orders.order_key().alias(key), begin.min().alias(name)], orders.order_status().eq(status)) \
            .group_by(orders.order_key())

    open_orders = first("O", "o_orderkey", "order_time")
    shipped = first("F", "shipped_orderkey", "ship_time")
    both = open_orders.join(shipped, lambda a, b: a.o_orderkey == b.shipped_orderkey, how="inner")
    return both.extend(duration=lambda r: between(r.order_time, r.ship_time)).aggregate(**{name: lambda r: r.duration.average()})


def _time_slider(orders, p, status):
    """The most orders (or, for a status, their value) at once in a range of system time: the times at which an order began or
    ended (a union of the two), joined to the orders that were current at each, whatever the condition: the time is between the
    order's begin and end. A group for each time is counted or summed, and the greatest is the answer."""
    in_range = (orders.sys_time_begin() < p.SYS_TIME_END).and_(orders.sys_time_end() > p.SYS_TIME_BEGIN)
    current = orders.find_all(
        None, None, [orders.sys_time_begin().alias("o_begin"), orders.sys_time_end().alias("o_end"),
                     orders.total_price().alias("price")],
        in_range if status is None else in_range.and_(orders.order_status().eq(status)))
    begins = orders.find_all(None, None, [orders.sys_time_begin().alias("event_time")], in_range)
    ends = orders.find_all(None, None, [orders.sys_time_end().alias("event_time")], in_range)
    return _busiest_time(current, begins.concatenate(ends), (lambda r: r.o_begin.count()) if status is None
                         else (lambda r: r.price.sum()))


def _busiest_time(current, times, total):
    """A temporal aggregation: each time is joined to the rows whose period holds it (begin <= time < end), the rows of each time
    are totalled, and the greatest total is the answer."""
    at = current.join(times, lambda o, t: (o.o_begin <= t.event_time) & (t.event_time < o.o_end), how="inner")
    totals = at.group_by(lambda r: r.event_time).aggregate(total=total)
    return totals.aggregate(max_total=lambda r: r.total.max())


def _extreme_stock_levels(supplies, p):
    """Every version of a supply at its lowest stock and at its highest, joined to the lowest and highest the supply ever had: the
    time between the two periods, whichever came first, as the difference of timestamps (an interval), and the smallest of them for
    each supply."""
    part, supplier = supplies.part().part_key(), supplies.supplier().supplier_key()

    def versions(prefix):
        return supplies.find_all(
            None, None, [part.alias(f"{prefix}_partkey"), supplier.alias(f"{prefix}_suppkey"),
                         supplies.avail_qty().alias(f"{prefix}_qty"), supplies.sys_time_begin().alias(f"{prefix}_from"),
                         supplies.sys_time_end().alias(f"{prefix}_to")])

    overall = supplies.find_all(
        None, None, [part.alias("ps_partkey"), supplier.alias("ps_suppkey"), supplies.avail_qty().min().alias("gmin"),
                     supplies.avail_qty().max().alias("gmax")]).group_by(part, supplier).filter(lambda g: g.gmax != g.gmin)
    at_least = overall.join(
        versions("min"), lambda g, v: (g.ps_partkey == v.min_partkey) & (g.ps_suppkey == v.min_suppkey) & (v.min_qty == g.gmin),
        how="inner")
    both = at_least.join(
        versions("max"), lambda g, v: (g.ps_partkey == v.max_partkey) & (g.ps_suppkey == v.max_suppkey) & (v.max_qty == g.gmax),
        how="inner")
    gaps = both.extend(diff1=lambda r: r.max_from - r.min_to, diff2=lambda r: r.min_from - r.max_to)
    positive = gaps.extend(diff=lambda r: when(r.diff1 > r.diff2).then(r.diff1).otherwise(r.diff2))
    smallest = positive.group_by(lambda r: (r.ps_partkey, r.ps_suppkey)).aggregate(min_diff=lambda r: r.diff.min())
    smallest = smallest.select("min_diff", "ps_partkey", "ps_suppkey")
    return smallest.order_by(smallest["min_diff"].ascending(), smallest["ps_partkey"].ascending(),
                             smallest["ps_suppkey"].ascending()).limit(p.HSIZE)


def _temporal_join(q, p):
    """The orders and the customers whose system periods overlap, joined on the customer: the condition is not an equality, and
    each side is the rows of the history in the range."""
    def in_range(finder):
        return (finder.sys_time_begin() < p.SYS_TIME_END).and_(finder.sys_time_end() > p.SYS_TIME_BEGIN)

    customers = q.customer.find_all(
        None, None, [q.customer.customer_key().alias("c_custkey"), q.customer.sys_time_begin().alias("c_begin"),
                     q.customer.sys_time_end().alias("c_end")],
        in_range(q.customer).and_(q.customer.acct_bal() < 5000))
    orders = q.order.find_all(
        None, None, [q.order.customer().customer_key().alias("o_custkey"), q.order.order_key().alias("o_orderkey"),
                     q.order.sys_time_begin().alias("o_begin"), q.order.sys_time_end().alias("o_end")],
        in_range(q.order).and_(q.order.order_status().eq("O")).and_(q.order.total_price() > 10))
    overlapping = customers.join(
        orders, lambda c, o: (c.c_custkey == o.o_custkey) & (c.c_end > o.o_begin) & (o.o_end > c.c_begin), how="inner")
    return overlapping.aggregate(orders=lambda r: r.o_orderkey.count_distinct())


B4 = {
    "R1_sys": FinderQuery(["c_name"], lambda q, p: _state_transition(
        q.customer, p, lambda columns, condition: q.customer.find_all(None, None, columns, condition),
        q.customer.sys_time_begin(), q.customer.sys_time_end())),
    "R1_app": FinderQuery(["c_name"], lambda q, p: _state_transition(
        q.customer, p, lambda columns, condition: q.customer.find_for_date_range(
            _FOREVER_START, _FOREVER_END, _NOW, columns, condition),
        q.customer.visible_time_begin(), q.customer.visible_time_end())),
    "R2_sys": FinderQuery(["avg_seconds"], lambda q, p: _average_time_to_state(
        q.order, lambda columns, condition: q.order.find_all(None, None, columns, condition),
        q.order.sys_time_begin(), lambda begin, end: begin.diff_seconds(end), "avg_seconds")),
    "R2_app": FinderQuery(["avg_days"], lambda q, p: _average_time_to_state(
        q.order, lambda columns, condition: q.order.find_for_date_range(_FOREVER_START, _FOREVER_END, _NOW, columns, condition),
        q.order.active_time_begin(), lambda begin, end: begin.diff_days(end), "avg_days")),
    "R3a": FinderQuery(["max_total"], lambda q, p: _time_slider(q.order, p, None)),
    "R3b": FinderQuery(["max_total"], lambda q, p: _time_slider(q.order, p, "O")),
    "R4": FinderQuery(["min_diff", "ps_partkey", "ps_suppkey"], lambda q, p: _extreme_stock_levels(q.partsupp, p)),
    "R5": FinderQuery(["orders"], lambda q, p: _temporal_join(q, p)),
    "R6": Unsupported(_FOREIGN_KEY),

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
