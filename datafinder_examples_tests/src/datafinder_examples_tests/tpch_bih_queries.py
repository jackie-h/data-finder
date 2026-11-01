"""The finder query for each benchmark case of tpch_bih_cases.py (the case holds the report's SQL and the expected rows).

An entry is a ``FinderQuery``, the finder columns it returns and the query, which is given the finders ``q`` and the values of the
case's parameters ``p``; or ``Unsupported`` with the reason the finders cannot ask the query as the report does. The
columns of the case that are compared, and in what order, are the case's ``select``: a query returns those columns.
"""
from __future__ import annotations

import datetime
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable

from datafinder.expressions import count_all
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
# B.5: ps1 is the supply of the given part, ps2 the supply of the other part that is returned. The finder's root is ps2, and
# ps1 is reached through the supplier, which gives it the dates of the root unless it is asked for at others.
# ---------------------------------------------------------------------------------------------------------------------

def _other_parts(f, p, app, sys, application=False, system=None, anchor_app=None):
    """The parts of ps2, DISTINCT. ``application``: ps1.VALIDITY_TIME_START <= ps2.VALIDITY_TIME_END AND ps1.VALIDITY_TIME_END >=
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
    return f.find_all(app, sys, [f.part().part_key()], condition).distinct().order_by(f.part().part_key().ascending())


_PART_COLUMN = ["Part Part Key"]

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

QUERIES: dict[str, FinderQuery | Unsupported] = {**B1, **B3, **B5}
