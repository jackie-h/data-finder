# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportIndexIssue=false, reportReturnType=false
# (pandas is not typed closely enough for frames filtered by boolean masks)
"""What the TPC-BiH queries return on the generated data, worked out with pandas from the benchmark's own SQL (Appendix B of
the report and the TPC-H specification), not with the finders. A spec compares what a finder returns with this.

Every function takes the application and system time a query is asked at (None is every version) and answers with the rows
the SQL would, as a numpy array of Python values.
"""
from __future__ import annotations

import datetime

import numpy as np
import pandas as pd

from datafinder_examples_tests.tpch_bih_data import as_of, date, stamp, table


def rows(frame: pd.DataFrame, kinds: dict[str, str] | None = None) -> np.ndarray:
    """A frame as an object array. ``kinds`` names the columns that are dates ("date") or timestamps ("stamp") as text."""
    kinds = kinds or {}
    result = np.empty(frame.shape, dtype=object)
    for j, column in enumerate(frame.columns):
        for i, value in enumerate(frame[column]):
            if kinds.get(column) == "date":
                value = date(value)
            elif kinds.get(column) == "stamp":
                value = stamp(value)
            elif isinstance(value, (np.integer,)):
                value = int(value)
            elif isinstance(value, (np.floating,)):
                value = float(value)
            result[i, j] = value
    return result


def _revenue(frame: pd.DataFrame) -> pd.Series:
    return frame.l_extendedprice * (1 - frame.l_discount)


def _asof(name, point):
    return as_of(name, point[0], point[1])


# ---------------------------------------------------------------------------------------------------------------------
# B.1 time travel
# ---------------------------------------------------------------------------------------------------------------------

def t1(app, sys) -> np.ndarray:
    """SELECT AVG(ps_supplycost), COUNT(*) FROM partsupp"""
    ps = as_of("partsupp", app, sys)
    return rows(pd.DataFrame({"avg": [ps.ps_supplycost.mean()], "count": [len(ps)]}))


def t2(app, sys) -> np.ndarray:
    """SELECT SUM(o_totalprice), COUNT(*) FROM orders WHERE o_orderstatus = 'O'"""
    o = as_of("orders", app, sys)
    o = o[o.o_orderstatus == "O"]
    return rows(pd.DataFrame({"revenue": [o.o_totalprice.sum()], "count": [len(o)]}))


def t6(app, sys) -> np.ndarray:
    """SELECT o_custkey, AVG(o_totalprice) FROM orders WHERE o_orderstatus = 'O' GROUP BY o_custkey ORDER BY o_custkey"""
    o = as_of("orders", app, sys)
    o = o[o.o_orderstatus == "O"]
    return rows(o.groupby("o_custkey", as_index=False).o_totalprice.mean().sort_values("o_custkey"))


def t7(app, sys) -> np.ndarray:
    """SELECT ps_partkey, AVG(ps_availqty) FROM partsupp GROUP BY ps_partkey ORDER BY ps_partkey"""
    ps = as_of("partsupp", app, sys)
    return rows(ps.groupby("ps_partkey", as_index=False).ps_availqty.mean().sort_values("ps_partkey"))


def _receivable(app, sys, day: str) -> pd.DataFrame:
    o = as_of("orders", app, sys)
    return o[(o.receivable_time_begin <= day) & (o.receivable_time_end > day)]


def t8(app, sys, day: str) -> np.ndarray:
    """SELECT COUNT(*) FROM orders WHERE receivable_time contains day"""
    return rows(pd.DataFrame({"count": [len(_receivable(app, sys, day))]}))


t9 = t8


# ---------------------------------------------------------------------------------------------------------------------
# B.2 H.1
# ---------------------------------------------------------------------------------------------------------------------

def h1(app, sys, cutoff: str) -> np.ndarray:
    l = as_of("lineitem", app, sys)
    l = l[l.l_shipdate <= cutoff].copy()
    l["disc_price"] = _revenue(l)
    l["charge"] = l.disc_price * (1 + l.l_tax)
    g = l.groupby(["l_returnflag", "l_linestatus"], as_index=False).agg(
        sum_qty=("l_quantity", "sum"), sum_base_price=("l_extendedprice", "sum"), sum_disc_price=("disc_price", "sum"),
        sum_charge=("charge", "sum"), avg_qty=("l_quantity", "mean"), avg_price=("l_extendedprice", "mean"),
        avg_disc=("l_discount", "mean"), count_order=("l_orderkey", "count"))
    return rows(g.sort_values(["l_returnflag", "l_linestatus"]))


# ---------------------------------------------------------------------------------------------------------------------
# B.3 key history on CUSTOMER
# ---------------------------------------------------------------------------------------------------------------------

def customer_versions(key: int, app, sys) -> pd.DataFrame:
    c = as_of("customer", app, sys)
    return c[c.c_custkey == key]


def busiest_customer() -> int:
    """The customer with the most versions: the key the K queries are asked about."""
    counts = table("customer").groupby("c_custkey").size()
    return int(counts.sort_values(ascending=False, kind="stable").index[0])


# ---------------------------------------------------------------------------------------------------------------------
# B.5 bitemporal self-join: the other parts of the suppliers that supply part 1
# ---------------------------------------------------------------------------------------------------------------------

def _overlap(a: pd.DataFrame, b: pd.DataFrame, prefix_a: str, prefix_b: str) -> pd.Series:
    return (a[prefix_a + "_begin"] < b[prefix_b + "_end"]) & (a[prefix_a + "_end"] > b[prefix_b + "_begin"])


def b5(app, sys, correlate=(), other_app="same", other_sys="same") -> np.ndarray:
    """SELECT DISTINCT ps1.ps_partkey FROM partsupp ps1, partsupp ps2 WHERE ps1.ps_suppkey = ps2.ps_suppkey AND
    ps2.ps_partkey = 1 AND ps1.ps_partkey <> 1 [AND the application and/or system periods of ps1 and ps2 overlap]"""
    a = as_of("partsupp", app, sys)
    b = as_of("partsupp", app if other_app == "same" else other_app, sys if other_sys == "same" else other_sys)
    joined = a.merge(b, on="ps_suppkey", suffixes=("_1", "_2"))
    joined = joined[(joined.ps_partkey_2 == 1) & (joined.ps_partkey_1 != 1)]
    if "application" in correlate:
        joined = joined[(joined.validity_time_begin_1 < joined.validity_time_end_2)
                        & (joined.validity_time_end_1 > joined.validity_time_begin_2)]
    if "system" in correlate:
        joined = joined[(joined.sys_time_begin_1 < joined.sys_time_end_2)
                        & (joined.sys_time_end_1 > joined.sys_time_begin_2)]
    return rows(pd.DataFrame({"ps_partkey": sorted(set(joined.ps_partkey_1))}))


# ---------------------------------------------------------------------------------------------------------------------
# TPC-H queries
# ---------------------------------------------------------------------------------------------------------------------

def _lines(point):
    """lineitem joined to its order, customer, part and supplier, each as of the point"""
    l = _asof("lineitem", point)
    o = _asof("orders", point)
    c = _asof("customer", point)
    j = l.merge(o, left_on="l_orderkey", right_on="o_orderkey", suffixes=("", "_o"))
    j = j.merge(c, left_on="o_custkey", right_on="c_custkey", suffixes=("", "_c"))
    return j


def q3(point, segment: str, day: str, limit: int = 10) -> np.ndarray:
    j = _lines(point)
    j = j[(j.c_mktsegment == segment) & (j.o_orderdate < day) & (j.l_shipdate > day)].copy()
    j["revenue"] = _revenue(j)
    g = j.groupby(["l_orderkey", "o_orderdate", "o_shippriority"], as_index=False).revenue.sum()
    g = g.sort_values(["revenue", "o_orderdate"], ascending=[False, True]).head(limit)
    return rows(g[["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]], {"o_orderdate": "date"})


def q5(point, region: str, first: str, last_exclusive: str) -> np.ndarray:
    j = _lines(point)
    s = _asof("supplier", point)
    j = j.merge(s, left_on="l_suppkey", right_on="s_suppkey", suffixes=("", "_s"))
    n = table("nation")
    r = table("region")
    j = j.merge(n, left_on="c_nationkey", right_on="n_nationkey")
    j = j.merge(r, left_on="n_regionkey", right_on="r_regionkey")
    j = j[(j.r_name == region) & (j.o_orderdate >= first) & (j.o_orderdate < last_exclusive)
          & (j.c_nationkey == j.s_nationkey)].copy()
    j["revenue"] = _revenue(j)
    g = j.groupby("n_name", as_index=False).revenue.sum().sort_values("revenue", ascending=False)
    return rows(g)


def q6(point, first: str, last_exclusive: str, discount: float, quantity: float) -> np.ndarray:
    l = _asof("lineitem", point)
    l = l[(l.l_shipdate >= first) & (l.l_shipdate < last_exclusive) & (l.l_discount >= round(discount - 0.01, 2))
          & (l.l_discount <= round(discount + 0.01, 2)) & (l.l_quantity < quantity)]
    return rows(pd.DataFrame({"revenue": [(l.l_extendedprice * l.l_discount).sum()]}))


def _nations(j: pd.DataFrame, point) -> pd.DataFrame:
    s = _asof("supplier", point)
    n = table("nation")
    j = j.merge(s, left_on="l_suppkey", right_on="s_suppkey", suffixes=("", "_s"))
    j = j.merge(n.add_suffix("_sn"), left_on="s_nationkey", right_on="n_nationkey_sn")
    j = j.merge(n.add_suffix("_cn"), left_on="c_nationkey", right_on="n_nationkey_cn")
    return j


def q7(point, nation1: str, nation2: str, first: str, last: str) -> np.ndarray:
    j = _nations(_lines(point), point)
    j = j[(((j.n_name_sn == nation1) & (j.n_name_cn == nation2)) | ((j.n_name_sn == nation2) & (j.n_name_cn == nation1)))
          & (j.l_shipdate >= first) & (j.l_shipdate <= last)].copy()
    j["revenue"] = _revenue(j)
    j["l_year"] = j.l_shipdate.str[:4].astype(int)
    g = j.groupby(["n_name_sn", "n_name_cn", "l_year"], as_index=False).revenue.sum()
    return rows(g.sort_values(["n_name_sn", "n_name_cn", "l_year"]))


def q8(point, nation: str, region: str, first: str, last: str, part_type: str) -> np.ndarray:
    j = _nations(_lines(point), point)
    r = table("region")
    n = table("nation")
    p = _asof("part", point)
    j = j.merge(p, left_on="l_partkey", right_on="p_partkey", suffixes=("", "_p"))
    j = j.merge(n[["n_nationkey", "n_regionkey"]], left_on="c_nationkey", right_on="n_nationkey", suffixes=("", "_c"))
    j = j.merge(r, left_on="n_regionkey", right_on="r_regionkey")
    j = j[(j.r_name == region) & (j.o_orderdate >= first) & (j.o_orderdate <= last) & (j.p_type == part_type)].copy()
    j["volume"] = _revenue(j)
    j["o_year"] = j.o_orderdate.str[:4].astype(int)
    j["national"] = j.volume.where(j.n_name_sn == nation, 0)
    g = j.groupby("o_year", as_index=False).agg(national=("national", "sum"), volume=("volume", "sum"))
    g["mkt_share"] = g.national / g.volume
    return rows(g.sort_values("o_year")[["o_year", "mkt_share"]])


def q10(point, first: str, last_exclusive: str, limit: int = 20) -> np.ndarray:
    j = _lines(point)
    n = table("nation")
    j = j.merge(n, left_on="c_nationkey", right_on="n_nationkey")
    j = j[(j.o_orderdate >= first) & (j.o_orderdate < last_exclusive) & (j.l_returnflag == "R")].copy()
    j["revenue"] = _revenue(j)
    keys = ["c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"]
    g = j.groupby(keys, as_index=False).revenue.sum().sort_values("revenue", ascending=False).head(limit)
    return rows(g[["c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment"]])


def q12(point, mode1: str, mode2: str, first: str, last_exclusive: str) -> np.ndarray:
    j = _asof("lineitem", point).merge(_asof("orders", point), left_on="l_orderkey", right_on="o_orderkey")
    j = j[j.l_shipmode.isin([mode1, mode2]) & (j.l_commitdate < j.l_receiptdate) & (j.l_shipdate < j.l_commitdate)
          & (j.l_receiptdate >= first) & (j.l_receiptdate < last_exclusive)].copy()
    high = j.o_orderpriority.isin(["1-URGENT", "2-HIGH"])
    j["high"] = high.astype(int)
    j["low"] = (~high).astype(int)
    g = j.groupby("l_shipmode", as_index=False).agg(high_line_count=("high", "sum"), low_line_count=("low", "sum"))
    return rows(g.sort_values("l_shipmode"))


def q14(point, first: str, last_exclusive: str) -> np.ndarray:
    l = _asof("lineitem", point)
    p = _asof("part", point)
    j = l.merge(p, left_on="l_partkey", right_on="p_partkey")
    j = j[(j.l_shipdate >= first) & (j.l_shipdate < last_exclusive)]
    revenue = _revenue(j)
    promo = revenue.where(j.p_type.str.startswith("PROMO"), 0)
    return rows(pd.DataFrame({"promo_revenue": [100.0 * promo.sum() / revenue.sum()]}))


def q19(point, branches: list[tuple[str, list[str], float, int]]) -> np.ndarray:
    l = _asof("lineitem", point)
    p = _asof("part", point)
    j = l.merge(p, left_on="l_partkey", right_on="p_partkey")
    keep = pd.Series(False, index=j.index)
    for brand, containers, quantity, max_size in branches:
        keep |= ((j.p_brand == brand) & j.p_container.isin(containers) & (j.l_quantity >= quantity)
                 & (j.l_quantity <= quantity + 10) & (j.p_size >= 1) & (j.p_size <= max_size)
                 & j.l_shipmode.isin(["AIR", "AIR REG"]) & (j.l_shipinstruct == "DELIVER IN PERSON"))
    return rows(pd.DataFrame({"revenue": [_revenue(j[keep]).sum()]}))


# ---------------------------------------------------------------------------------------------------------------------
# B.3 key history: the versions of one customer, and of every customer
# ---------------------------------------------------------------------------------------------------------------------

_KINDS = {"sys_time_begin": "stamp", "sys_time_end": "stamp", "visible_time_begin": "date", "visible_time_end": "date"}


def customers(app, sys, columns: list[str], order_by: list[str], key: int | None = None, overlapping: tuple | None = None,
             balance_above: float | None = None) -> np.ndarray:
    """SELECT columns FROM customer [WHERE c_custkey = key] [AND the version overlaps the range] [AND c_acctbal > x] ORDER BY ...

    ``overlapping`` is ("sys" | "visible", from, to): the versions whose period begins before ``to`` and ends after ``from``."""
    c = as_of("customer", app, sys)
    if key is not None:
        c = c[c.c_custkey == key]
    if overlapping:
        dimension, low, high = overlapping
        c = c[(c[dimension + "_time_begin"] < high) & (c[dimension + "_time_end"] > low)]
    if balance_above is not None:
        c = c[c.c_acctbal > balance_above]
    return rows(c.sort_values(order_by, kind="stable")[columns], _KINDS)


def minimum_balance(app, sys, key: int, overlapping: tuple | None = None) -> np.ndarray:
    c = as_of("customer", app, sys)
    c = c[c.c_custkey == key]
    if overlapping:
        dimension, low, high = overlapping
        c = c[(c[dimension + "_time_begin"] < high) & (c[dimension + "_time_end"] > low)]
    return rows(pd.DataFrame({"min_balance": [c.c_acctbal.min()]}))


def preceding_customer_version(app, sys, key: int, dimension: str, up_to: str) -> np.ndarray:
    """The version of a customer that ends when this one begins (versions are contiguous), as K.5 asks for the previous one."""
    c = as_of("customer", app, sys)
    c = c[c.c_custkey == key]
    j = c.merge(c, on="c_custkey", suffixes=("", "_other"))
    j = j[(j[dimension + "_time_begin"] <= up_to) & (j[dimension + "_time_end_other"] == j[dimension + "_time_begin"])]
    out = j[["c_acctbal_other", dimension + "_time_begin_other"]]
    return rows(out, {dimension + "_time_begin_other": "stamp" if dimension == "sys" else "date"})


# ---------------------------------------------------------------------------------------------------------------------
# B.4 range queries
# ---------------------------------------------------------------------------------------------------------------------

def r7(app, sys) -> np.ndarray:
    """The versions of a partsupp whose cost rose by more than 7.5% over the version before (the one that ends when it begins)."""
    ps = as_of("partsupp", app, sys)
    j = ps.merge(ps, on=["ps_partkey", "ps_suppkey"], suffixes=("", "_before"))
    j = j[(j.validity_time_end_before == j.validity_time_begin) & (j.ps_supplycost > j.ps_supplycost_before * 1.075)]
    j = j.sort_values(["validity_time_end", "validity_time_end_before"], ascending=[True, False], kind="stable")
    out = j[["ps_partkey", "ps_suppkey", "ps_supplycost", "validity_time_begin", "validity_time_end",
             "validity_time_begin_before", "validity_time_end_before", "ps_supplycost_before"]]
    return rows(out, {c: "date" for c in out.columns if c.startswith("validity")})
