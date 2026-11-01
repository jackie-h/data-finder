"""Generates the small TPC-BiH data set used by the tpch_bih specs.

Run with ``uv run datafinder_examples/tools/generate_tpch_bih_data.py``. It rewrites the ``tpch_bih_*.csv`` files in
``datafinder_examples/src/datafinder_examples``. The output is deterministic, so the CSVs are checked in and this script
is only needed to change the history.

The history is applied as the TPC-BiH update scenarios would: every change happens at a system time, closes the
versions it replaces and adds the new ones. Application time is left open ended (``9999-12-31``) until a change
bounds it.

System times:  S0 initial load (2020-01-01), S1 2021-01-01, S2 2022-01-01, S3 2023-01-01.
"""
import csv
import datetime
from pathlib import Path
from typing import Any

INF_D = datetime.date(9999, 12, 31)
INF_T = datetime.datetime(9999, 12, 31, 23, 59, 59)
S0, S1, S2, S3 = (datetime.datetime(y, 1, 1) for y in (2020, 2021, 2022, 2023))
D = datetime.date


class Table:
    """A bitemporal table: versions with an application period and a system period."""

    def __init__(self, name: str, key: tuple, app: str | None, columns: list[str], system: bool = True):
        self.name, self.key, self.app, self.columns, self.system = name, key, app, columns, system
        self.rows: list[dict] = []

    def values(self, row: tuple) -> dict[str, Any]:
        """The row's values by column name."""
        return dict(zip(self.columns, row))

    def _current(self, key):
        return [r for r in self.rows if r["sys_to"] == INF_T and tuple(r[k] for k in self.key) == key]

    def insert(self, sys_at, app_from=D(2019, 1, 1), app_to=INF_D, **values):
        self.rows.append({**values, "app_from": app_from, "app_to": app_to, "sys_from": sys_at, "sys_to": INF_T})

    def update(self, sys_at, key, app_from=None, app_to=INF_D, **changes):
        """Change values for part of the application time: the versions it overlaps are split around it."""
        for old in self._current(key):
            lo, hi = (app_from or old["app_from"]), app_to
            if self.app is None:
                lo, hi = old["app_from"], old["app_to"]
            if hi <= old["app_from"] or lo >= old["app_to"]:
                continue
            old["sys_to"] = sys_at
            base = {k: v for k, v in old.items() if k not in ("sys_from", "sys_to")}
            if old["app_from"] < lo:
                self.rows.append({**base, "app_to": lo, "sys_from": sys_at, "sys_to": INF_T})
            if hi < old["app_to"]:
                self.rows.append({**base, "app_from": hi, "sys_from": sys_at, "sys_to": INF_T})
            self.rows.append({**base, **changes, "app_from": max(lo, old["app_from"]), "app_to": min(hi, old["app_to"]),
                              "sys_from": sys_at, "sys_to": INF_T})

    def replace(self, sys_at, key, app_from, app_to, **changes):
        """Overwrite the application period of a row (and optionally values), which keeps one version."""
        for old in self._current(key):
            old["sys_to"] = sys_at
            base = {k: v for k, v in old.items() if k not in ("sys_from", "sys_to")}
            self.rows.append({**base, **changes, "app_from": app_from, "app_to": app_to,
                              "sys_from": sys_at, "sys_to": INF_T})

    def delete(self, sys_at, key):
        """Remove the row as of the system time, application time untouched."""
        for old in self._current(key):
            old["sys_to"] = sys_at

    def write(self, directory: Path):
        header = list(self.columns) + ([f"{self.app}_begin", f"{self.app}_end"] if self.app else []) + \
                 (["sys_time_begin", "sys_time_end"] if self.system else [])
        with open(directory / f"tpch_bih_{self.name}.csv", "w", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(header)
            for r in sorted(self.rows, key=lambda r: (tuple(r[k] for k in self.key), r["sys_from"], r["app_from"])):
                row = [r[c] for c in self.columns]
                if self.app:
                    row += [r["app_from"], r["app_to"]]
                if self.system:
                    row += [r["sys_from"], r["sys_to"]]
                w.writerow(row)


def build() -> list[Table]:
    region = Table("region", ("r_regionkey",), None, ["r_regionkey", "r_name"], system=False)
    nation = Table("nation", ("n_nationkey",), None, ["n_nationkey", "n_name", "n_regionkey"], system=False)
    for k, n in ((0, "AMERICA"), (1, "EUROPE")):
        region.insert(S0, r_regionkey=k, r_name=n)
    for k, n, r in ((0, "UNITED STATES", 0), (1, "GERMANY", 1), (2, "FRANCE", 1)):
        nation.insert(S0, n_nationkey=k, n_name=n, n_regionkey=r)

    # SUPPLIER: degenerate, system time only
    supplier = Table("supplier", ("s_suppkey",), None, ["s_suppkey", "s_name", "s_nationkey", "s_acctbal"])
    for k, n, nat, bal in ((1, "Supplier#1", 1, 1000.0), (2, "Supplier#2", 2, 500.0), (3, "Supplier#3", 0, 800.0)):
        supplier.insert(S0, s_suppkey=k, s_name=n, s_nationkey=nat, s_acctbal=bal)
    supplier.update(S2, (1,), s_acctbal=1200.0)
    supplier.update(S3, (2,), s_acctbal=450.0)

    # PART: availability_time
    part = Table("part", ("p_partkey",), "availability_time",
                 ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice"])
    for row in ((1, "Part#1", "Manufacturer#1", "Brand#13", "PROMO BRUSHED STEEL", 15, "SM BOX", 900.0),
                (2, "Part#2", "Manufacturer#2", "Brand#24", "STANDARD POLISHED BRASS", 20, "LG CASE", 1000.0),
                (3, "Part#3", "Manufacturer#1", "Brand#13", "PROMO PLATED TIN", 7, "SM BOX", 1100.0),
                (4, "Part#4", "Manufacturer#3", "Brand#35", "ECONOMY ANODIZED STEEL", 30, "MED BAG", 1200.0)):
        part.insert(S0, **part.values(row))
    part.update(S1, (2,), app_from=D(2021, 3, 1), p_retailprice=1050.0)  # price change
    part.replace(S2, (4,), D(2022, 6, 1), INF_D)  # availability delayed

    # PARTSUPP: validity_time
    partsupp = Table("partsupp", ("ps_partkey", "ps_suppkey"), "validity_time",
                     ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost"])
    for row in ((1, 1, 50, 100.0), (1, 2, 70, 80.0), (2, 1, 30, 200.0), (2, 3, 40, 210.0), (3, 2, 60, 300.0),
                (4, 3, 20, 400.0)):
        partsupp.insert(S0, **partsupp.values(row))
    partsupp.update(S1, (1, 1), app_from=D(2021, 6, 1), ps_supplycost=110.0)
    partsupp.update(S2, (1, 2), app_from=D(2022, 3, 1), ps_supplycost=82.0)
    partsupp.update(S2, (2, 1), app_from=D(2022, 1, 1), ps_availqty=80)
    partsupp.update(S3, (1, 1), app_from=D(2023, 3, 1), ps_supplycost=125.0)
    # a new supplier starts supplying part 1 in 2022
    partsupp.insert(S2, app_from=D(2022, 1, 1), ps_partkey=1, ps_suppkey=3, ps_availqty=10, ps_supplycost=95.0)

    # CUSTOMER: visible_time
    customer = Table("customer", ("c_custkey",), "visible_time",
                     ["c_custkey", "c_name", "c_nationkey", "c_acctbal", "c_mktsegment"])
    for row in ((1, "Customer#1", 0, 100.0, "BUILDING"), (2, "Customer#2", 1, 3000.0, "AUTOMOBILE"),
                (3, "Customer#3", 2, 4000.0, "BUILDING"), (4, "Customer#4", 0, 9000.0, "MACHINERY")):
        customer.insert(S0, **customer.values(row))
    customer.update(S1, (1,), app_from=D(2021, 1, 1), c_acctbal=6000.0)
    customer.update(S2, (2,), app_from=D(2022, 1, 1), c_nationkey=0)  # moves to the US
    customer.update(S3, (3,), app_from=D(2023, 1, 1), c_acctbal=2000.0)

    # ORDERS: active_time, plus receivable_time as ordinary dates
    orders = Table("orders", ("o_orderkey",), "active_time",
                   ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority",
                    "o_clerk", "o_shippriority", "receivable_time_begin", "receivable_time_end"])
    lineitem = Table("lineitem", ("l_orderkey", "l_linenumber"), "active_time",
                     ["l_orderkey", "l_linenumber", "l_partkey", "l_suppkey", "l_quantity", "l_extendedprice",
                      "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
                      "l_receiptdate", "l_shipmode"])

    def order(sys_at, key, cust, status, total, odate, prio, clerk, ship_prio, recv_from, active_from,
              active_to=INF_D):
        orders.insert(sys_at, app_from=active_from, app_to=active_to, o_orderkey=key, o_custkey=cust, o_orderstatus=status,
                      o_totalprice=total, o_orderdate=odate, o_orderpriority=prio, o_clerk=clerk,
                      o_shippriority=ship_prio, receivable_time_begin=recv_from, receivable_time_end=INF_D)

    def line(sys_at, okey, n, part_, supp, qty, price, disc, tax, flag, status, ship, commit, receipt, mode,
             active_from, active_to=INF_D):
        """A line is valid from its order's date until the order is delivered or cancelled."""
        lineitem.insert(sys_at, app_from=active_from, app_to=active_to,
                        l_orderkey=okey, l_linenumber=n, l_partkey=part_, l_suppkey=supp, l_quantity=qty,
                        l_extendedprice=price, l_discount=disc, l_tax=tax, l_returnflag=flag, l_linestatus=status,
                        l_shipdate=ship, l_commitdate=commit, l_receiptdate=receipt, l_shipmode=mode)

    # S0: orders 1 to 3
    order(S0, 1, 1, "F", 1000.0, D(2019, 11, 1), "1-URGENT", "Clerk#1", 0, D(2019, 12, 1), D(2019, 11, 1), D(2019, 12, 15))
    line(S0, 1, 1, 1, 1, 10.0, 9000.0, 0.05, 0.02, "R", "F", D(2019, 11, 10), D(2019, 11, 20), D(2019, 11, 25), "MAIL", D(2019, 11, 1), D(2019, 12, 15))
    line(S0, 1, 2, 2, 1, 5.0, 5000.0, 0.10, 0.02, "A", "F", D(2019, 11, 12), D(2019, 11, 18), D(2019, 11, 28), "SHIP", D(2019, 11, 1), D(2019, 12, 15))
    order(S0, 2, 2, "O", 2000.0, D(2019, 12, 10), "2-HIGH", "Clerk#2", 0, D(2019, 12, 20), D(2019, 12, 10))
    line(S0, 2, 1, 1, 2, 20.0, 18000.0, 0.00, 0.05, "N", "O", D(2019, 12, 12), D(2019, 12, 30), D(2020, 1, 10), "MAIL", D(2019, 12, 10))
    line(S0, 2, 2, 3, 2, 8.0, 8800.0, 0.07, 0.05, "N", "O", D(2019, 12, 15), D(2019, 12, 25), D(2020, 1, 12), "AIR", D(2019, 12, 10))
    order(S0, 3, 3, "O", 500.0, D(2019, 12, 20), "3-MEDIUM", "Clerk#1", 0, D(2020, 1, 5), D(2019, 12, 20))
    line(S0, 3, 1, 4, 3, 2.0, 2400.0, 0.02, 0.01, "N", "O", D(2020, 1, 5), D(2020, 1, 15), D(2020, 1, 20), "TRUCK", D(2019, 12, 20))

    # S1: order 4
    order(S1, 4, 4, "O", 3000.0, D(2020, 12, 15), "1-URGENT", "Clerk#3", 0, D(2021, 1, 5), D(2020, 12, 15))
    line(S1, 4, 1, 2, 3, 6.0, 6300.0, 0.04, 0.08, "N", "O", D(2021, 1, 5), D(2021, 1, 25), D(2021, 1, 30), "MAIL", D(2020, 12, 15))
    line(S1, 4, 2, 3, 1, 3.0, 3300.0, 0.06, 0.08, "N", "O", D(2020, 12, 20), D(2021, 1, 10), D(2021, 1, 15), "AIR", D(2020, 12, 15))

    # S2: order 5 placed, order 2 delivered (its active time ends, its status and its lines' status change)
    order(S2, 5, 1, "O", 800.0, D(2021, 12, 20), "5-LOW", "Clerk#2", 0, D(2022, 1, 10), D(2021, 12, 20))
    line(S2, 5, 1, 1, 1, 4.0, 3600.0, 0.00, 0.02, "N", "O", D(2022, 1, 5), D(2022, 1, 20), D(2022, 1, 25), "RAIL", D(2021, 12, 20))
    orders.replace(S2, (2,), D(2019, 12, 10), D(2021, 12, 30), o_orderstatus="F")
    for n in (1, 2):
        lineitem.replace(S2, (2, n), D(2019, 12, 10), D(2021, 12, 30), l_linestatus="F")

    # S3: order 6 placed, order 5 cancelled, order 3's price manipulated with the same application time
    order(S3, 6, 2, "O", 1500.0, D(2022, 12, 20), "2-HIGH", "Clerk#3", 0, D(2023, 1, 10), D(2022, 12, 20))
    line(S3, 6, 1, 2, 2, 9.0, 9450.0, 0.03, 0.04, "N", "O", D(2022, 12, 25), D(2023, 1, 5), D(2023, 1, 15), "SHIP", D(2022, 12, 20))
    orders.delete(S3, (5,))
    lineitem.delete(S3, (5, 1))
    orders.update(S3, (3,), o_totalprice=700.0)

    return [region, nation, supplier, part, partsupp, customer, orders, lineitem]


if __name__ == "__main__":
    out = Path(__file__).resolve().parent.parent / "src" / "datafinder_examples"
    for table in build():
        table.write(out)
        print(f"wrote tpch_bih_{table.name}.csv ({len(table.rows)} rows)")
