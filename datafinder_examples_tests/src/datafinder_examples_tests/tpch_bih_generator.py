"""Generates TPC-BiH data as the benchmark's report defines it.

The report is Kaufmann, Fischer, May, Kossmann, "Benchmarking Databases with History Support" (SAP/ETH, 2013), Appendix A:

* Version 1 is the TPC-H ``dbgen`` data. DuckDB's TPC-H extension is dbgen, so ``CALL dbgen(sf=...)`` is used.
* A.1 gives the application time periods of version 1, derived from the TPC-H dates.
* A.2 is the history generator: nine update scenarios chosen at random with the probabilities the report gives, while the
  system time moves linearly from 2000-01-01 to 2009-12-31.

``generate(scale, steps, seed)`` returns a Generator whose ``store`` holds every version of every table, and ``write`` puts
them in CSV files. The output is deterministic for a seed (dbgen is deterministic for a scale factor), so tests generate the
data when they run and nothing is checked in. ``datafinder_examples/tools/generate_tpch_bih_benchmark_data.py`` writes the
CSVs to look at.

The scale factor ``h`` is the TPC-H one, and the number of ``steps`` the history runs is the report's ``m``: at ``m = 1``
the report's Table 2 has about 0.2 new orders for each initial order, which is about one step for each initial order.

Where the report is not specific, this does the following (they are choices, not the report's words):

* The probabilities of the scenarios add up to 0.91, not 1, so they are used as weights.
* ``CURRENT_TIME`` is the simulated clock. It is a timestamp for the system time and its date for application times.
* An update is a new version: the current version of the row is ended at the system time and a new one begins. A row has
  one current version, and an update that sets an application time period replaces the period (the report's "overwrite
  application time"), it does not split the row.
* The operators in the report's PDF did not survive (``ACCTBAL  TOTALPRICE`` and so on). The logic fixes most of them:
  Deliver Order subtracts the price from the balance, Receive Payment adds it back, and Deliver Order subtracts the
  quantity from the stock. Manipulate Order Data multiplies the extended price by ``uniform(1, 10)``; the report does not
  settle whether it multiplies, adds or subtracts.
* In Deliver Order, "Set ORDERSTATUS := 'F'" is done when the order is complete (all lines finished and the date within the
  receivable time), which is where the report's indentation puts it.
* A customer or part with no orders or lines has no start of its application time (A.1 takes the minimum over none), so it
  is never visible, and its period is ``[9999-12-31, 9999-12-31)``.
* TPC-H makes ``(ps_partkey, ps_suppkey)`` the primary key of PARTSUPP, but dbgen repeats some pairs when there are few
  suppliers (a scale factor of 0.001 has 10, and gives 700 distinct pairs in 800 rows). The first row of each pair is kept.
* A line item's quantity, price, discount, tax, dates and shipping details, which A.2.1 does not give for a new order, are
  made as dbgen makes them.
"""
import argparse
import csv
import datetime
import random
from pathlib import Path
from typing import Any, Callable

import duckdb

INF_D = datetime.date(9999, 12, 31)
INF_T = datetime.datetime(9999, 12, 31, 23, 59, 59)
SYS_START = datetime.datetime(2000, 1, 1)
SYS_END = datetime.datetime(2009, 12, 31)

#: scenario, probability from the report
SCENARIOS = [
    ("new_order", 0.30), ("cancel_order", 0.01), ("deliver_order", 0.20), ("receive_payment", 0.20),
    ("update_stock", 0.05), ("delay_availability", 0.05), ("change_price", 0.05), ("update_supplier", 0.049),
    ("manipulate_order_data", 0.001),
]

_SYSTEM = ["sys_time_begin", "sys_time_end"]
COLUMNS: dict[str, list[str]] = {
    "region": ["r_regionkey", "r_name", "r_comment"],
    "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    "supplier": ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment", *_SYSTEM],
    "part": ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment",
             "availability_time_begin", "availability_time_end", *_SYSTEM],
    "partsupp": ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment",
                 "validity_time_begin", "validity_time_end", *_SYSTEM],
    "customer": ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment",
                 "visible_time_begin", "visible_time_end", *_SYSTEM],
    "orders": ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk",
               "o_shippriority", "o_comment", "receivable_time_begin", "receivable_time_end",
               "active_time_begin", "active_time_end", *_SYSTEM],
    "lineitem": ["l_orderkey", "l_linenumber", "l_partkey", "l_suppkey", "l_quantity", "l_extendedprice", "l_discount",
                 "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct",
                 "l_shipmode", "l_comment", "active_time_begin", "active_time_end", *_SYSTEM],
}
KEYS: dict[str, tuple[str, ...]] = {
    "supplier": ("s_suppkey",), "part": ("p_partkey",), "partsupp": ("ps_partkey", "ps_suppkey"),
    "customer": ("c_custkey",), "orders": ("o_orderkey",), "lineitem": ("l_orderkey", "l_linenumber"),
}

SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"]
PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
INSTRUCTIONS = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
MODES = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
WORDS = ["furiously", "carefully", "quickly", "regular", "express", "final", "pending", "bold", "ironic", "special",
         "deposits", "packages", "requests", "accounts", "theodolites", "foxes", "pinto", "beans", "platelets"]


def add_months(day: datetime.date, months: int) -> datetime.date:
    month = day.month - 1 + months
    year, month = day.year + month // 12, month % 12 + 1
    last = (datetime.date(year + (month == 12), month % 12 + 1, 1) - datetime.timedelta(days=1)).day
    return datetime.date(year, month, min(day.day, last))


class Store:
    """The versions of every table. A key has at most one current version, which ends when the key is updated or deleted."""

    def __init__(self) -> None:
        self.versions: dict[str, list[dict]] = {t: [] for t in COLUMNS}
        self.current: dict[str, dict[tuple, dict]] = {t: {} for t in KEYS}

    def key(self, table: str, row: dict) -> tuple:
        return tuple(row[c] for c in KEYS[table])

    def static(self, table: str, row: dict) -> None:
        self.versions[table].append(row)

    def insert(self, table: str, row: dict, at: datetime.datetime) -> dict:
        row = {**row, "sys_time_begin": at, "sys_time_end": INF_T}
        self.versions[table].append(row)
        self.current[table][self.key(table, row)] = row
        return row

    def update(self, table: str, key: tuple, at: datetime.datetime, **changes: Any) -> dict:
        old = self.current[table][key]
        if old["sys_time_begin"] == at:  # the same transaction: the version it made is changed, not versioned again
            old.update(changes)
            return old
        old["sys_time_end"] = at
        new = {**old, **changes, "sys_time_begin": at, "sys_time_end": INF_T}
        self.versions[table].append(new)
        self.current[table][key] = new
        return new

    def delete(self, table: str, key: tuple, at: datetime.datetime) -> None:
        old = self.current[table].pop(key)
        old["sys_time_end"] = at


class Generator:
    def __init__(self, scale: float, steps: int, seed: int) -> None:
        self.scale, self.steps = scale, steps
        self.rng = random.Random(seed)
        self.store = Store()
        self.log: list[str] = []  # the scenario of each step, and whether it changed anything
        self.lines_of: dict[int, list[tuple]] = {}
        self.next_order = self.next_customer = 0
        self.clerks = max(1, int(scale * 1000))

    # --- random values ------------------------------------------------------------------------------------------------

    def uniform_days(self, low: int, high: int) -> datetime.timedelta:
        return datetime.timedelta(days=self.rng.randint(low, high))

    def uniform_date(self, low: datetime.date, high: datetime.date) -> datetime.date:
        return low + datetime.timedelta(days=self.rng.randint(0, max(0, (high - low).days)))

    def text(self, words: int = 5) -> str:
        return " ".join(self.rng.choice(WORDS) for _ in range(words))

    @staticmethod
    def phone(nation: int, rng: random.Random) -> str:
        return f"{nation + 10}-{rng.randint(100, 999)}-{rng.randint(100, 999)}-{rng.randint(1000, 9999)}"

    # --- version 1: dbgen, and A.1 ------------------------------------------------------------------------------------

    def initialise(self) -> None:
        con = duckdb.connect()
        con.execute("LOAD tpch")
        con.execute(f"CALL dbgen(sf={self.scale})")

        def rows(table: str) -> list[dict]:
            cur = con.execute(f"SELECT * FROM {table} ORDER BY 1, 2" if table in ("partsupp", "lineitem")
                              else f"SELECT * FROM {table} ORDER BY 1")
            names = [d[0] for d in cur.description or []]
            return [{n: (float(v) if hasattr(v, "as_tuple") else v) for n, v in zip(names, r)} for r in cur.fetchall()]

        for r in rows("region"):
            self.store.static("region", r)
        for r in rows("nation"):
            self.store.static("nation", r)

        lines = rows("lineitem")
        # A.1: a line is active from the earliest to the latest of its dates
        order_lines: dict[int, list[dict]] = {}
        for line in lines:
            dates = (line["l_shipdate"], line["l_commitdate"], line["l_receiptdate"])
            line["active_time_begin"], line["active_time_end"] = min(dates), max(dates)
            order_lines.setdefault(line["l_orderkey"], []).append(line)

        orders = rows("orders")
        customer_begin: dict[int, datetime.date] = {}
        for order in orders:
            mine = order_lines[order["o_orderkey"]]
            begin, end = min(l["active_time_begin"] for l in mine), max(l["active_time_end"] for l in mine)
            order["active_time_begin"] = min(order["o_orderdate"], begin)
            order["active_time_end"] = end
            order["receivable_time_begin"] = self.uniform_date(begin, end)
            order["receivable_time_end"] = self.uniform_date(order["receivable_time_begin"], end)
            customer_begin[order["o_custkey"]] = min(order["active_time_begin"],
                                                     customer_begin.get(order["o_custkey"], INF_D))

        part_begin: dict[int, datetime.date] = {}
        for line in lines:
            part_begin[line["l_partkey"]] = min(line["active_time_begin"], part_begin.get(line["l_partkey"], INF_D))

        at = SYS_START
        for r in rows("supplier"):
            self.store.insert("supplier", r, at)
        for r in rows("part"):
            r["availability_time_begin"], r["availability_time_end"] = part_begin.get(r["p_partkey"], INF_D), INF_D
            self.store.insert("part", r, at)
        seen_supplies: set[tuple] = set()
        for r in rows("partsupp"):
            if (r["ps_partkey"], r["ps_suppkey"]) in seen_supplies:  # dbgen repeats a pair at a small scale factor
                continue
            seen_supplies.add((r["ps_partkey"], r["ps_suppkey"]))
            r["validity_time_begin"] = part_begin.get(r["ps_partkey"], INF_D)
            r["validity_time_end"] = INF_D
            self.store.insert("partsupp", r, at)
        for r in rows("customer"):
            r["visible_time_begin"], r["visible_time_end"] = customer_begin.get(r["c_custkey"], INF_D), INF_D
            self.store.insert("customer", r, at)
        for order in orders:
            self.store.insert("orders", order, at)
        for line in lines:
            self.store.insert("lineitem", line, at)
            self.lines_of.setdefault(line["l_orderkey"], []).append((line["l_orderkey"], line["l_linenumber"]))
        self.next_order = max(o["o_orderkey"] for o in orders) + 1
        self.next_customer = max(c["c_custkey"] for c in self.store.current["customer"].values()) + 1

    # --- A.2: the history ---------------------------------------------------------------------------------------------

    def run(self) -> None:
        self.initialise()
        span = (SYS_END - SYS_START) / self.steps
        names, weights = zip(*SCENARIOS)
        for step in range(1, self.steps + 1):
            now = (SYS_START + step * span).replace(microsecond=0)
            scenario = self.rng.choices(names, weights)[0]
            changed = getattr(self, scenario)(now, now.date())
            self.log.append(scenario if changed else scenario + ":skipped")

    def cur(self, table: str):
        return self.store.current[table]

    def visible(self, row: dict, begin: str, end: str, day: datetime.date) -> bool:
        return row[begin] <= day < row[end]

    def new_line(self, orderkey: int, number: int, today: datetime.date, at: datetime.datetime) -> dict | None:
        parts = [p for p in self.cur("part").values() if p["availability_time_begin"] < INF_D]
        if not parts:
            return None
        part = self.rng.choice(parts)
        suppliers = [ps for (pk, sk), ps in self.cur("partsupp").items() if pk == part["p_partkey"]]
        supplier = self.rng.choice(suppliers)
        quantity = float(self.rng.randint(1, 50))
        shipdate = today + self.uniform_days(1, 121)
        return {
            "l_orderkey": orderkey, "l_linenumber": number, "l_partkey": part["p_partkey"],
            "l_suppkey": supplier["ps_suppkey"], "l_quantity": quantity,
            "l_extendedprice": round(quantity * part["p_retailprice"], 2),
            "l_discount": self.rng.randint(0, 10) / 100, "l_tax": self.rng.randint(0, 8) / 100,
            "l_returnflag": "N", "l_linestatus": "O", "l_shipdate": shipdate,
            "l_commitdate": today + self.uniform_days(30, 90), "l_receiptdate": shipdate + self.uniform_days(1, 30),
            "l_shipinstruct": self.rng.choice(INSTRUCTIONS), "l_shipmode": self.rng.choice(MODES), "l_comment": self.text(),
            "active_time_begin": today, "active_time_end": INF_D,
        }

    def new_order(self, at: datetime.datetime, today: datetime.date) -> bool:  # A.2.1
        customers = [c for c in self.cur("customer").values()
                     if self.visible(c, "visible_time_begin", "visible_time_end", today)]
        if customers and self.rng.random() < 0.5:  # 1. an existing customer
            customer = self.rng.choice(customers)
            if self.rng.random() < 0.5:  # a. the customer's data changes
                nation = self.rng.randint(0, 24)
                self.store.update(
                    "customer", (customer["c_custkey"],), at,
                    visible_time_begin=today + self.uniform_days(1, 30), visible_time_end=INF_D, c_nationkey=nation,
                    c_address=self.text(3), c_phone=self.phone(nation, self.rng))
            custkey = customer["c_custkey"]
        else:  # 2. a new customer
            custkey, nation = self.next_customer, self.rng.randint(0, 24)
            self.next_customer += 1
            self.store.insert("customer", {
                "c_custkey": custkey, "c_name": f"Customer#{custkey:09d}", "c_address": self.text(3), "c_nationkey": nation,
                "c_phone": self.phone(nation, self.rng), "c_acctbal": 0.0, "c_mktsegment": self.rng.choice(SEGMENTS),
                "c_comment": self.text(), "visible_time_begin": today, "visible_time_end": INF_D}, at)
        orderkey = self.next_order
        self.next_order += 1
        lines = []
        for number in range(1, self.rng.randint(1, 7) + 1):
            line = self.new_line(orderkey, number, today, at)
            if line:
                lines.append(line)
        if not lines:
            return False
        total = round(sum(l["l_extendedprice"] * (1 - l["l_discount"]) * (1 + l["l_tax"]) for l in lines), 2)
        receivable = today + self.uniform_days(1, 14)
        self.store.insert("orders", {
            "o_orderkey": orderkey, "o_custkey": custkey, "o_orderstatus": "O", "o_totalprice": total, "o_orderdate": today,
            "o_orderpriority": self.rng.choice(PRIORITIES), "o_clerk": f"Clerk#{self.rng.randint(1, self.clerks):09d}",
            "o_shippriority": 0, "o_comment": self.text(), "receivable_time_begin": receivable,
            "receivable_time_end": INF_D, "active_time_begin": today, "active_time_end": INF_D}, at)
        for line in lines:
            self.store.insert("lineitem", line, at)
            self.lines_of.setdefault(orderkey, []).append((orderkey, line["l_linenumber"]))
        return True

    def pick(self, candidates: list):
        return self.rng.choice(candidates) if candidates else None

    def cancel_order(self, at: datetime.datetime, today: datetime.date) -> bool:  # A.2.2
        order = self.pick([o for o in self.cur("orders").values() if o["o_orderstatus"] != "F"])
        if order is None:
            return False
        if order["o_orderstatus"] == "P":
            customer = self.cur("customer")[(order["o_custkey"],)]
            self.store.update("customer", (customer["c_custkey"],), at,
                              c_acctbal=round(customer["c_acctbal"] + order["o_totalprice"], 2))
        for key in self.lines_of.pop(order["o_orderkey"], []):
            line = self.cur("lineitem")[key]
            if line["l_linestatus"] == "F":
                ps = self.cur("partsupp")[(line["l_partkey"], line["l_suppkey"])]
                self.store.update("partsupp", (ps["ps_partkey"], ps["ps_suppkey"]), at,
                                  ps_availqty=ps["ps_availqty"] + int(line["l_quantity"]))
            self.store.delete("lineitem", key, at)
        self.store.delete("orders", (order["o_orderkey"],), at)
        return True

    def deliver_order(self, at: datetime.datetime, today: datetime.date) -> bool:  # A.2.3
        candidates = [o for o in self.cur("orders").values()
                      if o["o_orderstatus"] == "P"
                      or (o["o_orderstatus"] == "O"
                          and self.cur("customer")[(o["o_custkey"],)]["c_acctbal"] - o["o_totalprice"] >= 0)]
        order = self.pick(candidates)
        if order is None:
            return False
        okey = (order["o_orderkey"],)
        if order["o_orderstatus"] == "O":
            customer = self.cur("customer")[(order["o_custkey"],)]
            self.store.update("customer", (customer["c_custkey"],), at,
                              c_acctbal=round(customer["c_acctbal"] - order["o_totalprice"], 2))
            order = self.store.update("orders", okey, at, o_orderstatus="P")
        for key in self.lines_of.get(order["o_orderkey"], []):
            line = self.cur("lineitem")[key]
            ps = self.cur("partsupp")[(line["l_partkey"], line["l_suppkey"])]
            part = self.cur("part")[(line["l_partkey"],)]
            if (line["l_linestatus"] == "O" and ps["ps_availqty"] - line["l_quantity"] > 0
                    and self.visible(part, "availability_time_begin", "availability_time_end", today)):
                self.store.update("partsupp", (ps["ps_partkey"], ps["ps_suppkey"]), at,
                                  ps_availqty=ps["ps_availqty"] - int(line["l_quantity"]))
                self.store.update("lineitem", key, at, l_linestatus="F", active_time_end=today)
        order = self.cur("orders")[okey]
        done = all(self.cur("lineitem")[k]["l_linestatus"] == "F" for k in self.lines_of.get(order["o_orderkey"], []))
        if self.visible(order, "receivable_time_begin", "receivable_time_end", today) and done:
            changes: dict[str, Any] = {"active_time_end": today, "o_orderstatus": "F"}
            if order["receivable_time_end"] == INF_D:
                changes["receivable_time_end"] = today
            self.store.update("orders", okey, at, **changes)
        return True

    def receive_payment(self, at: datetime.datetime, today: datetime.date) -> bool:  # A.2.4
        order = self.pick([
            o for o in self.cur("orders").values()
            if self.visible(o, "receivable_time_begin", "receivable_time_end", today) and o["o_orderstatus"] == "O"
            and self.cur("customer")[(o["o_custkey"],)]["c_acctbal"] - o["o_totalprice"] < 0])
        if order is None:
            return False
        customer = self.cur("customer")[(order["o_custkey"],)]
        self.store.update("orders", (order["o_orderkey"],), at, receivable_time_end=today)
        self.store.update("customer", (customer["c_custkey"],), at,
                          c_acctbal=round(customer["c_acctbal"] + order["o_totalprice"], 2))
        return True

    def update_stock(self, at: datetime.datetime, today: datetime.date) -> bool:  # A.2.5
        def short(line: dict) -> bool:
            ps = self.cur("partsupp")[(line["l_partkey"], line["l_suppkey"])]
            part = self.cur("part")[(line["l_partkey"],)]
            return (line["l_linestatus"] == "O" and ps["ps_availqty"] < line["l_quantity"]
                    and self.visible(part, "availability_time_begin", "availability_time_end", today))
        line = self.pick([l for l in self.cur("lineitem").values() if short(l)])
        if line is None:
            return False
        ps = self.cur("partsupp")[(line["l_partkey"], line["l_suppkey"])]
        self.store.update("partsupp", (ps["ps_partkey"], ps["ps_suppkey"]), at,
                          ps_availqty=ps["ps_availqty"] + 2 * int(line["l_quantity"]))
        return True

    def delay_availability(self, at: datetime.datetime, today: datetime.date) -> bool:  # A.2.6
        part = self.pick(list(self.cur("part").values()))
        if part is None:
            return False
        self.store.update("part", (part["p_partkey"],), at, availability_time_begin=today + self.uniform_days(1, 14),
                          availability_time_end=INF_D)
        return True

    def change_price(self, at: datetime.datetime, today: datetime.date) -> bool:  # A.2.7
        ps = self.pick(list(self.cur("partsupp").values()))
        if ps is None:
            return False
        begin = today + datetime.timedelta(days=round(self.rng.gauss(-15, 30)))
        change = self.rng.uniform(-100, 100)
        self.store.update("partsupp", (ps["ps_partkey"], ps["ps_suppkey"]), at,
                          ps_supplycost=round(abs(ps["ps_supplycost"] + change), 2), validity_time_begin=begin,
                          validity_time_end=INF_D)
        return True

    def update_supplier(self, at: datetime.datetime, today: datetime.date) -> bool:  # A.2.8
        supplier = self.pick(list(self.cur("supplier").values()))
        if supplier is None:
            return False
        self.store.update("supplier", (supplier["s_suppkey"],), at,
                          s_acctbal=round(abs(supplier["s_acctbal"] + self.rng.uniform(-100, 100)), 2))
        return True

    def manipulate_order_data(self, at: datetime.datetime, today: datetime.date) -> bool:  # A.2.9
        order = self.pick([o for o in self.cur("orders").values() if o["o_orderstatus"] == "F"
                           and o["receivable_time_end"] < INF_D and today > add_months(o["receivable_time_end"], 1)])
        if order is None:
            return False
        total = 0.0
        for key in self.lines_of.get(order["o_orderkey"], []):
            line = self.cur("lineitem")[key]
            price = round(line["l_extendedprice"] * self.rng.uniform(1, 10), 2)
            self.store.update("lineitem", key, at, l_extendedprice=price)
            total += price
        self.store.update("orders", (order["o_orderkey"],), at, o_totalprice=round(total, 2))
        return True

    # --- output -------------------------------------------------------------------------------------------------------

    def write(self, directory: Path, prefix: str = "tpch_bih_benchmark_") -> dict[str, int]:
        counts = {}
        for table, columns in COLUMNS.items():
            ordered = sorted(self.store.versions[table], key=lambda r: tuple(r.get(c) for c in
                             (*KEYS.get(table, (columns[0],)), "sys_time_begin") if c in r))
            with open(directory / f"{prefix}{table}.csv", "w", newline="") as f:
                w = csv.writer(f, lineterminator="\n")
                w.writerow(columns)
                for r in ordered:
                    w.writerow([_text(r[c]) for c in columns])
            counts[table] = len(ordered)
        return counts


def _text(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def generate(scale: float = 0.001, steps: int = 1000, seed: int = 1) -> Generator:
    generator = Generator(scale, steps, seed)
    generator.run()
    return generator
