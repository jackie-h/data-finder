"""The TPC-BiH data generator follows the report's Appendix A: A.1 (the application times of version 1) and A.2 (the history)."""
import csv
import datetime
from collections import Counter, defaultdict

import pytest

from datafinder_examples_tests.tpch_bih_generator import (
    COLUMNS, INF_D, INF_T, KEYS, SCENARIOS, SYS_END, SYS_START, Generator, add_months, generate,
)


@pytest.fixture(scope="module")
def data() -> Generator:
    return generate(scale=0.001, steps=600, seed=7)


def initial(data: Generator, table: str) -> list[dict]:
    """The rows as the initial load wrote them."""
    return [r for r in data.store.versions[table] if r["sys_time_begin"] == SYS_START]


class TestDeterminism:
    def test_the_same_seed_gives_the_same_data(self):
        a, b = generate(0.001, 150, seed=3), generate(0.001, 150, seed=3)
        assert a.store.versions == b.store.versions and a.log == b.log

    def test_another_seed_gives_other_data(self):
        assert generate(0.001, 150, seed=3).log != generate(0.001, 150, seed=4).log


class TestInitialisation:
    """A.1"""

    def test_version_1_is_dbgen(self, data):
        # dbgen at scale 0.001: 1500 orders, 150 customers, 200 parts, 10 suppliers, 5 regions and 25 nations, and 800
        # partsupp rows of which 700 have a different (part, supplier): the repeats are not loaded
        assert [len(initial(data, t)) for t in ("orders", "customer", "part", "supplier", "partsupp")] \
            == [1500, 150, 200, 10, 700]
        assert len(data.store.versions["region"]) == 5 and len(data.store.versions["nation"]) == 25

    def test_a_line_is_active_from_the_earliest_to_the_latest_of_its_dates(self, data):
        for line in initial(data, "lineitem"):
            dates = (line["l_shipdate"], line["l_commitdate"], line["l_receiptdate"])
            assert (line["active_time_begin"], line["active_time_end"]) == (min(dates), max(dates))

    def test_an_order_spans_its_lines_and_starts_no_later_than_its_date(self, data):
        lines = defaultdict(list)
        for line in initial(data, "lineitem"):
            lines[line["l_orderkey"]].append(line)
        for order in initial(data, "orders"):
            begin = min(l["active_time_begin"] for l in lines[order["o_orderkey"]])
            end = max(l["active_time_end"] for l in lines[order["o_orderkey"]])
            assert order["active_time_begin"] == min(order["o_orderdate"], begin)
            assert order["active_time_end"] == end
            assert begin <= order["receivable_time_begin"] <= order["receivable_time_end"] <= end

    def test_a_customer_is_visible_from_its_first_order(self, data):
        first = {}
        for order in initial(data, "orders"):
            first[order["o_custkey"]] = min(order["active_time_begin"], first.get(order["o_custkey"], INF_D))
        for customer in initial(data, "customer"):
            assert customer["visible_time_begin"] == first.get(customer["c_custkey"], INF_D)
            assert customer["visible_time_end"] == INF_D

    def test_a_part_is_available_from_its_first_line_and_so_are_its_supplies(self, data):
        first = {}
        for line in initial(data, "lineitem"):
            first[line["l_partkey"]] = min(line["active_time_begin"], first.get(line["l_partkey"], INF_D))
        for part in initial(data, "part"):
            assert part["availability_time_begin"] == first.get(part["p_partkey"], INF_D)
            assert part["availability_time_end"] == INF_D
        for supply in initial(data, "partsupp"):
            assert supply["validity_time_begin"] == first.get(supply["ps_partkey"], INF_D)
            assert supply["validity_time_end"] == INF_D

    def test_version_1_is_loaded_at_the_start_of_the_system_time(self, data):
        assert all(r["sys_time_begin"] == SYS_START for t in KEYS for r in initial(data, t))


class TestHistory:
    """A.2"""

    def test_the_scenarios_are_chosen_with_the_report_probabilities(self):
        log = generate(0.001, 4000, seed=11).log
        counts = Counter(entry.split(":")[0] for entry in log)
        total = sum(p for _, p in SCENARIOS)
        for name, probability in SCENARIOS:
            expected = probability / total
            assert abs(counts[name] / len(log) - expected) < 0.03, (name, counts[name] / len(log), expected)

    def test_system_time_moves_forward_inside_the_report_interval(self, data):
        for table in KEYS:
            for row in data.store.versions[table]:
                assert SYS_START <= row["sys_time_begin"] < row["sys_time_end"] <= INF_T
                assert row["sys_time_begin"] <= SYS_END

    def test_a_key_has_one_version_at_a_time_and_they_follow_each_other(self, data):
        for table in KEYS:
            by_key = defaultdict(list)
            for row in data.store.versions[table]:
                by_key[tuple(row[c] for c in KEYS[table])].append(row)
            for versions in by_key.values():
                versions.sort(key=lambda r: r["sys_time_begin"])
                for earlier, later in zip(versions, versions[1:]):
                    assert earlier["sys_time_end"] == later["sys_time_begin"]

    def test_a_deleted_order_has_no_current_lines(self, data):
        current_orders = {k[0] for k in data.store.current["orders"]}
        assert all(l["l_orderkey"] in current_orders for l in data.store.current["lineitem"].values())
        deleted = {r["o_orderkey"] for r in data.store.versions["orders"]} - current_orders
        assert deleted, "the history cancels some orders"

    def test_an_order_status_only_moves_forward(self, data):
        order = {"O": 0, "P": 1, "F": 2}
        by_key = defaultdict(list)
        for row in data.store.versions["orders"]:
            by_key[row["o_orderkey"]].append(row)
        for versions in by_key.values():
            versions.sort(key=lambda r: r["sys_time_begin"])
            ranks = [order[r["o_orderstatus"]] for r in versions]
            assert ranks == sorted(ranks)

    def test_a_new_order_starts_when_it_is_placed(self, data):
        first_new = max(r["o_orderkey"] for r in initial(data, "orders")) + 1
        placed = {}
        for row in sorted(data.store.versions["orders"], key=lambda r: r["sys_time_begin"]):
            if row["o_orderkey"] >= first_new:
                placed.setdefault(row["o_orderkey"], row)  # the version it was inserted as
        assert len(placed) > 100
        for row in placed.values():
            day = row["o_orderdate"]
            assert row["sys_time_begin"].date() == day and row["o_orderstatus"] == "O"
            assert (row["active_time_begin"], row["active_time_end"]) == (day, INF_D)
            assert day + datetime.timedelta(days=1) <= row["receivable_time_begin"] <= day + datetime.timedelta(days=14)
            assert row["receivable_time_end"] == INF_D

    def test_stock_is_never_negative(self, data):
        assert all(r["ps_availqty"] >= 0 for r in data.store.versions["partsupp"])

    def test_a_delivered_line_ends_when_it_was_delivered(self, data):
        by_key = defaultdict(list)
        for row in data.store.versions["lineitem"]:
            by_key[(row["l_orderkey"], row["l_linenumber"])].append(row)
        delivered = 0
        for versions in by_key.values():
            versions.sort(key=lambda r: r["sys_time_begin"])
            for before, after in zip(versions, versions[1:]):
                if before["l_linestatus"] == "O" and after["l_linestatus"] == "F":
                    delivered += 1
                    assert after["active_time_end"] == after["sys_time_begin"].date()
        assert delivered > 50

    def test_delaying_availability_starts_it_one_to_fourteen_days_ahead(self, data):
        by_key = defaultdict(list)
        for row in data.store.versions["part"]:
            by_key[row["p_partkey"]].append(row)
        delays = 0
        for versions in by_key.values():
            versions.sort(key=lambda r: r["sys_time_begin"])
            for after in versions[1:]:
                delays += 1
                ahead = (after["availability_time_begin"] - after["sys_time_begin"].date()).days
                assert 1 <= ahead <= 14 and after["availability_time_end"] == INF_D
        assert delays > 5

    def test_a_price_change_keeps_the_cost_positive_and_overwrites_the_validity(self, data):
        by_key = defaultdict(list)
        for row in data.store.versions["partsupp"]:
            by_key[(row["ps_partkey"], row["ps_suppkey"])].append(row)
        changes = 0
        for versions in by_key.values():
            versions.sort(key=lambda r: r["sys_time_begin"])
            for before, after in zip(versions, versions[1:]):
                if after["ps_supplycost"] != before["ps_supplycost"]:
                    changes += 1
                    assert after["ps_supplycost"] >= 0 and after["validity_time_end"] == INF_D
                    assert abs(after["ps_supplycost"] - before["ps_supplycost"]) <= 100.01  # |change| <= 100, then abs()
        assert changes > 5

    def test_the_history_changes_every_table_it_should(self, data):
        for table in ("supplier", "part", "partsupp", "customer", "orders", "lineitem"):
            assert len(data.store.versions[table]) > len(initial(data, table)), table


class TestOutput:
    def test_the_csv_has_the_columns_and_the_rows(self, data, tmp_path):
        counts = data.write(tmp_path)
        for table, columns in COLUMNS.items():
            with open(tmp_path / f"tpch_bih_benchmark_{table}.csv") as f:
                rows = list(csv.reader(f))
            assert rows[0] == columns and len(rows) - 1 == counts[table]

    def test_the_end_of_time_is_written_as_9999(self, data, tmp_path):
        data.write(tmp_path)
        with open(tmp_path / "tpch_bih_benchmark_orders.csv") as f:
            row = next(csv.DictReader(f))
        assert row["sys_time_end"].startswith(("9999-12-31 23:59:59", "20")) and len(row["sys_time_begin"]) == 19


def test_add_months_clamps_to_the_end_of_the_month():
    assert add_months(datetime.date(2000, 1, 31), 1) == datetime.date(2000, 2, 29)
    assert add_months(datetime.date(2000, 11, 30), 3) == datetime.date(2001, 2, 28)
