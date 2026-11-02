"""The TPC-BiH queries of Appendix B (B.1 time travel, B.3 key history, B.5 bitemporal) as the report writes them, with the
values of their parameters. tools/build_tpch_bih_cases.py runs each on the generated data and writes the result to
tpch_bih_cases.py, so the SQL here is the source of the expected rows.

The SQL is the report's text. Where it has a typo (B.5.5 has no AND after the first condition) the typo is fixed and noted.
The report draws parameters at random from a range; a value here is one in that range, chosen so the result is not empty.
The generated data is small (scale factor 0.001), so a few values are chosen to keep a result short enough to read:
the balance in K.6.
"""
import datetime

from datafinder_examples_tests.tpch_bih_case import BenchmarkCase

_D = datetime.date
_T = datetime.datetime

#: a point in application time and one in system time that every table has rows at, and the other points the queries take
_APP = _D(1996, 6, 1)
_SYS = _T(2005, 6, 1)

#: the customer with the most versions, and an application time at which it has several system versions
_CUSTOMER = 67
_CUSTOMER_APP = _D(2004, 1, 1)
#: the order with the most versions
_ORDER = 6052
#: a part, whose suppliers supply many other parts
_PART = 1

_CUSTOMER_COLUMNS = ("c_custkey", "c_name", "c_address", "c_phone", "c_acctbal")
_ORDER_COLUMNS = ("o_orderkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_shippriority")

# ---------------------------------------------------------------------------------------------------------------------
# B.1 Time travel
# ---------------------------------------------------------------------------------------------------------------------

B1_DEFINITIONS = [
    BenchmarkCase(
        id="T1", title="Simple time-travel query on a stable relation",
        sql="""SELECT AVG(ps_supplycost), count(*)
FROM partsupp
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
FOR BUSINESS_TIME AS OF '[APP_TIME]'""",
        parameters={"SYS_TIME": _SYS, "APP_TIME": _APP}, select=(0, 1), approximate=True),

    BenchmarkCase(
        id="T2", title="Simple time-travel query on a growing relation",
        sql="""SELECT SUM(o_totalprice) AS revenue, count(*)
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
FOR BUSINESS_TIME AS OF '[APP_TIME]'
WHERE o_orderstatus = 'O'""",
        parameters={"SYS_TIME": _SYS, "APP_TIME": _APP}, select=(0, 1), approximate=True),

    BenchmarkCase(
        id="T5", title="Retrieving all versions",
        sql="""SELECT o_custkey, MAX(o_totalprice), count(*) AS supp_revenue
FROM orders
FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30'
WHERE o_orderstatus = 'O'
GROUP BY o_custkey""",
        parameters={}, select=("o_custkey", 1, "supp_revenue"), approximate=True),

    BenchmarkCase(
        id="T6_sys", title="Slicing one temporal dimension: system time",
        sql="""SELECT o_custkey, AVG(o_totalprice) AS supp_revenue
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
WHERE o_orderstatus = 'O'
GROUP BY o_custkey""",
        parameters={"SYS_TIME": _SYS}, select=("o_custkey", "supp_revenue"), approximate=True),

    BenchmarkCase(
        id="T6_app", title="Slicing one temporal dimension: application time",
        sql="""SELECT o_custkey, AVG(o_totalprice) AS supp_revenue
FROM orders
FOR BUSINESS_TIME AS OF TIMESTAMP '[APP_TIME]'
FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30'
WHERE o_orderstatus = 'O'
GROUP BY o_custkey""",
        parameters={"APP_TIME": _APP}, select=("o_custkey", "supp_revenue"), approximate=True),

    BenchmarkCase(
        id="T7", title="Implicit vs. explicit current time",
        sql="""SELECT PS_PARTKEY, AVG(ps_availqty) AS avail
FROM partsupp
GROUP BY PS_PARTKEY""",
        parameters={}, select=("ps_partkey", "avail"), approximate=True),

    BenchmarkCase(
        id="T8", title="Time travel among multiple application times",
        sql="""SELECT count(*)
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
FOR BUSINESS_TIME AS OF '[APP_TIME]'
WHERE '[APP_TIME]' BETWEEN receivable_time_start AND receivable_time_end""",
        parameters={"SYS_TIME": _SYS, "APP_TIME": _APP}, select=(0,)),

    BenchmarkCase(
        id="T9", title="Time travel to a simulated/secondary application time",
        sql="""SELECT count(*)
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
WHERE '[APP_TIME]' BETWEEN receivable_time_start AND receivable_time_end""",
        parameters={"SYS_TIME": _SYS, "APP_TIME": _APP}, select=(0,)),
]

# ---------------------------------------------------------------------------------------------------------------------
# B.3 Pure-key queries (audit)
# ---------------------------------------------------------------------------------------------------------------------

B3_DEFINITIONS = [
    BenchmarkCase(
        id="K1_sys", title="All time versions of a tuple: system time",
        sql="""SELECT
o_orderkey, o_custkey, o_orderstatus, o_totalprice,
o_orderdate, o_orderpriority, o_shippriority,
sys_time_start
FROM
orders
FOR
SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
o_orderkey = [ORDER_KEY]
ORDER BY
sys_time_start""",
        parameters={"ORDER_KEY": _ORDER, "APP_TIME": _D(2002, 1, 1)}, select=(*_ORDER_COLUMNS, "sys_time_begin"),
        ordered=True, approximate=True),

    BenchmarkCase(
        id="K1_app", title="All time versions of a tuple: application time",
        sql="""SELECT
o_orderkey, o_custkey, o_orderstatus, o_totalprice,
o_orderdate, o_orderpriority, o_shippriority,
active_time_start
FROM
orders
FOR
BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE
o_orderkey = [ORDER_KEY]
ORDER BY
active_time_start""",
        parameters={"ORDER_KEY": _ORDER}, select=(*_ORDER_COLUMNS, "active_time_begin"), ordered=True, approximate=True),

    BenchmarkCase(
        id="K2_sys", title="A time version range of a tuple: system time",
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
sys_time_start
FROM
customer
FOR
SYSTEM_TIME FROM TIMESTAMP '[SYS_TIME_BEGIN]' TO TIMESTAMP '[SYS_TIME_END]'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
c_custkey = [CUSTOMER_KEY]
ORDER BY
sys_time_start""",
        parameters={"CUSTOMER_KEY": _CUSTOMER, "APP_TIME": _CUSTOMER_APP,
                    "SYS_TIME_BEGIN": _T(2003, 1, 1), "SYS_TIME_END": _T(2009, 1, 1)},
        select=(*_CUSTOMER_COLUMNS, "sys_time_begin"), ordered=True, approximate=True),

    BenchmarkCase(
        id="K2_app", title="A time version range of a tuple: application time",
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
visible_time_start
FROM
customer
FOR
BUSINESS_TIME FROM '[APP_TIME_BEGIN]' TO '[APP_TIME_END]'
WHERE
c_custkey = [CUSTOMER_KEY]
ORDER BY
visible_time_start""",
        parameters={"CUSTOMER_KEY": _CUSTOMER, "APP_TIME_BEGIN": _D(1992, 1, 1), "APP_TIME_END": _D(2010, 1, 1)},
        select=(*_CUSTOMER_COLUMNS, "visible_time_begin"), ordered=True, approximate=True),

    BenchmarkCase(
        id="K3_sys", title="Aggregation over system time",
        sql="""SELECT
MIN(c_acctbal)
FROM
customer
FOR
SYSTEM_TIME FROM TIMESTAMP '[SYS_TIME_BEGIN]' TO TIMESTAMP '[SYS_TIME_END]'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
c_custkey = [CUSTOMER_KEY]""",
        parameters={"CUSTOMER_KEY": _CUSTOMER, "APP_TIME": _CUSTOMER_APP,
                    "SYS_TIME_BEGIN": _T(2003, 1, 1), "SYS_TIME_END": _T(2009, 1, 1)},
        select=(0,), approximate=True),

    BenchmarkCase(
        id="K3_app", title="Aggregation over application time",
        sql="""SELECT
MIN(c_acctbal)
FROM
customer
FOR
BUSINESS_TIME FROM '[APP_TIME_BEGIN]' TO '[APP_TIME_END]'
WHERE
c_custkey = [CUSTOMER_KEY]""",
        parameters={"CUSTOMER_KEY": _CUSTOMER, "APP_TIME_BEGIN": _D(1992, 1, 1), "APP_TIME_END": _D(2010, 1, 1)},
        select=(0,), approximate=True),

    BenchmarkCase(
        id="K4_sys", title="A version count range of a tuple: system time",
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
sys_time_start, sys_time_end
FROM
customer
FOR
SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
c_custkey = [CUSTOMER_KEY]
ORDER BY
sys_time_start
FETCH FIRST [HSIZE] ROWS ONLY""",
        parameters={"CUSTOMER_KEY": _CUSTOMER, "APP_TIME": _CUSTOMER_APP, "HSIZE": 10},
        select=(*_CUSTOMER_COLUMNS, "sys_time_begin", "sys_time_end"), ordered=True, approximate=True),

    BenchmarkCase(
        id="K4_app", title="A version count range of a tuple: application time",
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
visible_time_start, visible_time_end
FROM
customer
FOR
BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE
c_custkey = [CUST_KEY]
ORDER BY
visible_time_start
FETCH FIRST [HSIZE] ROWS ONLY""",
        parameters={"CUST_KEY": _CUSTOMER, "HSIZE": 10},
        select=(*_CUSTOMER_COLUMNS, "visible_time_begin", "visible_time_end"), ordered=True, approximate=True),

    BenchmarkCase(
        id="K5_sys", title="The preceding version of a tuple: system time",
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
sys_time_start
FROM
customer
FOR
SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
c_custkey = [CUSTOMER_KEY] AND
sys_time_end =
(
SELECT max(sys_time_end)
FROM customer
FOR
SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
c_custkey = [CUSTOMER_KEY] and sys_time_END <= TIMESTAMP '[SYS_TIME]'
)""",
        parameters={"CUSTOMER_KEY": _CUSTOMER, "APP_TIME": _CUSTOMER_APP, "SYS_TIME": _T(2006, 1, 1)},
        select=("c_custkey", "c_acctbal", "sys_time_begin"), approximate=True),

    BenchmarkCase(
        id="K5_app", title="The preceding version of a tuple: application time",
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
visible_time_start
FROM
customer
WHERE
c_custkey = [CUSTOMER_KEY] AND
visible_time_end =
(
SELECT max(visible_time_end)
FROM customer
WHERE
c_custkey = [CUSTOMER_KEY] and VISIBLE_TIME_END <= '[APP_TIME]'
)""",
        parameters={"CUSTOMER_KEY": _CUSTOMER, "APP_TIME": _D(2010, 1, 1)},
        select=("c_custkey", "c_acctbal", "visible_time_begin"), approximate=True),

    BenchmarkCase(
        id="K6_sys", title="All versions of the tuples selected by value: system time",
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
sys_time_start
FROM
customer
FOR
SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
c_acctbal > [CUST_BAL]
ORDER BY
c_custkey, c_acctbal, sys_time_start""",
        # the report draws the balance from 0 to 5000; that is most of the 735 versions here, so a balance that leaves a few
        parameters={"APP_TIME": _CUSTOMER_APP, "CUST_BAL": 9990},
        select=("c_custkey", "c_acctbal", "sys_time_begin"), ordered=True, approximate=True),

    BenchmarkCase(
        id="K6_app", title="All versions of the tuples selected by value: application time",
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
visible_time_start
FROM
customer
FOR
BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE
c_acctbal > [CUST_BAL]
ORDER BY
c_custkey, c_acctbal, visible_time_start""",
        parameters={"CUST_BAL": 9990},
        select=("c_custkey", "c_acctbal", "visible_time_begin"), ordered=True, approximate=True),
]

# ---------------------------------------------------------------------------------------------------------------------
# B.5 Bitemporal queries: the other parts the suppliers of a part supply, with the application and system time of the two
# rows each point, correlated, past, current or agnostic. Each returns ps2.*; what is compared is the part it supplies.
# ---------------------------------------------------------------------------------------------------------------------

_OTHER_PART = ("ps_partkey",)


def _b5(case_id: str, title: str, sql: str, parameters: dict) -> BenchmarkCase:
    return BenchmarkCase(id=case_id, title=title, sql=sql, parameters={"PART_KEY": _PART, **parameters},
                         select=_OTHER_PART, distinct=True)


B5_DEFINITIONS = [
    _b5("B5_1", "App time current / system time current",
        """SELECT ps2.*
FROM partsupp FOR BUSINESS_TIME AS OF '[APP_TIME]' as ps1,
partsupp FOR BUSINESS_TIME AS OF '[APP_TIME]' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey""",
        {"APP_TIME": _D(1997, 6, 1)}),

    _b5("B5_2", "App time past / system time past",
        """SELECT ps2.*, ps1.ps_suppkey, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1.validity_time_end
FROM partsupp
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
FOR BUSINESS_TIME AS OF '[APP_TIME1]' as ps1,
partsupp
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
FOR BUSINESS_TIME AS OF '[APP_TIME2]' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey""",
        {"SYS_TIME": _SYS, "APP_TIME1": _D(1995, 6, 1), "APP_TIME2": _D(1997, 6, 1)}),

    _b5("B5_3", "App time correlation / system time current",
        """SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1. validity_time_end
FROM partsupp FOR BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30' as ps1,
partsupp FOR BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.VALIDITY_TIME_START <= ps2.VALIDITY_TIME_END AND
ps1.VALIDITY_TIME_END >= ps2.VALIDITY_TIME_START""",
        {}),

    _b5("B5_4", "App time current / system time correlation",
        """SELECT ps2.*, ps1.validity_time_start, ps1.validity_time_end
FROM partsupp FOR BUSINESS_TIME AS OF '[APP_TIME]'
FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as PS1,
partsupp FOR BUSINESS_TIME AS OF '[APP_TIME]'
FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as PS2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.sys_time_start < ps2.sys_time_END AND
ps2.sys_time_start < ps1.sys_time_END""",
        {"APP_TIME": _D(1997, 6, 1)}),

    # the report has no AND after the first condition
    _b5("B5_5", "App time correlation / system time correlation",
        """SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1. validity_time_end
FROM partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps1,
partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.sys_time_start < ps2.sys_time_END AND
ps2.sys_time_start < ps1.sys_time_END AND
ps1.VALIDITY_TIME_START <= ps2.VALIDITY_TIME_END AND
ps1.VALIDITY_TIME_END >= ps2.VALIDITY_TIME_START""",
        {}),

    _b5("B5_6", "App time correlation / system time correlation, as currently known",
        """SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1. validity_time_end
FROM partsupp as ps1, partsupp as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey""",
        {}),

    _b5("B5_7", "App time agnostic / system time past",
        """SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1.validity_time_end
FROM partsupp FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' as ps1,
partsupp FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' as ps2
WHERE ps1.ps_partkey = [PART_KEY]
AND ps1.ps_suppkey = ps2.ps_suppkey
AND ps1.ps_partkey <> ps2.ps_partkey""",
        {"SYS_TIME": _T(2002, 6, 1)}),

    _b5("B5_8", "App time agnostic / system time correlation",
        """SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1. validity_time_start, ps1. validity_time_end
FROM partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps1,
partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.sys_time_start < ps2.sys_time_END AND
ps2.sys_time_start < ps1.sys_time_END""",
        {}),

    _b5("B5_9", "App time current / system time agnostic",
        """SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1.validity_time_end
FROM partsupp
FOR BUSINESS_TIME AS OF '[APP_TIME]'
FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps1,
partsupp
FOR BUSINESS_TIME AS OF '[APP_TIME]'
FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.sys_time_start <= ps2.sys_time_START AND
ps2.sys_time_start < ps1.sys_time_END""",
        {"APP_TIME": _D(1997, 6, 1)}),

    _b5("B5_10", "App time correlation / system time agnostic",
        """SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1. validity_time_end
FROM partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps1,
partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.sys_time_start <= ps2.sys_time_START AND
pS2.sys_time_start < ps1.sys_time_END AND
ps1.VALIDITY_TIME_START <= ps2.VALIDITY_TIME_END AND
ps1.VALIDITY_TIME_END >= ps2.VALIDITY_TIME_START""",
        {}),

    _b5("B5_11", "App time agnostic / system time agnostic",
        """SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1. validity_time_end
FROM partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps1,
partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.sys_time_start <= ps2.sys_time_START AND
ps2.sys_time_start < ps1.sys_time_END""",
        {}),
]

DEFINITIONS = [*B1_DEFINITIONS, *B3_DEFINITIONS, *B5_DEFINITIONS]
