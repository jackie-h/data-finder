"""The TPC-BiH queries of Appendix B (B.1 time travel, B.2 H.1, B.3 key history, B.4 range, B.5 bitemporal) as the report writes them, with the
values of their parameters. tools/build_tpch_bih_cases.py runs each on the generated data and writes the rows to
tpch_bih_expected.py, so the SQL here, in one place, is the source of the expected rows.

The SQL is the report's text. Where it has a typo (B.5.5 has no AND after the first condition) the typo is fixed and noted.
The report draws parameters at random from a range; a value here is one in that range, chosen so the result is not empty.
The generated data is small (scale factor 0.001), so a few values are chosen to keep a result short enough to read:
the balance in K.6.
"""
import datetime

from datafinder_examples_tests.tpch_bih_case import BenchmarkCase
from datafinder_examples_tests.tpch_bih_data import CURRENT_SYS

_D = datetime.date
_T = datetime.datetime

#: a point in application time and one in system time that every table has rows at, and the other points the queries take
_APP = _D(1996, 6, 1)
_SYS = _T(2005, 6, 1)

#: a customer with three versions in application time (it moved twice) and 22 in system time, and an application time at which
#: it has several system versions
_CUSTOMER = 136
_CUSTOMER_APP = _D(2003, 1, 1)
#: the order with the most versions
_ORDER = 6052
#: a part with one supplier, which supplies 69 other parts: with the periods of the rows compared (below) the eleven B.5 queries
#: return six different results for it
_PART = 85

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

    BenchmarkCase(
        id="T3_sys", title="Aggregation at two defined points in time: system time",
        sql="""SELECT agg_t1.revenue old_revenue, agg_t2.revenue new_revenue
FROM
(
SELECT SUM(o_totalprice) as revenue
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME1]'
FOR BUSINESS_TIME AS OF TIMESTAMP '[APP_TIME]'
WHERE o_orderstatus = 'O'
) agg_t1
,
(
SELECT SUM(o_totalprice) as revenue
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME2]'
FOR BUSINESS_TIME AS OF TIMESTAMP '[APP_TIME]'
WHERE o_orderstatus = 'O'
) agg_t2""",
        parameters={"SYS_TIME1": _T(2003, 1, 1), "SYS_TIME2": _T(2007, 1, 1), "APP_TIME": _APP},
        select=("old_revenue", "new_revenue"), approximate=True),

    BenchmarkCase(
        id="T3_app", title="Aggregation at two defined points in time: application time",
        sql="""SELECT agg_t1.revenue old_revenue, agg_t2.revenue new_revenue
FROM
(
SELECT SUM(o_totalprice) as revenue
FROM orders
FOR BUSINESS_TIME AS OF TIMESTAMP '[APP_TIME1]'
WHERE o_orderstatus = 'O'
) agg_t1
,
(
SELECT SUM(o_totalprice) as revenue
FROM orders
FOR BUSINESS_TIME AS OF TIMESTAMP '[APP_TIME2]'
WHERE o_orderstatus = 'O'
) agg_t2""",
        parameters={"APP_TIME1": _D(1995, 6, 1), "APP_TIME2": _D(1997, 6, 1)},
        select=("old_revenue", "new_revenue"), approximate=True),

    BenchmarkCase(
        id="T4_sys", title="Aggregation of a limited number of items at two points in time: system time",
        sql="""SELECT revenue2 - revenue1 AS inc_revenue, agg_t1.o_custkey
FROM
(
SELECT SUM(o_totalprice) as revenue1, o_custkey
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME1]'
FOR BUSINESS_TIME AS OF TIMESTAMP '[APP_TIME]'
WHERE o_orderstatus = 'O'
GROUP BY o_custkey
) agg_t1
,
(
SELECT SUM(o_totalprice) as revenue2, o_custkey
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME2]'
FOR BUSINESS_TIME AS OF TIMESTAMP '[APP_TIME]'
WHERE o_orderstatus = 'F'
GROUP BY o_custkey
) agg_t2
WHERE agg_t1.o_custkey = agg_t2.o_custkey
ORDER BY inc_revenue DESC
FETCH FIRST 10 ROWS ONLY""",
        # a point at which an order is open as of the first system time and has been filled as of the second
        parameters={"SYS_TIME1": _T(2006, 1, 1), "SYS_TIME2": _T(2009, 1, 1), "APP_TIME": _D(2007, 10, 1)},
        select=("inc_revenue", "o_custkey"), ordered=True, approximate=True),

    BenchmarkCase(
        id="T4_app", title="Aggregation of a limited number of items at two points in time: application time",
        sql="""SELECT revenue2 - revenue1 AS inc_revenue, agg_t1.o_custkey
FROM
(
SELECT SUM(o_totalprice) as revenue1, o_custkey
FROM orders
FOR BUSINESS_TIME AS OF TIMESTAMP '[APP_TIME1]'
WHERE o_orderstatus = 'O'
GROUP BY o_custkey
) agg_t1
,
(
SELECT SUM(o_totalprice) as revenue2, o_custkey
FROM orders
FOR BUSINESS_TIME AS OF TIMESTAMP '[APP_TIME2]'
WHERE o_orderstatus = 'O'
GROUP BY o_custkey
) agg_t2
WHERE agg_t1.o_custkey = agg_t2.o_custkey
ORDER BY inc_revenue DESC
FETCH FIRST 10 ROWS ONLY""",
        parameters={"APP_TIME1": _D(1995, 6, 1), "APP_TIME2": _D(1997, 6, 1)},
        select=("inc_revenue", "o_custkey"), ordered=True, approximate=True),
]

# ---------------------------------------------------------------------------------------------------------------------
# B.2 Time travel for application queries: the TPC-H queries asked as of a point in each dimension
# ---------------------------------------------------------------------------------------------------------------------

B2_DEFINITIONS = [
    # the report writes lineitem twice, once with each clause; it is one table reference with both
    BenchmarkCase(
        id="H1", title="Pricing summary report",
        sql="""SELECT
l_returnflag,
l_linestatus,
SUM(l_quantity) AS sum_qty,
SUM(l_extendedprice) AS sum_base_price,
SUM(l_extendedprice*(1-l_discount)) AS sum_disc_price,
SUM(l_extendedprice*(1-l_discount)*(1+l_tax)) AS sum_charge,
AVG(l_quantity) AS avg_qty,
AVG(l_extendedprice) AS avg_price,
AVG(l_discount) AS avg_disc,
COUNT(*) AS count_order
FROM
lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
FOR BUSINESS_TIME AS OF TIMESTAMP '[APP_TIME]'
WHERE
l_shipdate <= DATE '1998-11-01'
GROUP BY
l_returnflag,
l_linestatus
ORDER BY
l_returnflag,
l_linestatus""",
        # the report takes the current version of each dimension; no line is active after 1999 here, so a day inside the data
        parameters={"SYS_TIME": CURRENT_SYS, "APP_TIME": _D(1998, 1, 1)},
        select=("l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "avg_qty",
                "avg_price", "avg_disc", "count_order"), ordered=True, approximate=True),
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
                    "SYS_TIME_BEGIN": _T(2003, 1, 1), "SYS_TIME_END": _T(2008, 1, 1)},
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
                    "SYS_TIME_BEGIN": _T(2003, 1, 1), "SYS_TIME_END": _T(2008, 1, 1)},
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
        parameters={"CUSTOMER_KEY": _CUSTOMER, "APP_TIME": _D(2002, 7, 7)},
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
# B.4 Range-timeslice queries
# ---------------------------------------------------------------------------------------------------------------------

_SYS_RANGE = {"SYS_TIME_BEGIN": _T(2003, 1, 1), "SYS_TIME_END": _T(2006, 1, 1)}

B4_DEFINITIONS = [
    # the report's nation is "US", which is TPC-H's UNITED STATES, and its ${NATION} is the nation table
    BenchmarkCase(
        id="R1_sys", title="Restrict interval for state transition: system time",
        sql="""SELECT now.C_NAME
FROM (
SELECT MAX(sys_time_end) time_until, c_custkey, c_name, c_nationkey
FROM customer
FOR SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE c_nationkey != (SELECT n_nationkey FROM nation WHERE n_name = '[NATION]')
group by c_custkey, c_name, c_nationkey
) prv, (
SELECT MIN(sys_time_start) time_from, c_custkey, c_name, c_nationkey
FROM customer
FOR SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE c_nationkey = (SELECT n_nationkey FROM nation WHERE n_name = '[NATION]')
GROUP BY c_custkey, c_name, c_nationkey
) now
WHERE
prv.c_custkey = now.c_custkey AND
prv.time_until < '[SYS_TIME_BEGIN]' AND
now.time_from BETWEEN '[SYS_TIME_BEGIN]' AND '[SYS_TIME_END]'""",
        parameters={"NATION": "UNITED STATES", **_SYS_RANGE}, select=("c_name",)),

    BenchmarkCase(
        id="R1_app", title="Restrict interval for state transition: application time",
        sql="""SELECT now.C_NAME
FROM (
SELECT MAX(visible_time_end) time_until, c_custkey, c_name, c_nationkey
FROM customer
FOR BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE c_nationkey != (SELECT n_nationkey FROM nation WHERE n_name = '[NATION]')
group by c_custkey, c_name, c_nationkey
) prv, (
SELECT MIN(visible_time_start) time_from, c_custkey, c_name, c_nationkey
FROM customer
FOR BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE c_nationkey = (SELECT n_nationkey FROM nation WHERE n_name = '[NATION]')
GROUP BY c_custkey, c_name, c_nationkey
) now
WHERE
prv.c_custkey = now.c_custkey AND
prv.time_until < '[SYS_TIME_BEGIN]' AND
now.time_from BETWEEN '[SYS_TIME_BEGIN]' AND '[SYS_TIME_END]'""",
        parameters={"NATION": "UNITED STATES", **_SYS_RANGE}, select=("c_name",)),

    # DuckDB has no AVG of an interval, so the difference of the two timestamps is taken in seconds
    BenchmarkCase(
        id="R2_sys", title="Average duration of a certain state: system time",
        sql="""SELECT AVG(EPOCH(shp.ship_time - ord.order_time))
FROM
(
SELECT MIN(sys_time_start) AS order_time, o_orderkey
FROM orders
FOR SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE o_orderstatus = 'O'
GROUP BY o_orderkey
) ord
,
(
SELECT MIN(sys_time_start) AS ship_time, o_orderkey
FROM orders
FOR SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE o_orderstatus = 'F'
GROUP BY o_orderkey
) shp
WHERE ord.o_orderkey = shp.o_orderkey""",
        parameters={}, select=(0,), approximate=True),

    BenchmarkCase(
        id="R2_app", title="Average duration of a certain state: application time",
        sql="""SELECT AVG(shp.ship_time - ord.order_time)
FROM
(
SELECT MIN(active_time_start) AS order_time, o_orderkey
FROM orders
FOR BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE o_orderstatus = 'O'
GROUP BY o_orderkey
) ord
,
(
SELECT MIN(active_time_start) AS ship_time, o_orderkey
FROM orders
FOR BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE o_orderstatus = 'F'
GROUP BY o_orderkey
) shp
WHERE ord.o_orderkey = shp.o_orderkey""",
        parameters={}, select=(0,), approximate=True),

    BenchmarkCase(
        id="R3a", title="Time-slider: time of the maximum number of unshipped items",
        sql="""SELECT MAX(total)
FROM
(
SELECT COUNT(*) AS total, times.event_time as timestmp
FROM
(
SELECT * FROM orders
FOR SYSTEM_TIME FROM '[SYS_TIME_BEGIN]' TO '[SYS_TIME_END]'
) ord,
(
SELECT sys_time_start AS event_time FROM orders
FOR SYSTEM_TIME FROM '[SYS_TIME_BEGIN]' TO '[SYS_TIME_END]'
UNION
SELECT sys_time_end AS event_time FROM orders
FOR SYSTEM_TIME FROM '[SYS_TIME_BEGIN]' TO '[SYS_TIME_END]'
) times
WHERE ord.sys_time_start <= times.event_time AND
times.event_time < ord.sys_time_end
GROUP BY times.event_time
) agg""",
        parameters=_SYS_RANGE, select=(0,)),

    BenchmarkCase(
        id="R3b", title="Time-slider: value of the maximum number of unshipped items",
        sql="""SELECT MAX(total)
FROM
(
SELECT SUM(ord.o_totalprice) AS total, times.event_time as timestmp
FROM
(
SELECT * FROM orders
FOR SYSTEM_TIME FROM '[SYS_TIME_BEGIN]' TO '[SYS_TIME_END]'
WHERE o_orderstatus = 'O'
) ord,
(
SELECT sys_time_start AS event_time FROM orders
FOR SYSTEM_TIME FROM '[SYS_TIME_BEGIN]' TO '[SYS_TIME_END]'
UNION
SELECT sys_time_end AS event_time FROM orders
FOR SYSTEM_TIME FROM '[SYS_TIME_BEGIN]' TO '[SYS_TIME_END]'
) times
WHERE ord.sys_time_start <= times.event_time AND
times.event_time < ord.sys_time_end
GROUP BY times.event_time
) agg""",
        parameters=_SYS_RANGE, select=(0,), approximate=True),

    # the report selects only glob.ps_partkey in the innermost query but groups by ps_suppkey, and it has no ORDER BY with FETCH
    # FIRST, so which 10 are first is not defined: glob.ps_suppkey is selected and the result ordered. glob is quoted, as sqlglot
    # reads an unquoted one as the GLOB operator
    BenchmarkCase(
        id="R4", title="Comparison of two extreme points of aggregation",
        sql="""SELECT MIN(diff), ps_partkey, ps_suppkey
FROM
(
-- Select positive value
SELECT (CASE WHEN (diff1 > diff2) THEN diff1 ELSE diff2 END) AS diff,
ps_partkey, ps_suppkey
FROM
(
SELECT (max_time_from - min_time_to) AS diff1,
(min_time_from - max_time_to) AS diff2, ps_partkey, ps_suppkey
FROM
(
-- Do not know if minimum occurs before maximum or vice versa
SELECT min_time_from, min_time_to, max_time_from, max_time_to,
smin.ps_availqty, smax.ps_availqty, "glob".ps_partkey, "glob".ps_suppkey
FROM
(
-- All cases where stock is minimal
SELECT sys_time_start AS min_time_from,
sys_time_end AS min_time_to,
ps_availqty, ps_partkey, ps_suppkey
FROM partsupp
FOR SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
) smin, (
-- All cases where stock is maximal
SELECT sys_time_start as max_time_from,
sys_time_end AS max_time_to,
ps_availqty, ps_partkey, ps_suppkey
FROM partsupp
FOR SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
) smax, (
-- Get global minimum and maximum of this product in history
SELECT MIN(ps_availqty) AS gmin, MAX(ps_availqty) AS gmax,
ps_partkey, ps_suppkey
FROM partsupp
FOR SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
GROUP BY ps_partkey, ps_suppkey
) "glob"
WHERE
"glob".ps_partkey = smin.ps_partkey AND
"glob".ps_partkey = smax.ps_partkey AND
"glob".ps_suppkey = smin.ps_suppkey AND
"glob".ps_suppkey = smax.ps_suppkey AND
smin.ps_availqty = "glob".gmin AND
smax.ps_availqty = "glob".gmax AND
"glob".gmax != "glob".gmin
) minmax
) aggr
)
GROUP BY ps_partkey, ps_suppkey
ORDER BY MIN(diff), ps_partkey, ps_suppkey
FETCH FIRST [HSIZE] ROWS ONLY""",
        parameters={"HSIZE": 10}, select=(0, "ps_partkey", "ps_suppkey"), ordered=True),

    BenchmarkCase(
        id="R5", title="Temporal join query",
        sql="""SELECT COUNT(DISTINCT(o_orderkey))
FROM
(
SELECT * FROM customer
FOR SYSTEM_TIME FROM '[SYS_TIME_BEGIN]' TO '[SYS_TIME_END]'
WHERE c_acctbal < 5000
) cust,
(
SELECT * FROM orders
FOR SYSTEM_TIME FROM '[SYS_TIME_BEGIN]' TO '[SYS_TIME_END]'
WHERE o_orderstatus = 'O' AND o_totalprice > 10
) ord
WHERE
c_custkey = o_custkey
AND NOT (cust.sys_time_end <= ord.sys_time_start
OR ord.sys_time_end <= cust.sys_time_start)""",
        parameters=_SYS_RANGE, select=(0,)),

    BenchmarkCase(
        id="R6", title="Aggregation over a temporal join query",
        sql="""-- compute temporal join of customers and orders
CREATE VIEW high_risk_orders AS
SELECT
o_totalprice AS orderprice,
CASE WHEN cust.sys_time_start > ord.sys_time_start
THEN cust.sys_time_start ELSE ord.sys_time_start END
AS time_start,
CASE WHEN cust.sys_time_end < ord.sys_time_end
THEN cust.sys_time_end ELSE ord.sys_time_end END
AS time_end
FROM
(
SELECT * FROM customer
FOR SYSTEM_TIME FROM '[SYS_TIME_BEGIN]' TO '[SYS_TIME_END]'
WHERE c_acctbal < 100
) cust,
(
SELECT * FROM orders
FOR SYSTEM_TIME FROM '[SYS_TIME_BEGIN]' TO '[SYS_TIME_END]'
WHERE o_orderstatus = 'O'
) ord
WHERE
c_custkey = o_custkey
AND NOT (cust.sys_time_end <= ord.sys_time_start
OR ord.sys_time_end <= cust.sys_time_start);
-- compute temporal aggregation
SELECT MAX(total) AS aggr_result
FROM
(
SELECT SUM(ord.orderprice) AS total, times.event_time as timestmp
FROM
(
SELECT * FROM high_risk_orders
) ord,
(
SELECT time_start AS event_time
FROM high_risk_orders
UNION
SELECT time_end AS event_time
FROM high_risk_orders
) times
WHERE ord.time_start <= times.event_time AND
times.event_time < ord.time_end
GROUP BY times.event_time
) agg""",
        parameters=_SYS_RANGE, select=("aggr_result",), approximate=True),

    BenchmarkCase(
        id="R7", title="Changes between versions",
        sql="""SELECT ps1.ps_partkey, ps1.ps_suppkey, ps1.ps_supplycost,
ps1.validity_time_start, ps1.validity_time_end,
ps2.validity_time_start, ps2.validity_time_end,
ps2.ps_supplycost
FROM
partsupp
FOR BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30' AS PS1,
partsupp
FOR BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30' AS PS2
WHERE
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey=ps2.ps_partkey AND
ps1.validity_time_end > ps2.validity_time_end AND
ps2.validity_time_end = (SELECT max(validity_time_end)
FROM partsupp
FOR BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30' as ps3
WHERE
ps3.ps_suppkey = ps1.ps_suppkey AND
ps3.ps_partkey=ps1.ps_partkey AND
ps1.validity_time_end > ps3.validity_time_end) AND
ps1.ps_supplycost > ps2.ps_supplycost*1.075
ORDER BY
ps1.validity_time_end, ps2.validity_time_end DESC""",
        parameters={}, select=(0, 1, 2, 3, 4, 5, 6, 7), ordered=True, approximate=True),
]

# ---------------------------------------------------------------------------------------------------------------------
# B.5 Bitemporal queries: the other parts the suppliers of a part supply, with the application and system time of the two
# rows each point, correlated, past, current or agnostic. Each returns ps2.*; what is compared is the part it supplies and the
# application and system time of that row, which is what the correlation between the two rows changes.
# ---------------------------------------------------------------------------------------------------------------------

# the part and the periods of the other supply: ps2 is the first of the result's columns with each name, so a name finds it
_OTHER_SUPPLY = ("ps_partkey", "validity_time_begin", "validity_time_end", "sys_time_begin", "sys_time_end")


def _b5(case_id: str, title: str, sql: str, parameters: dict) -> BenchmarkCase:
    return BenchmarkCase(id=case_id, title=title, sql=sql, parameters={"PART_KEY": _PART, **parameters},
                         select=_OTHER_SUPPLY, distinct=True)


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

DEFINITIONS = [*B1_DEFINITIONS, *B2_DEFINITIONS, *B3_DEFINITIONS, *B4_DEFINITIONS, *B5_DEFINITIONS]
