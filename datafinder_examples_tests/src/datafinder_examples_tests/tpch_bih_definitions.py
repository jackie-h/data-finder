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

# ---------------------------------------------------------------------------------------------------------------------
# B.2 The TPC-H queries as DuckDB's tpch extension has them, with the TPC-H qualification parameters except where a value is chosen
# for the result not to be empty (written as a [PLACEHOLDER] with its value in the parameters). Each is asked as of a point in each time
# dimension: every temporal table is read FOR SYSTEM_TIME AS OF [SYS_TIME] and, if it has one, FOR BUSINESS_TIME AS OF [APP_TIME].
# The data is small (scale factor 0.001), so the application time of each is a day at which its rows are active; Q2, Q17 and Q19
# return nothing at the TPC-H parameters. Q1 is H.1.
# ---------------------------------------------------------------------------------------------------------------------

TPCH_DEFINITIONS = [
    BenchmarkCase(
        id="Q2", title="Minimum cost supplier",
        sql="""SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM part FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', supplier FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]', partsupp FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]',
    nation,
    region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT
            min(ps_supplycost)
        FROM partsupp FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', supplier FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]',
            nation,
            region
        WHERE
            p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE')
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1996, 1, 1)},
        select=('s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment'), ordered=True),

    BenchmarkCase(
        id="Q3", title="Shipping priority",
        sql="""SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM customer FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', orders FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
    c_mktsegment = '[SEGMENT]'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('[DATE]' AS date)
    AND l_shipdate > CAST('[DATE]' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1995, 4, 1), 'SEGMENT': 'BUILDING', 'DATE': datetime.date(1995, 3, 15)},
        select=('l_orderkey', 'revenue', 'o_orderdate', 'o_shippriority'), ordered=True, approximate=True),

    BenchmarkCase(
        id="Q4", title="Order priority checking",
        sql="""SELECT
    o_orderpriority,
    count(*) AS order_count
FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
    o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)
    AND EXISTS (
        SELECT
            *
        FROM lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate)
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1993, 10, 1)},
        select=('o_orderpriority', 'order_count'), ordered=True),

    BenchmarkCase(
        id="Q5", title="Local supplier volume",
        sql="""SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', orders FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', supplier FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]',
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = '[REGION]'
    AND o_orderdate >= CAST('[DATE_FROM]' AS date)
    AND o_orderdate < CAST('[DATE_TO]' AS date)
GROUP BY
    n_name
ORDER BY
    revenue DESC""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1994, 7, 1), 'REGION': 'AMERICA', 'DATE_FROM': datetime.date(1994, 1, 1), 'DATE_TO': datetime.date(1995, 1, 1)},
        select=('n_name', 'revenue'), ordered=True, approximate=True),

    BenchmarkCase(
        id="Q6", title="Forecasting revenue change",
        sql="""SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
    l_shipdate >= CAST('[DATE_FROM]' AS date)
    AND l_shipdate < CAST('[DATE_TO]' AS date)
    AND l_discount BETWEEN [DISCOUNT_LOW]
    AND [DISCOUNT_HIGH]
    AND l_quantity < [QUANTITY]""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1994, 1, 1), 'DATE_FROM': datetime.date(1994, 1, 1), 'DATE_TO': datetime.date(1995, 1, 1), 'DISCOUNT_LOW': 0.05, 'DISCOUNT_HIGH': 0.07, 'QUANTITY': 24},
        select=('revenue',), approximate=True),

    BenchmarkCase(
        id="Q7", title="Volume shipping",
        sql="""SELECT
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) AS revenue
FROM (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        extract(year FROM l_shipdate) AS l_year,
        l_extendedprice * (1 - l_discount) AS volume
    FROM supplier FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]', lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', orders FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', customer FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]',
        nation n1,
        nation n2
    WHERE
        s_suppkey = l_suppkey
        AND o_orderkey = l_orderkey
        AND c_custkey = o_custkey
        AND s_nationkey = n1.n_nationkey
        AND c_nationkey = n2.n_nationkey
        AND ((n1.n_name = '[NATION1]'
                AND n2.n_name = '[NATION2]')
            OR (n1.n_name = '[NATION2]'
                AND n2.n_name = '[NATION1]'))
        AND l_shipdate BETWEEN CAST('[DATE_FROM]' AS date)
        AND CAST('[DATE_TO]' AS date)) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1995, 10, 1), 'NATION1': 'ARGENTINA', 'NATION2': 'PERU', 'DATE_FROM': datetime.date(1995, 1, 1), 'DATE_TO': datetime.date(1996, 12, 31)},
        select=('supp_nation', 'cust_nation', 'l_year', 'revenue'), ordered=True, approximate=True),

    BenchmarkCase(
        id="Q8", title="National market share",
        sql="""SELECT
    o_year,
    sum(
        CASE WHEN nation = '[NATION]' THEN
            volume
        ELSE
            0
        END) / sum(volume) AS mkt_share
FROM (
    SELECT
        extract(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) AS volume,
        n2.n_name AS nation
    FROM part FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', supplier FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]', lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', orders FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', customer FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]',
        nation n1,
        nation n2,
        region
    WHERE
        p_partkey = l_partkey
        AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r_regionkey
        AND r_name = '[REGION]'
        AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN CAST('[DATE_FROM]' AS date)
        AND CAST('[DATE_TO]' AS date)
        AND p_type = '[TYPE]') AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1997, 4, 1), 'NATION': 'BRAZIL', 'REGION': 'AMERICA', 'DATE_FROM': datetime.date(1995, 1, 1), 'DATE_TO': datetime.date(1996, 12, 31), 'TYPE': 'ECONOMY ANODIZED STEEL'},
        select=('o_year', 'mkt_share'), ordered=True, approximate=True),

    BenchmarkCase(
        id="Q9", title="Product type profit measure",
        sql="""SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        extract(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM part FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', supplier FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]', lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', partsupp FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', orders FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]',
        nation
    WHERE
        s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%') AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(2009, 7, 1)},
        select=('nation', 'o_year', 'sum_profit'), ordered=True, approximate=True),

    BenchmarkCase(
        id="Q10", title="Returned item reporting",
        sql="""SELECT
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM customer FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', orders FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]',
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= CAST('[DATE_FROM]' AS date)
    AND o_orderdate < CAST('[DATE_TO]' AS date)
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1994, 1, 1), 'DATE_FROM': datetime.date(1993, 10, 1), 'DATE_TO': datetime.date(1994, 1, 1)},
        select=('c_custkey', 'c_name', 'revenue', 'c_acctbal', 'n_name', 'c_address', 'c_phone', 'c_comment'), ordered=True, approximate=True),

    BenchmarkCase(
        id="Q11", title="Important stock identification",
        sql="""SELECT
    ps_partkey,
    sum(ps_supplycost * ps_availqty) AS value
FROM partsupp FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', supplier FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]',
    nation
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = '[NATION]'
GROUP BY
    ps_partkey
HAVING
    sum(ps_supplycost * ps_availqty) > (
        SELECT
            sum(ps_supplycost * ps_availqty) * [FRACTION]
        FROM partsupp FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', supplier FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]',
            nation
        WHERE
            ps_suppkey = s_suppkey
            AND s_nationkey = n_nationkey
            AND n_name = '[NATION]')
ORDER BY
    value DESC""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1992, 4, 1), 'NATION': 'IRAN', 'FRACTION': 0.1},
        select=('ps_partkey', 'value'), ordered=True, approximate=True),

    BenchmarkCase(
        id="Q12", title="Shipping modes and order priority",
        sql="""SELECT
    l_shipmode,
    sum(
        CASE WHEN o_orderpriority = '1-URGENT'
            OR o_orderpriority = '2-HIGH' THEN
            1
        ELSE
            0
        END) AS high_line_count,
    sum(
        CASE WHEN o_orderpriority <> '1-URGENT'
            AND o_orderpriority <> '2-HIGH' THEN
            1
        ELSE
            0
        END) AS low_line_count
FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= CAST('[DATE_FROM]' AS date)
    AND l_receiptdate < CAST('[DATE_TO]' AS date)
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1994, 4, 1), 'DATE_FROM': datetime.date(1994, 1, 1), 'DATE_TO': datetime.date(1995, 1, 1)},
        select=('l_shipmode', 'high_line_count', 'low_line_count'), ordered=True),

    BenchmarkCase(
        id="Q13", title="Customer distribution",
        sql="""SELECT
    c_count,
    count(*) AS custdist
FROM (
    SELECT
        c_custkey,
        count(o_orderkey)
    FROM customer FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
    LEFT OUTER JOIN orders FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]' ON c_custkey = o_custkey
    AND o_comment NOT LIKE '%special%requests%'
GROUP BY
    c_custkey) AS c_orders (c_custkey,
        c_count)
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1995, 7, 1)},
        select=('c_count', 'custdist'), ordered=True),

    BenchmarkCase(
        id="Q14", title="Promotion effect",
        sql="""SELECT
    100.00 * sum(
        CASE WHEN p_type LIKE 'PROMO%' THEN
            l_extendedprice * (1 - l_discount)
        ELSE
            0
        END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', part FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= date '[DATE_FROM]'
    AND l_shipdate < CAST('[DATE_TO]' AS date)""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1995, 7, 1), 'DATE_FROM': datetime.date(1995, 9, 1), 'DATE_TO': datetime.date(1995, 10, 1)},
        select=('promo_revenue',), approximate=True),

    BenchmarkCase(
        id="Q15", title="Top supplier",
        sql="""SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM supplier FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]',
    (
        SELECT
            l_suppkey AS supplier_no,
            sum(l_extendedprice * (1 - l_discount)) AS total_revenue
        FROM lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
        WHERE
            l_shipdate >= CAST('1996-01-01' AS date)
            AND l_shipdate < CAST('1996-04-01' AS date)
        GROUP BY
            supplier_no) revenue0
WHERE
    s_suppkey = supplier_no
    AND total_revenue = (
        SELECT
            max(total_revenue)
        FROM (
            SELECT
                l_suppkey AS supplier_no,
                sum(l_extendedprice * (1 - l_discount)) AS total_revenue
            FROM lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
            WHERE
                l_shipdate >= CAST('1996-01-01' AS date)
                AND l_shipdate < CAST('1996-04-01' AS date)
            GROUP BY
                supplier_no) revenue1)
ORDER BY
    s_suppkey""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1996, 1, 1)},
        select=('s_suppkey', 's_name', 's_address', 's_phone', 'total_revenue'), ordered=True, approximate=True),

    BenchmarkCase(
        id="Q16", title="Parts/supplier relationship",
        sql="""SELECT
    p_brand,
    p_type,
    p_size,
    count(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', part FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
    p_partkey = ps_partkey
    AND p_brand <> 'Brand#45'
    AND p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps_suppkey NOT IN (
        SELECT
            s_suppkey
        FROM supplier FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
        WHERE
            s_comment LIKE '%Customer%Complaints%')
GROUP BY
    p_brand,
    p_type,
    p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(2007, 7, 1)},
        select=('p_brand', 'p_type', 'p_size', 'supplier_cnt'), ordered=True),

    BenchmarkCase(
        id="Q17", title="Small-quantity-order revenue",
        sql="""SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', part FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
        WHERE
            l_partkey = p_partkey)""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1996, 1, 1)},
        select=('avg_yearly',), ),

    BenchmarkCase(
        id="Q18", title="Large volume customer",
        sql="""SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
FROM customer FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', orders FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
    o_orderkey IN (
        SELECT
            l_orderkey
        FROM lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
        GROUP BY
            l_orderkey
        HAVING
            sum(l_quantity) > [QUANTITY])
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(2009, 1, 1), 'QUANTITY': 250},
        select=('c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice', 'sum(l_quantity)'), ordered=True, approximate=True),

    BenchmarkCase(
        id="Q19", title="Discounted revenue",
        sql="""SELECT
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]', part FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
WHERE (p_partkey = l_partkey
    AND p_brand = 'Brand#12'
    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND l_quantity >= 1
    AND l_quantity <= 1 + 10
    AND p_size BETWEEN 1 AND 5
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10
        AND l_quantity <= 10 + 10
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20
        AND l_quantity <= 20 + 10
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1995, 9, 1)},
        select=('revenue',), ),

    BenchmarkCase(
        id="Q20", title="Potential part promotion",
        sql="""SELECT
    s_name,
    s_address
FROM supplier FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]',
    nation
WHERE
    s_suppkey IN (
        SELECT
            ps_suppkey
        FROM partsupp FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
        WHERE
            ps_partkey IN (
                SELECT
                    p_partkey
                FROM part FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
                WHERE
                    p_name LIKE 'forest%')
                AND ps_availqty > (
                    SELECT
                        0.5 * sum(l_quantity)
                    FROM lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
                    WHERE
                        l_partkey = ps_partkey
                        AND l_suppkey = ps_suppkey
                        AND l_shipdate >= CAST('1994-01-01' AS date)
                        AND l_shipdate < CAST('1995-01-01' AS date)))
            AND s_nationkey = n_nationkey
            AND n_name = '[NATION]'
        ORDER BY
            s_name""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1994, 7, 1), 'NATION': 'ETHIOPIA'},
        select=('s_name', 's_address'), ordered=True),

    BenchmarkCase(
        id="Q21", title="Suppliers who kept orders waiting",
        sql="""SELECT
    s_name,
    count(*) AS numwait
FROM supplier FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]', lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]' l1, orders FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]',
    nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT
            *
        FROM lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]' l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey)
    AND NOT EXISTS (
        SELECT
            *
        FROM lineitem FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]' l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate)
    AND s_nationkey = n_nationkey
    AND n_name = '[NATION]'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1992, 7, 1), 'NATION': 'PERU'},
        select=('s_name', 'numwait'), ordered=True),

    BenchmarkCase(
        id="Q22", title="Global sales opportunity",
        sql="""SELECT
    cntrycode,
    count(*) AS numcust,
    sum(c_acctbal) AS totacctbal
FROM (
    SELECT
        substring(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal
    FROM customer FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
    WHERE
        substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > (
            SELECT
                avg(c_acctbal)
            FROM customer FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
            WHERE
                c_acctbal > 0.00
                AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17'))
            AND NOT EXISTS (
                SELECT
                    *
                FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' FOR BUSINESS_TIME AS OF '[APP_TIME]'
                WHERE
                    o_custkey = c_custkey)) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1994, 7, 1)},
        select=('cntrycode', 'numcust', 'totacctbal'), ordered=True, approximate=True),
]

DEFINITIONS = [*B1_DEFINITIONS, *B2_DEFINITIONS, *B3_DEFINITIONS, *B4_DEFINITIONS, *B5_DEFINITIONS, *TPCH_DEFINITIONS]
