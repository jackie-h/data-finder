"""The TPC-H queries of B.2 as DuckDB's tpch extension has them, with the TPC-H qualification parameters except where a value is
chosen for the result not to be empty (written as a [PLACEHOLDER] with its value in the parameters).

B.2 asks each as of a point in each time dimension: every temporal table of a query is read FOR SYSTEM_TIME AS OF [SYS_TIME] and, if
it has one, FOR BUSINESS_TIME AS OF [APP_TIME]. The data is small (scale factor 0.001), so the application time of each is a day at
which its rows are active; Q2, Q17 and Q19 return nothing at the TPC-H parameters. Q1 is H.1, in tpch_bih_definitions.py.
"""
import datetime

from datafinder_examples_tests.tpch_bih_case import BenchmarkCase
from datafinder_examples_tests.tpch_bih_data import CURRENT_SYS

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
    AND p_size = [SIZE]
    AND p_type LIKE '[TYPE]'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = '[REGION]'
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
            AND r_name = '[REGION]')
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100""",
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(2005, 1, 1), 'SIZE': 31, 'TYPE': '%TIN', 'REGION': 'AFRICA'},
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
    o_orderdate >= CAST('[DATE_FROM]' AS date)
    AND o_orderdate < CAST('[DATE_TO]' AS date)
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
        parameters={'SYS_TIME': CURRENT_SYS, 'APP_TIME': datetime.date(1993, 10, 1), 'DATE_FROM': datetime.date(1993, 7, 1), 'DATE_TO': datetime.date(1993, 10, 1)},
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
