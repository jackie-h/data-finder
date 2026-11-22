SELECT t0.gsn AS "Gsn",t0.quantity AS "Quantity" FROM (SELECT t1.gsn AS "gsn",SUM(t1.qty) AS "quantity" FROM org.trades AS t1 GROUP BY t1.gsn) AS t0 ORDER BY t0.gsn ASC NULLS FIRST
