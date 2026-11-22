SELECT t0.category AS "Category",COUNT(*) AS "Count" FROM main.companies AS t0 GROUP BY t0.category ORDER BY t0.category ASC NULLS FIRST
