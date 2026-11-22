SELECT t0.category AS "Category",COUNT(DISTINCT t0.name) = COUNT(t0.name) AS "all distinct" FROM main.companies AS t0 GROUP BY t0.category ORDER BY t0.category ASC NULLS FIRST
