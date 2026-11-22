SELECT t0.category AS "Category",COUNT(t0.id) AS "Id Count" FROM main.companies AS t0 GROUP BY t0.category ORDER BY COUNT(t0.id) DESC NULLS LAST, t0.category ASC NULLS FIRST
