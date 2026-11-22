SELECT t0.category AS "Category",SUM(t0.id) AS "total" FROM main.companies AS t0 GROUP BY t0.category HAVING t0.category <> 'Finance' OR t0.category IS NULL ORDER BY SUM(t0.id) ASC NULLS FIRST
