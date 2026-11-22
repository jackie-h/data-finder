SELECT t0.category AS "Category",STRING_AGG(t0.name, ', ' ORDER BY t0.name ASC NULLS FIRST) AS "Join Name" FROM main.companies AS t0 GROUP BY t0.category ORDER BY t0.category ASC NULLS FIRST
