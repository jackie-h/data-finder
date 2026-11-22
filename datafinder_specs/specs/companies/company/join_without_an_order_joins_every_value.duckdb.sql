SELECT t0.category AS "Category",STRING_AGG(t0.name, ',') AS "Join Name" FROM main.companies AS t0 WHERE t0.category = 'Manufacturing' GROUP BY t0.category
