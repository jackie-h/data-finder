SELECT t0.category AS "Category",BOOL_AND(t0.id > 0) AS "All",BOOL_OR(t0.id > 3) AS "Any" FROM main.companies AS t0 GROUP BY t0.category ORDER BY t0.category ASC NULLS FIRST
