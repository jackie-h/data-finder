SELECT t0.name AS "Name",CASE WHEN t0.category = 'Technology' THEN 'tech' ELSE 'other' END AS "Expression" FROM main.companies AS t0 ORDER BY t0.id ASC NULLS FIRST LIMIT 3
