SELECT t0.name AS "Name" FROM main.companies AS t0 WHERE CASE WHEN t0.category = 'Technology' THEN TRUE ELSE FALSE END ORDER BY t0.id ASC NULLS FIRST
