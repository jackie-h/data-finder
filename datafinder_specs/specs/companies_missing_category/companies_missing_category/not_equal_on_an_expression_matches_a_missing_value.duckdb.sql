SELECT t0.id AS "Id" FROM main.companies AS t0 WHERE LENGTH(t0.category) <> 7 OR LENGTH(t0.category) IS NULL ORDER BY t0.id ASC NULLS FIRST
