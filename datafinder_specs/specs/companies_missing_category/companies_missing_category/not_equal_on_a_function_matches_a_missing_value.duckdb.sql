SELECT t0.id AS "Id" FROM main.companies AS t0 WHERE UPPER(t0.category) <> 'FINANCE' OR UPPER(t0.category) IS NULL ORDER BY t0.id ASC NULLS FIRST
