SELECT t0.id AS "Id" FROM main.companies AS t0 WHERE t0.name <> 'Acme Corp' OR t0.name IS NULL ORDER BY t0.id ASC NULLS FIRST
