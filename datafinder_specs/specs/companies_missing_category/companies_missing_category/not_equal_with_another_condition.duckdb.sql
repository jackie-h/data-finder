SELECT t0.id AS "Id" FROM main.companies AS t0 WHERE (t0.category <> 'Finance' OR t0.category IS NULL) AND t0.id < 3 ORDER BY t0.id ASC NULLS FIRST
