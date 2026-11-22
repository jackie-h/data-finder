SELECT t0.id < 3 AS "small",SUM(t0.id) AS "total" FROM hr.employees AS t0 WHERE t0.name <> 'Dave' OR t0.name IS NULL GROUP BY t0.id < 3 ORDER BY "small" ASC NULLS FIRST
