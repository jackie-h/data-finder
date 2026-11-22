SELECT t0.name AS "employeeName",LENGTH(t0.name) AS "size" FROM hr.employees AS t0 WHERE LENGTH(t0.name) > 4 AND (t0.name <> 'Alice' OR t0.name IS NULL) ORDER BY t0.name ASC NULLS FIRST
