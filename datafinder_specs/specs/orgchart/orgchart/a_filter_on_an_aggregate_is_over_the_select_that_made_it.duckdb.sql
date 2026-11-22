SELECT t0.total AS "total" FROM (SELECT SUM(t1.id) AS "total" FROM hr.employees AS t1) AS t0 WHERE t0.total > 5
