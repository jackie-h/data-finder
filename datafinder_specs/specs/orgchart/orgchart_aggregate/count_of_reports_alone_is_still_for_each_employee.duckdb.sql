SELECT COUNT(t1.id) AS "Employees Id Count" FROM hr.employees AS t0 LEFT OUTER JOIN hr.employees AS t1 ON (t0.id = t1.manager_id) GROUP BY t0.id ORDER BY t0.id ASC NULLS FIRST
