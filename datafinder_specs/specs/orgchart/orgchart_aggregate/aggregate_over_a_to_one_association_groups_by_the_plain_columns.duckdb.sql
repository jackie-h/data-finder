SELECT t1.name AS "Manager Name",COUNT(t0.id) AS "Id Count" FROM hr.employees AS t0 LEFT OUTER JOIN hr.employees AS t1 ON (t1.id = t0.manager_id) GROUP BY t1.name ORDER BY t1.name ASC NULLS FIRST
