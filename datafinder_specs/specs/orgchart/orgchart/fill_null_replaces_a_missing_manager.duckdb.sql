SELECT t0.name AS "Name",COALESCE(t1.name, 'nobody') AS "Fill Null Manager Name" FROM hr.employees AS t0 LEFT OUTER JOIN hr.employees AS t1 ON (t1.id = t0.manager_id) ORDER BY t0.id ASC NULLS FIRST
