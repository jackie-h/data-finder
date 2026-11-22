SELECT t0.id AS "Id",t1.name AS "Manager Name" FROM org.staff AS t0 LEFT OUTER JOIN org.staff AS t1 ON (t0.manager_id = t1.id AND t0.active = 1 AND t1.active = 1) ORDER BY t0.id ASC NULLS FIRST
