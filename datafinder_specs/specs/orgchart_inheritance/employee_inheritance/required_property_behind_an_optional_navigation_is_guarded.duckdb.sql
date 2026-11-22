SELECT t0.first_name AS "First Name" FROM hr.employees AS t0 LEFT OUTER JOIN hr.employees AS t1 ON (t1.emp_id = t0.manager_id) WHERE t1.first_name IS NOT NULL AND t1.first_name LIKE 'B%'
