SELECT t0.first_name AS "First Name" FROM hr.employees AS t0 WHERE t0.department IS NOT NULL AND t0.department LIKE 'Eng%' ORDER BY t0.first_name ASC NULLS FIRST
