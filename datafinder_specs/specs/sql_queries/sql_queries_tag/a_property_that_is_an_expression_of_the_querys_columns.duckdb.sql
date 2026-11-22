SELECT t0.person_id AS "Id",t0.text AS "Text" FROM (SELECT t1.person_id AS "person_id",'Mr ' || t1.first_name AS "text" FROM org.people AS t1) AS t0 ORDER BY t0.person_id ASC NULLS FIRST
