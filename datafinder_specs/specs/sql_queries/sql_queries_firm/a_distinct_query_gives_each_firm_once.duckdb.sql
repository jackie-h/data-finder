SELECT t0.firm_id AS "Id",t0.firm_name AS "LegalName" FROM (SELECT DISTINCT t1.firm_id AS "firm_id",t1.firm_name AS "firm_name" FROM org.people AS t1) AS t0 ORDER BY t0.firm_id ASC NULLS FIRST
