SELECT t0.code AS "Code",t0.amount AS "Amount" FROM ranged.rates AS t0 WHERE t0.from_z <= '2021-06-01' AND t0.thru_z > '2021-06-01' ORDER BY t0.code ASC NULLS FIRST
