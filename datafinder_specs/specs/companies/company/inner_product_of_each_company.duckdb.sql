SELECT t0.id AS "Id",ROUND(INNER_PRODUCT(t0.embedding, CAST(ARRAY[1.0, 0.2, 0.0] AS VECTOR(3))), 4) AS "Round Inner Product Embedding" FROM main.companies AS t0 ORDER BY t0.id ASC NULLS FIRST
