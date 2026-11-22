SELECT t0.id AS "Id",ROUND(L2_DISTANCE(t0.embedding, CAST(ARRAY[1.0, 0.2, 0.0] AS VECTOR(3))), 4) AS "Round L2 Distance Embedding" FROM main.companies AS t0 ORDER BY t0.id ASC NULLS FIRST
