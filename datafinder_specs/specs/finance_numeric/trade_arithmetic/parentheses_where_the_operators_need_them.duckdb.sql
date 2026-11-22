SELECT (t0.price + 1) * 2 AS "Expression" FROM trading.trades AS t0 WHERE t0.sym = 'GOOG' AND t0.in_z <= '2030-01-01 00:00:00' AND (t0.out_z > '2030-01-01 00:00:00' OR t0.out_z IS NULL)
