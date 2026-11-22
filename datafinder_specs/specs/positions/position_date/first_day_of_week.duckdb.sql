SELECT CAST(DATE_TRUNC('week', t0.trade_date) AS DATE) AS "First Day Of Week Trade Date" FROM main.positions AS t0 WHERE t0.id = 1
