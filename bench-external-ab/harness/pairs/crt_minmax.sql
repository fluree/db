-- crt_minmax : MIN/MAX over a fact measure. Answerable from Iceberg column stats
-- without a full scan (a "DuckDB wins now, Fluree should win later from stats" case).
-- MIN/MAX pick actual values -> deterministic, exact numeric compare.
SELECT MIN(order_total) AS mn, MAX(order_total) AS mx FROM {{fact_order}};
