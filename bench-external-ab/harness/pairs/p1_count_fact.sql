-- p1_count_fact : COUNT(*) over the largest single fact table (no join).
-- Fluree counterpart: corpus q036_count_orders (verbatim).
-- {{table}} tokens are bound by run_pair.py per target: read_parquet(...) for the
-- parquet floor, or <catalog>.<schema>.TABLE for the iceberg-REST substrate.
SELECT COUNT(*) AS n
FROM {{fact_order}};
