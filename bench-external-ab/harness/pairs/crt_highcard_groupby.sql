-- crt_highcard_groupby : high-cardinality GROUP BY (~customer count groups) over the 1M-row
-- fact. Run at normal memory AND with duckdb memory_limit pinned LOW (spill candidate).
-- customer_key IS NOT NULL matches SPARQL's UNBOUND-omit semantics (edw:customer only binds
-- events that have a customer). Integer COUNT -> exact compare.
SELECT 'http://data.fluree.dev/edw/customer/' || customer_key AS c, COUNT(*) AS n
FROM {{fact_web_event}} WHERE customer_key IS NOT NULL
GROUP BY customer_key ORDER BY c;
