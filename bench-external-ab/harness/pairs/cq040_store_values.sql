-- cq040 : VALUES-constrained subject set (SQL IN). Corpus q040.
-- Returns raw order_total (double) cells -> numeric multiset compare.
SELECT 'http://data.fluree.dev/edw/store/' || store_key AS store, order_total AS tot
FROM {{fact_order}} WHERE store_key IN (1, 2, 3);
