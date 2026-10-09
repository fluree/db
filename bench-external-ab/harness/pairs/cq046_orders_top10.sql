-- cq046 : DESC top-k with a unique tiebreaker so the selected rows are exact. Corpus q046.
SELECT order_id AS oid, order_total AS tot
FROM {{fact_order}} ORDER BY order_total DESC, order_id LIMIT 10;
