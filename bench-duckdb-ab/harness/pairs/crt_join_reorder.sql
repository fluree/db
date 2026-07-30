-- crt_join_reorder : 4-table join with two selective dim filters -> a real join-order
-- decision (DuckDB's public strength: hash joins with reordering). Shape blends q012+q008.
-- SUM(quantity) is xsd:integer -> exact compare.
SELECT p.category AS cat, SUM(ol.quantity) AS u
FROM {{fact_order_line}} ol
JOIN {{fact_order}} o     ON ol.order_key   = o.order_key
JOIN {{dim_customer}} c   ON o.customer_key = c.customer_key
JOIN {{dim_product}} p    ON ol.product_key = p.product_key
WHERE c.segment = 'Enterprise' AND p.is_current = true
GROUP BY p.category ORDER BY u DESC, cat;
