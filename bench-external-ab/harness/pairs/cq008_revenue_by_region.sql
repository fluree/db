-- cq008 : fused fact->dim->dim rollup (Order->Customer->Geography).
-- Corpus q008. SUM(order_total) is xsd:double -> numeric tolerance.
SELECT g.region AS region, SUM(o.order_total) AS rev
FROM {{fact_order}} o
JOIN {{dim_customer}} c ON o.customer_key = c.customer_key
JOIN {{dim_geography}} g ON c.geography_key = g.geography_key
GROUP BY g.region ORDER BY region;
