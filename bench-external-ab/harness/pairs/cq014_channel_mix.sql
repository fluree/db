-- cq014 : fused single-table aggregate (COUNT + SUM) grouped by channel.
-- Corpus q014. SUM(order_total) is xsd:double -> compare with numeric tolerance.
SELECT order_channel AS ch, COUNT(*) AS n, SUM(order_total) AS rev
FROM {{fact_order}} GROUP BY order_channel ORDER BY ch;
