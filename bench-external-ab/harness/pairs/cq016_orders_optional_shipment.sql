-- cq016 : OPTIONAL (left-join) fact-to-fact, unordered LIMIT. Corpus q016.
-- Unordered LIMIT -> compare ROW COUNT only (rows_only).
SELECT o.order_id AS oid, s.ship_status AS st
FROM {{fact_order}} o
LEFT JOIN {{fact_shipment}} s ON s.order_key = o.order_key
LIMIT 5000;
