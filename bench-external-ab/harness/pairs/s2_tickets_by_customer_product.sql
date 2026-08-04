-- s2_tickets_by_customer_product : GROUP BY TWO referenced-entity keys (customer AND
-- product) -- the S2 multi-ref-IRI group-key shape (db #1589). Support tickets counted per
-- (customer, product) pair, resolved through two independent FK->IRI maps. Fluree emits the
-- customer/product ENTITY IRIs as the group key; the checker reduces each IRI to its
-- trailing surrogate key for compare against these integer keys (the cq040 IRI-reduction
-- convention). IS NOT NULL matches the SPARQL UNBOUND-omit (product_key is nullable, so an
-- absent product triple drops the ticket -- an inner join on both keys). COUNT -> exact.
SELECT customer_key AS c,
       product_key  AS p,
       COUNT(*)     AS n
FROM {{fact_support_ticket}}
WHERE customer_key IS NOT NULL
  AND product_key IS NOT NULL
GROUP BY customer_key, product_key
ORDER BY n DESC, c, p;
