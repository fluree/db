-- s1_multifilter_tickets_by_category : ONE group-key branch + TWO semi-join filters off
-- one fact -- the S1 K>=2 semi-join shape (db #1589). Enterprise-customer support tickets
-- handled by the Support department, counted by product category. The two INDEPENDENT
-- population filters (customer.segment AND agent.department), each off a distinct fact FK,
-- are the widening beyond crt_join_reorder's single semi-join branch. FactSupportTicket is
-- the only >=3-FK fact in the schema (customer/product/agent); OrderLine has just
-- {order, product}, so a 3rd filter branch there would collide with the product group-key
-- branch (branches must be disjoint). No verbatim corpus member. COUNT -> exact gate.
SELECT p.category AS cat,
       COUNT(*)   AS n
FROM {{fact_support_ticket}} t
JOIN {{dim_customer}} c ON t.customer_key = c.customer_key
JOIN {{dim_product}}  p ON t.product_key  = p.product_key
JOIN {{dim_employee}} e ON t.agent_key    = e.employee_key
WHERE c.segment = 'Enterprise'
  AND e.department = 'Support'
GROUP BY p.category
ORDER BY n DESC, cat;
