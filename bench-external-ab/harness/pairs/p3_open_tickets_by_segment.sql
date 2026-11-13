-- p3_open_tickets_by_segment : filtered fact -> dim grouped aggregate.
-- Open (status != 'Closed') support tickets counted by customer segment.
-- FILTER != const is the discriminating shape. No verbatim corpus member;
-- nearest corpus relatives are q025_category_csat_having / q026_resolution_by_priority.
-- {{table}} tokens bound per target by run_pair.py. Same iceberg join-pushdown caveat as p2.
SELECT c.segment AS seg,
       COUNT(*) AS n
FROM {{fact_support_ticket}} t
JOIN {{dim_customer}} c
  ON t.customer_key = c.customer_key
WHERE t.status <> 'Closed'
GROUP BY c.segment
ORDER BY seg;
