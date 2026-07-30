-- p2_category_rollup : fact -> dim grouped aggregate (units sold by product category).
-- FACT_ORDER_LINE joined to DIM_PRODUCT on product_key, SCD is_current filter.
-- Shape-equivalent to corpus q012_top_products_by_units (which rolls up by product
-- NAME + top-10); this rolls up by CATEGORY per the "category rollup" pair spec.
-- {{table}} tokens bound per target by run_pair.py.
-- NOTE: on the iceberg-REST substrate this join hits DuckDB's bloom-filter pushdown
-- gap (see PREP.md spike) unless SET disabled_optimizers='join_filter_pushdown'.
SELECT p.category AS cat,
       SUM(ol.quantity) AS u
FROM {{fact_order_line}} ol
JOIN {{dim_product}} p
  ON ol.product_key = p.product_key
WHERE p.is_current = true
GROUP BY p.category
ORDER BY u DESC, cat;
