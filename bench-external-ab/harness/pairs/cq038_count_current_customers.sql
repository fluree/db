-- cq038 : un-fused filtered COUNT over a dimension with the SCD current filter.
-- Corpus q038. Fluree declines to full materialize (~390k rows) to count matches.
SELECT COUNT(*) AS n FROM {{dim_customer}} WHERE is_current = true;
