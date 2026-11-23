-- cq027 : largest single-table fused grouped COUNT (1M rows).
-- Corpus q027. Integer COUNT -> exact compare.
SELECT event_type AS et, device_type AS dev, COUNT(*) AS n
FROM {{fact_web_event}} GROUP BY event_type, device_type ORDER BY et, dev;
