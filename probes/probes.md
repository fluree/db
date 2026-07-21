# A3 Probe Battery — virtual-dataset coverage gaps

Ready-to-run SPARQL probes that each isolate ONE suspected optimization gap in the R2RML/Iceberg virtual-dataset path. Written to mirror corpus dataset-member style (`FROM <enterprise-sf01-v:main>`, the deployed solo chat / DatasetOperator shape). To be run by the bench agent in wave 2 against the same enterprise-sf01 virtual target the corpus uses. Predicates/IRIs/types verified against `fluree-bench-virtual/targets/enterprise-sf01-mapping.ttl` on audit tip `10e073fe9`.

Each probe pairs the WHERE-shape with a code-grounded hypothesis. Read telemetry the vbench way (per the iceberg-observability conventions): a fast path FIRED ⇒ `files_selected=0` and `r2rml.scan_table` + `iceberg.scan_plan` in `spans_missing`; a scan RAN ⇒ `files_selected>0` and an `r2rml.scan_table` span present. For pruning probes the discriminator is `files_selected` / `files_pruned`; for budget/top-k probes it is emitted-rows-before-LIMIT and window size; for fan-out probes it is `scan_table` n (distinct tables). Where a control sibling exists (`-b`), run BOTH and report the delta — the delta IS the gap.

Priority order (consequence × isolation cleanliness):

| # | file | gap class | primary signal | runnable now |
|---|------|-----------|----------------|--------------|
| 1 | probe-02-filter-in-not-lowered | IN-set never lowered (no ScanCmpOp::In) | files_pruned=0 on big FACT | yes |
| 2 | probe-03-scalar-values-not-lowered | scalar VALUES un-lowered (round3b #9 shape) | full FACT_GL_JOURNAL scan / maybe DNF | yes |
| 3 | probe-01-asc-topk-cliff | ASC top-k unsupported (DESC-only) + no budget | files_pruned=0 vs q046 DESC | yes |
| 4 | probe-04 / 04b optional-budget-cliff | LIMIT budget swallowed by OPTIONAL | window not capped vs 04b | yes |
| 5 | probe-09-multi-fact-join-aggregate | fact-fact aggregate join uncovered | two full FACT scans | yes |
| 6 | probe-07-timestamp-range-no-prune | xsd:dateTime not pushable (EVENT_TS) | files_pruned=0 on 1M fact | yes |
| 7 | probe-05-count-distinct-decline | COUNT DISTINCT declines fused + no PR-1 shortcut | scan vs q036 shortcut | yes |
| 8 | probe-06-minmax-decline | MIN/MAX decline; missed stats shortcut | full scan for a stats-answerable query | yes |
| 9 | probe-08 / 08b decimal-constant-object | constant-object decimal/double/IRI no prune | files_pruned=0 vs 08b FILTER | yes |
| 10 | probe-18-notexists-correlate-cost | anti-join not batched like OPTIONAL | scan_table n / span count on FACT_PAYMENT | yes |
| 11 | probe-12-expression-orderby-declines | expr ORDER BY kills fused AND top-k | files_pruned=0 vs q046 | yes |
| 12 | probe-16 / 16b having-not-in-select | HAVING(unprojected agg) declines fused | scan vs 16b fused | yes |
| 13 | probe-11-distinct-limit-budget-cliff | DISTINCT swallows LIMIT budget | full scan for tiny LIMIT | yes |
| 14 | probe-15-deep-offset-asc-sort | deep-OFFSET ASC pagination = full sort/page | wall independent of OFFSET | yes |
| 15 | probe-17-groupconcat-sample-decline | GROUP_CONCAT/SAMPLE decline + memory | full materialize + peak mem | yes |
| 16 | probe-10-minus-unexercised | MINUS has zero corpus coverage | correctness + negated-side exec | yes |
| 17 | probe-13-ask-existence-budget | ASK may not forward LIMIT-1 budget | emitted rows ~1 vs full | yes |
| 18 | probe-14-describe-crawl-fanout | DESCRIBE may skip bound-subject prune | scan_table n (3 vs 16) | yes |

NOT runnable against the static corpus (needs a special fixture — flagged as the #1 correctness item in the report): merge-on-read POSITION/EQUALITY DELETE files are never applied (`parse_manifest_list` drops delete manifests; `planner.rs:224` skips `is_deletes()`; no reader subtracts deleted rows). To probe, register a virtual target over an Iceberg table that has undergone a row-level DELETE/UPDATE (Snowflake MoR) and compare a COUNT / row-return against the source engine — a stale (pre-delete) result is the bug. The bench dataset is append-only, so it cannot exhibit this; construct a fixture table.

Controls: probe-04b (plain LIMIT budgets), probe-08b (FILTER decimal prunes), probe-16b (projected-HAVING fuses). Run each with its primary probe.

Cross-surface note: every probe is written for the SPARQL `FROM`/dataset path. For surface-parity coverage, the bench agent should ALSO run probe-02, probe-03, probe-05, probe-09 as JSON-LD queries (the agent/system path) — the IR is shared so admission SHOULD match, but the 900s JSON-LD-multi-query incident says verify the fused-agg / rewrite admission is actually reached, not silently missed.
