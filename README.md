# Virtual-dataset audit — R2RML / Iceberg / S3-Parquet read path (2026-07)

This directory is the durable, outward-facing record of a technical audit of Fluree
`db`'s **virtual dataset** read path: SPARQL / JSON-LD queries answered live over
R2RML-mapped Apache Iceberg tables backed by S3 Parquet, without materializing the
data into a native ledger. It was produced by a structured audit + implementation
program that traced the engine, benchmarked it against a native oracle on a live
Snowflake-managed Iceberg dataset, adversarially verified every headline claim, and
sequenced a recommendations ladder — Tiers 0-2 of which were implemented as the
consolidated PRs these documents accompany.

Every claim in these documents carries an inline receipt label:

- `READ(file:line)` — read directly in the source at the audit tip
- `MEASURED` — measured this session (bench run, `du`, live query)
- `AWS(api)` — a live AWS CLI/API call
- `WEB(url)` — an external source
- `PR(url)` — a GitHub receipt
- `INFERRED` — reasoning from the above
- `RELAYED(...)` — verified in a companion document of this set; cited, not re-derived
- References to `prior` assessments (`deployed-forensics`, `strategy slate`, `A-doc`) are provenance labels for internal working papers that predate this set and are not published here

Audit tip: `feat/lambda-usability = 10e073fe9` (PR #1514). Baseline: `main`
(virtual path identical to the #1450 merge `a49ac3fc2`). Bench dataset: a synthetic
enterprise data-warehouse star (`ENTERPRISE_DEMO.DW`, `data.fluree.dev` vocabulary)
at scale factors sf01 and sf20 (200×). No customer data appears anywhere in this
corpus.

## Reading order

1. **`00-MASTER-AUDIT.md`** — the flagship. Executive summary, the findings catalog
   (F-AUD-1..22), the empirical results, the materialization and third-way-strategy
   framing, and the Tier-0→Tier-3 recommendations ladder with implementation status.
   Start here; §0 is a 10-minute read of every verdict.

### Architecture & coverage (the "what the engine is" layer)

- `A1-architecture.md` — definitive architecture + data-flow map of the read path
  (entry → planner/rewrite → operators → provider seam → Iceberg catalog/IO/decode),
  plus the cache inventory and concurrency/memory model.
- `A2-strategy-inventory.md` — every shipped optimization mechanism, mapped to its
  PR/wave and switch.
- `A3-coverage-matrix.md` — where optimization coverage is sparse or missing across
  query shapes, folds, surfaces, and Iceberg features. Cross-references the probe
  battery in `probes/`.
- `B1-sota-comparison.md` — the read path vs. the state of the art (DataFusion,
  apache/iceberg-rust), the honest DataFusion-adoption pricing (Step-1 vs Step-2),
  and the capability gap table. Appendix D covers the arrow/iceberg-rust enablers.

### Empirical results (the "what it measures" layer)

- `C1-constraint-envelope.md` — the serverless (AWS Lambda) constraint envelope: the
  deployed 180s cap + 20s heartbeat + 300s deadline reality, the query-class × wall
  matrix, and the families that remain over the wall.
- `C2-bench-wave1.md` — wave-1 corpus benchmark: parity (0 hash mismatches; 45/45
  deterministic queries byte-identical native == virtual), the fused-aggregate cost
  regime, and the stale-baseline finding.
- `C2b-bench-wave2-probes.md` — wave-2 probe battery results: every predicted
  coverage gap reproduced live, and the cross-surface (SPARQL == JSON-LD) proof.
- `C3-materialization-facts.md` — fact base for materializing Iceberg into native
  ledgers (build/serve/sync arithmetic, the pack substrate, size/time anchors).
- `C4-solo-pipelines.md` — map of the serverless ingestion / materialization
  pipelines the strategy options build on.

### Verification (independent adversarial passes — high review value)

- `V1-mor-verification.md` — verification of the merge-on-read delete-file gap
  (correctness finding F-AUD-1), with live AWS metadata measurement.
- `V2-membudget-verification.md` — verification of the scan-path memory-budget blind
  spot (F-AUD-3), an attempt-to-refute that failed.
- `RT1-redteam.md` — red-team of the strategic core (the DataFusion routing trace
  that reframed the Step-1/Step-2 recommendation).
- `RT2-redteam-empirics.md` — red-team recomputation of the empirical headline
  numbers (the per-core ceiling arithmetic, the baseline-staleness floor).

### Strategy

- `D1-strategy-options.md` — the design space of "third-way" acceleration strategies
  (result cache, warm tier, sidecar cubes, MPP, native-twin promotion, async), each
  graded against the residual timeout families, with the snapshot-keyed correctness
  lemma and the dominance/composition insights.

### Parity (browse-path serialization & shape verification)

- `parity/P1-serializer-verify.md` — the crawl-formatter serialization defect chain.
- `parity/P2-boundsubject-pathmap.md` — the three bound-term code paths and the
  VALUES-drop correctness hazard.
- `parity/P3-shape-matrix-empirical.md` — 23 live browse queries vs. the native twin.
- `parity/P4-famc-probe.md` — the two production-DNF shapes, code trace + measurement.

### Implementation-phase reviews

- `pr-reviews-impl.md` — the adversarial diff reviews of the implementation PRs
  (the MoR guard, the memory-budget cluster, the coverage widenings, the harness/trust
  layer, the browse-parity package, and the materialization-builder).

### Reproducibility artifacts

- `probes/` — the runnable coverage-gap probe battery: `probes.md` (the human-readable
  battery description + priority order), `manifest.json` (the survey corpus manifest),
  and `queries/*.rq` (21 SPARQL probes, 18 gap-isolators + 3 controls).
- `data/` — benchmark run records (JSONL): `native-full-*` and `virtual-full-*` are
  the wave-1 parity run (the 0-mismatch receipt); `virtual-rebless-merged` is the
  re-blessed baseline; `probe-survey.*` are the wave-2 probe telemetry. Bench-host and
  local-home identifiers in the run metadata have been neutralized; all measurement
  fields are intact.
