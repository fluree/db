# Virtual-dataset perf switch inventory

The one page an operator reads before touching the virtual (R2RML/Iceberg) query
path. Every performance lever introduced or exercised by the 2026-07 virtual-dataset
burndown, with its default, what it does, and **what turning it OFF restores** (the
kill-scope — the whole point of a switch is a byte-for-byte fallback to the prior
behavior when a lever misfires in the field).

**Falsy convention (all boolean `FLUREE_*` switches below).** A switch is ON unless
its value is one of `0`, `false`, `off`, `no` (case-insensitive, trimmed) — anything
else, including empty, reads as ON. This is the shared `env_switch_enabled`
(`fluree-db-query/src/r2rml/mod.rs`); the two Iceberg booleans below inline the same
set. So `FOO=off` and `FOO=0` both disable; `FOO=1`/`FOO=true`/unset all enable.
Boolean switches are read **once** and cached for the process (`OnceLock`) — set them
in the environment before launch, not mid-run.

## R2RML rewrite / operator levers (the burndown PRs)

| Switch | Default | Mechanism | Finding / PR | OFF restores |
|---|---|---|---|---|
| `FLUREE_FUSED_R2RML_AGG` | on | Fuse a single-table `GROUP BY`/aggregate into one manifest-driven scan (Σ from record counts where sound) instead of materializing the star. | PR-6 (#1490) | Per-row materialize + generic aggregate. |
| `FLUREE_FUSED_R2RML_AGG_JOIN` | on | Extend the fused aggregate across a dimension join (rollup) — declines on a dup join-key conflict (see #1490 §5.5). | PR-6 (#1490) | Unfused join then aggregate. |
| `FLUREE_R2RML_STAR_TM_PRUNE` | on | Prune bound-subject TriplesMaps a star cannot reach (detail-view over-scan: 16→3 tables). | PR-3 (star over-scan) | Scan every candidate TriplesMap. |
| `FLUREE_R2RML_PARENT_MEMO` | on | Query-scoped, cross-operator-rebuild memo of `RefObjectMap` parent lookups, keyed `(graph_source_id, parent_tm, cols, as_of_t)`. | PR-8b (#1492) | Per-operator lookup cache only (rebuilds re-scan). |
| `FLUREE_R2RML_PARENT_MEMO_TOTAL_WINDOWS` | `2` (× materialize window) | Caps the SUM of memo rows across a query's parents (per-entry is already ≤ one window). | PR-8b (#1492) | — (a cap value; `0`/unparseable ⇒ default 2). |
| `FLUREE_R2RML_BATCHED_OPTIONAL` | on | Batched hash-left-join for R2RML OPTIONAL instead of per-seed correlated re-scan. | PR-4b | Correlated per-seed OPTIONAL. |
| `FLUREE_R2RML_BATCHED_OPTIONAL_STAR` | on | Admit the star-shaped OPTIONAL body into the batched path (hash-join-safe; completeness from referenced_vars). | PR-4c (#1493) | Non-star OPTIONAL stays batched; star falls back. |
| `FLUREE_R2RML_OPTIONAL_SEED_COALESCE` | on | Coalesce the WHOLE driving side into ONE seed → one inner scan (F14: kills per-window re-scan). | PR-4d (#1501) | Per-outer-batch windowed inner scans. |
| `FLUREE_R2RML_OPTIONAL_SEED_COALESCE_CAP` | `524288` (512×1024) | Max driving rows buffered into one seed before the inner scans (peak-memory bound for an UNBOUNDED OPTIONAL). | PR-4d (#1501) | — (a cap; beyond it, cap-sized windows). |
| `FLUREE_R2RML_TOPK_PUSHDOWN` | on | Scan-side top-k: forward the ORDER BY + LIMIT into the scan so it stops early (q046 99.87% pruned). Declines when the sort predicate doesn't map to exactly one POM. | PR-5 (#1495) | Full scan then sort+limit. |
| `FLUREE_R2RML_LIMIT_PUSHDOWN` | on | Forward a plain LIMIT row-budget into the scan wrapper. | PR-5 family | Full scan then limit. |
| `FLUREE_R2RML_UNION_BUDGET` | on | **Forward the LIMIT row-budget through UNION and BIND** (both reclassified as forwarders in the `Operator::set_row_budget` contract), + a budget-met branch-skip lever (q029 125s→2.6s). **Scope note:** the name says UNION but it gates BIND forwarding too (a BIND-under-LIMIT with no UNION is affected) — see `fluree-db-query/src/r2rml/mod.rs` doc. | F17 (#1507) | UNION/BIND absorb the budget (full branch re-drive). |
| `FLUREE_R2RML_REF_TARGET_PRUNE` | on | Propagate a `RefObjectMap`'s target class to prune downstream shared-predicate resolution (q031 fan-out 7→2 loadTables). Declines unless every binding source of the var is provably that one ref. | F20 (#1502) | Resolve the shared predicate against all mapping dims. |
| `FLUREE_R2RML_CURIE_ALIGN` | on | CURIE-align virtual graph-source `Binding::Iri` in `sparql_json` so IRIs render like native (`@context`/PREFIX-driven `compact_id`). `sparql_json` only. | F9 (#1499) | Raw full IRIs on the virtual side (cosmetic divergence). |

## Iceberg scan / catalog-cache levers

| Switch | Default | Mechanism | Finding / PR | OFF restores |
|---|---|---|---|---|
| `FLUREE_ICEBERG_NUMERIC_STATS` | on | Row-group pruning from Parquet numeric (double + FLBA-decimal) column stats (q019 cold 38.8s→4.0s). NaN bound ⇒ keep (F15 over-prune guard). | PR-7 (#1494) | No numeric row-group pruning. |
| `FLUREE_ICEBERG_LOADTABLE_PTR_CACHE` | on | Persist the **credential-free** `lt_key → metadata_location` pointer to disk, so a fully disk-warm query resolves the location with zero REST loadTable GET / OAuth. | #1503 | Storage stays eager; pointer rung skipped (byte-identical to pre-#1503). |
| `FLUREE_ICEBERG_LOADTABLE_PTR_TTL_SECS` | `300` | Freshness bound on the persisted pointer (older ⇒ ignored, forces a GET that refreshes pointer + creds). `0` disables pointer persistence entirely. **When both this and the 60s in-memory cross-query cache are live, this 300s pointer is the WIDER latest-read staleness bound and governs** (disk-cache-is-steady-state ruling; see #1503 review, TTL-default note). | #1503 | — (a TTL; `0` = off). |
| `FLUREE_ICEBERG_LOADTABLE_CACHE` | on | In-memory, process-wide cross-query cache of the WHOLE REST loadTable response (incl. creds) — the pre-existing 60s cache the pointer sits in front of. | pre-#1503 (context) | No cross-query loadTable reuse. |
| `FLUREE_ICEBERG_LOADTABLE_TTL_SECS` | `60` | TTL for the in-memory loadTable-response cache above (short because it holds credentials). | pre-#1503 (context) | — (a TTL). |

> F21 (open, register): the pointer cache is TTL-window-bounded (300s + prune-on-read
> ⇒ an intra-window win) and only FACT tables get a pointer today — small
> loadTable-dominated dim queries still pay the GET. A per-table/keying follow-up is
> filed as F21, pending its own design sketch. This inventory row states the *current*
> governing bound; F21 tracks widening it.

## Corpus & bench-harness levers (vbench)

These are not engine switches — they shape how the corpus MEASURES, and the one an
operator must not misread is the timeout.

- **Per-query `timeout_s` (manifest field, not an env var).** Each corpus query
  carries a `timeout_s` in the manifest. **It is CI-stability headroom, not the perf
  bar.** A query that completes at 168s under a 300s `timeout_s` is *slow*, not
  failing; exceeding `timeout_s` is a DNF (a hard stop), reported distinctly from a
  perf-violation ratio against a blessed baseline. Do not read a large `timeout_s` as
  an expectation — the honest wall is recorded in the ROADMAP row regardless (e.g.
  q056 168s under a 300s gate is 93% headroom, deliberately flagged as fragile). The
  north-star bar (≤ low single-digit seconds, cache-thrashed / first-ask) is a
  separate criterion the `timeout_s` never encodes.
- **`FLUREE_QUERY_TIMEOUT_MS` (engine).** The engine-level per-query deadline
  (query cancellation). The bench harness sets its own per-query deadline from
  `timeout_s`; a live agent/chat path sets it via the `x-query-timeout-secs` header,
  not body opts. Distinct from the corpus `timeout_s` above.
- **`FLUREE_BENCH_SPAN_ALLOWLIST`.** Restricts which tracing spans the corpus counts
  as pathway evidence (`scan_table`, `load_table`, `count_manifest`, …). The gates
  assert span COUNTS (e.g. `scan_table.n` 253→2), so a must-fire span group missing
  is an `xERR`, not a silent pass.
- **`FLUREE_BENCH_TRACING` / `FLUREE_BENCH_PROFILE` / `FLUREE_BENCH_SCALE` /
  `FLUREE_BENCH_RUNTIME`.** Standard bench knobs (enable span capture; profile/scale
  selection; runtime target). Not perf levers — measurement configuration.

## Kill-switch philosophy

Every ON-by-default lever above has a byte-for-byte OFF fallback to the pre-lever
behavior — that is the contract that lets a lever ship: if it ever produces a wrong
or slow answer in the field, `SWITCH=off` restores the old path without a redeploy.
The R2RML operator levers additionally DECLINE (fall through to the generic pipeline)
whenever their soundness precondition isn't met, so "on" is never "on unconditionally"
— it is "on when provably safe, else the same path off would take."
