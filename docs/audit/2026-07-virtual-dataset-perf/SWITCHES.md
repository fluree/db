# Virtual-dataset perf switch inventory

The one page an operator reads before touching the virtual (R2RML/Iceberg) query
path. Every performance, correctness, and capacity lever on that path — with its
default, what it gates, and **what turning it OFF restores** (the kill-scope; the
whole point of a switch is a byte-for-byte fallback to the prior behavior when a
lever misfires in the field) — plus its introducing PR.

This inventory was regenerated for the 2026-07 audit (F-AUD-19) by enumerating
**every** `FLUREE_*` read across `fluree-db-query`, `fluree-db-iceberg`, and
`fluree-db-api` and verifying each row against the code (A2 is the map; the code is
the truth). The previous version documented ~19 of the ~40 on-path switches and
omitted PR-8, PR-2, C1, C4, R3-B, the #1520/#1521/#1522 additions, and the baseline
levers — an operator could not revert those from the doc.

**Falsy convention (all boolean `FLUREE_*` switches below).** A switch is ON unless
its value is one of `0`, `false`, `off`, `no` (case-insensitive, trimmed) — anything
else, including empty, reads as ON. This is the shared `env_switch_enabled`
(`fluree-db-query/src/r2rml/mod.rs`); the Iceberg booleans inline the same set. So
`FOO=off` and `FOO=0` both disable; `FOO=1`/`FOO=true`/unset all enable. Boolean
switches are read **once** and cached for the process (`OnceLock`) — set them in the
environment before launch, not mid-run. **The sole exceptions** are noted per row:
`FLUREE_ICEBERG_ALLOW_MOR_DELETES` is an inverted-polarity *escape hatch* read fresh
on each call (its default-OFF is the SAFE state), the three fused-aggregate switches
(`FLUREE_FUSED_VECTOR_FOLD` / `FLUREE_FUSED_R2RML_OUTPUT_BOUND` /
`FLUREE_FUSED_R2RML_MULTIFACT`) are read at fused-operator **construction** (per
query) into operator fields rather than a process `OnceLock` (id=3717339910; still
"set at launch" in practice), and the numeric/TTL knobs are values, not booleans.

## 1. R2RML rewrite / operator levers

| Switch | Default | What it gates | OFF restores | Introduced |
|---|---|---|---|---|
| `FLUREE_FUSED_R2RML_AGG` | on | Fuse a single-table `GROUP BY`/aggregate into one manifest-driven scan (Σ from record counts where sound) instead of materializing the star. **Widened in-place** by #1514 (string keys, Q2 lang/IRI decline, O1/O2 guards) and by #1522 items 9/9b (MIN/MAX fold + filtered-COUNT constraint application) — OFF reverts ALL of these, back to per-row materialize + generic aggregate. | Per-row materialize + generic aggregate (incl. MIN/MAX & filtered-COUNT). | baseline (#1450) |
| `FLUREE_FUSED_R2RML_AGG_JOIN` | on | Extend the fused aggregate across a linear fact→dim FK join (rollup); declines on branch/merge/cycle/composite-FK/dup-join-key. Gates the E2 + W4-2 join widenings too. | Unfused join then aggregate. | PR-6 (#1490) |
| `FLUREE_FUSED_VECTOR_FOLD` | on | **N1** vectorized GROUP BY fold: a borrowed-key dense-id dict (`HashTable`) in place of a `HashMap<Vec<GKey>, Vec<Acc>>` that clones a fresh owned key every row. Read at operator construction (per query). | Byte-identical owned-key `HashMap` fold. | #1582 |
| `FLUREE_FUSED_R2RML_OUTPUT_BOUND` | on | Emit a GROUP BY rollup in bounded chunks (≤8192 groups/batch) across `next_batch` calls so a high-cardinality result never fully materializes at once. Read at operator construction (per query). | Single-batch emission (same rows). NB: a bare `LIMIT` with no `ORDER BY` can return a different prefix on vs off — group order is unspecified. | #1582 |
| `FLUREE_FUSED_R2RML_MULTIFACT` | on | **P3** fused multi-fact branching-star join (one GROUP-KEY branch + one SEMI-JOIN branch, keep-min-then-filter membership) — the crt_join_reorder class. Read at operator construction (per query). | Decline the branching star to the generic pipeline (pre-P3, byte-identical). | #1582 |
| `FLUREE_R2RML_SHARED_PREDICATE_FUSION` | on | **E1** shared-predicate class collapse: when strong fusion fails, still collapse a separate class scan into the star for a base predicate SHARED across disjoint-subject classes (the `ex:category` round-2 fix). | Falls through to the weaker pre-E1 `class_prune_hint` (star + separate class scan, still correct). Distinct from `CRAWL_CLASS_FUSION`. | E1 #1514, **retro-switched by this PR** |
| `FLUREE_R2RML_STAR_TM_PRUNE` | on | Prune bound-subject TriplesMaps a star cannot reach (detail-view over-scan 16→3 tables). | Scan every candidate TriplesMap. | PR-3 (#1484) |
| `FLUREE_R2RML_REF_TARGET_PRUNE` | on | Propagate a `RefObjectMap`'s target class to prune downstream shared-predicate resolution (q031 fan-out 7→2 loadTables). Declines unless every binding source of the var is provably that one ref. | Resolve the shared predicate against all mapping dims. | F20 (#1502) |
| `FLUREE_R2RML_IN_PUSHDOWN` | on | Lower `FILTER … IN (…)` / single-var `VALUES` over an exactly-one-scalar-column POM to an `Expression::In` scan filter (q070). Whole-or-nothing per set. FK-IRI IN-sets are NOT lowered (documented follow-on; q069 parity member). | In-engine FILTER/VALUES, full FACT scan. | #1522 item 7 |
| `FLUREE_R2RML_IN_PUSHDOWN_MAX` | `64` | Cap on the IN/VALUES set size that will lower (a larger set declines the WHOLE set rather than truncate — a truncated IN would drop rows). | — (a cap; `0`/non-numeric ⇒ default 64). | #1522 item 7 |
| `FLUREE_R2RML_TOPK_PUSHDOWN` | on | Scan-side top-k: forward `ORDER BY … LIMIT` into the scan so it stops early (q046 99.87% pruned). Declines when the sort predicate doesn't map to exactly one POM or a residual filter is present. | Full scan then sort+limit. | PR-5 (#1495) |
| `FLUREE_R2RML_TOPK_ASC` | on | Admit **ASC** order into the scan-side top-k, but ONLY for a schema-`required` (non-nullable) column (so no SPARQL-unbound row can sort first and be wrongly pruned). DESC is unaffected. | ASC declines top-k (DESC still pushes); full scan then sort+limit for ASC. | #1522 item 8 |
| `FLUREE_R2RML_LIMIT_PUSHDOWN` | on | Record a plain top-of-tree LIMIT row-budget on the topmost 1:1 scan for early termination. | Full scan then limit. | baseline (#1450); forwarding extended by C1/F17 |
| `FLUREE_R2RML_DATASET_BUDGET` | on | Thread a top-of-tree LIMIT budget / ORDER-BY-LIMIT top-k into each dataset member's subtree (the `FROM <ledger>` path chat SPARQL always runs). | UNION-of-members re-drives every member fully (Sort/Distinct absorb the budget). Byte-identical. | C1 / #1514 |
| `FLUREE_R2RML_UNION_BUDGET` | on | Forward the LIMIT row-budget through UNION (each branch) and BIND (1:1), + a budget-met branch-skip lever (q029 125s→2.6s). **Scope note:** the name says UNION but it gates BIND-under-LIMIT forwarding too. | UNION/BIND absorb the budget (full branch re-drive). | F17 (#1507) |
| `FLUREE_R2RML_BUDGET_OPTIONAL` | on | Forward the LIMIT row-budget to an OPTIONAL's REQUIRED (outer) side only — sound (each required row emits ≥1 output), closes probe-04's 68,828× read amplification. The optional side stays unbudgeted; MINUS is NOT forwarded (unsound anti-join absorb). | OPTIONAL absorbs the budget (full outer scan). Byte-identical. | #1522 item 11 |
| `FLUREE_R2RML_BATCHED_OPTIONAL` | on | Batched hash-left-join for a correlated R2RML OPTIONAL leaf instead of per-seed re-scan. | Correlated per-seed OPTIONAL. | PR-4b (#1487) |
| `FLUREE_R2RML_BATCHED_OPTIONAL_STAR` | on | Admit a same-subject STAR OPTIONAL body into the batched path (completeness from `referenced_vars`). | Star OPTIONAL falls back to per-seed; non-star still batched. | PR-4c (#1493) |
| `FLUREE_R2RML_OPTIONAL_SEED_COALESCE` | on | Coalesce the WHOLE driving side into ONE seed → one inner scan (F14: kills per-window re-scan). | Per-outer-batch windowed inner scans. | PR-4d (#1501) |
| `FLUREE_R2RML_OPTIONAL_SEED_COALESCE_CAP` | `524288` | Max driving rows buffered into one seed before the inner scans (peak-memory bound for an unbounded OPTIONAL). | — (a cap; beyond it, cap-sized windows). | PR-4d (#1501) |
| `FLUREE_R2RML_PARENT_MEMO` | on | Query-scoped, cross-operator-rebuild memo of `RefObjectMap` parent lookups, keyed `(graph_source_id, parent_tm, cols, as_of_t)`. | Per-operator lookup cache only (rebuilds re-scan). | PR-4 (#1485) / PR-8b (#1492) |
| `FLUREE_R2RML_PARENT_MEMO_TOTAL_WINDOWS` | `2` (× materialize window) | Caps the SUM of memo rows across a query's parents (per-entry is already ≤ one window). | — (a cap; `0`/unparseable ⇒ default 2). | PR-8b (#1492) |
| `FLUREE_R2RML_PARALLEL_CATALOG` | on | Warm per-table catalog contexts (loadTable GET + metadata) CONCURRENTLY before the serial scan loop (cold-start). Best-effort, side-effect-only (failure swallowed; the real scan re-resolves). | Serial per-table catalog resolution. | PR-8 (#1491) |

## 2. Iceberg scan / pruning levers

| Switch | Default | What it gates | OFF restores | Introduced |
|---|---|---|---|---|
| `FLUREE_ICEBERG_PREDICATE_PUSHDOWN` | on | Drop row groups whose column min/max prove no row matches the residual (int/date/string). **Also the apply gate** for the W4-1 multi-predicate/const-key scan-FILTER pushdown and the W4-1b folded-crawl const-object prune (both unswitched — see §7). | No row-group pruning; W4-1/W4-1b prunes also go inert. | baseline (#1450) |
| `FLUREE_ICEBERG_NUMERIC_STATS` | on | Extend row-group pruning to double + FLBA-decimal column stats (q019 cold 38.8s→4.0s). NaN bound ⇒ keep (F15 over-prune guard). | No numeric row-group pruning. | PR-7 (#1494) |
| `FLUREE_ICEBERG_TIMESTAMP_STATS` | on | `xsd:dateTime` predicates prune at the **manifest** level (frame-matched tz-aware↔`timestamptz`, naive↔`timestamp`). No row-group arm (Parquet INT64 logical-unit ambiguity). | Timestamp predicates never reach pruning; in-engine FILTER authoritative. | #1522 item 10 |
| `FLUREE_ICEBERG_FOOTER_FROM_CACHE` | on | Parse the Parquet footer from already-fetched whole-file bytes instead of a separate footer round-trip (~190ms/file). Whole-file tiers only (disk hit / ≤32MB). | Footer-first round-trip on every file. Byte-identical. | PR-2 Lever A (#1482) |
| `FLUREE_ARROW_DIRECT_DECODE` | on | **N2** build a `Column` DIRECTLY from an Arrow array, skipping the per-cell `Vec<Option<ColumnValue>>` intermediate the two-hop `arrow_column_to_values` + `build_columns_from_values` path allocates. | Byte-identical two-hop decode. | #1582 |
| `FLUREE_ICEBERG_ROWGROUP_PARALLELISM` | on | Decode a single-/few-file table's surviving row groups across N blocking tasks (uses idle cores). Declines for a single row group or when a sequential read would skip groups. | Sequential row-group decode. Byte-identical. | C4 (#1514) |
| `FLUREE_ICEBERG_PARALLEL_RANGE_GETS` | on | Fetch a scan's coalesced byte ranges CONCURRENTLY via `read_ranges` (order-preserving `buffered`, both storage impls). | Sequential range GETs. Byte-identical. | #1522 item 12 |
| `FLUREE_ICEBERG_SCAN_CONCURRENCY` | `min(cores, files, 32)` | Scan-side decode concurrency ceiling. PR-2 **raised the default 8→32**; an explicit override is uncapped and never lowers a prior default. | — (a value; default was 8 pre-PR-2). | PR-2 Lever B (#1482) |

## 3. Iceberg catalog / cache levers

| Switch | Default | What it gates | OFF restores | Introduced |
|---|---|---|---|---|
| `FLUREE_ICEBERG_CATALOG_DISK_CACHE` | on | Persist catalog metadata + manifests to disk (a cold process skips the S3 re-read). Content-addressed by immutable `metadata_location`, no TTL; 512MiB oldest-first prune. Master switch for the pointer cache below. | No disk catalog cache (cold process re-reads S3); also disables the loadTable-pointer cache. | PR-8 (#1491) |
| `FLUREE_ICEBERG_LOADTABLE_PTR_CACHE` | on | Persist the **credential-free** `lt_key → metadata_location` pointer so a disk-warm query resolves location with zero REST loadTable GET / OAuth. | Storage stays eager; pointer rung skipped (byte-identical to pre-#1503). | #1503 |
| `FLUREE_ICEBERG_LOADTABLE_PTR_TTL_SECS` | `300` | Freshness bound on the persisted pointer (older ⇒ ignored, forces a GET). **The WIDER latest-read staleness bound; governs over the 60s in-memory cache** when both live. `0` disables pointer persistence. | — (a TTL; `0` = off). | #1503 |
| `FLUREE_ICEBERG_LOADTABLE_CACHE` | on | In-memory, process-wide cross-query cache of the WHOLE REST loadTable response (incl. creds). | No cross-query loadTable reuse. | baseline |
| `FLUREE_ICEBERG_LOADTABLE_TTL_SECS` | `60` | TTL for the in-memory loadTable-response cache above (short because it holds credentials). | — (a TTL). | baseline |
| `FLUREE_ICEBERG_REST_CLIENT_TTL_SECS` | (see code) | TTL for the cached REST catalog client (OAuth token + conn pool), keyed by config fingerprint; reused by scan + `/info`. | — (a TTL). | baseline |
| `FLUREE_ICEBERG_CATALOG_CONCURRENCY` | `8` | Process-wide catalog-request semaphore permit count (429/503 hardening). | — (a value). | PR-8 (#1491) |
| `FLUREE_ICEBERG_CATALOG_MAX_RETRIES` | `4` | Max retries on a 429/503, honoring `Retry-After` else exp backoff + full jitter (401-refresh preserved). | — (a value; `0` = no retry). | PR-8 (#1491) |
| `FLUREE_ICEBERG_CATALOG_BACKOFF_BASE_MS` | `250` | Backoff base (cap 8s) when no `Retry-After` header is present. | — (a value). | PR-8 (#1491) |

> F21 (open, register): the pointer cache is TTL-window-bounded (300s + prune-on-read
> ⇒ an intra-window win) and only FACT tables get a pointer today — small
> loadTable-dominated dim queries still pay the GET. A per-table keying follow-up is
> filed as F21. This inventory states the *current* governing bound; F21 tracks widening it.

## 4. Correctness / safety / capacity levers

| Switch | Default | What it gates | Polarity / OFF semantics | Introduced |
|---|---|---|---|---|
| `FLUREE_ICEBERG_ALLOW_MOR_DELETES` | **off** (guard ACTIVE) | **Inverted-polarity escape hatch, read fresh each call.** The fail-closed MoR guard REFUSES any scan/stats over a snapshot carrying merge-on-read position/equality delete files (results would silently include deleted rows). | **Default-OFF is the SAFE state** (refuse). Set `=1`/`true` to BYPASS the guard and read anyway (results may include deleted rows + over-count COUNT). This is the one switch whose ON is the unsafe state. | #1520 (F-AUD-1) |
| `FLUREE_QUERY_MEMORY_BUDGET_BYTES` | ~78% of the detected cgroup/mem limit (8GiB fallback) | Byte budget for in-memory join-build / aggregate-fold / scan-window guards; aborts typed `MemoryBudgetExceeded` (→507) before a hard OOM. Polls only — NO engine time limit. | `0` disables the guard entirely; an explicit value overrides the 78% derivation. | R3-B (#1514) |
| `FLUREE_QUERY_BUDGET_SHARE_DIV` | `1` | Divisor applied to the per-query memory ceiling at the runner attach point, for concurrent-overcommit protection (set to the query Lambda's reserved concurrency). SOUND static form (a mid-flight query cannot be re-pinned). | Default `1` == today (no division / zero protection by design); the completing action is a deploy-config value. | #1521 (F-AUD-3) |
| `FLUREE_SCAN_MEM_ACCOUNTING` | on | `record_alloc`+`checkpoint` on the R2RML scan window and the fact-parent-build batch loop, so a wide crawl trips a typed 507 instead of OOMing (the non-aggregate blind spot). | No scan-path accounting (the non-aggregate scan/crawl path is invisible to the budget again). | #1521 (F-AUD-3) |

## 5. /info + result-format levers

| Switch | Default | What it gates | OFF restores | Introduced |
|---|---|---|---|---|
| `FLUREE_R2RML_CURIE_ALIGN` | on | CURIE-align virtual graph-source `Binding::Iri` in `sparql_json` so IRIs render like native (`@context`/PREFIX-driven `compact_id`). `sparql_json` ONLY; native `Binding::Sid` untouched. | Raw full IRIs on the virtual side (cosmetic divergence). | F9 (#1499) |
| `FLUREE_ICEBERG_INFO_COUNT_BUDGET_MS` | `10000` | Time budget for the empty-shell `/info` per-table snapshot-summary row-count fetch (metadata-only, bounded concurrency). Counts abandoned past budget → structure-only. | `0` disables the row-count fetch (structure from mapping only). | baseline; C2 (#1514) rerouted the empty-shell path to it |
| `FLUREE_R2RML_INFO_MEMBER_ROUTING` | on | Per-member `/info` routing: a graph-source ledger serves manifest stats regardless of native `t`; a HYBRID (t>0 + graph source) merges native + virtual (UNION by IRI, graph-source wins collision — no double-count). MoR-delete tables flagged as `mor-approximate-tables` upper bounds. | Strict `t == 0` empty-shell reroute (the prior behavior). | #1522 item 14 |

## 6. Baseline (pre-burndown) levers still on the path

Documented for completeness — these predate #1450's burndown but an operator may still
need to revert them.

| Switch | Default | What it gates | OFF restores | Introduced |
|---|---|---|---|---|
| `FLUREE_R2RML_CRAWL_CLASS_FUSION` | on | Class-constrain an injected `?s ?p ?o` browse-crawl wildcard, pruning TM fan-out 16→1 + merging the type-var into one budgeted scan. **This is the actual crawl-wildcard-fusion switch** — the audit brief's `FLUREE_R2RML_WILDCARD_CLASS_FUSION` does NOT exist (name drift; A2 D2). | Unfused crawl (and, coupled, crawl expansion off — see below). | baseline (#1450) |
| `FLUREE_R2RML_CRAWL_EXPAND` | on | Master enable for browse-crawl wildcard expansion. **Coupled** to `CRAWL_CLASS_FUSION`: expand-on + fusion-off would route a browse through the UNFUSED 16-table fan-out (429 storm), so disabling fusion also disables expansion. | No crawl expansion (fast empty/declined browse). | baseline (#1450) |
| `FLUREE_R2RML_FILTER_CONSUMPTION` | on | Move a fully scan-local FILTER INTO the single R2RML scan (`consumed_filter`) so a LIMIT row-budget can reach the scan. | FILTER stays a separate plan operator. | baseline (#1450) |
| `FLUREE_R2RML_SCAN_CACHE` | on | Cache inner correlated scan results per lookup key (never caches a pruned top-k subset). | No per-child-batch scan cache. | baseline (#1450) |
| `FLUREE_R2RML_MATERIALIZE_WINDOW_ROWS` | `524288` (512×1024) | Bounded window size for exploding a columnar scan into `Binding` rows (caps a ~14GB full-table materialize). | — (a value; `0`/unparseable ⇒ default). | baseline (#1450) |
| `FLUREE_OPTIONAL_HASH_JOIN` | on | Master enable for the batched OPTIONAL hash-join (inverted `disabled` logic: off only on explicit `0`/`false`/`off`). | Correlated OPTIONAL (no hash-join). | baseline |
| `FLUREE_HASH_JOIN` | on | General hash-join enable (used by the R2RML join/OPTIONAL paths). | Nested-loop join. | baseline |

## 7. Unswitched-mechanism exemptions (documented, not retro-switched)

F-AUD-19 flagged three mechanisms that shipped without a dedicated kill switch. E1 is
retro-switched above (`FLUREE_R2RML_SHARED_PREDICATE_FUSION`, this PR). The other two
are documented exemptions — a dedicated fine-grained switch was judged too invasive
(they share their apply site with the baseline pushdown they can't be cleanly split
from), and each has a coarse field-revert plus a commit-revert:

- **W4-1** (multi-predicate / constant-key scan-FILTER pushdown + datatype coercion,
  commit `9fd2dd7a5`). It has no dedicated switch but **rides
  `FLUREE_ICEBERG_PREDICATE_PUSHDOWN`** (apply) + `FLUREE_ICEBERG_NUMERIC_STATS`
  (numeric): setting `FLUREE_ICEBERG_PREDICATE_PUSHDOWN=off` reverts it (coarse — it
  also disables baseline int/date/string pushdown). A dedicated switch is invasive
  because W4-1's additions and the baseline single-predicate pushdown share the
  `collect_pushdowns`/`build_scan_filters`/`to_scan_value` apply path. Commit-revert:
  revert `9fd2dd7a5`.
- **W4-1b** (const-object fold onto a co-located crawl wildcard, commit `fd660d229`).
  Results-neutral by itself (the folded members apply in-engine either way); its
  **prune** rides `FLUREE_ICEBERG_PREDICATE_PUSHDOWN` (off ⇒ the prune goes inert,
  results unchanged). Commit-revert: revert `fd660d229`.

## 8. Corpus & bench-harness levers (vbench)

These are not engine switches — they shape how the corpus MEASURES, and the one an
operator must not misread is the timeout.

- **Per-query `timeout_s` (manifest field, not an env var).** Each corpus query
  carries a `timeout_s`. **It is CI-stability headroom, not the perf bar.** A query
  that completes at 168s under a 300s `timeout_s` is *slow*, not failing; exceeding
  `timeout_s` is a **DNF** (a hard stop), reported distinctly from a perf-violation
  ratio against a blessed baseline. **A DNF is NEVER blessed as a perf baseline** — the
  bless path (`baseline.rs::write_perf`, guarded by `is_unblessable_wall`) records any
  non-completion (a DNF timeout cap, an error/abort time, an expected-error) as
  *no baseline / must-fix*, so a timeout wall (e.g. 180000ms) or a memory-abort time can
  never masquerade as a budget (the F-AUD-18 pathology this audit re-blessed away). The north-star bar (≤ low
  single-digit seconds, cache-thrashed / first-ask) is a separate criterion the
  `timeout_s` never encodes.
- **`FLUREE_QUERY_TIMEOUT_MS` (engine).** The engine-level per-query deadline (query
  cancellation). The bench harness sets its own per-query deadline from `timeout_s`; a
  live agent/chat path sets it via the `x-query-timeout-secs` header, not body opts.
- **`FLUREE_BENCH_SPAN_ALLOWLIST`.** Restricts which tracing spans the corpus counts as
  pathway evidence (`scan_table`, `load_table`, `count_manifest`, …). The gates assert
  span COUNTS, so a must-fire span group missing is an `xERR`, not a silent pass.
- **`FLUREE_BENCH_TRACING` / `FLUREE_BENCH_PROFILE` / `FLUREE_BENCH_SCALE` /
  `FLUREE_BENCH_RUNTIME`.** Standard bench knobs (span capture; profile/scale/runtime
  selection). Not perf levers — measurement configuration.

## 9. Kill-switch philosophy

Every ON-by-default lever above has a byte-for-byte OFF fallback to the pre-lever
behavior — that is the contract that lets a lever ship: if it ever produces a wrong or
slow answer in the field, `SWITCH=off` restores the old path without a redeploy. The
R2RML operator levers additionally DECLINE (fall through to the generic pipeline)
whenever their soundness precondition isn't met, so "on" is never "on unconditionally"
— it is "on when provably safe, else the same path off would take." The one deliberate
inversion is `FLUREE_ICEBERG_ALLOW_MOR_DELETES` (§4): its default-OFF is the *safe*
(refuse) state and turning it ON opts into a known-unsafe read — the escape hatch for
an operator who accepts approximate results over a hard refusal.
