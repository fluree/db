# Virtual-Dataset (Iceberg / R2RML) — Diagnosis & Hypothesis Verdicts (WP7)

**Date:** 2026-07-11
**Branch:** `bench/virtual-dataset-corpus` (worktree `db-vbench`)
**Companions:** `01-pathway-inventory.md` (strategies §N + code anchors), `02-hypothesis-map.md` (H1–H8), `04-findings-register.md` (F1–F8).
**Method:** Six targeted deep-dives against the live `virtual-sf01` (Snowflake-managed Iceberg via R2RML), sequential and 2 s-paced, using the release `fluree` CLI with `RUST_LOG=fluree_db_api,fluree_db_iceberg` and the `vbench` span counters. DNF probes were time-boxed to 45–75 s — the shape shows early. No engine code was patched; where a probe needs a code-side span to close it, it is noted as a WP8 pre-task.

Each deep-dive gives the dominant **cost center** (code-anchored) and a **classification**: `extend-existing-strategy` (tune/extend a strategy already in the inventory), `new-strategy` (a capability the pathway lacks), or `wrong-turn-redesign` (a strategy actively working against us). The H1–H8 verdicts and a WP8-ready cost-center ranking follow.

---

## 0. The cross-cutting fact: fact tables are 7,670 tiny files, decoded ~serially-per-8

Every fact-touching DNF has the **same** root, measured identically across three independent probes:

| Probe | scan_table starts | FACT_ORDER `files_selected` | whole-file decodes in window | rate |
|---|---|---|---|---|
| q046 (ORDER BY+LIMIT) | **1** | **7,670** | 2,322 / 60 s | **~39 files/s** |
| q008 (fact⋈dim GROUP BY) | **1** | 7,670 | 2,354 / 60 s | ~39 files/s |
| q036 (COUNT(*)) | **1** | 7,670 | 1,735 / 45 s | ~39 files/s |

`FACT_ORDER` = **7,670 data files for 180,000 rows ≈ 23 rows/file** — Snowflake exported its micro-partitions as thousands of tiny Parquet files. The decode wall is therefore **file-count-bound, not row-volume-bound**: ~200 ms of fixed per-file cost (footer parse + column-chunk setup + whole-file disk-cache read + Arrow decode) × 7,670 files, mitigated ~8× by the reader's default concurrency `min(available_parallelism, files, 8)` clamped at 8 (`iceberg_scan_concurrency`, `r2rml.rs:36-52`) ⇒ **7,670 × ~200 ms / 8 ≈ 197 s → DNF** at the 180 s cap. This single number (~39 files/s ⇒ ~197 s full decode) explains q046, q008, q036, q040, q053, and the SF20 stress cliff. **It is the master bottleneck; most fact-touching queries never reach their downstream operators.**

---

## Deep-dive 1 — q046 (ORDER BY + LIMIT): single-scan decode wall

**Shape.** `?o a edw:Order ; edw:orderId ?oid ; edw:orderTotal ?tot ORDER BY DESC(?tot) LIMIT 10`. Its A/B twin q045 (pure `LIMIT 10`) = **2.47 s** (fresh-process, REST/catalog-floor-inclusive; the warm-process hot median is 266 ms — the number the brief cites).

**Evidence.** **1** FACT_ORDER scan, `files_selected=7,670`, 2,322 whole-file decodes in 60 s (~39 files/s), DNF. No scan-count explosion, no operator churn — a single scan grinding through 7,670 files. **Toggle proof:** q045 with `FLUREE_R2RML_LIMIT_PUSHDOWN=0` went **2.47 s → DNF@75 s** — i.e. removing the LIMIT budget reproduces q046's full scan exactly. So the ORDER BY converts the fast pure-LIMIT into the slow full-decode.

**Dominant cost center.** The **top-k sort absorbs the LIMIT budget** (`sort.rs` `new_topk` has no `set_row_budget`; inventory §12) so the scan cannot early-terminate and decodes all 7,670 files (H1 wall, `send_parquet` whole-file path). This is the §12 **"not budget-fixable"** case — top-k must see every row to rank.

**Classification: `new-strategy`.** Budget pushdown (which makes q045 fast) fundamentally cannot help here. The fix is a **scan-side top-k**: push the sort key + `k` into the reader so it maintains a bounded heap and prunes row groups / files by the running k-th bound (or requires pre-sorted data). Distinct from H2's budget path.

---

## Deep-dive 2 — q008 (fact⋈dim GROUP BY revenue rollup): decode wall gates the join

**Shape.** `?o a edw:Order ; edw:orderTotal ?tot ; edw:customer ?c . ?c edw:geography ?g . ?g edw:region ?region GROUP BY ?region`.

**Evidence.** **1** FACT_ORDER scan, 2,354 files decoded in 60 s, DNF. The DIM_CUSTOMER / DIM_GEOGRAPHY parent lookups **did not fire in 60 s** — the operator is still decoding the FACT_ORDER outer scan and never reaches the join/group phase.

**Dominant cost center.** **H1 decode wall (FACT_ORDER, 7,670 files).** The H6 cost (a GROUP BY over a join declines the fused aggregate, inventory §11, so it would materialize all 180K fact rows into bindings) and any H3 parent churn are **secondary and gated behind the decode** — the scan can't complete to reach them. So the "H3 vs H6 share" question resolves to **neither dominates yet; H1 does.** H6 becomes the bottleneck only once H1 is fixed.

**Classification: `extend-existing-strategy` (H1) + deferred `new-strategy` (H6).** Fix H1 first (see §0 / cost-center ranking); a fused-aggregate-over-single-join (H6) is a real but lower-priority follow-on because it is invisible until the scan completes.

---

## Deep-dive 3 — q050 (dims-only OPTIONAL): correlated re-scan explosion (the H3 alarm)

**Shape.** `?p a edw:Product ; edw:isCurrent true ; edw:name ?pn OPTIONAL { ?p edw:supplier ?s . ?s edw:rating ?r }`. Native: **95 ms**. Every table is a single-file dimension.

**Evidence (75 s, DNF).** Scan-table starts decompose as: **DIM_SUPPLIER ×153, DIM_PRODUCT ×78**, plus DIM_STORE/EMPLOYEE/CUSTOMER/ACCOUNT ×1 each. 235 scans in 75 s ⇒ ~**377 at the 120 s cap** (matching F5's count). A dims-only query DNFs on **scan count**, not decode.

**Dominant cost center.** Two compounding costs: **(a) H3 correlated-join rebuild** — the OPTIONAL `?p edw:supplier ?s . ?s edw:rating` re-resolves the DIM_SUPPLIER parent lookup **per product batch** with no cross-batch memoization (inventory §8/§9; the main-table `scan_cache` at `operator.rs:713-734` covers unfiltered inner scans, but the parent-lookup path `operator.rs:889-897` bypasses it), and DIM_PRODUCT is itself re-scanned per correlation; **(b) F8 fan-out** — the Product star's base predicate `edw:name` is shared, so the 4 unrelated name-dims are each scanned once (dead work, see Deep-dive 5).

**Classification: `extend-existing-strategy`.** Memoize the parent lookup across child batches (extend the §8 `scan_cache` to the parent-scan path, keyed by `(parent_tm, join_cols)` — the `LookupCacheKey` already exists at `operator.rs:64-65`). Combined with the F8 fix, a dims-only OPTIONAL should be ~2 scans, not ~377.

---

## Deep-dive 4 — q036 (COUNT(*)): full decode for a count that is free in the manifest

**Shape.** `SELECT (COUNT(*) AS ?n) WHERE { ?s a edw:Order }`.

**Evidence.** **1** FACT_ORDER scan, `files_selected=7,670`, **`estimated_row_count=180,000` printed in the scan plan** — the exact answer — yet the operator decodes 1,735 files in 45 s (~39 files/s ⇒ ~199 s full) and DNFs.

**Dominant cost center.** **H5 — no manifest shortcut.** The scan plan already sums the manifest `record_count` (that's the `estimated_row_count=180,000`; `stats.rs:120-133` computes it), and the fused-aggregate COUNT path exists (`fused_aggregate.rs`), but it still `scan_table`s and folds row-by-row (`fused_aggregate.rs:910`) instead of returning the manifest sum for a bare unfiltered `COUNT(*)`.

**Classification: `extend-existing-strategy`.** For an unfiltered `COUNT(*)` (no WHERE FILTER, no join), return the manifest `record_count` sum directly. **~199 s → sub-second.** Highest ROI / lowest risk on the board — the number is already computed and thrown away.

---

## Deep-dive 5 — q001 (typed dim star): 6-table fan-out (F8, root-caused)

**Shape.** `?s a edw:Store ; edw:name ?n ; edw:channel ?ch ; edw:storeType ?t`. Should be one DIM_STORE scan (500 rows).

**Evidence (RUST_LOG).** **7 scans across 6 dimensions:** DIM_CUSTOMER `[CUSTOMER_KEY, FULL_NAME]` (390K), DIM_ACCOUNT (15K), DIM_PRODUCT (37.5K), DIM_SUPPLIER (2K), DIM_EMPLOYEE (5K), DIM_STORE `[CHANNEL, STORE_KEY, STORE_NAME, STORE_TYPE]` (500), + a 7th DIM_STORE `[STORE_KEY]` scan. **Sum of est-rows = 450,000** (matches F8's `estimated_row_count`). Cold, each of the 6 is a separate `loadTable` (~2 s OAuth/catalog) ⇒ the 20.8 s cold wall.

**Dominant cost center.** The star's **base predicate is `edw:name`**, which is **shared across all six name-bearing dimensions** (`FULL_NAME`/`ACCOUNT_NAME`/`PRODUCT_NAME`/…). TriplesMap resolution in `build_progress` filters by the base predicate only (`operator.rs:595-610`), so it resolves **6 maps**; the class `edw:Store` is **not fused** into the star because `class_fusion_is_safe` (`rewrite.rs:604-624`) correctly refuses (not every name-map declares Store), so the class runs as the separate 7th subject-only DIM_STORE scan. The 5 non-Store scans are pure dead work — their subject IRIs (different templates) never join the Store subjects, and they lack `channel`/`storeType` so they materialize nothing.

**Classification: `extend-existing-strategy`.** Two viable fixes (inventory §1/§2): **(a)** choose the star's base predicate by **selectivity** — resolve TriplesMaps by the intersection of *all* star-member predicates (a map must have `name` ∧ `channel` ∧ `storeType`), which alone prunes to DIM_STORE since `channel`/`storeType` are Store-exclusive; or **(b)** when a `?s a Class` is co-located, **class-constrain the star's map resolution** (scan only maps declaring the class) even when full fusion is refused — the subsequent join with the class scan makes this sound. Either eliminates 5 of 6 loadTables ⇒ ~3× cold-latency win on the most common BI shape.

---

## Deep-dive 6 — confirmation toggles (kill-switch A/B)

| Toggle | Baseline | Toggled | Verdict |
|---|---|---|---|
| **q045** `FLUREE_R2RML_LIMIT_PUSHDOWN=0` | 2.47 s (10 rows, fresh-process) | **DNF @ 75 s** | **H2 confirmed load-bearing.** The LIMIT budget pushdown (§5) is *exactly* why pure-LIMIT is fast; without it the full 7,670-file scan runs. This is the same budget the ORDER BY/DISTINCT/GROUP BY modifiers absorb. |
| **q022** `FLUREE_FUSED_R2RML_AGG=0` | 2.63 s | 3.90 s (**1.48×**) | **H6 fused-agg confirmed a real win.** Single-table GROUP BY takes the fused path and saves ~⅓ by folding from column batches instead of materializing bindings. (Consistent with the native 1.52× ratio in F6.) |

(Reminder honored: `FLUREE_FUSED_R2RML_AGG` matches exactly `"0"`/`"false"` — inventory §0 note.)

---

## Hypothesis verdicts (H1–H8)

| H | Verdict | Evidence | Refinement |
|---|---|---|---|
| **H1** fact decode wall | **CONFIRMED — the master bottleneck** | ~39 files/s across q046/q008/q036; 7,670 files/fact | **File-count-bound, not row-bound:** 23 rows/file (tiny Snowflake micro-partitions); ~200 ms fixed per-file cost × 7,670 / 8-way concurrency ≈ 197 s. Decode concurrency **clamped at 8** (`r2rml.rs:36-52`) — a lever. Dominates so hard that fact queries never reach downstream operators. |
| **H2** budget-absorb modifiers | **CONFIRMED** | q045 toggle DNF@75s; q046 = same full scan | The pure-LIMIT budget path works and is load-bearing; ORDER BY/DISTINCT/GROUP BY absorb it. ORDER BY+LIMIT is **not budget-fixable** — needs scan-side top-k (Deep-dive 1). |
| **H3** correlated-join rebuild | **CONFIRMED — but shape-specific** | q050: SUPPLIER ×153 + PRODUCT ×78 (no cross-batch memoization) | Bites when the inner is **re-scanned per batch** (OPTIONAL / correlated refs). For q008 the parents were small and built once — **no churn**. So H3 is an OPTIONAL/correlated-ref cost, not universal. |
| **H4** decimal/double pruning blind | **PARTIALLY CONFIRMED** | F6: q011 date FILTER pruned **7,579/91 files** (98.8%) — pruning path works | The decimal-blind case (q019) was **not** toggled this round; it needs a `files_pruned` counter on `scan_plan` (F7 gap). **WP8 pre-task.** |
| **H5** no COUNT manifest shortcut | **CONFIRMED — dramatic** | q036: `estimated_row_count=180,000` in the plan, yet 7,670-file decode | The answer is already computed (`stats.rs:120-133`) and discarded. ~199 s → sub-second. Highest-ROI fix. |
| **H6** agg+join misses fused path | **CONFIRMED — but secondary** | q022 toggle 1.48× (fused is real); q008 joined GROUP BY declines it | Real win on single-table; on joined rollups it is **gated behind the H1 decode wall** (q008 never reaches the group phase). Fix H1 first. |
| **H7** cold/warm structure | **CONFIRMED** | F8 cold q001 = 6 `loadTable` (~12 s); ~2 s OAuth/catalog each | The `loadTable` fixed cost is the cold penalty, **multiplied by the over-scan** (F8): 6× fan-out = 6× cold catalog cost. |
| **H8** non-lowered forms | **CONFIRMED (correctness + perf)** | F1/F2 (transitive path, subquery) silently empty; q040 VALUES DNF | Non-lowered forms either **silently return wrong answers** (transitive path/subquery, F1/F2 — the worst outcome) or **full-scan** (VALUES). |

---

## Cost-center ranking → WP8 pre-tasks

Ranked by ROI (impact ÷ risk). All are `extend-existing` except where noted.

1. **H5 — manifest `COUNT(*)` shortcut.** Return the manifest `record_count` sum for a bare unfiltered COUNT (no FILTER/join). ~199 s → sub-second. Trivial, low-risk — the sum is already computed (`stats.rs:120-133`). **`extend-existing`.**
2. **H1 — the 7,670-tiny-file decode wall.** The dominant cost for *every* fact query. Levers, in order of tractability: (a) **raise the decode-concurrency cap** (`r2rml.rs:52` clamps to 8; the env override is uncapped — tune for the high-latency remote store, S3-fan-out-bounded); (b) **manifest/row-group-count fast paths** for aggregates that don't need row values; (c) **file compaction** (data-side, out of engine scope but the biggest structural lever — 7,670 → tens of files). **`extend-existing`** (a,b); data-side (c).
3. **F8 / §2 — typed-dim-star over-scan.** 6× fan-out on the single most common BI shape (typed dimension list). Selectivity-aware base predicate or class-constrained star resolution (Deep-dive 5). ~3× cold-latency win. **`extend-existing`.**
4. **H3 — parent-lookup cross-batch memoization.** Extend the §8 `scan_cache` to the parent-scan path so OPTIONAL/correlated refs stop re-scanning dims per batch (q050: ~377 → ~2 scans). **`extend-existing`.**
5. **H2 — scan-side top-k for ORDER BY+LIMIT.** Push sort-key + `k` into the reader (bounded heap + row-group pruning by the running k-th bound). **`new-strategy`** — the only non-extend item in the top 5, and the only fix for the ORDER-BY-DNF class.
6. **H6 — fused-aggregate-over-single-join.** Extend the fused path to admit one RefObjectMap join so `Fact⋈Dim GROUP BY` folds without materializing bindings. **`new-strategy`, deferred** — invisible until H1 is fixed.

**WP8 pre-tasks (need a code-side span before they can be closed, per F7):** (i) a `files_pruned`/`row_groups_pruned` counter on `scan_plan` to quantify H4-decimal (toggle q019 vs the int/date controls q021/q020); (ii) a `rows_decoded` vs `rows_emitted` reader counter to separate H1 decode-volume from per-file overhead definitively. Neither was patched this round (diagnosis-only).

**No `wrong-turn-redesign` found.** Every strategy in the inventory is directionally correct; the gaps are missing extensions (H5 shortcut, H1 concurrency, F8 selectivity, H3 memoization) and two genuinely new capabilities (scan-side top-k, fused-agg-over-join). The pathway's architecture is sound; it is under-optimized for the 7,670-tiny-file reality and the shared-predicate star shape.
