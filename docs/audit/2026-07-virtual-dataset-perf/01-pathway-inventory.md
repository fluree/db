# Virtual-Dataset (Iceberg / R2RML) Query Pathway — Performance Strategy Inventory

**Date:** 2026-07-10
**Branch:** `bench/virtual-dataset-corpus` (worktree `db-vbench`), at `a5528e880`
**Scope:** Every performance strategy on the SPARQL/FQL → R2RML → Iceberg virtual-dataset read path: pattern rewriting (`fluree-db-query/src/r2rml/`), the correlated scan operator, the Iceberg Parquet reader + pruning (`fluree-db-iceberg/`), and the REST/moka/disk cache tiers (`fluree-db-api/src/graph_source/`, `fluree-db-core/src/disk_cache.rs`).
**Method:** Every load-bearing claim was read directly against source in this worktree and is tagged `[verified <file:line>]`. Where a claim in the source inventory was wrong or imprecise, it is corrected here and flagged in §0. The claims that drive the roadmap ranking (H1, H2, H5, and the §0 corrections) additionally survived an **adversarial second pass** — an attempt to *falsify* each rather than re-confirm it (§0.1 records what that pass changed). Line numbers are current as of `a5528e880`.

This document is the reference substrate for `02-hypothesis-map.md` (H1–H8) and the perf roadmap that follows. Precision over prose: cite the anchor, not the vibe.

---

## 0. Corrections to the source inventory (read first)

The exploration pass that seeded this document made seven claims that verification changed. Three are **substantive** (a wrong name, a wrong symbol location, a wrong API shape); three are **location** fixes that matter because the roadmap will edit these exact sites; one (C2) is a **refinement that leaves the original claim standing** for a sharper reason than stated. The strategy semantics are otherwise sound.

| # | Claim as received | Correction | Anchor |
|---|---|---|---|
| C1 **(substantive)** | Wildcard/class-fusion kill switch is `FLUREE_R2RML_WILDCARD_CLASS_FUSION` | The env var is **`FLUREE_R2RML_CRAWL_CLASS_FUSION`**. The reader fn is *named* `wildcard_class_fusion_enabled()` but reads the `CRAWL` var; it is also read (coupled with crawl-expand) in `crawl.rs`. A/B scripts using the received name would be **no-ops**. | `rewrite.rs:634`; `crawl.rs:335` |
| C2 **(refined — claim stands)** | `!=` never prunes | **Correct on the R2RML path** — and for a sharper reason than the phrasing implied. No `!=` scan filter is ever *produced*: `cmp_op` returns `None` for `!=` (`rewrite.rs:452`) and `build_scan_filters` emits **only** `ScanCmpOp::Eq` (`operator.rs:443, 469`). The shared pruning engine *does* support a degenerate `!=` prune (`min==max==lit`, `pruning.rs:192-196`) and `build_iceberg_filter` maps `NotEq→NotEq` (`r2rml.rs:88`), but **both are unreachable from R2RML** (no producer). My first pass wrongly implied `!=` pruning was live here; the adversarial pass (grep for `ScanCmpOp::NotEq` producers) found none. | produce-nothing: `rewrite.rs:452`, `operator.rs:443,469`; dead branches: `pruning.rs:192-196`, `r2rml.rs:88` |
| C3 **(substantive)** | `ScanValue = Int/Bool/Date/Str ONLY`, defined near `pruning.rs` | `ScanValue` lives in **`fluree-db-query/src/r2rml/provider.rs:43-56`** and has **five** variants: `Bool`, `Int(i64)`, `Date(i32)`, `Str(String)`, **`TemplateKey(String)`**. The "no Decimal / no Double" point holds (both are operator-only); `TemplateKey` (reversed subject-key) was omitted. | `provider.rs:43-56` |
| C4 **(substantive)** | Per-query snapshot pin via `IcebergCatalogSession.load_tables` *method* | `load_tables` is a **field** (`Mutex<HashMap<..>>`), not a method. The pin is served by `cached_load_table` / `store_load_table` / `pinned_metadata_location`. Behavior: the `metadata_location` is pinned on the **first** store and never changed; a later store (creds refresh) mutates **only** `credentials`. | `catalog_session.rs:88, 99, 115, 131` |
| C5 *(location)* | `scan_table_inner` spans `r2rml.rs:754-859` | `scan_table_inner` starts at **`r2rml.rs:688`** and runs to ~1220; `754-859` is only the REST-catalog arm inside it. The three-tier dispatch is at `792-796`. | `r2rml.rs:688, 792-796` |
| C6 *(location)* | Disk-cache key `(source path + size)` composed in `disk_cache.rs` | `DiskArtifactCache` is **generic** (keyed by the `target: PathBuf` the caller passes). The parquet `(path,size)` key is composed by the **caller**: `iceberg-{xxh64(source_path)}-{expected_size}.parquet`. | `send_parquet.rs:126-129` |
| C7 *(clarify)* | `set_row_budget` no-op default at `operator.rs:98` | Correct — but the file is **`fluree-db-query/src/operator.rs:98`** (the `Operator` trait), not `r2rml/operator.rs`. The trait doc there explicitly enumerates the absorb set. | `operator.rs:81-98` |

Additionally, the received env-switch list was **incomplete**. Five switches that gate this pathway were missing; they are added to the master table in §13: `FLUREE_R2RML_MATERIALIZE_WINDOW_ROWS`, `FLUREE_ICEBERG_LOADTABLE_CACHE`, `FLUREE_ICEBERG_SCAN_CONCURRENCY`, `FLUREE_R2RML_CRAWL_EXPAND`, and the corrected `FLUREE_R2RML_CRAWL_CLASS_FUSION`.

One consistency note for A/B tooling: the fused-aggregate switch matches **exactly** `Ok("0" | "false")` (`fused_aggregate.rs:283-286`), while every other R2RML switch trims + lowercases and also accepts `off` (and, for crawl fusion, `no`). `FLUREE_FUSED_R2RML_AGG=off` or `=FALSE ` therefore does **not** disable the fused aggregate.

### 0.1 What the adversarial pass changed

Four load-bearing claims were stress-tested by trying to break them. Two survived unchanged and stronger, one was refined (C2, above), one had its memory characterization corrected:

- **H1 decode wall — survived, stronger.** `with_row_filter` / `RowSelection` appear **only in comments** across the whole `fluree-db-iceberg` crate (`arrow_reader.rs:10, 136-137`); grep finds **zero** call sites. There is no conditional or flagged row-level-filter path — the decode wall is unconditional.
- **H2 budget absorb — survived, but "fully materialize" was wrong for ORDER BY.** `ORDER BY … LIMIT k` is compiled to a **streaming top-k** `SortOperator` (bounded heap of `k` (+offset) rows), not a full sort buffer (`operator_tree.rs:3308-3345`, `SortOperator::new_topk` `:3316, 3339`). But top-k **forwards no budget** (no `set_row_budget` in `sort.rs`) and *must* see every input row to rank, so the R2RML scan below is **still fully drained** — H2's scan-cost claim holds; only §12's memory wording is corrected. The native order/limit fast paths (`fast_star_const_order_topk.rs`, `fast_post_order_limit.rs`) are `fast_path_store`-gated (native binary-index only, `fast_star_const_order_topk.rs:50`) and never fire on an Iceberg source, so they don't rescue this case. See §12 for the resulting **fixable-vs-scan-bound** split of the four modifiers.
- **H5 COUNT gap — survived.** The `R2rmlTableProvider` trait exposes **only** `scan_table` (`provider.rs:150-171`) — no count / row-count entrypoint. The `estimated_row_count` values (`r2rml.rs:1069-1149`) are scan-plan estimates for logging and window sizing, **not** a COUNT answer. There is no shortcut to miss.

---

## 1. Framing: the workload this pathway serves

Live Snowflake-managed-Iceberg profile (from the 2026-06-30 handoff; treat as reported, not code-derived):

- **Dimensions:** ~2.13M rows, ~36 MB, **1 data file** per table.
- **Facts:** ~550M rows, ~10.5 GB, **~7,670 data files** per table (~90 KB per file avg at SF20).
- **Measured baselines:** a simple typed-dimension query was 18.4 s / 6 scans before the bplatz tier merges, now ~3 s / 1 scan; a `Store ⋈ Geography LIMIT 10` was 165.7 s / 36 scans.

The bplatz tier-1/2/3 caches merged via `#1406`/`#1411`/`#1413`. This branch adds: crawl routing + class-constrain (`d4d993071`), FK-templated crawl refs + type-var fusion (`6fdcedb80`), bound-subject TriplesMap prune + crawl `OFFSET` + cancellation (`a5528e880`), and hot-path trace spans (`988f44ab3`).

The strategies below exist because of two structural facts: (a) the reader **cannot** apply a row-level filter during decode (§5, the DELTA_BINARY_PACKED wall), so every surviving row group is fully decoded; and (b) materialization explodes compact columnar batches into fat `Binding` rows (a full 6M-row scan is ~14 GB — `operator.rs:130-135`), so the operator must stream in bounded windows. Almost every optimization is an attempt to **scan fewer files** or **materialize fewer rows**.

---

## 2. Strategy 1 — Star fusion (same-subject predicate coalescing)

**What it does.** Multiple same-subject triples with a variable subject and constant predicates are accumulated by subject and fused into **one** `R2rmlScanOperator` — `star_bindings` (var-object members, one column each) plus `star_constraints` (constant-object equality members) — eliminating the O(N²) self-join a per-predicate plan would build.

**Anchors.**
- Accumulate by subject: `[verified rewrite.rs:105-181]` (grouping into `star_groups` at `:114-127`).
- Emit fused scan: `[verified rewrite.rs:188-235]` — partition var/const members `:191-192`, distinct-object-var guard `:206-215`, build `star_constraints` `:217-220`, set `star_bindings` `:224-233`.
- Eligibility predicate: `star_member_subject` `[verified rewrite.rs:527-539]`.
- Operator consumes the fused predicate set: `pattern_predicates` `[verified operator.rs:354-366]`, schema build `[verified operator.rs:294-298]`.

**Trigger conditions.** ≥2 same-subject triples, variable subject, constant predicates, and **distinct** fresh object vars (or constant-object equalities). First-seen order preserved.

**Bypass conditions.** Bound subjects (`subject_var = None` → never grouped, `rewrite.rs:113-114`); variable predicates; **shared / non-distinct** object vars (a self-join, not a star — `:206-215`); single-member groups (fall to the normal single-object path); members carrying a class or `triples_map_iri` filter (excluded by `star_member_subject`).

**Kill switch.** None dedicated. (Star grouping is structural; the correlated-scan caching it feeds is separately switchable — §8.)

---

## 3. Strategy 2 — Class fusion + subject-only prune + wildcard class-constrain

**What it does.** Three related moves collapse `rdf:type` work:
1. A lone same-subject `?s a Class` is fused into that subject's star by setting the star base's `class_filter`, removing a separate correlated class re-scan — **only when provably safe**.
2. A class that is neither star- nor wildcard-fused falls back to a **subject-only scan**: the operator projects only subject columns and scans **no** RefObjectMap parents.
3. A standalone wildcard (`?s ?p ?o`, injected by the browse crawl) is **class-constrained** — its TriplesMap fan-out is pruned to the queried class (16→1 for a per-table Iceberg mapping). On the crawl path a co-located `?s a ?type` is **merged** into the wildcard (one budgeted scan) when exactly one type-var exists.

**Anchors.**
- `fuse_class_if_safe` `[verified rewrite.rs:572-596]`; safety predicate `class_fusion_is_safe` (every predicate-map must also declare the class) `[verified rewrite.rs:604-624]`.
- Subject-only detection `is_subject_only_pattern` `[verified operator.rs:382-386]`; subject-only projection + no-parent path `[verified operator.rs:673-689, 797-803]`.
- Wildcard fusion `try_fuse_wildcard_class` `[verified rewrite.rs:688-770]`; type-var merge decision `do_merge = crawl_active && type_var_count == 1` `[verified rewrite.rs:724-728]`; subject-template disjointness safety `wildcard_class_fusion_is_safe` `[verified rewrite.rs:791-824]`.
- Reasoning refusal `[verified rewrite.rs:699-701]`.

**Trigger conditions.** (1) A single class per subject co-located with a star whose base predicate co-locates with the class in the same TriplesMap. (3) `crawl_active` (from `ExecutionContext::trust_fk_refs`), a standalone wildcard present, mapping available, and all non-class TriplesMaps prefix-disjoint from the class's subject template.

**Bypass conditions.** `reasoning_active` (exact `rr:class` match could drop a superclass-entailed subject — `rewrite.rs:699-701`); mapping unavailable (`:706-708`); vertically-partitioned mapping where a predicate/subject map lacks the class (`class_fusion_is_safe` / `wildcard_class_fusion_is_safe` return false); a column/constant subject map (can't prove disjointness); >1 class on the subject; >1 type-var (can't merge into one `Option<VarId>`). On refusal the class runs as its own subject-only scan — the always-correct pre-fusion path.

**Kill switch.** `FLUREE_R2RML_CRAWL_CLASS_FUSION` `[verified rewrite.rs:632-641]` **(corrected — C1)**. Coupled: with fusion off, crawl expansion is also forced off (`rewrite.rs:626-631` doc; `crawl.rs:335`), because an unfused crawl is a 16-table fan-out + shared-catalog 429 storm.

---

## 4. Strategy 3 — Three-tier REST catalog caching

**What it does.** Cuts the OAuth + `loadTable` round-trips that dominate cold latency to at most one per (config, table) per TTL window. Three tiers, cheapest first.

**Anchors (dispatch order at `[verified r2rml.rs:792-796]`, inside `scan_table_inner` `[verified r2rml.rs:688]` — C4/C5):**
1. **Per-query snapshot pin** — `IcebergCatalogSession` `load_tables` **field** (`Mutex<HashMap>`) `[verified catalog_session.rs:88]`; read `cached_load_table` `[verified catalog_session.rs:99]`, write `store_load_table` `[verified catalog_session.rs:131]`. `metadata_location` pinned on first store, only `credentials` updated on refresh → every scan in one query reads one Iceberg snapshot. Consulted at `r2rml.rs:796`.
2. **Process-wide `rest_load_tables`** moka, TTL **60 s** default (`DEFAULT_REST_LOADTABLE_TTL_SECS` `[verified cache.rs:42]`; env `FLUREE_ICEBERG_LOADTABLE_TTL_SECS` `[verified cache.rs:46]`), **creds-expiry gated** — a hit with expired vended creds is invalidated and treated as a miss `[verified cache.rs:296-299]`. Consulted at `r2rml.rs:805-806`.
3. **Real REST load** — `catalog.load_table` `[verified r2rml.rs:820]`, which populates both the cross-query cache and the per-query pin `[verified r2rml.rs:836, 858]`.

Underneath, a `rest_clients` moka caches the authenticated client itself: **max capacity 64** `[verified cache.rs:173]`, TTL **900 s** default (`DEFAULT_REST_CLIENT_TTL_SECS` `[verified cache.rs:62]`; env `FLUREE_ICEBERG_REST_CLIENT_TTL_SECS` `[verified cache.rs:66]`), keyed by `graph_source_id + \u{1f} + fingerprint(raw config JSON)` `[verified r2rml.rs:69-71]` (`config_fingerprint` `[verified r2rml.rs:57]`).

**Trigger conditions.** Any Iceberg-backed R2RML scan. Tier 1 always; tiers 2/3 gated by the master switch below.

**Bypass conditions.** `FLUREE_ICEBERG_LOADTABLE_CACHE` off (master switch, `catalog_session.rs:34`) disables all catalog caching; a TTL env set to `0` disables that specific layer (`rest_loadtable_ttl_secs()==0` short-circuits, `cache.rs:292`).

**Hazard (verified, mitigated).** Because the client key is a fingerprint of the config **text**, rotating a secret referenced by that config does **not** change the key — the stale client + cached token serves 401s until the TTL evicts it. The TTL is the mitigation (self-heal ≤900 s). Documented `[verified cache.rs:52-60]`.

---

## 5. Strategy 4 — LIMIT row-budget pushdown

**What it does.** A top-of-tree `LIMIT` seeds an advisory `row_budget` down the **row/order-preserving** operator chain so an eager scan can stop early instead of draining the whole table. In the R2RML operator the budget caps the materialize window and exhausts the operator once `emitted` reaches it.

**Anchors.**
- Trait default is **ABSORB / no-op** `[verified operator.rs:98]` — the trait doc `[verified operator.rs:81-98]` explicitly names the absorb set: *Bind, Filter, Sort, Distinct, GroupAggregate, hash-join build*.
- Forwarders (the only overrides that pass a budget to children): `Limit` seeds child before open `[verified limit.rs:80-95]`; `Offset` forwards `budget + offset` `[verified offset.rs:145-148]`; `Project` forwards unchanged `[verified project.rs:58-60]`; `Graph` threads it into the per-parent inner subplan `[verified graph.rs:270-272, 407-413, 512-518]`.
- R2RML operator: `row_budget` field `[verified operator.rs:237-243]`; `set_row_budget` override that records but does **not** forward to child (correlated / not row-preserving) `[verified operator.rs:1885-1895]`; window cap to remaining budget `[verified operator.rs:917-929]`; emit-on-budget-met (with a re-check after the consumed filter) `[verified operator.rs:1957-1985]`.

**Trigger conditions.** A `LIMIT` whose path to the scan is **entirely** row/order-preserving (`Limit`/`Offset`/`Project`/`Graph`).

**Bypass conditions — the critical gap.** Any absorbing operator between the `LIMIT` and the scan swallows the budget, and the scan drains fully:
- `SortOperator`, `DistinctOperator`, `GroupAggregateOperator`, `UnionOperator` have **zero** `set_row_budget` overrides (confirmed by grep of `fluree-db-query/src/`; only `operator.rs:98`, `offset.rs`, `join.rs`, `limit.rs`, `graph.rs`, `project.rs`, `r2rml/operator.rs` define one). So `ORDER BY` / `DISTINCT` / `GROUP BY` / `UNION` between a `LIMIT` and an R2RML scan ⇒ full scan.
- `join.rs` **absorbs**: it records the budget only to cap its own first accumulator flush (`batched_flush_threshold`) and does **not** forward to `self.left` `[verified join.rs:1114-1127]`.
- The R2RML operator does **not** forward to its own child `[verified operator.rs:1886-1891]` (the child seeds a correlated scan, not row-preserving).
- `distinct.rs` streams a per-row dedup but drains its child batch-by-batch `[verified distinct.rs:132-153]`.

**Kill switch.** `FLUREE_R2RML_LIMIT_PUSHDOWN` `[verified operator.rs:149-158]` (only `0`/`false`/`off` disable; restores full-window materialization under a LIMIT).

---

## 6. Strategy 5 — Row-group pruning + predicate pushdown (Iceberg reader)

**What it does.** Before decoding a Parquet file, drop row groups whose column statistics prove they cannot satisfy the pushed comparisons; after decode, apply an exact `filter_record_batch` mask. Prunable comparison constants are **date / int / bool / string** only.

**Anchors.**
- FILTER → pushdown collection `collect_pushdowns` `[verified rewrite.rs:415-441]`; op mapping `cmp_op` (`!=` returns `None`) `[verified rewrite.rs:445-466]`; constant → `ScanValue` `to_scan_value` `[verified rewrite.rs:511-519]`.
- `ScanValue` def (5 variants, C3) `[verified provider.rs:43-56]`.
- Filter → Iceberg `Expression` `build_iceberg_filter` `[verified r2rml.rs:77-152]`.
- Surviving-group selection `surviving_row_groups` `[verified send_parquet.rs:64-88]`; `row_group_can_contain` `[verified pruning.rs:211-259]`; `bounds_can_contain` (per-op) `[verified pruning.rs:184-204]`; `prunable_stats` `[verified pruning.rs:272-283]`; `stat_bounds` `[verified pruning.rs:289-321]`.
- Decode path `decode_batches_arrow` `[verified arrow_reader.rs:71-229]`; post-decode mask `[verified arrow_reader.rs:186-191]`.

**Type blindness (H4 fuel).**
- **Decimal** → operator-only twice over: `const_object` yields `ObjectConstant::Decimal` (no scan filter, `rewrite.rs:501`), and `prunable_stats` returns `None` for a decimal column `[verified pruning.rs:279-281]`.
- **Double/Float** → operator-only (`rewrite.rs:502`) and, even if a stat survived, `stat_bounds` has **no** Float32/Float64 arm → `_ => (None,None)` `[verified pruning.rs:319]` (conservative keep).
- **Date** pushed only against a physically-`date` column, else skipped `[verified r2rml.rs:102-105]`.
- **Int** outside i32 on an `int` column skipped (no silent `as`-wrap) `[verified r2rml.rs:110-114]`; `TemplateKey` int→int/long/decimal, string→string, else skip `[verified r2rml.rs:127-138]`.
- **`!=`** (C2): never prunes on this path — no `NotEq` scan filter is ever produced (`cmp_op` returns `None` `[verified rewrite.rs:452]`; `build_scan_filters` emits only `Eq` `[verified operator.rs:443, 469]`). The engine's degenerate-`!=` prune (`min==max==lit` `[verified pruning.rs:192-196]`) and `build_iceberg_filter`'s `NotEq` arm `[verified r2rml.rs:88]` exist but are unreachable from R2RML (no producer).

**The decode wall (H1's root).** Arrow's `with_row_filter` / `RowSelection` is **deliberately unused**: `RowSelection.skip_records` panics on Snowflake's `DELTA_BINARY_PACKED` integer columns in parquet-rs 54 (`DeltaBitPackDecoder::skip`) `[verified arrow_reader.rs:135-143]`. Consequence: **every surviving row group is fully decoded**, then masked (`:186-191`). Pruning operates at row-group granularity only; there is no row-level skip. The in-engine `FILTER` stays authoritative for correctness.

**Trigger conditions.** A conjunctive `?var <op> const` FILTER (or a scalar constant-object equality) over a plain single-column `rr:column` object map, on a date/int/bool/string column with row-group statistics present.

**Bypass conditions.** Decimal/double predicates; `!=` from the FILTER path; date-on-string / int-overflow columns; a predicate that maps to >1 object map or a non-column object map (`build_scan_filters` `operator.rs:405-420`); missing statistics (conservative keep); any file whose surviving groups still contain the needle (decode is unavoidable).

**Kill switch.** `FLUREE_ICEBERG_PREDICATE_PUSHDOWN` `[verified send_parquet.rs:50]` (fn `predicate_pushdown_enabled` at `:47`).

---

## 7. Strategy 6 — Bound-subject pushdown + TriplesMap prefix-prune

**What it does.** A bound subject IRI (`<iri> ?p ?o`, the UI subject inspector) is (a) reversed through each TriplesMap's subject template to an equality on the key column so Iceberg can prune to the subject's rows, and (b) used to **skip whole TriplesMaps** whose constant template prefix the IRI does not start with — turning a fan-out over every table into a scan of the subject's own table.

**Anchors.**
- Reverse-template scan filter in `build_scan_filters` `[verified operator.rs:450-474]` (whole fn `392-476`); emits `ScanValue::TemplateKey` at `:470`.
- `TemplateKey` physical-type coercion `[verified r2rml.rs:127-138]` (int→int; int→long/decimal; string→string; else skip — the operator still enforces subject equality).
- TriplesMap prefix-prune (necessary-condition skip) `[verified operator.rs:611-630]`; column/constant subject maps kept `[verified operator.rs:622-623]`; `constant_prefix` helper `[verified rewrite.rs:830-835]`.

**Trigger conditions.** `subject_constant` set and the TriplesMap has a template subject with a resolvable key column. Prefix-prune fires for any template-subject TriplesMap.

**Bypass conditions.** Column/constant (non-template) subject maps (kept — can't prove disjointness or reverse); template shapes `reverse_subject_template` can't unambiguously invert; physical types other than int/long/decimal/string (pushdown skipped, operator enforces). A bound *subject* pattern is never star/class-eligible (§2, §3).

**Kill switch.** Shares `FLUREE_ICEBERG_PREDICATE_PUSHDOWN` for the *applied* filter (§6); the prefix-prune itself has none (it is a correctness-neutral necessary-condition skip).

---

## 8. Strategy 7 — `trust_fk_refs` parent-scan skip (crawl-only)

**What it does.** On the trusted browse-crawl path, a RefObjectMap's parent subject IRI is rendered **directly from the child row's own FK columns** via the parent's subject template — skipping the parent-table scan and existence check entirely. A matched FK renders a byte-identical IRI to the scan path; a dangling FK renders the templated IRI instead of dropping the triple (acceptable for browse).

**Anchors.** `RefShortcut` struct + doc `[verified operator.rs:67-84]`; `build_ref_shortcut` `[verified operator.rs:97-126]`; application (shortcut vs scan) `[verified operator.rs:846-858]`; gate `ref_template_shortcut` (true-wildcard shape only) `[verified operator.rs:782-785]`.

**Trigger conditions.** `ctx.trust_fk_refs` true, the injected true-wildcard scan (`object_var` set, no predicate filter, no star members), and a parent subject that is a **pure IRI template whose placeholder columns are all the single-column FK** join columns.

**Bypass conditions.** General queries (`trust_fk_refs` false by default — §12); composite FKs (`join_conditions.len() != 1`, `operator.rs:103`); non-template / column / constant / blank-node parent subjects (`:99`); a template placeholder that is not an FK column (`:117-122`); predicate-filtered or star/predicate-list crawls (`:782-785`). Falls back to the authoritative parent scan.

**Kill switch.** None dedicated; gated entirely by `trust_fk_refs` (set only by the crawl, via `QueryExecutionOptions`).

---

## 9. Strategy 8 — Join strategy (correlated nested-loop, child-always-build-side)

**What it does.** The R2RML operator is a correlated scan: for each buffered child batch it resolves the TriplesMap(s), scans the Iceberg table (streamed, windowed), and joins produced rows against the child. The **child** side is always indexed into a `full_index` HashMap (the build side); the Iceberg side is always the streamed probe.

**Anchors.** Module doc `[verified operator.rs:8-31]`; `JoinPlan` enum (only `Cross` / `Hash`, child indexed) `[verified operator.rs:166-179]`; `build_join_plan` `[verified operator.rs:514-564]`; per-child setup `build_progress` `[verified operator.rs:570-938]` (parent-lookup building `769-903`); `build_parent_lookup` call `[verified operator.rs:900]`; probe `emit_produced_window` `[verified operator.rs:1157-1238]`.

**Costs / non-optimizations (H3 fuel).**
- Parent lookups are **not memoized across child batches**: `parent_lookups` is a local map rebuilt every `build_progress`, stored per `TmStream`; the parent scan `[verified operator.rs:889-897]` bypasses the main-table scan cache.
- There is **no** `record_count`-driven build-side selection — the child is *always* the build side regardless of cardinality (JoinPlan has no size input). Confirmed by grep: no `record_count` reference in `fluree-db-query/src/r2rml/`.
- Mitigations that exist: parents assumed small and collected fully into the lookup `[verified operator.rs:887-898]`; same-parent POMs share a `LookupCacheKey` so a second POM doesn't re-scan `[verified operator.rs:826-833]`; the main-table scan cache reuses unfiltered+unbudgeted inner scans across child batches, capped at one materialize window `[verified operator.rs:713-734]` (fields `:228-236`).

**Trigger conditions.** Any R2RML scan feeding a join or correlated under a `GRAPH` seed.

**Bypass conditions.** The scan cache is bypassed for **filtered** scans (a pruned subset must not replay for another filter) and **budgeted** scans (caching collects a full window, defeating the LIMIT) `[verified operator.rs:720-733]`.

**Kill switch.** `FLUREE_R2RML_SCAN_CACHE` (inner-scan cache) `[verified operator.rs:1059-1068]`; `FLUREE_R2RML_MATERIALIZE_WINDOW_ROWS` sizes the window (default 512K) `[verified operator.rs:138-144]`.

---

## 10. Strategy 9 — Disk + moka caches (whole-file + metadata)

**What it does.** Two independent layers below the REST tier (§4): a set of moka in-process caches for compiled mappings / table metadata / scan files / footers, and a process-global on-disk cache of whole Parquet files.

**Anchors.**
- `R2rmlCache` fields `[verified cache.rs:89-129]`: `compiled_mappings` (cap 64, no TTL, `:91`), `table_metadata` (cap 128, `:95`), `scan_files` (`:99`), `parquet_footers` (an `Arc<ParquetFooterCache>`, **not** moka, sized `max(cap/2,32)`, `:104`), `direct_metadata_locations` (TTL **2 s**, hardcoded, `:110` + `:32`), `rest_clients` (§4), `rest_load_tables` (§4).
- Disk cache `DiskArtifactCache` (generic; built `for_dir(binary_store_cache_dir())`) `[verified r2rml.rs:987-988]`; process-global singleton by dir `[verified disk_cache.rs:180-190]`.
- Parquet key `iceberg-{xxh64(source_path)}-{expected_size}.parquet` composed by the caller `[verified send_parquet.rs:126-129]` (size-validated `:133-138`) **(C6)**.
- Byte budget from `FLUREE_DISK_CACHE_BUDGET_BYTES` (`0` disables writes; else configured, else 9/10 available disk) `[verified disk_cache.rs:216-243]`; **mtime-LRU** eviction `evict_until` sorts by `modified` ascending `[verified disk_cache.rs:313-348]` (`sort_by_key(|e| e.modified)` `:323`).
- Generation-checked removal (ABA guard on in-flight slots) `[verified disk_cache.rs:59-66, 442-447]`.

**Whole-file admission is const-gated (not env).** `send_parquet.rs`: `WHOLE_FILE_MAX_BYTES` 32 MB (`:99`), `WHOLE_FILE_MIN_SHARE_PCT` 50 (`:103`), `MAX_SPARSE_BUFFER_SIZE` 64 MB (`:95`), `MIN_SPARSE_FILE_BYTES` 1 MB (`:108`), `LARGE_FILE_ADMIT_MAX_BYTES` 256 MB (`:113`). These decide sparse-range vs whole-file reads and disk-cache admission.

**Trigger / bypass.** Metadata caches: any Iceberg scan (iceberg-gated). Disk cache: active when `budget_bytes() > 0`; a fact file above `LARGE_FILE_ADMIT_MAX_BYTES` is not whole-file-admitted.

**Kill switch.** `FLUREE_DISK_CACHE_BUDGET_BYTES=0` (disk); `FLUREE_ICEBERG_LOADTABLE_CACHE` (the moka catalog layer).

---

## 11. Strategy 10 — Fused R2RML aggregate

**What it does.** A single R2RML scan (no joins) under an implicit aggregate or `GROUP BY`-with-aggregates folds `COUNT`/`SUM`/`AVG` **directly from typed `ColumnBatch` values** — never building subject IRIs or per-row bindings.

**Anchors.** Detection `detect_fused_r2rml_aggregate` `[verified fused_aggregate.rs:281-396]`; operator-tree wiring (build fallback, wrap ORDER BY/OFFSET/LIMIT on top) `[verified operator_tree.rs:2438-2467]`; column fold loop `[verified fused_aggregate.rs:927-999]`; table scan `[verified fused_aggregate.rs:910]`.

**Trigger conditions.** `GRAPH <iri> { triples [+ one FILTER] }` + optional top-level desugared-aggregate `BIND`s; projection exactly = group-keys + agg outputs; any ORDER BY sorts only those.

**Bypass conditions.** DISTINCT aggregates (no dedup in the fold) `[verified fused_aggregate.rs:356-368]`; non-COUNT/SUM/AVG (`:359-365`); expression ORDER BY / synthetic sort var (`order_binds`, `:292`); `HAVING` or post-agg binds (`:311-313`); `GROUP BY` without aggregates (`:308-309`); a non-Triple / multi-FILTER inner pattern, i.e. OPTIONAL/UNION/subquery in the GRAPH (`:327-333`); a single non-grouped aggregate **with** a FILTER — declined as a cost guard, the normal pipeline's pruning + vectorized filter wins (`:349-351`).

**GAP (H5).** No Iceberg-manifest `record_count` shortcut for `COUNT(*)`: the fused path still `scan_table`s and folds row-by-row (`:910`, `:927-999`), even though the authoritative row count is free in manifest metadata — `aggregate_column_stats` already sums `df.record_count` `[verified stats.rs:120-133]`. The native PSOT-leaflet `COUNT` fast path (`fast_count.rs`, `operator_tree.rs:2469-2474`) is **native-index only** — it emits via `FastPathOperator` when `fast_path_store(ctx)` is available `[verified fast_count.rs:1-7]`, which an Iceberg source is not.

**Kill switch.** `FLUREE_FUSED_R2RML_AGG` `[verified fused_aggregate.rs:283-286]` — exact `"0"|"false"` match only (see §0 note).

---

## 12. Strategy 11 — ORDER BY / GROUP BY / DISTINCT (generic, non-R2RML-aware)

**What it does — and doesn't.** These sit as generic engine operators **above** the scan and are **not** R2RML-aware. **None overrides `set_row_budget`** (grep-confirmed, §5), so each is a budget sink: a `LIMIT` above them cannot early-terminate the R2RML scan below. The memory footprint varies (`ORDER BY … LIMIT` is a bounded top-k heap, not a full sort buffer — `operator_tree.rs:3308-3345`, `[verified sort.rs:544 new_topk]`), but the **scan is fully drained in every case**.

This is the structural reason H2 dominates the BI workload: the common analytical shape (`ORDER BY … LIMIT`, `SELECT DISTINCT … LIMIT`, `GROUP BY … LIMIT`, `UNION … LIMIT`) places a budget-absorbing operator between the LIMIT and the fact scan. But the four modifiers are **not equally hard to fix** — an adversarial look splits them:

| Modifier | Why the scan is full today | Roadmap tractability |
|---|---|---|
| `ORDER BY … LIMIT` | Top-k heap `[sort.rs:544]` must see **every** row to rank; forwards no budget | **Scan-bound.** Not budget-fixable — needs a scan-side top-k (heap pushed into the reader on a pushable sort key) or pre-sorted input |
| `GROUP BY … LIMIT` | Must see every row to form groups (unless sorted by group key) | **Scan-bound** (except the single-table fused-agg case, §11) |
| `SELECT DISTINCT … LIMIT` | `DistinctOperator` drains its child `[verified distinct.rs:132-153]`; no budget | **Tractable** — a distinct-aware budget could stop after `k` distinct rows are emitted |
| `UNION … LIMIT` | `UnionOperator` absorbs; branches are individually row-preserving | **Tractable** — the budget could be forwarded to each branch |

**Kill switch.** None (generic operators). The relevant lever is §5's `FLUREE_R2RML_LIMIT_PUSHDOWN` for A/B on the pure row-preserving chain (the control that isolates budget *plumbing* from the modifier *sink*).

---

## 13. SPARQL → R2RML routing

**What it does.** When a `GRAPH` targets an R2RML source, `rewrite_patterns_for_r2rml` converts the contained **triples** to `Pattern::R2rml` leaves; structural containers are recursed but themselves evaluated generically above the scan.

**Anchors.** Entry / routing `[verified graph.rs:227-258]` (second seeded variant `[verified graph.rs:383-397]`); rewriter `[verified rewrite.rs:83-309]`.
- **Recursed to R2RML leaves** (container evaluated generically above): `Optional`, `Union`, `Minus`, `Exists`, `NotExists`, `Service` `[verified rewrite.rs:140-160]`.
- **NOT converted** (preserved as-is): `Filter`, `Bind`, `Unwind`, `Values`, `Subquery`, `PropertyPath`, `ShortestPath`, `IndexSearch`, `VectorSearch`, nested `Graph`, `GeoSearch`, `S2Search`, `EdgeAnnotation`, `AnnotationTarget`, `DefaultGraphSource` `[verified rewrite.rs:162-179]`.
- **Unconvertible triples fail the whole scope.** A lang-tagged or custom (non-XSD) datatype object, or a bound object that can't convert, returns `None` from `convert_triple_to_r2rml` `[verified rewrite.rs:942-962]`; any `unconverted_count > 0` errors the entire GRAPH scope `[verified graph.rs:245-253]` (no ledger index to fall back to).

**Filter handling.** Scan-local FILTERs are **consumed** into the single scan's `consumed_filter` so the LIMIT budget can reach it — only when the group is purely R2RML scans + FILTERs and there is exactly one scan `[verified rewrite.rs:328-392]`. Metadata-read filters and unanalyzable expressions stay in-engine (fail-closed). Kill switch `FLUREE_R2RML_FILTER_CONSUMPTION` `[verified rewrite.rs:314-323]`.

**Non-lowered forms (H8).** `VALUES`, subqueries, and property paths are preserved (not converted), so they evaluate generically **above** a full scan — a `VALUES`-constrained subject does not become a bound-subject prune (§7) the way an equivalent bound-IRI pattern would.

---

## 14. Master kill-switch & tunable table

| Env var | Default | Effect | Anchor |
|---|---|---|---|
| `FLUREE_R2RML_FILTER_CONSUMPTION` | on | Fold scan-local FILTER into the scan (lets LIMIT reach it) | `rewrite.rs:314` |
| `FLUREE_R2RML_CRAWL_CLASS_FUSION` **(C1)** | on | Wildcard→class fusion; **off also forces crawl-expand off** | `rewrite.rs:634`; `crawl.rs:335` |
| `FLUREE_R2RML_CRAWL_EXPAND` | on | Master switch for browse-crawl expansion | `crawl.rs:328` |
| `FLUREE_R2RML_LIMIT_PUSHDOWN` | on | Row-budget pushdown into the scan | `operator.rs:151` |
| `FLUREE_R2RML_SCAN_CACHE` | on | Correlated inner-scan reuse across child batches | `operator.rs:1061` |
| `FLUREE_R2RML_MATERIALIZE_WINDOW_ROWS` | 512×1024 | Materialize window size (binding footprint cap) | `operator.rs:139` |
| `FLUREE_FUSED_R2RML_AGG` | on | Fused COUNT/SUM/AVG fold (exact `0`/`false` only) | `fused_aggregate.rs:284` |
| `FLUREE_ICEBERG_PREDICATE_PUSHDOWN` | on | Row-group + predicate pruning in the reader | `send_parquet.rs:50` |
| `FLUREE_ICEBERG_LOADTABLE_CACHE` | on | Master switch for all Iceberg catalog caching | `catalog_session.rs:34` |
| `FLUREE_ICEBERG_LOADTABLE_TTL_SECS` | 60 | Cross-query `loadTable` cache TTL (`0` disables layer) | `cache.rs:46` |
| `FLUREE_ICEBERG_REST_CLIENT_TTL_SECS` | 900 | REST client cache TTL (`0` rebuilds per query) | `cache.rs:66` |
| `FLUREE_ICEBERG_SCAN_CONCURRENCY` | (impl) | Data-file read concurrency (positive int, uncapped) | `r2rml.rs:37` |
| `FLUREE_DISK_CACHE_BUDGET_BYTES` | auto (9/10 disk) | Whole-file disk cache byte budget (`0` disables) | `disk_cache.rs:216` |

`trust_fk_refs` (§8) is **not** an env var — it is a `QueryExecutionOptions` field set only by the browse-crawl path, default `false` `[verified context.rs:159, 290; with_trust_fk_refs 673]`.

---

## 15. Where the strategies leave gaps (forward pointers to `02-hypothesis-map.md`)

- **No row-level filter during decode** (§6) → every surviving row group fully decoded → **H1**.
- **Four budget-absorbing operators** (§5, §12) between the dominant BI-shape LIMIT and the scan → **H2**.
- **Child-always-build-side + no cross-batch parent memoization** (§9) → **H3**.
- **Decimal/double pruning blindness** (§6) → **H4**.
- **No manifest `record_count` shortcut for COUNT(\*)** (§11) → **H5**.
- **Fused aggregate declines any join** (§11) → fact⋈dim rollups fall to full materialization → **H6**.
- **Cold OAuth + loadTable + footer/disk tiers** (§4, §10) → cold/warm ratio → **H7**.
- **VALUES / subquery / path not lowered** (§13) → generic eval over full scans → **H8**.
