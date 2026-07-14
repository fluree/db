# Virtual-Dataset (Iceberg / R2RML) Performance — Ranked PR Roadmap (WP8)

**Date:** 2026-07-11
**Branch:** `bench/virtual-dataset-corpus` (worktree `db-vbench`)
**Inputs:** `01-pathway-inventory.md` (§N + anchors), `02-hypothesis-map.md` (H1–H8), `03-corpus-design.md` (queries `qNNN`), `04-findings-register.md` (F1–F8), `05-diagnosis.md` (verdicts + cost-center ranking, committed `6df7c1f1d`).

A ranked PR slate. **Correctness ships before speed** — a wrong answer returned as success is a trust bug on the product surface and outranks any DNF. Within speed, ordering is ROI (impact ÷ risk), with the trivial/dramatic wins first and the largest design surface last.

Every PR carries: **scope + code anchors**, **class** (`correctness` / `surgical` / `structural` / `floor`), **impact** grounded in the specific corpus queries it should flip (dnf→ok, or ratio→target), **risk + blast radius**, and a **DoD**. The shared DoD clauses (see §Sequencing) are: target corpus queries hit their goal · native↔virtual parity hashes green (`hash_gate` honored) · W3C SPARQL suite green · BSBM + native-corpus perf budgets unregressed · **kill-switch off ⇒ byte-identical old behavior**.

The master finding reframes the whole slate: **fact tables are 7,670 tiny Parquet files (~23 rows/file); the decode wall is file-count-bound at ~39 files/s ⇒ ~197 s ⇒ DNF.** Most fact queries never reach their downstream operators, so the H1 lever (PR-2) gates the value of everything below it.

---

## PR-0 — Correctness batch: stop returning wrong answers *(class: correctness)*

Two independent trust bugs from the first parity run. Ships first despite modest perf impact.

**0a. Silent-empty guard for non-lowered sub-scopes (F1, F2).** A `PropertyPath` (transitive `+`/`*`) or `Subquery` inside a GRAPH scope is not converted to an R2RML leaf (`rewrite.rs:162-179`) and is evaluated by the generic operator against the graph source's empty native index — returning **0 rows as success** (q034: 0 vs native 4514; q051: 0 vs 247). The whole-scope error guard `if unconverted_count > 0` (`graph.rs:245-253`) only counts unconverted top-level **triples**, so a sub-scope escapes it.
- **Scope (smallest safe fix):** extend the guard to **error loudly** when a GRAPH-over-R2RML scope contains an unconvertible sub-scope pattern (`PropertyPath`, `Subquery`, and audit the rest of the `rewrite.rs:162-179` preserve-arm: `Values`, `Unwind`, path forms). Fail with the same `InvalidQuery` message shape as the triple path. (A *later, larger* PR can lower sequence paths / correlated subqueries into R2RML leaves for capability; this PR only converts silent-wrong into loud-refuse.)
- **Anchors:** `rewrite.rs:140-179`, `graph.rs:245-253`.
- **Impact:** q034, q051 (and any transitive-path/subquery query) go **silent-empty → explicit error**. Manifest: flip their `expected_status.virtual` to `error` (they already tolerate this via the `ExpectedError` wiring).
- **Risk / blast radius:** LOW, but user-visible — a query that "worked" (returned nothing) now errors. That is the correct behavior (it was wrong), but note it in release notes. No kill-switch (correctness).

**0b. Bound-subject wildcard emits the `rdf:type` triple (F3).** `<iri> ?p ?o` on virtual emits the 7 POM predicates but **omits the `rr:class`-derived `rdf:type`** (q042: 21 vs native 24; solo UI subject inspector shows no `@type`). The `#1450` (`81b0ec601`) predicate/type-var binding covers the subject-VAR wildcard; the bound-subject prefix-prune path (`a5528e880`) skips class emission.
- **Scope:** in the bound-subject wildcard materialization branch, emit `?s rdf:type <class>` for each matching TriplesMap's `rr:class`, honoring the same class semantics as the subject-var path.
- **Anchors:** `fluree-db-query/src/r2rml/operator.rs` (bound-subject materialize path; `#1450`/`a5528e880`), `enterprise.ttl:85` (`rr:class`).
- **Impact:** q042 21 → **24 (hash-parity with native)**; every virtual-dataset subject regains `@type`.
- **Risk:** LOW, additive rows. Blast radius = bound-subject wildcard only. Guard behind a debug-assert that a matched wildcard emits exactly one type row per declared class.

**DoD:** q034/q051 error (documented) · q042 hash-parity green · W3C suite green · no perf budget change.

---

## PR-1 — `COUNT(*)` manifest shortcut *(class: surgical)* — the trivial/dramatic win

**Scope.** For a bare unfiltered `COUNT(*)` over a single class (no WHERE FILTER, no join), return the **manifest `record_count` sum** instead of decoding files. The value is already computed — the scan plan literally prints `estimated_row_count=180000` (`stats.rs:120-133` sums `df.record_count`) — and then the fused-aggregate path decodes all 7,670 files anyway (`fused_aggregate.rs:910`).
- **Anchors:** `fused_aggregate.rs:281-396` (detect), `:910` (the scan it should skip), `stats.rs:120-133` (the free count), `fast_count.rs` (the native analogue to mirror).
- **Class:** surgical. Kill-switch: reuse `FLUREE_FUSED_R2RML_AGG` (off ⇒ old full-scan behavior).

**Impact.** q036 (Order), q037 (WebEvent 1M), q039 (GL) **DNF → sub-second** (~199 s → the manifest read). The single highest ROI ÷ risk on the board.

**Risk / blast radius.** LOW. Only fires for the exact bare-COUNT shape; anything with a FILTER/join falls through to today's path. The one correctness subtlety — Iceberg positional/equality **deletes** — is handled because `aggregate_column_stats` already accounts for delete files (`stats.rs`), but the DoD must assert the count matches a full scan on a table with deletes.

**DoD:** q036/q037/q039 dnf→ok with **exact** count == native · parity hash green · delete-file correctness test · kill-switch off = full scan.

---

## PR-2 — The H1 decode-wall lever set *(class: structural / surgical)* — gates everything below

The master bottleneck. Split into three tracks; ship 2a first because it is measurement-gated.

**2a. Diagnose + parameterize the per-file cost (the possible 10×).** The measured ~200 ms per file is **suspicious for small cached files** — the whole-file disk-cache read is local, so ~200 ms is unlikely to be I/O. Candidate culprits: Parquet **footer parse** per file, Arrow reader **setup** per file, or **`tokio::spawn` scheduling** overhead in the `buffer_unordered` over `tokio::spawn(read_task)` fan-out. First **instrument the composition** (a `rows_decoded`/`file_open`/`footer_parse` span split — see harness pre-task), then act: if it is per-file fixed overhead, batching manifest→reader or reusing reader state across a file group could be a **10× win without touching data layout**. Also raise/parameterize the concurrency cap: `iceberg_scan_concurrency` clamps the default to **8** (`r2rml.rs:36-52`); for a high-latency remote store with 7,670 tiny files, a higher default (memory/S3-fan-out-bounded) is warranted.
- **Anchors:** `r2rml.rs:36-52` (concurrency clamp), `send_parquet.rs` (whole-file decode + `arrow_reader.rs`), the tracing-span-in-spawn idiom (memory: create `debug_span!` in the `.map` closure **before** the spawn, `.instrument` inside).
- **Class:** structural. Kill-switch: `FLUREE_ICEBERG_SCAN_CONCURRENCY` already exists; the composition span is debug-gated.

**2b. Manifest-level fast paths (generalize PR-1).** Extend the manifest-shortcut idea to aggregates that don't need row values (e.g. `MIN`/`MAX` from column stats where sound; `COUNT` per group where the group key is a partition column). Bounded, opt-in.

**2c. File compaction — data-side GUIDANCE, not an engine fix.** Document that 7,670 files / 180K rows is a pathological source layout; recommend Snowflake-side compaction (`OPTIMIZE`/larger target file size) in the connect guide. **Not a fix we can require of customers** — the engine must be fast on the layout it's given, which is why 2a is the real lever.

**Impact.** If 2a finds per-file fixed overhead, the entire fact-touching class (q046-scan, q008, q040, q053, all fact `COUNT`/rollups) improves proportionally; even the raise-concurrency-cap piece alone is ~linear in the added parallelism until S3-bound. Target: fact full-scan **~197 s → sub-60 s** (corpus timeout) as a floor, better if 2a lands a batching win.

**Risk / blast radius.** MED. Concurrency raises S3 request fan-out (429 risk — coordinate with PR-8 backoff) and peak memory (bounded by window). Reader-state reuse touches the hot decode loop — differential-test against the W3C + native corpus. Kill-switch off (concurrency=today's default, no reader reuse) ⇒ old behavior.

**DoD:** the per-file composition is **measured and documented** · fact scans land under the corpus timeout · parity hashes green on all fact queries · native/BSBM budgets unregressed · kill-switch off = old timing.

**PR-2 residual — broad-projection mid-size files still pay footer-first.** Lever A's whole-fetch tier stops at `WHOLE_FILE_MAX_BYTES` (32MB): a 32–64MB file with no local cache copy and a broad (≥50%-share) projection is still fetched whole by `admit_whole_file`'s `broad` branch, but only *after* the 2-RT footer read — broadness is computed from footer metadata (projected column-chunk bytes), so covering this tier means speculatively fetching whole before the footer. Deliberately left conservative in PR-2; the leftover win is bounded to that tier.

---

## PR-3 — Typed-dim-star over-scan (F8) *(class: surgical → structural)*

**Scope.** A typed star `?s a Store ; edw:name … ; edw:channel …` scans **6 dimensions** (450K est rows) because the star's base predicate `edw:name` is shared across every name-bearing dim and `class_fusion_is_safe` correctly refuses to fuse (not all name-maps declare Store) — so TriplesMap resolution fans out (`operator.rs:595-610`), plus a 7th subject-only class scan. Two fixes (either suffices; both are §1/§2 extensions):
- **(a) selectivity-aware base predicate** — resolve TriplesMaps by the **intersection of all star-member predicates** (a map must have `name` ∧ `channel` ∧ `storeType`); the Store-exclusive members prune to DIM_STORE; **or**
- **(b) class-constrained star resolution** — ~~when a `?s a Class` is co-located, restrict the star's map resolution to class-declaring maps even when full fusion is refused (sound because the subsequent join with the class scan drops non-class subjects).~~ **CORRECTED to (b') — the original (b) is UNSOUND (verified 2026-07-13).** Its soundness claim conflates "the join drops non-class SUBJECTS" (true) with "we don't need the non-class map for the BINDINGS" (false under vertical partitioning). Counterexample: `TM_STORE`(subj `store:{k}`, name+channel, **no class**) + `TM_STORECLASS`(subj `store:{k}`, class Store, no POMs); query `?s a Store ; name ?n ; channel ?ch`. Today: star resolves `TM_STORE` by `name`, materializes the bindings, joins the standalone Store scan (`TM_STORECLASS`) on `?s` → correct. Raw (b) prunes the star to class-declaring maps → `TM_STORECLASS` (no POMs) → 0 rows. So (b) drops exactly the split-TriplesMap rows `class_fusion_is_safe` protects.
- **(b') class-constrained star resolution, template-disjoint** — the sound replacement: class-constrain the star's resolution **only when `wildcard_class_fusion_is_safe(class)` holds** (every non-class-declaring map is subject-template prefix-DISJOINT from every class-declaring map). Disjoint prefixes ⇒ disjoint IRIs ⇒ the pruned map's subjects could never survive the class join anyway; a same-template (vertically partitioned) map is kept. **Reuses the existing `wildcard_class_fusion_is_safe` predicate** — no new string test. Recorded on the star as a resolution-only `class_prune_hint` (never affects rdf:type materialization). Covers the shared-member shape (`?s a Store ; name ?n`) that (a) can't prune.
- **Anchors:** `rewrite.rs` (`fuse_class_if_safe` sets `class_prune_hint`; `class_fusion_is_safe` / `wildcard_class_fusion_is_safe`), `operator.rs` (`tm_passes_star_prune` in the resolution filter; `star_tm_prune_enabled`), `03-inventory §1/§2`. Switch: `FLUREE_R2RML_STAR_TM_PRUNE`.
- **Related:** the **FQL wildcard 'browse instances' crawl** fan-out is **ALREADY SHIPPED** (`try_fuse_wildcard_class`, `81b0ec601`) — it class-constrains the crawl wildcard via `wildcard_class_fusion_is_safe` (16→1). No PR-3 crawl work. The residual Snowflake Horizon **429** on the *unfused* path (reasoning-on / switch-off) is PR-8 (backoff).
- **Residual (F10, pre-existing):** star *formation* fuses same-subject members unconditionally, so required members split across template-sharing TMs (the data-data analog of the (b') counterexample) yield zero star rows — pre-PR-3 and post-PR-3 alike; fix (a)'s prune is result-preserving *given* that behavior. Future fix in the rewrite: refuse to fuse when no TM covers all members. Registered as **F10** in `04-findings-register.md` (recorded from the PR-3 review, 2026-07-13).

**Impact.** q001 **6-table → 1** (est 450K → 500 rows); cold latency ~20.8 s → ~**3× better** (6 loadTables → 1). Contributes the 4 dead single-scans to q050's fix. This is the **most common BI shape** (typed dimension list), so the fleet-wide impact is high.

**Risk / blast radius.** MED — touches star map-resolution, the correctness-sensitive vertical-partition seam (§2). Must keep `class_fusion_is_safe`'s vertical-partition protection (a subject spread across maps must not drop rows). Differential-test against the corpus + a synthetic vertically-partitioned mapping. Kill-switch: gate behind a resolution-strategy flag; off ⇒ today's base-predicate resolution.

**DoD:** q001 loads 1 table (assert via `scan_table` span count) · hash-parity green · vertical-partition regression test green · kill-switch off = old fan-out.

---

## PR-4 — Correlated parent-lookup memoization (H3) *(class: surgical)*

**Scope.** An OPTIONAL/correlated ref (`?p edw:supplier ?s . ?s edw:rating`) rebuilds the parent-dimension lookup **per child batch** with no cross-batch memoization: the main-table `scan_cache` (`operator.rs:713-734`) covers unfiltered inner scans, but the **parent-scan path bypasses it** (`operator.rs:889-897`). Memoize parent lookups across child batches keyed by the existing `LookupCacheKey` (`operator.rs:64-65`, `(parent_tm, sorted_join_cols)`).
- **Anchors:** `operator.rs:889-897` (parent scan bypass), `:713-734` (scan_cache), `:64-65` (`LookupCacheKey`), `05-diagnosis §Deep-dive 3`.

**Impact.** q050 (dims-only OPTIONAL) **377 scans → ~2** (DIM_SUPPLIER ×153 + DIM_PRODUCT ×78 → once each); **DNF@120s → native-class ms** once combined with PR-3's fan-out fix. The q050 decomposition in `05-diagnosis` is the DoD case.

**Risk / blast radius.** LOW–MED. Parent lookups are already collected fully (small dims); memoizing them is a cache-lifetime extension, not a semantic change. Watch memory for a large parent (cap like the main-table window). Kill-switch: `FLUREE_R2RML_SCAN_CACHE` (extend to cover the parent path); off ⇒ per-batch rebuild.

**DoD:** q050 scan_table span count ≤ ~2 per parent · dnf→ok within native ratio target · parity hash green · kill-switch off = old rebuild count.

---

## PR-5 — Scan-side top-k for `ORDER BY … LIMIT` *(class: structural, NEW strategy)*

**The only non-extend item in the core slate**, and the only fix for the ORDER-BY-DNF class. Budget pushdown (PR-of-record §5) fundamentally cannot help — a top-k must see every row to rank (§12 "not budget-fixable"), confirmed by the q045 toggle (pushdown-off reproduces q046's DNF).

**Design sketch.** Push the sort key + `k` (+ offset) into the R2RML scan as a **bounded top-k heap**:
1. The planner detects `ORDER BY <pushable-cols> LIMIT k` directly above a single R2RML scan (no intervening non-order-preserving op) and hands the scan a `TopK { keys, k, dir }` directive (analogous to the existing `row_budget`, `operator.rs:243`).
2. The scan maintains a size-`k` heap while streaming; after the first full window it holds the running k-th bound.
3. **Row-group / file pruning by the running bound** — reuse the `pruning.rs` min/max machinery (§6): once the heap is full, a row group whose max (for `DESC`) is below the heap's k-th key cannot contribute and is skipped. This is where the win comes from on 7,670 files — most files get pruned after the first few.
4. Fall back to full sort when the sort key is not a pushable scalar column (expression ORDER BY, ref/IRI keys), or when a non-order-preserving operator intervenes.
- **Anchors:** `sort.rs:544` (`new_topk`, today's absorb), `operator.rs:243` (`row_budget` field to mirror), `pruning.rs:184-320` (min/max bounds to reuse), inventory §6/§12.

**Impact.** q046 (and q012, q005-class top-k) **DNF → ok** — potentially *faster than native* on selective ORDER BY, since file pruning by the k-th bound reads far fewer than 7,670 files. The single biggest analytical-shape unlock (dashboards are "top N by measure").

**Risk / blast radius.** MED–HIGH (largest design surface). Correctness hinges on the pruning bound being sound (only prune a group that provably cannot beat the k-th) — differential-test heavily against full-sort results, including ties (the corpus tiebreakers help). Kill-switch: `FLUREE_R2RML_TOPK_PUSHDOWN`; off ⇒ today's full-materialize top-k. Land behind the differential harness.

**DoD:** q046/q012 dnf→ok with results **identical** to full-sort (hash-parity) · files_pruned > 0 on selective ORDER BY (needs the F7 counter) · W3C ORDER BY/LIMIT suite green · kill-switch off = old top-k.

---

## PR-6 — Fused aggregate over a single join (H6) *(class: structural, NEW — deferred behind PR-2)*

**Scope.** Extend the fused-aggregate path (`fused_aggregate.rs`, today single-scan only, declines any join `:327-333`) to admit **one RefObjectMap join** so `Fact ⋈ Dim GROUP BY dim-attr (agg)` folds from column batches without materializing 180K bindings.
- **Impact.** q008, q010, q013, q025, q032 (rollup class). **Deferred** — `05-diagnosis` Deep-dive 2 shows H6 is *invisible until H1 is fixed* (q008 never reaches the group phase). Sequence after PR-2 so its impact is measurable.
- **Risk.** MED–HIGH (agg correctness over a join). Kill-switch `FLUREE_FUSED_R2RML_AGG`.
- **DoD:** rollup queries improve vs post-PR-2 baseline · exact aggregates vs native · kill-switch off = generic path.

---

## PR-7 — Decimal/double predicate pushdown (H4) + its measurement pre-task *(class: surgical)*

**Pre-task (harness).** Add a `files_pruned` / `row_groups_pruned` counter to the `scan_plan` span and a `rows_decoded` vs `rows_emitted` reader counter (also feeds PR-2a). Without them H4-decimal cannot be quantified (`04-findings §F7`, `05-diagnosis §H4`).

**Scope.** Money filters on `xsd:decimal` (`GlJournalEntry.debitAmount`/`creditAmount`) and `xsd:double` never prune: `const_object` keeps decimal operator-only (`rewrite.rs:501`), and `prunable_stats` returns `None` for decimal (`pruning.rs:279-281`); doubles lack a `stat_bounds` arm (`pruning.rs:319`). Add decimal/double → row-group min/max pruning where the physical column carries decimal/float statistics.
- **Impact.** q019 (GL debits > 1M) prunes files instead of full 250K-file... (GL) scan; the H4 A/B (q019 decimal vs q020 date / q021 int controls) becomes measurable. q011 already proves the date path prunes **7,579/91 = 98.8%** (F6) — this brings decimal to parity.
- **Risk.** LOW–MED (decimal comparison + scale). Kill-switch `FLUREE_ICEBERG_PREDICATE_PUSHDOWN`. DoD: q019 `files_pruned > 0`, correct rows vs full scan, kill-switch off = operator-only.

---

## PR-8 — Cold/floor & caching program (H7) *(class: floor)*

**Scope.** Attack the fixed cold cost (F8/H7: cold q001 ~20.8 s dominated by `loadTable` OAuth/catalog). Items:
- **Persistent catalog state** across process restarts — persist the `rest_load_tables` / `rest_clients` cache (or a metadata-location snapshot) so a cold process doesn't re-OAuth + re-`loadTable` every table. Cold ~17 s → target (a warm-disk-class number).
- **Catalog 429 backoff + concurrency cap** (memory: the wildcard-crawl fan-out trips the Snowflake Horizon 429) — exponential backoff on 429 and a catalog-request concurrency cap, so PR-2's raised scan concurrency and PR-3's crawl don't storm the catalog. `crawl.rs`, catalog client.
- **PR-1 residual — COUNT(*)-shortcut decline pays the manifest read twice.** When `table_row_count` declines (delete manifests present), the fallback scan's planner re-reads the manifest list the shortcut just read; `cache.get_scan_files` absorbs it warm, but a first cold COUNT on a delete-bearing table pays two manifest reads. Fold into the catalog/manifest round-trip caching here.
- **Anchors:** `cache.rs` (three-tier caches, §3/§4), `catalog_session.rs`, inventory §3/§10.
- **Impact.** Every query's cold column; the `loadTable` term that PR-3 reduces 6×, PR-8 reduces further and makes durable. Risk: LOW–MED (cache persistence correctness — honor creds expiry; the rotated-secret hazard §4 must still self-heal). Kill-switches: the existing TTL/cache env vars.
- **DoD:** cold-protocol subset (05 §cold) hits the cold target · 429 backoff verified under a forced-429 stub · rotated-secret still self-heals within TTL.

---

## Harness follow-ups (not perf PRs, but gating)

- **`scan_plan` span coverage (F7).** The span fires only on the pushdown branch (2/16 smoke queries); tune the `EXPECTED_FOR_VIRTUAL` set so it isn't false-flagged as missing, or emit it unconditionally with `files_pruned=0`. Feeds PR-2/PR-5/PR-7 measurement. **Harness side DONE on this branch:** `EXPECTED_FOR_VIRTUAL` is now `scan_table` only, so `spans_missing` no longer false-flags; unconditional engine-side emission remains open.
- **`files_pruned` / `rows_decoded` counters** (PR-7 pre-task, PR-2a) — the two counters flagged in `02-hypothesis-map` as H1/H4 confirm-gaps.
- **`hash_gate` compare-side wiring** — ~~`baseline::check` must skip the hash assertion on `RowsOnly` (described in `03 §5.1`; field is ready, gate not yet wired — WP6/baseline owner).~~ **DONE on this branch:** `compare_one` gates `RowsOnly` on row count (dashboard applies the same rule), and `baseline --expected` re-bless tolerates hash drift on `RowsOnly` oracles.
- **`ns@v2` store-state fingerprint TODO** — the run `TargetFingerprint` (`schema.rs:57-64`) keys on `fluree_home` only; add a native store-state / namespace-v2 fingerprint so a re-indexed or drifted native ledger is detected as incomparable across runs (prevents a stale-baseline false pass).

---

## Sequencing & measurement discipline

**One PR at a time through the corpus gate.** Merge order = this document's order: **PR-0 (correctness) → PR-1 → PR-2 → PR-3 → PR-4 → PR-5**, then PR-6/7/8 as capacity allows. PR-2 gates the *measured* value of PR-6 (H6 is invisible until the decode wall drops), so it precedes it.

**Per-PR gate (the shared DoD, enforced every PR):**
1. **Corpus gate** — the PR's target queries hit their goal (dnf→ok, or ratio→target) on `virtual-sf01`, and **no other corpus query regresses** (full `vbench run` compare).
2. **Parity** — native↔virtual result hashes green for every affected query, honoring `hash_gate` (`rows_only` queries gate on count + invariants).
3. **Correctness suites** — W3C SPARQL suite green; the native/JSON-LD IR-parity tests green.
4. **Perf budgets** — BSBM + native-corpus budgets **unregressed** (the virtual work must not slow the native path; enforced every PR, not just at the end).
5. **Kill-switch fidelity** — with the PR's kill-switch off, behavior is **byte-identical to pre-PR** (the corpus hash + timing prove it).
6. **Bless-after-merge** — re-bless the virtual baselines from the post-merge native corpus so the next PR compares against a current reference.

**Noise discipline.** Virtual timings are Snowflake-live and variable; gate DNF→ok on *finishing under the corpus timeout*, and ratio targets on the median of ≥3 paced reps, not a single run. Cold-column claims run under the `05-diagnosis` cold protocol subset only.

**North star.** The corpus is the contract: a PR is done when its queries are green through the gate and the kill-switch proves it changed nothing else. The architecture is sound (`05-diagnosis`: no `wrong-turn-redesign`); this slate is disciplined extension, correctness-first.

---

## NORTH-STAR BURN-DOWN (clean re-baseline on the F9 head #1499, 2026-07-14)

**Closure criterion (AJ, bar semantics RULED 2026-07-14):** every corpus query (excl. the 3 expected-error) ≤ low single-digit seconds **CACHE-THRASHED / FIRST-ASK** — the latency a BI user sees on a fresh question with no cache-state control, measured by the full-corpus cache-thrashed protocol (per-query manifest `timeout_s`, no DNFs). **Warm-hot does NOT satisfy the bar** (AJ: "the second reading … let's proceed with that"), so q031's 188 ms-warm does not count and **the cold-floor item (re-scoped F18) is REQUIRED slate work, not optional polish.** The clean re-baseline below is the hot/warm floor (priming + 3-rep, post-PAT-swap, healthy net) — it establishes which queries are ALREADY under the bar warm; the cache-thrashed number is the one that gates closure. **42 of 50 ok-queries are already ≤ 3 s warm.** Top-k healthy (q046 125 ms, 10/7660 pruned), q019 pruning intact (792 ms, 7670 pruned), q012 fine via fallback (491 ms). The **8-query tail** (>3 s) and its mechanism + remedy:

| q | hot | signature (spans) | mechanism | remedy (mirrors) |
|---|---:|---|---|---|
| **q031** | **72.1 s** | `scan_table.n=1448`, `load_table.n=7` (21 s) | **PRE-EXISTING, NOT a regression — root now F20 (RESOLUTION FAN-OUT), after a 4-flip investigation (memo→bisect→cold-floor→measurement).** MEASURED (2026-07-14): q031 loads **7 DISTINCT tables once each** (FACT_INVENTORY_SNAPSHOT + the 6 `edw:name`-bearing dims), pin held across 72–115 s — NOT a leak, NOT residency (fact is 51 MB). The 21.2 s `load_table` is a **RefObjectMap-target resolution FAN-OUT**: `?p edw:name ?pn` resolves against all 6 name-dims because `?p`'s `edw:product` RefObjectMap parent (DIM_PRODUCT) isn't propagated to constrain it (F8/PR-3 family). The 1448 re-scans ≈ 241 batches × 6 fan-out. Bisect + gate forensics EXONERATE every PR. F18 (memo/limit, then cold-floor) and F19 (memo Arc) are both superseded for the wall; F19 stays a latent standalone. | **F20 resolution-prune** (`20-pr-q031-refprune.md`): propagate the RefObjectMap target (DIM_PRODUCT) to prune `?p edw:name` resolution → `load_table` **7→2**, collapse the fan-out re-scans. **Soundness:** prune only when every binding source of `?p` is provably that one RefObjectMap; else DECLINE. Standalone PR, own kill switch. **Expected: low-single-digit s cache-thrashed WITHOUT PR-2a** (residual = one 7,670-file fact decode ≈ ~1 s per q018/q044). |
| **q029** | 125 s | `scan_table.n=253`, `files_selected=15340`, 1.94 M reads | **F17** — UNION absorbs the LIMIT budget; both FACT_WEB_EVENT branch scans re-driven ~253×. | UNION budget forwarding (mechanism-class D; mirror PR-5 wrapper-forwarding). **NOT subsumed by PR-4d** — q029 is a `UnionOperator` re-drive, a DIFFERENT operator than the batched OPTIONAL; it shares the re-drive *symptom* only. Separate fix. |
| **q038** | 48.6 s (**1 row**) | iceberg reads=3, `files_selected=2`, `prefetch.n=331` (2 s); **~42 s unaccounted by iceberg spans** | un-fused `COUNT(*)` over a constant-object star (`isCurrent true`, ~427 K customers) — the wall is in **generic eval/materialization above the scan**, not iceberg. Adjacent to F12 (constant-object star + fused-agg). | fused-COUNT pushdown (mirror PR-6 fused-aggregate); avoid materializing the full star. New F-number. |
| **q016** | 43.1 s | `scan_table.n=182`, 1.4 M reads | **F14** — batched-OPTIONAL is per-OUTER-batch (`optional.rs:2029/2039`); q016's outer FACT_ORDER emits ~180 budget-windowed batches, each driving a fresh FACT_SHIPMENT inner scan (182 = 2 outer + 180 inner). | **PR-4d** (`19-pr4d-optional-seed-coalesce.md`): coalesce the whole driving side into ONE seed → one inner scan + hash-partition. **Gate on scan-count 182→single-digit + parity**; the cache-thrashed WALL residual is ONE cold FACT_SHIPMENT scan = the SAME cold-floor as q031 (needs L2 fact residency), so the wall is deferred to the cold-floor item, not PR-4d alone. |
| **q041** | 13.1 s | `scan_table.n=3`, 15 341 reads (full FACT_ORDER), 1100 result rows | `FILTER(?store = A||B||C)` ref-value OR-filter **not pushed** → full FACT_ORDER scan then residual-filter to 3 stores. | push the ref-value IN-set into the scan (value-pushdown / IN-set prune). New F-number. |
| **q053** | 13.6 s | 15 342 reads, `scan_table.n=4` | `FILTER NOT EXISTS { Order customer ?c }` correlated anti-join over Customers. | hash / batched anti-join (semi-join). New F-number (shared w/ q017). |
| q028 | 3.9 s (borderline) | 35 392 reads, `scan_table.n=13` | constant-object star (`eventType "purchase"`, non-pruning) + product-dim join. | marginal; improves with q031's memo fix; eventType non-prune inherent. |
| q017 | 3.1 s (borderline, 0 rows) | 38 350 reads, `scan_table.n=5` | `FILTER NOT EXISTS { ?sh order ?o }` anti-join (Order without Shipment) — same family as q053. | same anti-join remedy. |

**Slate for AJ (design-first PRs, in impact order — all end-state figures are the cache-thrashed full-corpus bar, not warm-only; AJ SIGNED 2026-07-14):** (1) **q031 RESOLUTION-PRUNE (F20)** — propagate the `edw:product` RefObjectMap target (DIM_PRODUCT) to constrain the `?p edw:name` resolution, killing the 6-dim fan-out (`load_table` 7→2, ~15 s + the fan-out re-scans; 72 s → low-single-digit s cache-thrashed). Standalone, own kill switch, join-var-provenance soundness invariant. **Supersedes the F18 cold-floor framing** (measurement proved the pin isn't leaking and the 51 MB fact needs no residency). (2) **PR-4d** batched-OPTIONAL whole-seed (q016 scan-count 182→single-digit). (3) **F17 UNION budget forwarding** (q029 125 s → low-single-digit s; distinct operator from PR-4d). (4) **q038 fused-COUNT** (48 s → sub-second/low-s). (5) **q041 ref-IN pushdown** + (6) **q053/q017 semi-join** (13 s → low-single-digit s; borderline q028/q017 likely fall out). **Expected end-state:** all 50 ok-queries ≤ ~3 s cache-thrashed.

**Shared cold-floor observation (2026-07-14).** After the plan-shape fixes, several tail queries bottom out on the SAME residual: **one full 7,670-tiny-file fact decode** (FACT_INVENTORY_SNAPSHOT for q031-post-F20; FACT_SHIPMENT for q016-post-PR-4d; the fact tables are all 7,670 files / ~180-300 K rows / ~45-55 MB — file-count-bound, not byte-bound). Empirically that single scan is **~1 s hot** (q018 1.06 s, q044 0.96 s — full 7,670-file scans in the clean baseline), so the plan-shape fixes are expected to land their queries ≤ 3 s cache-thrashed **without** re-opening **PR-2a** (the per-file decode-wall / master lever). PR-2a stays CLOSED unless a gate misses — at which point it becomes the shared lever for the whole fact-scanning tail at once. The bar being cache-thrashed/first-ask (AJ) means the disk artifact cache is legitimate steady-state (priming + production both populate it); "first-EVER-S3-touch" (~197 s for 7,670 files) is reported, not gated.

**Gate-protocol lesson (from the q031 non-regression).** q031's PR-8b "sentinel" number was **188 ms** — a fully-warm reps=3 measurement — while the SAME gate's reps=1 headline recorded **65 s** (cache-thrashed). That is a **~350× optimism factor** hidden inside one gate run, and it made a stable-since-PR-8b query look like a fresh regression. The full-corpus north-star baseline (which cache-thrashes, evicting each fact/dim table) is what surfaced the true cost. **Improvement:** sentinel/headline perf numbers must be reported cache-thrashed (cold protocol or full-corpus run order), or report warm AND cold side-by-side — never a fully-warm single-query figure alone. Consider a full-corpus baseline per stacked-PR head (not just the PR's sentinel set), since q031 wasn't in PR-5's sentinels and only the full corpus caught its real wall.
