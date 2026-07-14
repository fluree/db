# PR-5 — scan-side top-k for `ORDER BY … LIMIT` (the FINAL roadmap item) — DESIGN SKETCH

**Branch:** `perf/r2rml-pr5-scan-topk` (off `perf/r2rml-pr7-numeric-stats` HEAD `2a07bbbc4`)
**Status:** SKETCH — **STOP for lead review** (largest new design surface; the roadmap's original PR-5 sketch predates five shipped PRs, and PR-7 changed the marginal-cost calculus).
**Substrate:** ROADMAP PR-5, `09-stacked-rebaseline.md` (§3/§5/§D/§6 — PR-5 is cold-only, PR-7-gated, PR-8-gated), and PR-7's shipped numeric stats (#1494).

## (a) Target, honestly re-derived — recommendation: PROCEED, tightly scoped (not defer)

**The corpus reach is one query.** Of the three ORDER-BY-LIMIT queries, only **q046** qualifies for scan-side top-k: `SELECT ?oid ?tot WHERE { ?o a edw:Order ; edw:orderId ?oid ; edw:orderTotal ?tot } ORDER BY DESC(?tot) ?oid LIMIT 10` — a single-TM FACT_ORDER scan whose sort keys are both scan columns (ORDER_TOTAL `xsd:double`, ORDER_ID). **q005** has an intervening ref-join (`?g edw:region ?region`) and an IRI tiebreaker (`?sup`) → out. **q012** sorts on a post-aggregate `SUM(?qty)` (a computed value, not a scan column — you cannot prune files by an aggregate that spans them) → out. So the honest corpus target is **q046 alone**.

**And only cold.** Warm, q046 is already **445 ms** (PR-2's memo made the full 7,670-file scan cheap warm), and PR-5's heap-only leg is *worthless warm* — it still decodes every file. PR-5's entire value is the **running-k-th-bound FILE prune**, which reads ≪ 7,670 files; that only shows up on the **cold** first-touch scan (**~37 s**, `09` §5). PR-8 did **not** erase this: it persists the *catalog* floor (loadTable/metadata/manifest), not the 7,670 Parquet *data* files (DiskArtifactCache is cleared cold), so the ~37 s cold data-fetch survives — as the dispatch confirms (q027/q046 still ~37–43 s cold).

**Why PROCEED anyway (my recommendation).** (1) **The marginal cost dropped.** The roadmap tagged PR-5 "largest design surface / MED-HIGH risk" because the pruning bound needed numeric column stats that didn't exist. **PR-7 shipped exactly those** (double/decimal file+row-group bounds, strict-superset, NaN-safe, tested). So PR-5 is now "TopK directive + MAX-ordered file read + running-bound prune + defer-to-sort-above" on top of a *proven* pruning primitive — a much smaller, lower-risk surface than the original estimate. (2) **The cold win is large and distribution-free** (see (e)): ~37 s → ~2–3 s, reading ~10–15 files. (3) It is the **only** fix for the raw-column top-k class — the canonical dashboard shape ("top N by measure"). (4) It is the roadmap's designated final item; finishing it closes the series.

**The honest case for DEFER** (so you can weigh it): one corpus query, cold-only, and PR-8 already softened cold. If you weight *corpus breadth* or *warm-path impact* over *per-query cold depth + roadmap completion*, DEFER is defensible. **My call is PROCEED-scoped** — the post-PR-7 marginal cost is low enough that a ~12–15× cold win on the canonical analytical shape is worth the contained surface. If you'd rather bank PR-7 and stop, I'll take that ruling.

## (b) If proceeding — the mechanism

**1. Directive plumbing (mirror `row_budget`).** `set_row_budget` (`limit.rs:85` → `r2rml/operator.rs:2209`) is the template: a trait method that threads a directive from an upper operator down to the scan. `row_budget` does NOT cross the blocking `SortOperator` (it needs all rows to rank — the reason top-k never truncates the scan today). PR-5 adds a **parallel** directive `set_topk(TopK { sort_col, dir, k, offset })` that **only the `SortOperator` sets, and only onto a directly-below R2RML operator**, when its own `topk` is `Some(k)` and the shape qualifies. Non-R2RML children ignore it (default no-op), so it is inert everywhere else.

**2. Eligibility (compound-comparator rule).** Push only when **every** sort key resolves to a scan column of **one** TM, there is **no** intervening non-order-preserving op between the sort and the scan (no GROUP BY / DISTINCT-that-reorders / join), and the **primary** sort key is a pushable-stat column (int/date/string/**double**/**decimal** — the PR-7 set). q046 qualifies: DESC(ORDER_TOTAL), ORDER_ID, both FACT_ORDER, ORDER_TOTAL is double. The corpus tiebreakers made every ORDER BY compound; the compound-ness gates *eligibility* (so the authoritative sort above can produce the exact order) but the **file prune uses only the primary key** (below).

**3. MAX-ordered file read — the crux.** The scan sorts its file list by the manifest `upper_bound(sort_col)` (for DESC; `lower_bound` for ASC) **before** the read loop, and reads highest-bound-first. This is what makes the prune *near-optimal and distribution-free*: the heap fills with the true top values in the first few files, the k-th bound settles at the true `V_k`, and every remaining (lower-MAX) file prunes immediately. Without MAX-ordering the same files eventually prune, but read order could be adversarial (top values in the last physical file → read everything) — so MAX-ordering is essential, not optional. The manifest bounds are already in memory (the pruning path reads them); sorting 7,670 entries is trivial.

**4. Running-bound file prune (reuse `pruning.rs`).** The scan keeps a size-`(k+offset)` min-heap (DESC) of the **primary** key's values only, purely to compute the running k-th bound. Once full, a file whose `upper_bound(sort_col) < bound` (DESC; `lower_bound > bound` for ASC) provably cannot contribute → skip it (and, since files are MAX-sorted, all subsequent files too). The comparison reuses PR-7's `bounds_can_contain`/`TypedValue` machinery. **Strict `<`** (keep on `==`) so ties at exactly `V_k` are over-kept.

**Descending bound semantics:** DESC → the k-th **lower** bound (smallest of the current top-k) is the threshold; prune files whose **max** < it. ASC → k-th **upper** bound; prune files whose **min** > it.

## (c) Tie/dedup ownership — scan prunes, `sort.rs` decides

The scan-side heap is a **pruning accelerator only**: it never truncates rows and never decides the answer. It streams **every row of every non-pruned file** upward. `SortOperator::new_topk` (`sort.rs:542`) above remains the **sole authority** for the final compound order, tie resolution, OFFSET, and LIMIT. Because the scan only skips files it *proved* cannot contain a top-k row (strict-`<` bound, keep on tie), the rows reaching `sort.rs` are a **superset** of the true top-k — so the final result is **byte-identical to full-sort** (over-keep semantics, exactly like pruning). No dedup, no split-brain comparator: the scan owns *which files to read*, `sort.rs` owns *the answer*. This also means the scan-side heap needs only the **primary** key (the bound); the secondary key never enters the prune decision.

## (d) Kill switch + fallback

`FLUREE_R2RML_TOPK_PUSHDOWN` (default on; api- or query-side OnceLock per where the directive is set). Off ⇒ the `SortOperator` never sets the directive ⇒ today's full-materialize top-k (scan streams all rows, `sort.rs` heap keeps k). **Fallback (no directive) whenever the shape is ineligible** (b): expression/ref/IRI primary key, non-pushable-stat primary column, multi-TM, or any intervening non-order-preserving op. Fallback is the current behavior, so an unhandled shape is never wrong — only unaccelerated.

## (e) Gates + the prunable-fraction estimate (BEFORE promising)

**Prunable fraction — distribution-free for top-k.** From the q046 oracle the top-10 `?tot` are **distinct**, in [4999.60, 4999.98] (ORDER_TOTAL is capped near 5000, dense at the top — the day-partition "per-file max varies" case). That density is a *red herring* for pruning: by definition only the **10 largest** orders have `?tot ≥ V10 = 4999.60`, so only the **≤10 files** that contain them have `upper_bound ≥ V10`; **every other file's max is ≤ the 11th-largest < V10 → prunes**, regardless of how the totals are distributed. The density only tightens the *band* (V1..V10 span 0.38), not the *count* above V10. The one thing that would erode it is heavy **ties at exactly V10** (many orders == 4999.60 → their files over-kept); doubles in a ~10⁵-order synthetic set make that ~0–2 files. **Estimate: read ~10–15 files of ~7,670 → files_pruned ≈ 7,655/7,670 (~99.8%).** Cold: ~15/7670 × 37 s ≈ 0.07 s of data-fetch + the PR-8 catalog floor (~1.5–2 s) + manifest read ≈ **~2–3 s cold (from ~37 s, ~12–15×)**. I am *not* promising a specific cold ms until the live gate; I am promising files_pruned > ~99% and cold ≪ 10 s.

**Gates:**
- **Cold q046 headline:** `files_pruned > 0` (expect ~7,655/7,670) and cold wall ~37 s → target < 5 s. (F7 caveat: the `iceberg.scan_plan` span only records `files_selected/pruned` on the pruning branch — it reads 0 today for q046 with no filter; the TopK directive activates that branch, same as PR-7's q019.)
- **Hot q046 / q005 no-regression:** warm q046 ≤ current 445 ms (fewer files decoded; must not regress from directive overhead); q005 (ineligible → fallback) byte-identical and unchanged wall.
- **ORDER-BY-class parity sweep:** q005 / q012 / q046 all hash-parity vs the blessed oracle; q012 (post-aggregate) and q005 (join/IRI) provably **unchanged** (fallback path), q046 identical to full-sort.
- **Differential (the core correctness gate):** switch ON vs OFF byte-identical on q046 **including the tie boundary** (construct/inspect a tie at V_k); the scan-side prune must be a strict superset.
- **W3C ORDER BY / LIMIT suite green**; native 54/54 0-mismatch.

**Switch:** `FLUREE_R2RML_TOPK_PUSHDOWN` (my call, matching the roadmap name). `pf5_` artifact prefix.

## Change surface (on nod)

query: `SortOperator` topk-directive detection (`operator_tree.rs` apply_solution_modifiers) + `set_topk` trait method (mirror `set_row_budget`); `r2rml/operator.rs` — accept `TopK`, MAX-order the file list, size-(k+offset) primary-key heap, running-bound file skip (reuse `bounds_can_contain`). iceberg: none new (PR-7's double/decimal bounds + `pruning.rs` reused as-is). Tests: a hermetic directive/heap/bound unit (prune vs keep vs tie-boundary; ASC + DESC) + the differential (switch on/off byte-identical incl. tie) + the H2 cold-q046 live gate + parity sweep.

## Ruling: PROCEED (tightly scoped) + four riders + one execution-path refinement

Lead approved PROCEED. The distribution-free argument is the decider; the cold-only/single-corpus-query framing goes in the PR body verbatim.

**Rider 1 — NULL sort values (the edge the sketch didn't name).** Two invariants make NULLs sound under DESC, both TESTED: (i) **prune only when the heap is full** (k non-null values seen) — if fewer than k non-null rows exist, the heap never fills, the prune precondition never fires, every file is read, and the NULL-ordered rows (which sort LAST in DESC) legitimately reach `sort.rs`; (ii) **a file with no `upper_bound` for the sort column (an all-NULL column) is never pruned** (conservative keep). A partial-NULL file (null_count>0 but has an upper_bound) prunes by that bound and is sound because when the heap is full the k non-nulls all beat any null. Hermetic cases: partial-null-prunes, all-null-file-kept, k-exceeds-non-null-count-reads-everything.

**Rider 2 — ASC declines to fallback (my call): DESC-ONLY.** ASC is NOT a clean mirror: SPARQL orders unbound values FIRST in ASC, so any NULL row always occupies a top-k slot and no null-bearing file could be pruned (a `lower_bound` heap would silently drop nulls that belong in the result). A sound ASC would need a "sort column provably non-null" guard (every file null_count=0, or a `required` column); that extra surface is declined for the final item. DESC covers q046 and the canonical top-N-by-measure (largest) shape. ASC `ORDER BY … LIMIT` takes the fallback (today's full-sort).

**Rider 3 — read-order insensitivity.** The MAX-ordered read changes the physical file read order for eligible scans **even when nothing prunes**. This is safe: `sort.rs::new_topk` above fully orders the result, corpus/result hashes are multiset-based, and no downstream operator may assume manifest/file order (the scan already reads files with `buffer_unordered`, i.e. completion-order, today — so nothing depends on order already).

**Rider 4 — F13** rides on this branch (`408b2052e`); the PR body cites F13 in one line.

**Execution-path refinement (discovered in the code, within the approved design).** Today the scan streams files with `buffer_unordered(concurrency)` — parallel and completion-ordered — which cannot host the sequential heap + running-bound early-stop. So the TopK path is an **additive branch**: when `ScanConfig.topk` is set, the api scan takes a new **sequential MAX-ordered** read (sort tasks by decoded `upper_bound(sort_col)` DESC; read in order; maintain a size-(k+offset) min-heap of the non-null sort values; after each file, if the heap is full and the next task's `upper_bound < k-th bound` (strict), drop the remaining tasks; yield every read file's batches). Non-topk scans keep the existing parallel path byte-for-byte. Losing parallelism on the topk path is a non-issue — the whole point is that it reads ~10-15 files, not 7,670.

**STOP gate cleared — implementing.**

---

**F13 rider (in progress, parallel):** native-sf01 5-rep re-bless launched on the quiet machine (`pf_f13_rebless.sh` → `baselines/perf/native-sf01.json`), to retire the recurring q034/q050 false SLOWs before PR-5's gates. Will commit the refreshed baseline with an F13-citing note and report the delta.
