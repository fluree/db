# PR-8b — q031 inner-join parent-memo — DESIGN SKETCH (short)

**Branch:** `perf/r2rml-pr8b-innerjoin-memo` (off `perf/r2rml-pr8-cold-floor` HEAD `c4a9b799e`)
**Status:** Option A **APPROVED** (lead, with 3 requirements) → **IMPLEMENTED** + unit-tested; live gates next.
Mechanism already diagnosed conclusively in `09-stacked-rebaseline.md` §2 (q031). This is the *seam confirmation + fix decision*, not a re-derivation.

## The seam (confirmed)

q031 (`?inv … edw:product ?p . ?p edw:name ?pn . FILTER(?oh < ?rp)` LIMIT 5000) builds a **DimProduct RefObjectMap parent lookup** in `build_progress` (`build_parent_lookup`, `operator.rs:2036`) and **rebuilds it ~1,305 times** — one per driving batch — with no reuse. Arithmetic (09 §2): 8,973 file reads = 7,668 (one near-complete fact scan, cut at the 180 s DNF) + 1,305 single-file DimProduct re-scans; `scan_table.n = 1306 = 1 + 1305`.

**Why PR-4's memo doesn't catch it.** PR-4's `parent_lookup_cache` is a **per-operator field**, cleared on `close`. q031's correlation is an **inner join with an interposed non-pushable `FILTER(?oh<?rp)` + LIMIT**, and (`operator.rs:298`) a correlated join **rebuilds the whole operator per driving batch**, resetting that per-operator cache. So the lookup is content-identical every time but the cache that would hold it doesn't outlive the rebuild. (PR-4b admits R2RML leaves only to the *OPTIONAL* batched hash-join, so it doesn't catch an inner join either.)

## Proposed fix (option A — cache-lifetime extension)

Hoist the parent-lookup memo from the **per-operator** `parent_lookup_cache` to a **query-scoped** cache on `ExecutionContext`, mirroring the existing `const_sid_cache: Arc<Mutex<FxHashMap<…>>>` pattern (`context.rs:57,250`). Same key (`LookupCacheKey = (parent_tm_iri, join_cols)`), same content, same window-cap, same switch (`FLUREE_R2RML_PARENT_MEMO`). Because the lookup content is fixed by `(parent_tm, join_cols)` at a stable `as_of_t` (PR-4's own invariant), a query-scoped share across the rebuilt operator instances is valid — it just survives the per-batch rebuild that the operator-field cache doesn't. `build_progress` reads/writes the ctx cache instead of (or in addition to) `self.parent_lookup_cache`.

Expected: q031 `scan_table.n` 1306 → ~2 (one fact scan + one DimProduct lookup); DNF@180 s → completing.

**Alternative (option B, from 09 §2):** admit this inner-join R2RML correlation to a **batched hash-join** (PR-4b's inner-join sibling). Larger; a plan-seam change, not a cache change. Only if A is unsafe.

## Why this needs your nod

The fix moves the memo to a **different (query) operator lifetime** — the exact case you flagged. Option A is a clean lifetime extension (matches the "cache-lifetime extension of PR-4's LookupCacheKey" framing in 09 §5's tail order) and is my recommendation; option B is the fallback. One correctness point to confirm: a query-scoped parent-lookup cache is shared across *all* R2RML operators in the query — safe because the key pins `(parent_tm, join_cols)` and the content is `as_of_t`-stable, but it is a wider share than PR-4's per-operator cache, so worth an explicit ack.

## Out of scope (noted, not fixed)

The F8 co-factor (09 §2): `load_table.n = 7` for a 2-table query — the shared `edw:name` predicate fans `?p edw:name ?pn` to every name-bearing dim at resolution because there's no co-located `?p a Class`. That's a PR-3-widening / ref-target-class-resolution issue, independent of the re-scan memo.

## Gates (on nod)

Counting-mock test (N driving batches ⇒ 1 parent scan, mirroring PR-4's test) · q031 DNF→ok, `scan_table` 1306→single-digit · q015 fact-parent no-OOM sentinel · q050/q008 no-regression · corpus native 0-mismatch · suites.

## Implemented (Option A + the lead's 3 requirements)

A query-scoped `r2rml_parent_memo: Arc<Mutex<R2rmlParentMemoInner>>` on `ExecutionContext` (mirroring `const_sid_cache`), consulted in `build_progress` AFTER PR-4's per-operator cache (which is kept unchanged — its within-operator test still passes) and populated alongside it. On a ctx hit the per-operator cache is seeded too, so later batches of the same operator instance take the fast lock-free path.

1. **Key includes `graph_source_id` + `as_of_t`.** `R2rmlParentMemoKey = (graph_source_id, parent_tm_iri, sorted_join_cols, as_of_t)` — the wider (cross-operator, cross-source) share can't alias two sources' same-named tables or two snapshots. (PR-4's per-operator key stays the 2-tuple; an operator is single-source.)
2. **Total-rows bound.** `R2rmlParentMemoInner.try_insert` refuses an entry that would push the memo past `parent_memo_total_cap_rows()` — default **2× the materialize window** (`FLUREE_R2RML_PARENT_MEMO_TOTAL_WINDOWS`), on top of PR-4's per-entry `≤ 1 window` guard (q015 fact-as-parent). A refused insert falls through to a per-batch rebuild for that key, so a many-parent query can't grow the cache unbounded.
3. **Tests** (`r2rml::operator::tests::pr8b`): `parent_lookup_survives_operator_rebuild` (5 FRESH operators against one ctx ⇒ parent scanned once — the q031 seam; memo-off ⇒ 5) and `parent_memo_isolated_by_graph_source` (same table, `gs:A`/`gs:B` ⇒ 2 scans). PR-4's `parent_lookup_memoized_across_child_batches` (within-operator) still passes.

Switch: `FLUREE_R2RML_PARENT_MEMO` (reused — off ⇒ neither cache populates, today's per-batch rebuild). Compiles clean; full query crate lib suite 1229/0.
