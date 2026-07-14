# PR-F18 — q031 LIMIT-pushdown × correlated-driving re-scan — DESIGN SKETCH

**Branch:** `fix/f18-q031-memo-limit` (off `fix/f9-virtual-curie`, #1499 head e1ac1317f)
**Status:** SKETCH — **STOP for lead review** (no engine code until approved).
**North-star item 1** (AJ-signed slate). Target: q031 72 s → ≤~3 s cache-thrashed.

## Headline correction (evidence-forced, from the sketch investigation)

The F18 register/burndown framing ("parent-memo doesn't engage on the LIMIT-pushdown path; fix = make the memo engage") is **half right and needs sharpening**. New A/B (q031, hot):

| config | scan_table.n | wall |
|---|---:|---:|
| default (LIMIT_PUSHDOWN=1, PARENT_MEMO=1) | 1448 | 72 s |
| LIMIT_PUSHDOWN=1, PARENT_MEMO=0 | 1448 | 69 s |
| LIMIT_PUSHDOWN=0, PARENT_MEMO=1 | **8** | **81 s** |
| LIMIT_PUSHDOWN=0, PARENT_MEMO=0 | **8** | **76 s** |

Two facts fall out: **(1) the parent-memo is a NO-OP for q031** — on/off is identical in BOTH limit-pushdown states (1448 vs 8). So "make the memo engage" is not, by itself, the fix. **(2) neither current setting is fast:** LIMIT_PUSHDOWN=1 is 1448 re-scans (72 s); LIMIT_PUSHDOWN=0 is 8 scans but **~76-81 s** because with no budget the operator materializes the WHOLE `FACT_INVENTORY_SNAPSHOT` (no LIMIT cut). The real problem is a **LIMIT-pushdown row-budget × correlated-inner-join driving interaction**: the budget windows the driving so `build_progress` is re-invoked ~724× (each re-scanning fact + the DIM_PRODUCT parent → 1448 scans), and neither the per-operator cache (PR-4) nor the query-scoped memo (PR-8b) engages across those windowed rebuilds.

## (1) Mechanism — the code path

q031 = `?inv a InventorySnapshot ; edw:onHandQty ?oh ; edw:reorderPoint ?rp ; edw:product ?p . ?p edw:name ?pn . FILTER(?oh<?rp) LIMIT 5000`. Plan shape (per `12-pr8b` §, confirmed): a **correlated inner join** with an interposed non-pushable `FILTER(?oh<?rp)` + `LIMIT`; the R2RML scan is re-driven per driving batch.

- `set_row_budget` (`r2rml/operator.rs:2300`) records `self.row_budget = Some(budget)` under `limit_pushdown_enabled()`, and deliberately does **NOT** forward it to the child (2301: "an inner correlated scan must still produce every row the join needs").
- `next_batch` (2350) loops: step 2 advances an in-flight scan by ONE materialization window (`advance_one_window`, 2416); step 3, when no scan is in flight, pulls the next child batch and starts a fresh scan via `build_progress` (2427-2429). So **`build_progress` runs once per child batch.**
- The budget caps the window to the remaining budget (`window_rows = materialize_window_rows().min(b - emitted)`, ~1273). Under a budget the driving is chopped into many small windows/batches; without a budget it is one 512K-row window (`DEFAULT_MATERIALIZE_WINDOW_ROWS`, 206) — hence ~724 `build_progress` calls with the budget vs a handful without.
- Each `build_progress` runs the parent-lookup loop (1075-1250): for the `edw:product` RefObjectMap it consults the PR-4 per-operator cache (1108) then the PR-8b query-scoped memo (`ctx.r2rml_parent_memo.lock().get(ctx_key)`, 1135), and on a miss **scans DIM_PRODUCT** (1198) and publishes to both caches (1224/1242), gated by `lookup.len() <= materialize_window_rows()` (1223 — passes: DIM_PRODUCT ≪ 512K).

**The open sub-question (the fix must pin this):** the query-scoped memo key `(graph_source_id, parent_tm, join_cols, as_of_t)` is budget-independent and stable, and the memo lives on `ctx` (survives operator rebuild by design) — yet across the ~724 windowed `build_progress` calls it never hits (PARENT_MEMO on/off identical at 1448). So either (a) the correlated driver re-creates the operator with a fresh `ExecutionContext`/memo per driving batch (defeating the query-scoped share — the memo would then be populated-and-discarded each time), or (b) the ~724 re-invocations are NOT operator rebuilds but repeated `build_progress` on the same instance whose `parent_lookup_cache` was cleared, and the query-scoped consult is somehow skipped on this path. **Item 1 of implementation = a targeted trace on the memo get/insert + the `build_progress` invocation site to distinguish (a) vs (b)** — the fix differs by which it is.

## (2) Fix design — get the LIMIT cut AND a single fact+dim scan

The goal (lead's words): **8 scans AND the LIMIT cut** — neither current setting achieves it (8-scan path has no cut → 76 s; budget path cuts but re-scans → 72 s). The fix is NOT the memo alone. Two candidate directions, to settle at review:

- **(A) Hoist the parent-lookup out of the per-driving-batch `build_progress`.** Build the RefObjectMap parent lookups ONCE per operator-open (or make the query-scoped memo genuinely survive the correlated driver), and drive only the fact scan under the budget window. This keeps PR-5's LIMIT-cut (row_budget) AND removes the per-window dim re-scan. Extends **PR-8b** (the query-scoped memo is the right home) but requires fixing why it doesn't survive the driver (sub-question above).
- **(B) Decouple the LIMIT cut from the driving granularity.** Drive the correlated structure at full-window materialization (one build_progress, few scans — the 8-scan shape) but honor the budget as an early-termination on OUTPUT rows, so the fact scan stops once 5000 post-FILTER survivors are emitted WITHOUT re-invoking build_progress per window. This is a change to the budget×window interaction in `next_batch`/`advance_one_window`, i.e. **PR-4a's** limit-early-termination lifecycle.

Recommendation to decide at review: **(B) if the ~724 re-invocations are the dominant cost** (they are — 1448 scans), because it removes the re-invocation entirely rather than patching the memo to paper over it; (A) if the driver genuinely rebuilds the operator (then the memo-survival fix is unavoidable and also fixes any sibling shape). The sub-question's answer picks the lane.

## (3) Invariants

- **LIMIT-cut correctness:** exactly 5000 **post-FILTER** survivors (the FILTER is a residual `consumed_filter`; `emitted` counts survivors, 2383-2405). The fix must not emit rows the FILTER would drop, nor stop before 5000 survivors.
- **Parent-lookup content stability:** the lookup is fixed at `as_of_t` (1191/1231); building it once and reusing is valid for every window (no novelty between windows in one query). No stale/duplicate parent rows when the budget truncates mid-window.
- **Scan-cache interaction:** the scan cache is disabled under a budget (`cacheable = … && self.row_budget.is_none()`, 989) — because a budget-truncated scan is a partial result that must never poison the `(table, projection)` cache. The fix must preserve that (a hoisted parent-lookup is a full dim scan, not budget-truncated, so it remains cacheable — good).
- **Top-k interaction:** `set_topk` mirrors `set_row_budget` (both "topmost row-preserving scan only", not forwarded to child, 2312-2319); q031 has no ORDER BY so topk is inert, but the fix must not perturb the topk cache-bypass (`cacheable && topk.is_none()`).

## (4) Kill switch

Rides an existing switch, chosen by the fix lane: **(B)** is a change to the LIMIT-pushdown driving → gate under **`FLUREE_R2RML_LIMIT_PUSHDOWN`** (with it off, the pre-fix full-window/no-cut path is restored — byte-identical, already the OFF baseline). **(A)** is a memo-survival fix → gate under **`FLUREE_R2RML_PARENT_MEMO`** (off = today's per-batch rebuild). Byte-identical-off argument: the corpus hash + the ON/OFF differential (the DoD's hermetic + live) must show OFF == current head byte-for-byte on every corpus query. New switch only if the fix spans both knobs; prefer riding an existing one.

## (5) Blast radius

**Shape = correlated inner-join to a dimension via RefObjectMap + a non-pushable residual FILTER + LIMIT** (the driving-batch re-invocation). Corpus scan:
- **q031** — the clear case (this PR).
- **q028** (WebEvent→product, `LIMIT 5000`, no residual var-FILTER) is a sibling dim-join but only 3.9 s — likely fewer driving batches (no selective FILTER interposed); check it doesn't regress and note whether the fix helps it.
- **q015** (fact-as-parent, `LIMIT`) — `12-pr8b` flags it as the memo's fact-parent guard (not retained); confirm the fix leaves it unchanged (its parent is a fact, `lookup.len() > window` → 1223 refuses; the hoist must respect that guard).
- **q016/q050** are batched-OPTIONAL (F14/PR-4d), a different driver — out of scope.

**Expected walls (honest, cache-thrashed AND warm):** q031 → **low-single-digit s cache-thrashed / sub-second warm** (one fact scan cut at 5000 survivors + one DIM_PRODUCT scan). Not 0.2 s (warm-only artifact). Meets the ≤~3 s bar. No other corpus query's wall or hash may move (full-corpus differential).

## DoD (fixed by the lead)

1. **q031-shaped hermetic differential:** N driving batches under LIMIT pushdown ⇒ **exactly 1 parent scan** (mirrors the PR-8b counting-mock test but with a row_budget set); ON/OFF byte-identical output; a counter/span asserting build_progress-count and dim-scan-count.
2. **Live q031 ≤~3 s cache-thrashed** on virtual-sf01, with scan/memo counters proving the single-scan engagement (not a warm-cache artifact — measure in full-corpus/cold-thrashed order).
3. **Full-corpus baseline at the PR head** (the new gate protocol: cache-thrashed sentinels, per-query manifest `timeout_s`, priming + 3-rep) — no other query regresses in wall or hash.
4. Native 54/54 + W3C + unit sweeps green; **zero native-path change** (this is r2rml-operator-only; native never instantiates R2rmlScanOperator).

**STOP — design review before implementation.** Key decisions for the lead: (i) fix lane A vs B (pending the sub-question trace — which I'll run first if you want the answer before you rule); (ii) which kill switch; (iii) confirm the blast-radius set (q031 only, q028 watched).
