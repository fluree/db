# 22 — PR-F17: UNION (and BIND) row-budget forwarding for q029

**Status:** IMPLEMENTED + live-gated (branch `perf/r2rml-f17-union-budget`). *(class: structural, mechanism-class D — budget propagation through UNION, never shipped; PR-5 shipped only the ORDER-BY top-k leg.)*

> **MEASURED OUTCOME (2026-07-15, gate `pf_f17_gate.log`) — supersedes the pre-measurement predictions below.** The fix works, but the win is **re-drive elimination, not file-level early-stop**: virtual q029 **166.7 s → 45.9 s (3.6×)**; parquet decodes **1,940,510 → 15,406 (126×)**; bytes fetched **18.8 GB → 150 MB**; `scan_table` 253 → 4. Two predictions **REFUTED**:
> 1. **NOT byte-identical (finding-4 below is wrong).** ON hash ≠ OFF hash. `edw:eventType "purchase"` is a *post-decode residual* (constant-object / `consumed_filter`, operator.rs:1679-1688) and the scan window is geometric-growth (operator.rs:1382, ×4→512K), so the budget materializes a *different valid* first-100 → `rows_only`, same class as q055/q057. Each branch still decodes a **full 7,670-file sweep** to accumulate 100 rare matches, so the residual = the **PR-2a per-file decode floor** (45.9 s ≈ 15,406 tiny-file decodes × per-file overhead), shared with q016/q031/q056. The budget bounds OUTPUT rows; a low-yield residual is only met after a near-full INPUT scan — the budget cannot skip files.
> 2. **The "§4 correlated-lever is nil for q029" claim is also refuted.** q029's two branches each scan fully because the union runs both before the budget is met (union.rs:299), so the deferred secondary lever (break the union loop at `pending_output_rows ≥ row_budget`) *would* skip branch-2 → ~1 sweep ≈ **~23 s**. q029 stays a burndown row (~46 s as shipped; ~23 s with the lever); **low-seconds needs PR-2a either way.** Load-bearing pre-measurement claims are marked inline below.

## TL;DR

q029 = `{ WebEvent, eventType="purchase" } UNION { WebEvent, eventType="add_to_cart" } LIMIT 100` costs **~150 s / 1.94 M file reads for a 100-row answer** because the `LIMIT 100` row budget never reaches the two `FACT_WEB_EVENT` branch scans, so each scans the whole 7,670-file table. The budget dies at **two absorb points** on the way down: the `UnionOperator` and the `BindOperator` (both inherit the trait's no-op `set_row_budget`). Fix = teach both to forward the budget — **UNION forwards the full `k` to each branch; BIND forwards `k` to its child** — so the R2RML scan caps its materialize window at `k`. [⚠️ the sketch predicted this stops the scan "≈ at the first file"; MEASURED, it does not — `eventType` is a low-yield *post-decode* residual, so each branch still sweeps the whole table for 100 matches; the win is killing the 253× re-drive, not file-level early-stop. See banner.] Both are sound row/order-preserving pass-throughs; the existing absorb-boundary contract already makes every unsafe case (ORDER BY, DISTINCT, FILTER, in-branch reorder) decline automatically. One kill switch, `rows_only` parity (NOT byte-identical — refuted), corpus blast radius = **q029 only**.

## The query and the verified plan

```sparql
SELECT ?e ?et
WHERE { { ?e a edw:WebEvent ; edw:eventType "purchase"     . BIND("purchase"     AS ?et) }
        UNION
        { ?e a edw:WebEvent ; edw:eventType "add_to_cart" . BIND("add_to_cart" AS ?et) } }
LIMIT 100
```

Each branch is a single-subject star with a **constant-object** constraint. Per `r2rml/rewrite.rs:273-315` + `r2rml/operator.rs:508-657`, `edw:eventType "purchase"` fuses into the R2RML scan as a `star_constraints` / `object_constant` (operator.rs:623-624: "a scalar constant-object equality pushes as a scan filter … the operator enforces correctness") — **there is no separate `FilterOperator`.** So the branch operator tree is:

```
Limit(100) → Project(?e,?et) → Union → [per branch] Bind("…" AS ?et) → R2rmlScan[FACT_WEB_EVENT, eventType=const]
```

The scan emits **only matching rows** and honors a `row_budget` by capping its materialize window (operator.rs:1273-1276) and early-stopping once `emitted >= budget` (operator.rs:2390 / 2405). So a budget that *reaches the scan* cuts it to ~`k` rows ≈ the first file.

## Why the budget dies today — two absorb points

`set_row_budget` overrides today (grep): `offset.rs:145`, `join.rs:1132`, `limit.rs:93`, `graph.rs:639`, `project.rs:58`, `r2rml/operator.rs:2300`. Everything else inherits the trait **no-op** default (`operator.rs:98`) — i.e. **absorbs**. The chain for q029:

- `Limit.open()` computes `budget = min(inherited, limit)` and calls `child.set_row_budget(budget)` **before** opening (limit.rs:85). ✓
- `Project.set_row_budget` forwards to its child (project.rs:58). ✓
- `Union` — **inherits the no-op ⇒ the budget dies here.** (absorb point #1) ✗
- Even if it forwarded, `Bind` — **inherits the no-op ⇒ dies here too.** (absorb point #2) ✗
- `R2rmlScan` — would honor it. ✓

Both #1 and #2 must forward for the budget to reach the scan.

## Why a union-level drain cap is NOT enough (rejected alternative)

A tempting simpler fix: leave Bind/scan alone and have the union **stop draining a branch after `k` rows** (union.rs:311 `while let Some(batch) = branch_op.next_batch()`). This does not work: without a `row_budget`, the scan's materialize window is `DEFAULT_MATERIALIZE_WINDOW_ROWS = 512*1024` (operator.rs:206, 1275). FACT_WEB_EVENT is ~427 K rows, so the **first `next_batch` materializes the entire table in one window** (reading all 7,670 files) before emitting the first `ctx.batch_size` batch. Stopping the union's pull afterward saves nothing — the reads already happened. The budget must reach the scan so the *window itself* is capped at `k` (operator.rs:1274). ⇒ forwarding is mandatory; a drain cap is not a substitute.

## (1) Semantics — why per-branch budget `= k` (not `k/n`) is sound, and exact

The `UnionOperator` drains its branches **in order** into one output buffer (union.rs:299-325); the consuming `LIMIT k` takes the first `k` of that concatenation `B1 ++ B2 ++ … ++ Bn`.

**Claim [⚠️ REFUTED by measurement 2026-07-15 — see the banner. The argument below assumes a deterministic per-branch scan order; the R2RML scan's window is geometric-growth and its window materialization is parallel/not order-stable, so budgeting materializes a *different valid* first-`k`. The forwarding is sound under `rows_only` (correct 100-row multiset, native-parity green), NOT byte-identical. Kept for the trail.].** Giving *each* branch a `row_budget` of `k` yields the identical first-`k` rows as the unbudgeted plan (exact-LIMIT-preserving, not merely rows_only).

**Argument.** A budgeted branch `Bi` emits the same prefix `Bi[0 .. min(k,|Bi|)]` it would unbudgeted, in the same order (the scan reads files in deterministic order and stops after `k` *emitted* rows; row/order-preserving operators above it preserve that order). Let branch `j` contain the `k`-th union row. For `i < j`, `LIMIT` consumes all of `Bi`, and the rows consumed from `B1..Bj-1` total `< k`, so each `|Bi| ` actually consumed `≤ k = budget` — the budget never truncates a *consumed* row. For `i = j`, `LIMIT` consumes a prefix of `Bj` of length `≤ k = budget` — present. For `i > j`, every row is discarded by `LIMIT` regardless. Hence the first `k` union rows are byte-identical to the unbudgeted plan. ∎

**Consequences.** (a) Each branch needs the *full* `k` — a single branch may supply all `k` rows, so `k/n` would under-produce (CONFIRMED). (b) [⚠️ REFUTED — see banner] the claim that ON == OFF is byte-identical is false: the scan order is *not* deterministic (geometric window + parallel materialize), so the budget picks a different valid `k`-subset. The honest gate is `rows_only` (100-row count + `?et ∈ {purchase, add_to_cart}` invariant + native-parity), same class as q055/q057. (c) Budgeting still doesn't change the correct *answer set* under `rows_only` — it avoids the re-drive, not the file fan-out.

**In-branch soundness is automatic.** If a branch internally reorders or drops rows (an in-branch `ORDER BY`, hash-join build, `DISTINCT`, `FILTER`), the reordering/dropping operator **absorbs** the forwarded budget by the existing contract (operator.rs:90-98 doc) and the scan below runs full — so "first `k` with budget" can never diverge from "first `k` without", branch-internally too. F17 inherits this for free.

## (2) Forwarding mechanics

**BindOperator** (`bind.rs`): add
```rust
fn set_row_budget(&mut self, budget: usize) { self.child.set_row_budget(budget); }
```
BIND is 1:1 and order-preserving (a failed eval yields UNBOUND, the row still passes), so forwarding a row-budget is always sound. Bind was simply **missing** from the row-preserving forwarding set alongside Project/Offset/Limit. (Update the operator.rs:90-98 doc: move `Bind` out of the "absorb" list into the forwarding list.)

**UnionOperator** (`union.rs`): add a field `row_budget: Option<usize>`, override
```rust
fn set_row_budget(&mut self, budget: usize) { self.row_budget = Some(budget); }
```
and in `next_batch`, after building each `branch_op` (union.rs:302-308) and **before** `branch_op.open()` (union.rs:310):
```rust
if let Some(b) = self.row_budget { branch_op.set_row_budget(b); }
```
Branches are rebuilt per input row (union.rs:302 `build_where_operators_seeded`), so the budget is **re-applied to each fresh branch tree** — no decrement / no replenish bookkeeping. Each `(input-row, branch)` scan is independently capped at `k`, which is a sound upper bound (§1).

**Reset / replenish across branch switches:** none needed. `self.row_budget` is set once (before `open`, via the pass-through chain) and re-applied verbatim to every branch build. It is *not* a shared decrementing budget — each branch gets `k`.

**`set_topk` is deliberately NOT forwarded through UNION** (stays the no-op). A per-branch scan-side top-k *would* be sound for `UNION … ORDER BY LIMIT` (global top-k ⊆ ∪ per-branch top-k), but that is a separate, larger optimization; F17 is the unordered row-budget leg only. Leaving `set_topk` absorbed means ordered unions correctly decline (§3).

## (3) Decline cases — all handled by the existing absorb boundary

| case | why unsound to budget | how it declines |
|---|---|---|
| `ORDER BY` above the union | sort must see every row to rank; a branch stopping at `k` can miss a lower-sorted qualifier | the `Sort` between LIMIT and UNION inherits the no-op `set_row_budget` ⇒ **absorbs** ⇒ the union never receives a budget. Automatic. |
| `DISTINCT` above the union | `k` unique may need `> k` raw rows; capping branches at `k` can yield `< k` distinct | `Distinct` absorbs (no-op) ⇒ union gets no budget. Automatic. |
| `FILTER` between LIMIT and the scan (row-dropping) | scan's `k` emitted → filter drops some → `< k` pass | `Filter` absorbs (no-op). Automatic. (q029's `eventType` is *not* this case — it is a fused scan constraint, so the scan's `emitted` already counts matches.) |
| in-branch reorder / dedup | as above, branch-internal | the in-branch reordering op absorbs; scan below runs full. Automatic. |
| nested unions `{{A} UNION {B}} UNION {C} LIMIT k` | — (sound) | the inner union is built inside the outer's branch; the outer forwards `k` to it, the inner (now overriding `set_row_budget`) forwards `k` to *its* branches. Recurses correctly; each leaf branch supplies up to `k`. |

**Net:** the only *new* forwarding is UNION + BIND. Every decline is enforced by an operator that already absorbs — F17 adds **no new decline logic**, which is the strongest possible soundness story (no new predicate to get wrong).

## (4) Blast radius

- **Corpus, UNION:** `q029` (UNION+LIMIT → **fixed**) and `q042` (UNION, **no LIMIT** → no budget is ever set → unaffected). No corpus UNION carries `ORDER BY`/`DISTINCT`, so the union-decline paths are exercised by hermetic tests only.
- **Corpus, BIND:** `q029` is the **only** corpus query with `BIND(...)` under a `LIMIT`, so the Bind-forwarding change touches **exactly q029** in the corpus. Other BIND uses (if any) carry no LIMIT ⇒ no budget reaches them.
- **Outside the corpus:** Bind-forwarding is general query-engine (not R2RML-specific). Any `LIMIT … Bind … <budget-honoring scan>` shape now pushes the budget one level deeper. The native ledger scan's `row_budget` handling must be re-checked (if it honors budgets, native `Bind+LIMIT` shapes get earlier stops — sound but a perf/behavior change; if it ignores budgets, Bind-forwarding is a native no-op). Covered by the DoD's **BSBM + native-corpus budgets unregressed** and **kill-switch off = byte-identical** clauses.
- **Expected q029:** each branch scan 7,670 files → ~1-2 files (`k=100` from the first file, purchase/add_to_cart are common event types); reads 1.94 M → ~few hundred; **~150 s → low-single-digit s** cache-thrashed. If `eventType` were rare the win shrinks (budget bounds *output*, and the scan reads files until `k` matches accumulate) — note but not a q029 concern.

**Secondary lever — the union runs all branches before the budget is met [⚠️ CORRECTED 2026-07-15: this DOES affect q029, contra the original "nil for q029"].** The union's branch loop (union.rs:299) runs *every* branch for an input row before returning, so once branch-1 fills the budget, branch-2 still scans fully. For q029 (1 unit-seed input row, 2 branches) that means **branch-2 decodes a whole extra 7,670-file sweep after branch-1 already supplied 100** — measured as the 2nd of the two sweeps in the 45.9 s. The lever — break the union's input/branch loop once `pending_output_rows >= row_budget` (and for a correlated union, the per-`(row,branch)` cap of `k` also bounds the multi-input case) — skips branch-2 → ~1 sweep ≈ **~23 s**. Still bottoms out on the 1-sweep PR-2a floor. **INCLUDED in F17 (lead-ruled 2026-07-15, deferral reversed once its premise died):** the union sets a `budget_met` flag and stops building further branches / pulling further input once the budget is met, same `FLUREE_R2RML_UNION_BUDGET` switch. Sound on two counts — a budget reaches the union only under row/order-preserving operators up to the LIMIT (Sort/Distinct absorb) ⇒ arbitrary-k-subset (`rows_only`), and the check fires only when the budget is actually MET so an under-filled branch-1 still runs branch-2. Build-count hermetic tests: fills⇒branch-2 skipped, under-fills⇒branch-2 built, no-budget⇒both run. Wall re-gate pending AJ's network window (quarantined).

## (5) Hermetic tests, kill switch, gate / DoD

**Kill switch.** Single env gate for both overrides, e.g. `FLUREE_R2RML_UNION_BUDGET` (default on; `=0` ⇒ both `UnionOperator` and `BindOperator` `set_row_budget` become the no-op ⇒ byte-identical old behavior). *Open q for lead: one switch for both, or fold under the existing `limit_pushdown_enabled()` that already gates the scan's budget storage (operator.rs:2307)?* Recommendation: a dedicated F17 switch so the Bind reclassification can be toggled independently of PR-5's pushdown.

**Hermetic operator tests** (no Snowflake; a `CountingProvider`/in-memory scan that records `emitted` per branch):
1. `{A} UNION {B} LIMIT k` ⇒ each branch scan sees budget `k`; total scanned ≤ `2k`; result == unbudgeted first-`k` (byte-identical).
2. `Bind(Scan) LIMIT k` ⇒ scan sees budget `k` (the reclassification, standalone).
3. **DECLINE:** `{A} UNION {B} ORDER BY ?x LIMIT k` ⇒ branch scans see **no** budget (Sort absorbs); full scan; result correct.
4. **DECLINE:** `DISTINCT { {A} UNION {B} } LIMIT k` ⇒ no budget reaches branches; distinct count correct (would under-count if budgeted).
5. **DECLINE:** in-branch `FILTER` below the BIND ⇒ scan runs full (Filter absorbs), `k` correct.
6. Nested `{{A} UNION {B}} UNION {C} LIMIT k` ⇒ all three leaf scans see budget `k`.
7. Kill-switch off ⇒ every above scan sees no budget (byte-identical to pre-F17).

**Corpus gate [measured 2026-07-15].** q029 ON vs OFF is **`rows_only`** (100-row count + `?et ∈ {purchase, add_to_cart}` invariant + oracle-parity green) — NOT byte-identical (the finding-4 prediction was refuted; the budget picks a different valid 100). The **primary gate is the decode collapse**: parquet decodes 1,940,510 → 15,406 (`scan_table` 253 → 4), mirroring PR-4d's "gate on scan-count + parity, not wall". Full-corpus per-head baseline: 59/59 (incl. the untouched exploration family) 0-mismatch; **q042** (no-LIMIT union) unaffected; BSBM + native unregressed with the switch **on**.

**DoD (shared clauses):** q029 hits low-single-digit s cache-thrashed (or: scan-count collapse + parity, wall credited to the shared fact-decode floor if the residual is the cold FACT_WEB_EVENT read) · parity hashes green (`hash_gate` honored) · W3C SPARQL suite green · BSBM + native budgets unregressed · kill-switch off ⇒ byte-identical.

## Open questions for the lead

1. **Bind reclassification** — OK to move `BindOperator` from "absorb" to "forward" generally (gated by the F17 switch), updating the operator.rs:90-98 contract doc? It is sound (1:1, order-preserving) and the corpus blast radius is exactly q029, but it is a general-engine change, not R2RML-scoped. Alternative (more surgical, rejected for complexity): thread the budget into `build_where_operators_seeded` and set it directly on the branch's scan leaf, leaving Bind's global contract untouched.
2. **Kill switch** — dedicated `FLUREE_R2RML_UNION_BUDGET`, or fold under `limit_pushdown_enabled()`?
3. **Correlated-union secondary lever** (break the union's input/branch loop at `pending_output_rows >= row_budget`) — include now or defer? No corpus query needs it (q029 is top-level).
4. **Overlap with PR-4d** — the register flags q029's ~253× re-drive as the same re-scan *family* as F14/PR-4d, but they are different operators (UnionOperator vs batched OPTIONAL); PR-4d does **not** touch UNION. Confirm F17 ships as its own PR (recommended).
