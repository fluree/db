# Fluree DB — Architecture & Rust-Practice Audit

**Date:** 2026-06-11
**Scope:** Full workspace (38 crates, ~460k lines of non-test Rust; ~594k total). Seven parallel deep-explorations: crate layering, core data model & ID spaces, binary-index/overlay contract, query engine organization, state lifecycle, write path, and a metrics-driven hygiene sweep. Load-bearing claims were verified directly against source.

---

## 1. Executive summary

**The performance architecture is sound and the engineering discipline is real.** Workspace-wide `thiserror`, CI `-D warnings`, newtypes already in place for most ID spaces (`PredicateId`, `StringId`, `LangId`, `DatatypeDictId`, `TxnT`), a shared `fast_path_common.rs` that already deduplicated ~1.1k LOC, regression tests pinned to past bugs, and genuinely good design docs (`docs/design/query-execution.md`). The hygiene sweep grades the codebase **B+**: issues are incidental, not systemic rot.

**The systemic risk is precisely the one you named.** The three most important contracts in the system are enforced by *convention and runtime discipline* rather than *by construction*:

1. **The row↔columnar translation contract** — overlay `Flake`s (row world, `Sid`-based) translated into binary-index integer-ID space (`s_id`/`p_id`/`o_key`). There are **5 distinct merge implementations** and **2 translation wrappers** with subtly different fact-identity definitions and failure behaviors.
2. **The ledger-state coherence contract** — `LedgerState` is four independently mutable `pub Arc` fields (`snapshot`, `novelty`, `dict_novelty`, `runtime_small_dicts`) whose consistency, plus the *Arc-pointer-identity* of `dict_novelty` inside `BinaryRangeProvider`, is maintained by hand at every mutation site.
3. **The fast-path eligibility contract** — 33 shape detectors and 13 specialized operators each independently choose one of two overlay-safety gates; a wrong choice produces silently wrong results, not an error.

Every chronic stateful bug class in project memory (disappearing properties on failed overlay translation, namespace-code drift after index attach, stale `DictNovelty` after refresh, lost graph-registry deltas) maps onto one of these three conventions. **The audit found the conventions are currently held everywhere checked** — all 12 fast-path gates are correctly applied today — but nothing except review discipline keeps them held.

The plan in §4 converts each convention into a compile-time or single-chokepoint guarantee **without touching the hot inner loops**: newtypes are `#[repr(transparent)]` (zero cost), the merge chokepoint is a refactor of dispatch not of kernels, and the coherent-state swap (`ArcSwap`) is *cheaper* on the read path than the current `RwLock`.

---

## 2. How the system coheres today

The implicit layering is healthy and close to what `docs/reference/crate-map.md` claims:

```
server / cli
   └─ api  (facade + orchestration: ledger_manager, import/export, formatting, datasets, cross-ledger)
       ├─ query ←─ sparql (lowering)        ├─ transact (write pipeline orchestrator)
       │    └─ policy / reasoner             │
       ├─ ledger (snapshot + novelty state)  ├─ shacl
       ├─ indexer (build) / binary-index (read)
       ├─ novelty / connection / nameservice
       └─ core (Flake, Sid, ContentId, range, LedgerSnapshot) ── vocab
```

Verified deviations from the ideal:

- **[LAYERING]** `fluree-db-indexer` optionally depends *upward* on `fluree-db-ledger` via the `embedded-orchestrator` feature (`fluree-db-indexer/Cargo.toml:12,19`), used only by `orchestrator.rs`. Orchestration belongs above the index builder, not inside it.
- **[TYPE-ERASURE]** `TypeErasedStore(pub Arc<dyn Any + Send + Sync>)` (`fluree-db-ledger/src/lib.rs:53`) exists so `LedgerState` can carry a `BinaryIndexStore` without `ledger → indexer`/`binary-index` coupling, downcast in `fluree-db-api/src/ledger_manager.rs`. It is a *deliberate* dependency inversion — but it is also a hole in the type system at the exact point where the state-coherence bugs live. A small `ledger-state` trait crate (or moving the store handle into a typed field once Phase 2 lands) would retire it.
- **[GOD-CRATE]** `fluree-db-api` (77k lines, 110 files) is a legitimate facade that has accreted non-facade business logic: `import.rs` alone is **5,880 lines**, plus `format/` (~16 output formats), `cross_ledger/`, `ledger_manager.rs`, `export.rs`. The crate name says "API"; the content says "API + ETL + formatting + state cache + governance."
- **No duplicate domain types across crates** — `Flake`, `Commit`, `ContentId` are defined once. Duplication lives *within* the query crate (§3.4), not across crate boundaries.

---

## 3. Findings by area

### 3.1 ID spaces and core types — good bones, three holes

The core types are mostly newtyped. The exceptions are exactly where past bugs happened:

| Finding | Evidence | Why it matters |
|---|---|---|
| `GraphId` is a bare alias: `pub type GraphId = u16` | `fluree-db-core/src/ids.rs:73` | Freely cross-assignable with namespace codes (also bare `u16`). Graph scoping and namespace encoding are both pervasive `u16`s in the same functions. |
| Namespace codes are bare `u16` everywhere, and there are **two distinct namespace spaces** (snapshot-space vs binary-store-space) carried in the same type | `ns_encoding.rs`, `binary_scan.rs`; past bug fixed in `it_namespace_new_after_index.rs` | The "new namespace after index attach" bug was precisely a cross-space confusion. The fix is convention ("keep bound SIDs in snapshot space, decode→re-encode for store filters") that the type system cannot see. |
| Datatype code width disagreement: `DecodedRow.dt: u32` vs `OverlayOp.dt: u16` vs `RunRecord.dt: u16` | `fluree-db-binary-index/src/types.rs:72,100`; `format/run_record.rs:83` | Same semantic value, three widths, implicit widening/narrowing across the overlay-translation pipeline — the system's most bug-prone seam. |
| `LedgerSnapshot` exposes coupled `pub` fields: `t`/`base_t`, `subject_watermarks`/`string_watermark` (must mirror `DictNovelty` watermarks), `graph_registry` (must mirror commit envelope deltas) | `fluree-db-core/src/db.rs:64-134` | Any caller can break the pairing; the invariants are stated in comments, enforced nowhere. |
| `DictNovelty` guards initialization with an unconditional runtime panic, and its correctness depends on being **the same Arc instance** shared with `BinaryRangeProvider` | `fluree-db-core/src/dict_novelty.rs:25,63,136`; `fluree-db-api/src/ledger_manager.rs:341-347` | Arc *identity* (not value) as a correctness requirement is invisible to the compiler, to clippy, and to most reviewers. |

The bright side: `namespace_codes`/`namespace_reverse` are already private with mutation funneled through methods — the pattern to extend, not invent.

### 3.2 The overlay merge contract — one concept, five implementations

Distinct implementations of "merge columnar base with row overlay," each with its own fact-identity and tie-breaking:

| # | Site | Semantics |
|---|---|---|
| 1 | `BinaryCursor::merge_overlay_into_batch` — `fluree-db-binary-index/src/read/binary_cursor.rs:457-612` | Two-pointer merge on V3 identity `(s_id, p_id, o_type, o_key, o_i)`; overlay replaces/suppresses base on equal identity |
| 2 | `fast_post_order_limit::collect_post_desc_topk_overlay` — `fluree-db-query/src/fast_post_order_limit.rs:512-678` | Descending row-set merge; asserts sorted DESC, retracts as hash set |
| 3 | `count_predicate_overlay_delta` — `fluree-db-query/src/fast_path_common.rs:2291-2396` | Arithmetic: `base_total − base(touched) + merged(touched)` per leaf |
| 4 | Export/graph-crawl raw-flake post-pass — `fluree-db-api/src/export.rs:79-122`, `format/graph_crawl.rs` | Parallel translated + *untranslated raw Flake* streams; dedup by `FlakeValue` identity (NOT the V3 tuple); latest-t-wins, tie prefers retract |
| 5 | `range_with_overlay` — `fluree-db-core/src/range.rs:68-142` | Trait-delegated; the generic fallback everyone else must agree with |

Translation (`Flake → OverlayOp`) is funneled through `translate_one_flake_v3_pub` (`fluree-db-query/src/binary_scan.rs:2268-2360`) but its **failure handling differs per call site**: `Ok(None)`-fallback at one (`:1811→:1844`), untranslated-list at another, and **WARN-and-drop for non-`Unsupported` errors in the export path** (`binary_scan.rs:2238-2242`) — a residual instance of the exact bug class (silent data drop) that was previously fixed in graph crawl.

The gating contract (`fast_path_common.rs:2663-2705`): strategy (a) `fast_path_store`/`allow_fast_path` (bail when *any* overlay/time-travel/policy/multi-ledger), strategy (b) `allow_cursor_fast_path` (admit overlay because the cursor merges it). **All 12 fast paths were verified to use a correct gate today.** But the gate choice is one function call in each operator's `open()`; nothing structural prevents the 14th fast path from picking wrong, and a wrong pick is silently wrong results.

### 3.3 State lifecycle — the torn-state machine

A ledger's in-memory life is an implicit state machine (genesis → indexed → indexed+trailing-novelty → mid-refresh → reloading) spread across `LedgerHandle { RwLock<LedgerState>, RwLock<Option<Arc<BinaryIndexStore>>> }` (`fluree-db-api/src/ledger_manager.rs:102-135`) and four CoW Arc fields in `LedgerState` (`fluree-db-ledger/src/lib.rs:81-125`).

Highest-confidence findings (flagged by the lifecycle audit; **confirm before fixing**, they are read-level analyses, not reproduced bugs):

- **[ARC-IDENTITY]** `apply_index_v2` builds `BinaryRangeProvider` with `Arc::clone(&state.dict_novelty)` (`ledger_manager.rs:341-347`). A later commit's `Arc::make_mut` on `state.dict_novelty` (CoW because the provider holds a ref) detaches the provider's copy → overlay translation misses post-refresh novelty IDs. This is the same family as the gotcha already recorded in project memory; the audit suggests the *structural* exposure is still present.
- **[TORN-STATE]** `apply_loaded_db` (`fluree-db-ledger/src/lib.rs:548`) merges old namespace codes into the new snapshot (`:606-622`) but does **not** replay graph-registry deltas from remaining novelty commits — candidate for graph-IRI loss after refresh.
- **[RACE]** Reload swap condition `new_state.t() >= current.t()` (`ledger_manager.rs:~1157`) compares a max(novelty, snapshot) watermark that a concurrent commit can move mid-load.
- **[LOCKING]** `state`-before-`binary_store` lock ordering is documented (`ledger_manager.rs:116`) but convention-only, and `sync_binary_store_from_state` (`:187-200`) — written to close the snapshot/store TOCTOU — is not invoked in `apply_index_v2` (`:351`).
- **[TEST-GAP]** No test exercises refresh-while-querying, commit-during-refresh, or reload-vs-commit. The one regression test (`it_dict_novelty_apply_loaded_db.rs`) validates sequential behavior only.

### 3.4 Query engine — phenomenal machinery, organizational debt

- **[FAST-PATH-SPRAWL]** 33 `detect_*` shape recognizers (verified) dispatching to 13 `fast_*.rs` operator files (~10k LOC). `operator_tree.rs` (3,692 lines) runs them as a 600+-line sequential if-chain, each arm recursively building the generic tree as `fallback`. The *operators* are well-built; the *dispatch and gating* are the sprawl.
- **[PLANNER-MONOLITH]** `where_plan.rs` (3,570 lines) interleaves pattern reordering, filter-pushdown analysis, OPTIONAL coalescing, star fusion, and operator construction. Variable-readiness logic is implemented three times (`partition_eligible_filters` `:697`, `apply_eligible_binds` `:725`, `inline_eligible_filters` `:979`).
- **[EXPLAIN-GAP]** When a fast path bails at runtime (`Ok(None)` → fallback), EXPLAIN still shows the *planned* operator. For a system whose contributor-facing question is "which path did my query take," this is the single cheapest comprehension win available.
- **[BINDING-STATES]** `Binding` has 11 variants spanning three regimes (decoded, late-materialized `Encoded*`, control-flow `Poisoned`). Two latent contracts are doc-comment-only: `Poisoned` must block matching (documented at `binding.rs:35-37`), and `EncodedSid` is single-ledger-only (raw `s_id` comparison is wrong cross-ledger; currently prevented by construction in `DatasetOperator`, enforced nowhere).
- Error/panic style inside operators is disciplined: invariant-guarded unwraps, `catch_unwind` around parallel workers. `join.rs` (3,755 lines) carries ~20 such unwraps without invariant comments.

### 3.5 Write path — clean pipeline, opt-in safety

The transact pipeline is a clear 10-stage sequence (parse → WHERE resolve → instantiate/cancel → policy → SHACL → commit build → CAS publish → novelty apply → index trigger) and **reuses the query engine for WHERE resolution — no read/write duplication**. Crash-consistency is sound by design (commit blob + nameservice CAS durable; novelty volatile and rebuilt by replay). Three structural notes:

- **[VALIDATION-BYPASS]** Policy enforcement is `policy_ctx: Option<&PolicyContext>` threaded per call (`fluree-db-transact/src/stage.rs:80,382-398`). The default `transact()` path passes `None`. Whether writes are policy-checked is a property of the *call site*, not the *ledger*. Same opt-in shape for SHACL.
- **[VALIDATION-TIMING]** SHACL validates pre-state + staged flakes, never final post-state; a transaction that deletes the last `:name` of a `Person` passes a min-count shape.
- **[PARTIAL-FAILURE]** Lost CAS race leaves a durable, unreferenced commit blob with no GC path.

### 3.6 Hygiene metrics (workspace-wide)

| Metric | Value | Read |
|---|---|---|
| `.unwrap()` / `.expect()` (incl. tests) | ~9.2k / ~5.3k | Sampled hot-path instances are invariant-guarded but undocumented |
| `unsafe` blocks | 46 (~20 without `SAFETY:`) | Mostly justified (UTF-8 post-validation, libc in CLI); docs missing |
| Error style | `thiserror` in 36/37 crates | Coherent but shallow: `Other(String)`/`Storage(String)` catch-alls in `fluree-db-core/src/error.rs:10-54` |
| Lints/CI | 26 workspace deny rules, clippy `-D warnings` | Strong |
| Files >3k lines | `import.rs` 5,880 · `lib.rs`(api) 4,268 · `join.rs` 3,755 · `operator_tree.rs` 3,692 · `where_plan.rs` 3,570 · `count_plan_exec.rs` 2,965 | The split candidates are §4 Phase 4 |
| Integration tests | api: 146 files · query: **5 files** (plus inline units) | Query correctness is mostly tested *through* the api crate; planner/operator regressions surface late and far from cause |
| Async | 1 intentional `block_on` bridge; no lock-across-await anti-patterns found | Healthy |

---

## 4. First principles, then the roadmap

### Design principles (what "good" looks like for this codebase)

1. **One coherent state, swapped atomically.** Readers receive an immutable, internally consistent bundle; writers build the next bundle and swap. No observable intermediate states, no Arc-identity contracts.
2. **An ID is a type, not an integer.** Two values that must never be cross-assigned get two types; where the same number exists in two coordinate systems (snapshot-ns vs store-ns), the *space* is part of the type.
3. **One chokepoint per contract.** Overlay translation and overlay merge each get exactly one entry point with an explicit, typed outcome — variants (`RowMerge`, `OrderedMerge`, `CountDelta`, `RawPostPass`) are strategies behind it, not parallel implementations.
4. **Capabilities, not scattered booleans.** A query's plan-time environment (`history`, `overlay-epoch`, `policy`, `multi-ledger`, `time-travel`, index features) is computed once into a capability set; fast paths *declare* requirements and a registry matches them. Adding a feature means adding a capability, and the compiler/registry — not 33 detectors — decides who is disabled.
5. **Fallback is architecture — keep it, but type it.** The `Ok(None) ⇒ run fallback` philosophy is genuinely good (correctness never depends on flawless translation). Promote it from convention to a `FastPathOutcome::{Proceed, Fallback(reason)}` that EXPLAIN and metrics can observe.
6. **Validation is a ledger property, not a call-site argument.**

### Phase 0.0 — Performance guardrails *(the precondition for every phase below)*

Correctness guardrails (the differential harness, 0.7) tell us a refactor didn't change *answers*; these tell us it didn't change *speed or memory*. The rule for the whole roadmap: **no phase begins until its pre-baseline is captured, and no phase closes until the post-baseline shows every tuple within budget — with improvements recorded and budgets tightened where they appear.**

What exists today: a criterion chassis (`fluree-bench-support`) with scale (`tiny`→`large`) and profile (`quick`/`full`) knobs, `regression-budget.json` with per-`(crate, bench, scale)` budgets, and a CI smoke gate. What's missing, and what this phase adds:

| # | Item | Gap it closes |
|---|---|---|
| G1 | **Baseline capture & compare tool** (`bench-baseline` bin in `fluree-bench-support`): `capture` walks criterion's `estimates.json` output into a labeled, git-stamped `bench-baselines/<label>.json`; `compare` re-walks current results against a named baseline using `regression-budget.json` budgets, prints a per-scenario table (regressions *and* improvements), exits nonzero on breach | Budget comparison was aspirational — budgets existed but no baseline file, no capture workflow, no compare command anyone can run locally before/after a change |
| G2 | **Memory metrics** (`fluree-bench-alloc` tracking allocator + `mem` sidecar recording in bench-support): per-scenario peak and net allocated bytes recorded alongside time, merged into the same baseline file, compared with the same budget machinery | Criterion measures time only; an allocation regression (e.g., a refactor that clones where it borrowed) was invisible |
| G3 | **Condition-matrix bench** (`query_overlay_matrix`): the same query shapes run at `base` (indexed, epoch 0), `overlay` (indexed + trailing novelty), and `novelty` (no index) conditions, × scales | Every existing hot-path bench runs at epoch 0 — the overlay-merge lane, which Phases 1–2 refactor directly, had zero benchmark coverage; a regression confined to the overlay lane would have passed every gate |
| G4 | **Per-phase protocol** (below) | "Benchmark before and after" was implicit; now it is a written, mechanical procedure |

**Per-phase protocol (G4):**

1. **Pre-baseline.** On the commit a phase branches from: `quick` profile at `tiny`+`small` for fast iteration, `full` profile at `small`+`medium` for the gate, `large` where the phase touches scan/merge paths. Capture → `bench-baselines/phase-<N>-pre.json`, committed with the phase's first PR so reviewers and CI share the reference.
2. **In-flight.** Every PR in the phase runs `compare` against the pre-baseline for the bench subset its diff can affect (at minimum: `query_overlay_matrix` for Phases 1–2, `query_hot_bsbm` for Phase 3, `transact_commit`/`novelty_replay` for write-path changes).
3. **Post-validate.** At phase close, capture `phase-<N>-post.json` under the same profile/scale matrix on the same hardware class; every tuple must be ≤ budget vs pre. Improvements are recorded in the phase's closing PR and budgets tightened to bank them (a banked improvement can't silently erode later).
4. **Noise discipline.** Local runs use `quick` for direction, `full` for decisions; the budget JSON keeps per-scale slack (10% tiny, 5% small/medium, 3% where we've banked wins) because small scales flap more.

Scale-tier honesty: `tiny`/`small`/`medium` run in CI and locally; `large` and the BSBM-100M-class datasets in project memory are nightly/manual — phases that touch leaf-walk or merge kernels (1, 2) require at least one manual `large` validation before close, recorded in the closing PR.

### Phase 0 — Make invariants visible and tested *(days–2 weeks each, LOW risk, HIGH leverage)*

| # | Item | Impact / Risk |
|---|---|---|
| 0.1 | Newtype `GraphId(u16)` and `NsCode(u16)`; optionally phantom-tag namespace space (`NsCode<Snapshot>` vs `NsCode<Store>`) at the binary_scan boundary where the past bug lived | High / Low — `#[repr(transparent)]`, zero runtime cost, mechanical |
| 0.2 | Unify datatype code to one `DtCode(u16)` across `DecodedRow`/`OverlayOp`/`RunRecord` | High / Low–Med |
| 0.3 | **Confirm-then-fix** the three lifecycle flags: graph-registry replay in `apply_loaded_db`; call `sync_binary_store_from_state` inside `apply_index_v2` before releasing the state lock; reload `t()` swap race | High / Low once confirmed |
| 0.4 | Concurrency regression tests: query-during-refresh, commit-during-refresh, reload-vs-commit, detached-`dict_novelty` detection | High / None — pure test investment, and the safety net for Phases 1–2 |
| 0.5 | Fix export-path WARN-and-drop on non-`Unsupported` translation errors (`binary_scan.rs:2238-2242`) — route through the raw-flake post-pass like graph crawl | Med / Low |
| 0.6 | `# Invariants` rustdoc on `LedgerSnapshot`, `LedgerState`, `DictNovelty`, `Binding`; `SAFETY:`/invariant comments on the ~20 undocumented unsafes and `join.rs` unwraps | Med / None |
| 0.7 | **Differential test harness**: property tests asserting fast-path ≡ generic-path results under randomized base+overlay+time-travel. This is the single highest-ROI test artifact for this codebase and the gate for everything below | Very High / None |

### Phase 1 — One overlay contract *(3–6 weeks, MED risk, the highest correctness ROI)*

Create an `overlay` module (likely in `fluree-db-binary-index` or a thin `fluree-db-overlay`) owning:
- **One canonical `FactKey`** (the V3 identity tuple) — today export dedups by `FlakeValue`, cursors by the V3 tuple; pick one, convert at the edge.
- **One translation entry** returning `enum Translation { Ok(OverlayOp), Untranslated(Flake, Reason), Fatal(Error) }` — callers choose *policy* (fallback / raw post-pass / error) but can no longer silently drop.
- **Merge strategies behind one trait/enum**: `RowMerge` (cursor two-pointer), `OrderedMerge` (desc top-k), `CountDelta`, `RawPostPass` (lifecycle resolution: latest-t-wins, tie prefers retract — written once).

Migrate the five implementations one at a time, each landing behind the Phase 0.7 differential harness and the `regression-budget.json` benchmark gate. The inner kernels (two-pointer merge, leaflet arithmetic) move, not change.

### Phase 2 — Coherent ledger state *(4–8 weeks, MED-HIGH risk, kills the bug class structurally)*

```rust
pub struct CoherentLedgerState {       // all fields private
    snapshot: Arc<LedgerSnapshot>,
    novelty: Arc<Novelty>,
    dict_novelty: Arc<DictNovelty>,
    runtime_small_dicts: Arc<RuntimeSmallDicts>,
    index: Option<AttachedIndex>,      // store + provider built together, typed (retires TypeErasedStore)
}
// LedgerHandle: ArcSwap<CoherentLedgerState>; transitions are methods returning a NEW bundle:
// .with_commit(...), .with_index_applied(...), .trimmed_to(...)
```

- Readers: one atomic `load()` — strictly cheaper than today's `RwLock` read; no torn states by construction.
- Writers: build-then-swap; the `BinaryRangeProvider` is constructed *inside* the transition from the same bundle, so Arc-identity coupling cannot recur.
- Sequencing of writers stays as-is (commits already serialize via nameservice CAS).
- This phase retires: both lifecycle `RwLock`s' ordering convention, `sync_binary_store_from_state`, the detached-Arc class, and `TypeErasedStore`.

### Phase 3 — Capability-gated planning *(2–4 weeks, LOW-MED risk)*

- `PlanCapabilities` computed once from `ExecutionContext` (overlay epoch, to_t vs index_t, from_t, policy, multi-ledger, history, index features like `lex_sorted_string_ids`, leaflet-FIRST availability).
- `FastPathRegistry`: table of `{ detector, builder, required_capabilities }` replacing the if-chain in `operator_tree.rs:2073-2700`. Gate strategies (a)/(b) become *declared data* (`requires: NoOverlay` vs `requires: CursorMergeable`) instead of a per-operator function call.
- EXPLAIN records **actual** path: when an operator falls back at `open()`, stamp `FastPathOutcome::Fallback(reason)` into the execution context so explain/tracing show planned *and* executed. (This piece alone is shippable in days and is the best contributor-experience win per line changed.)

### Phase 4 — Organizational refactors *(incremental, LOW risk each, schedule opportunistically)*

1. Split `where_plan.rs` into `plan` (pure analysis → a serializable `WherePlan` struct) and `build` (plan → operators); consolidate the 3× variable-readiness logic. Unit-test planning without executing.
2. Extract `fluree-db-api/src/format/` → its own crate; split `import.rs` (5,880 lines) into parse/resolve/build stages; consider `ledger_manager` → `fluree-db-connection` once Phase 2 reshapes it.
3. Move indexer orchestration out of `fluree-db-indexer` (drop the `embedded-orchestrator` upward dep).
4. Make policy/SHACL enforcement a ledger-level configuration checked at the commit chokepoint (one place all commit-building paths must pass), with explicit `root`/`system` bypass — not an `Option` parameter. Offer post-state SHACL as a config mode (validate after novelty apply against the new bundle from Phase 2, reject by not swapping).
5. Structured error variants for the cross-crate seams that matter (translation, state transitions) — leave user-facing `String` variants alone.
6. `fluree-db-query/tests/` integration suite (planner shapes, operator correctness, fast-path/fallback parity) so query bugs stop being discovered through `fluree-db-api`'s 146 test files.

### Explicitly deferred / not recommended

- **Phantom-typed `Binding<Maturity>`**: real safety, but it makes every operator generic — viral complexity for a contract better held by the registry + a debug-assert at batch boundaries. Revisit only if Poisoned/Encoded leaks actually occur.
- **Per-ledger commit queue**: CAS + retry is correct and scales; don't serialize writes harder than the nameservice already does.
- **Detector DSL/macro for query shapes**: appealing, but a registry of plain functions gets 90% of the benefit without a new meta-language to maintain.

---

## 5. What must not change

The audit's job was also to mark the load-bearing performance walls:

- The 13 fast-path operator **kernels** (leaflet-FIRST boundary tricks, POST tail walks, per-leaf count arithmetic, `ColumnSet::CORE`-only decoding, binary-searched o_key runs) — these are the measured wins (BSBM Q5 ~27%, ORDER BY ~200×). Phases 1/3 move their *dispatch and gating*, never their loops.
- The columnar/row dual representation itself — it is the right design; the goal is one *contract* between the worlds, not one representation.
- Late materialization (`Encoded*` bindings) and the streaming batch model.
- The fallback-over-failure philosophy (`Ok(None)` ⇒ slower correct path) — formalized, not removed.
- Landing rule for every phase: differential tests green (Phase 0.7), baseline compare green per the Phase 0.0 protocol (time *and* memory, including the overlay-condition lanes), one migration per PR.

## 6. Sequencing rationale

Phase 0.0 comes first because it is the measurement substrate every later claim depends on — "no regression" is only meaningful against a captured baseline, and the overlay-condition lane must be benchmarked *before* the phases that refactor it. Phase 0 is pure insurance and takes effect immediately. Phase 1 attacks the seam where every historical data-correctness bug originated, with the differential harness making it safe. Phase 2 is the structural kill of the stateful-condition class you called out — it is the riskiest, which is why it comes *after* the test harness and the confirmed point-fixes have stabilized the ground. Phase 3 is leverage for future contributors (every new optimization slots into a registry instead of a 600-line if-chain). Phase 4 is steady-state hygiene that can interleave with feature work indefinitely.
