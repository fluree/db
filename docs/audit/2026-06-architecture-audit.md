# Fluree DB — Architecture & Rust-Practice Audit

**Date:** 2026-06-11 (audit) · **Progress ledger updated:** 2026-06-16
**Scope:** Full workspace (38 crates, ~460k lines of non-test Rust; ~594k total). Seven parallel deep-explorations: crate layering, core data model & ID spaces, binary-index/overlay contract, query engine organization, state lifecycle, write path, and a metrics-driven hygiene sweep. Load-bearing claims were verified directly against source.

---

## 0. Progress ledger

Living status of the §4 roadmap. **A new session should read this first**, then the phase detail in §4 and §6. The work is landing as a **stack of draft PRs off `main`**, each branch based on the previous: `bench/phase0-baseline-guardrails` (#1311) → `test/phase0-differential-harness` (#1313) → `fix/fastpath-divergences` (#1314) → `fix/lifecycle-state-gaps` (#1315) → `refactor/phase0-newtypes` (#1316) → `refactor/phase0-nscode` (#1344, NsCode rename `97f064a0a` **+ Phase-0 finalization**: snapshot/store invariant docs `830657ddb`, `TxnGraphId` `733ec0fb7`, invariants rustdoc `abb5fbbd3`, 0.4 concurrency tests `82a57f879`). The Phase-1 sub-stack continues off it: `refactor/phase1-translation-contract` (#1346, step A). Continue new work by branching from the current **stack head** (`refactor/phase1-translation-contract`) unless a phase is logically independent. All PRs are draft pending the user's review; do not merge.

| Phase | Item | Status | PR / branch |
|---|---|---|---|
| 0.0 | Performance guardrails: `bench-baseline` capture/compare, `fluree-bench-alloc` tracking allocator, `query_overlay_matrix` base/overlay/novelty bench, `scenario_mem` gating | **Done** | #1311 (+`scenario_mem` refinement in #1314) |
| 0.7 | Differential fast-path harness (`it_differential_fastpath.rs`) + planner kill switch (`set_fast_paths_disabled`) | **Done** — found FD-1/2/3 on first run | #1313 |
| — | FD-1 (AVG datatype), FD-2 (MIN/MAX-string lex gate), FD-3 (per-predicate count from leaf directories) | **Done** — all three enforced by the harness | #1314 |
| 0.3 | Confirm-then-fix lifecycle candidates | **Done** — 1 real fix (export WARN-drop, = item 0.5), 1 regression pin (graph-registry `apply_loaded_db`), 2 false positives cleared (`apply_index_v2` TOCTOU, reload `t()` race) | #1315 |
| 0.5 | Export-path WARN-and-drop → keep raw flakes | **Done** (folded into #1315) | #1315 |
| 0.1 | ID-space newtypes | **Done** — `GraphId` (`ffe90b60b`); `NsCode(u16)` plain newtype (#1344 `97f064a0a`; "Option B" — `Sid.namespace_code`/vocab/`NsLookup` are `NsCode`, storage maps/`SubjectId`/wire-delta stay raw `u16`, all wire/serde/Display/key forms preserved via `.as_u16()`). The snapshot-vs-store space distinction is **documented as invariants** at the `binary_scan` boundary with a live `debug_assert` (`830657ddb`) rather than a `StoreSid` newtype — investigation showed a type fights the design (the input fast path *exploits* canonical snapshot==store code agreement; the output `p_sids` table deliberately *blends* both spaces). `TxnGraphId(u16)` for the txn-local graph-id space (`733ec0fb7`; genuinely distinct from ledger `GraphId`, unlike the NsCode spaces). `SubjectId` intentionally kept `u16`. | #1316 + #1344 |
| 0.2 | Unify datatype code width | **Done — by deletion.** The `u32`-vs-`u16` mismatch lived only in dead pre-V3 types in `binary-index/src/types.rs`; live V3 types are already width-coherent. Deleted. | #1316 |
| 0.4 | Concurrency regression tests | **Done** — `it_lifecycle_concurrency.rs` (`82a57f879`): deterministic **detached-`dict_novelty` detection** (the Arc-identity contract) + **query-during-refresh** torn-state stress. `commit-during-refresh` / `reload-vs-commit` are covered by the existing `it_concurrent_update_reconcile` (stale-cached-writer reconcile) + `it_refresh`; `it_graph_registry_apply_loaded_db` landed in #1315. | #1315 + #1344 |
| 0.6 | `# Invariants` rustdoc + `SAFETY:` comments | **Type-invariants DONE** (`abb5fbbd3`): `# Invariants` blocks on `LedgerSnapshot`, `LedgerState` (incl. the load-bearing `dict_novelty` Arc-identity contract), `DictNovelty`, `Binding`. **REMAINING (opportunistic, low-value):** the `SAFETY:`/invariant-comment sweep over the ~20 undocumented unsafes + `join.rs` unwraps. | #1344 |
| 1 | Single overlay-merge chokepoint (the highest correctness ROI; `query_overlay_matrix` shows ~9–128× base→overlay headroom) | **Done** — sub-plan **A→B→C** (investigation map: 5 merge impls; tie-breaking already consistent; **two legitimate fact-identity domains** — V3 `(s_id,p_id,o_type,o_key,o_i)` for the translated lane, row-world `Flake` `(s,p,o,dt,m)` for the raw-post-pass lane; the audit's "silent-drop bug" was found to be **dead code**, not live). **A DONE** (#1346): deleted the dead `translate_overlay_flakes` lane + typed `Translation { Translated / Untranslated(reason) }` contract. **B DONE** (#1347, `refactor/phase1b-factkey`): unified the **three** `FactKeyV3` copies (a third lived in the indexer's `novelty_merge`, build-side) behind one canonical type + shared `OI_NONE`; documented the two identity domains on the type and at the translation seam. **C DONE** (`refactor/phase1c-merge-contract`): one `resolve_overlay` producer all three consumers funnel through (includes the C1 export overlay-resolve **bug fix**), `FactKeyV3` identity named via `eq_row_vs_overlay` + OvRow/CountDelta strategy docs, one row-world resolver, and the `OverlayMergeStrategy` enum — all perf-neutral behind the 0.7 harness. | #1346 (A), #1347 (B), `phase1c` (C) |
| 2 | `CoherentLedgerState` + `ArcSwap` (kills the `dict_novelty` Arc-identity bug class) | **Done** — P1 one `attach_range_provider` chokepoint, P2a two locks → one `RwLock<CoherentLedgerState>`, P2b `ArcSwap` + commit `Mutex` (readers lock-free; torn state impossible by construction). The `TypeErasedStore` *field* stays: a hard `ledger→binary-index→spatial→ledger` cycle forbids typing it, but the reader path IS typed (`AttachedIndex`) and the bug class is structurally dead. | `refactor/phase2-coherent-state` |
| 3 | `PlanCapabilities` + fast-path registry + EXPLAIN-actual-path | **NOT STARTED** | — |
| 4 | Org refactors (split `where_plan.rs`/`import.rs`, extract `format/`, mandatory policy at commit chokepoint, `fluree-db-query/tests/` suite) | **NOT STARTED** | — |

**Suggested next steps:** Phase 0 + **Phase 1 + Phase 2 complete**. Phase 2 (`refactor/phase2-coherent-state`) landed the `CoherentLedgerState` bundle behind `ArcSwap`: readers are lock-free, writers build-then-swap under a commit `Mutex`, and the `dict_novelty` Arc-identity bug class is **structurally dead** (one provider-rebuild chokepoint; immutable bundle; atomic publish — the 0.4 torn-state + detached-Arc tests pin it). Next is **Phase 3** — `PlanCapabilities` + fast-path registry + EXPLAIN-actual-path (LOW-MED risk; the EXPLAIN-actual-path piece alone is a days-scale contributor-experience win).

---

### Session handoff — RESUME HERE (2026-06-17)

**Live draft-PR stack off `main`** (each branch based on the previous): #1316 `refactor/phase0-newtypes` → #1344 `refactor/phase0-nscode` (**Phase 0 complete**) → #1346 `refactor/phase1-translation-contract` (**Phase 1A**) → #1347 `refactor/phase1b-factkey` (**Phase 1B; current stack head**). All PRs draft; do not merge. **Branch new work from `refactor/phase1b-factkey`.** Each branch carries a `chore:` cleanup commit that untracked the `.claude/worktrees/*` agent gitlinks which were breaking CI's recursive submodule init (root cause `fe2208cba`; now gitignored — the only real submodule is `testsuite-sparql/rdf-tests`; **never `git add` `.claude/worktrees/`**, and stage code explicitly, never `-A` from the repo root).

**Phase 1B — DONE (#1347).** Unified the **three** structurally-identical `FactKeyV3` copies behind one canonical type at `fluree-db-binary-index/src/read/types.rs`. The handoff expected only two (the `pub` one in `read/types.rs` and the private dup in `read/replay.rs`); a **third** lived in `fluree-db-indexer/src/run_index/build/novelty_merge.rs` (build-side history dedup). Lifted `from_batch`/`from_hist` + a shared `pub OI_NONE` onto the canonical type; replay and the indexer now import it (canonical derive set is a superset of both removed copies → behavior-preserving). Documented the two identity domains on the type and reciprocally at the `Translation` seam (`binary_scan.rs`): encoded V3 `(s_id,p_id,o_type,o_key,o_i)` vs row-world `Flake` `(s,p,o,dt,m)`. The precise reason the raw-`Flake` domain is *required*: `FactKeyV3` **cannot express a language tag**, so two novelty-only langStrings (`"x"@en` vs `"x"@fr`) collapse to one V3 identity — translation declines them (`Unsupported`) and they dedup on the full Flake tuple instead (#1273). (Note: datatype *is* folded into `o_type`/`o_key`, so `30^^int` vs `30.0^^float` do **not** collide — the earlier handoff example was wrong; the lang tag is the real collision.)

**Phase 1C — COMPLETE** (one PR: `refactor/phase1c-merge-contract`, branched off `phase1b-factkey`). Scope = **consolidation** (user-chosen over a leaky trait): one producer + canonical identity routed into kernels + a strategy enum; hot loops untouched. **All of A/B/C have landed — the overlay-merge chokepoint is now one `resolve_overlay` producer + one `FactKeyV3` identity + four documented `OverlayMergeStrategy` variants.**

*Landed so far:* **C0** `bench-baselines/phase-1-pre.json` (Phase-1 pre-baseline — its memory was re-captured in `64983fefa` because the first capture measured `scenario_peak_bytes` anomalously low, making every commit look like a uniform ~1.58 MiB mem regression even on `count_base` which runs no overlay code; **lesson: re-measure an untouched control before trusting a fresh baseline**). **C1 (`edc8d996a`) — a REAL export data-correctness fix:** RDF export (`apply_time_travel`) skipped `resolve_overlay_ops`, so insert+upsert-before-index then export left a fact's assert+retract unresolved in the cursor overlay → debug-assert panic in debug, **stale pre-upsert value leaked in release**; confirmed+fixed via `export_applies_novelty_retractions` in `it_select_star_novelty_retract.rs`. **C2a (`259fbb6fd`) — producer unification:** `translate_overlay_flakes_with_untranslated` → **`resolve_overlay`** (now sorts+resolves internally, takes `order`); the main scan and export both route through it so the resolve can't be skipped at a call site again. Validated perf-neutral vs the corrected baseline (overlay-scan times ±1%, all `scenario_mem` ±0.04%; only bench breaches are the noisy no-index `*_novelty` quick-profile scenarios 1C doesn't touch). **C2b (`d99bb531e`) — producer fully unified:** gave `resolve_overlay` an `Option<&Sid>` predicate filter and made `collect_resolved_overlay_ops` (the 9-caller fast-path lane) a thin wrapper over it, so the main scan, export, *and* the fast-path lane all funnel through the one producer; the translate→sort→resolve pipeline lives in exactly one place. Validated perf-neutral (overlay/base/mem flat; same `*_novelty` quick-noise). **C2 is complete.** **C3 (`eea25ddc8`) — RowMerge identity named:** extracted `merge_overlay_into_batch`'s inline 5-field compare into `eq_row_vs_overlay` (in `read/types.rs`, beside `cmp_row_vs_overlay` + `FactKeyV3`), field-wise so no key is constructed in the hot loop; perf-flat. **C4 (`1e82d4dfe`) — strategy docs:** documented `OvRow` as a gated `FactKeyV3` projection (`p_id`/`o_type` elided under the single-predicate/single-`o_type` gate) and `CountDelta` as owning no merge logic (translates via the producer, merges via the RowMerge cursor).

**C5 (`56eea4af6`) — done:** consolidated the two duplicate row-world resolvers into one sort-independent `resolve_overlay_retractions` (now `pub`; export shares it, `surviving_untranslated` deleted) and added the `OverlayMergeStrategy` enum naming RowMerge / OrderedMerge / CountDelta / RawPostPass over the single producer.

**Phase 2 — DONE** (`refactor/phase2-coherent-state`, off the 1C head; 5 commits: corrected `phase-2-pre` baseline → **P1** `attach_range_provider` chokepoint → **P2a** two locks → one `RwLock<CoherentLedgerState>` → **P2b** `ArcSwap` + commit `Mutex`). The `dict_novelty` Arc-identity bug class is structurally dead: the provider is rebuilt coherently in one chokepoint, the bundle is immutable, and it is swapped atomically, so a reader can never observe a detached `dict_novelty` or torn state (the 0.4 `it_lifecycle_concurrency` tests pin both — 3× green under ArcSwap; full cnasty 6918/6918; perf-neutral, A/B-proven against thermal noise). **2b (full `TypeErasedStore` removal) was abandoned by design:** `ledger → binary-index → spatial → ledger` is a hard Cargo cycle (binary-index embeds spatial providers), so `LedgerState` cannot name `Arc<BinaryIndexStore>`; the erasure is a layering necessity (doc on `TypeErasedStore` now explains this), and the reader path is already typed via `AttachedIndex`. *Next:* push `refactor/phase2-coherent-state` + open its draft PR, then **Phase 3** (`PlanCapabilities` + fast-path registry + EXPLAIN-actual-path).

The four kernels (moved-not-changed; outputs genuinely differ so there is no uniform `merge()`): **RowMerge** = `binary_cursor::merge_overlay_into_batch` (→ `ColumnBatch`); **OrderedMerge** = `fast_post_order_limit::collect_post_desc_topk_overlay` (→ `Vec<TopKRow>`); **CountDelta** = `fast_path_common::count_predicate_overlay_delta` (→ `u64`; delegates the merge to a RowMerge cursor then counts); **RawPostPass** = export `surviving_untranslated` (→ `Vec<Flake>`, row-world domain). `range_with_overlay` is dispatch + a genesis special case, **not** a 5th impl. Gate each commit: differential harness (`it_differential_fastpath`) + the novelty/export suite + a `query_overlay_matrix` compare (focus overlay+base+mem; `*_novelty` time is noisy at quick profile — judge at full).

**Carry-forward findings from the Phase-1 investigation:** tie-breaking is **already consistent** everywhere (latest-t-wins; assert-beats-retract on equal t) — write it once, don't re-derive. The audit's "residual silent-drop bug" was **dead code** (`translate_overlay_flakes`, deleted in 1A), so Phase 1 is structural *drift-prevention*, not bug remediation. Gate with the **`cnasty`** alias (= full-workspace `cargo nextest run --all-features`); skip the Docker/LocalStack tests locally (they hang on testcontainers). The working exclusion (ran **6917 green** for 1B): `cargo nextest run --workspace --all-features -E 'not (binary(it_iceberg_direct) | binary(it_storage_s3_testcontainers) | binary(it_import_remote))'`.

---

**Lesson from the NsCode rename (reinforces the newtype-pitfall memory):** the newtype `Display` is `NsCode(N)`, not `N`. Several `format!("{}", sid.namespace_code)` sites *compiled cleanly* but would have silently emitted `NsCode(N)` — found in `property_join`, `explain`, `block_fetch`, `eval/value.rs::into_string_value` (the `STR()` fallback), and `api/tx.rs`. A **line-based** grep misses multi-line `format!`/`writeln!` macros where the arg sits on a separate line (this hid the `tx.rs` and `eval/value.rs` cases through the first test run); use a `rg -U --pcre2` multiline sweep for `(format!|write!|…)\(…\.namespace_code(?!\.as_u16)`. Gate on `cargo test`, not just `cargo check` — the surviving traps surfaced only as failing unit tests (`eval::value` STR fallbacks) and a Debug-digest contract test (`novelty_equivalence_contract`, regenerated). Skip the Docker/LocalStack integration tests (`it_iceberg_direct`, `it_storage_s3_testcontainers`, `it_import_remote`) when running the suite locally — they hang on testcontainers.

**Process invariants for this work (learned the hard way — see repo memory):** unsigned commits via `git -c commit.gpgsign=false …` on *every* history-creating op (commit, rebase, amend); no AI-attribution trailers in commits or PR bodies; gate "done" on `cargo test --workspace --all-targets --all-features` **plus** the relevant integration suite (feature-gated + runtime round-trip bugs escape a plain `cargo check`); run the `bench-baseline` compare vs a pre-phase baseline before closing a phase.

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
