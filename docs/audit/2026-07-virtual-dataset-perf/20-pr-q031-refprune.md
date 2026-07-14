# PR-q031 (F20) — RefObjectMap-target resolution prune — DESIGN SKETCH

**Branch:** to stack as `perf/r2rml-q031-refprune` (off `perf/r2rml-pr4d`, itself off the docs branch off #1499). Stacking (lead-ruled): #1499 → docs branch → `perf/r2rml-pr4d` → `perf/r2rml-q031-refprune` (PR-4d implements first; this sketch queues the lead's review meanwhile).
**Status:** SKETCH — **STOP for lead review**. No engine code until approved.
**North-star slate item 1** (F20). Supersedes the F18 cold-floor framing (measurement refuted the pin-leak/residency premise — `18-pr8tail-...` MEASUREMENT ADDENDUM; register F20).
**Target (corrected by measurement, §3):** q031 72 s → **~3–5 s cache-thrashed, loadTable-bound** (deterministic gate = `load_table.n` 7→2 + `scan_table` collapse). A hard ≤3 s first-ask is gated on a follow-up loadTable/creds cache, NOT this PR. PR-2a stays closed (the fact scan is ~1 s; the residual is the catalog round-trip, not the decode).

## The seam (measured, code-anchored)

q031 = `?inv a edw:InventorySnapshot ; edw:onHandQty ?oh ; edw:reorderPoint ?rp ; edw:product ?p . ?p edw:name ?pn . FILTER(?oh<?rp) LIMIT 5000`.

Measured (fresh-cache run): **7 DISTINCT tables loaded once each** — `FACT_INVENTORY_SNAPSHOT` + `DIM_ACCOUNT, DIM_CUSTOMER, DIM_EMPLOYEE, DIM_PRODUCT, DIM_STORE, DIM_SUPPLIER`. The 6 dims are **exactly** the 6 that map `edw:name`. The per-query pin held (no leak); the 21.2 s is a **resolution fan-out**.

**Mechanism.** TriplesMap resolution is base-predicate-driven (`rewrite.rs:690` "the base predicate drives TriplesMap selection"). The second triple `?p edw:name ?pn` is a **variable-subject, shared-base-predicate** pattern with **no class assertion on `?p`**, so `edw:name` resolves to every map bearing it → the 6-dim fan-out (6 dead `loadTable`s incl. 390 K-row DIM_CUSTOMER, and ≈ `241 batches × 6` of the 1448 `scan_table` re-scans). The class-fusion machinery (`fuse_class_if_safe`/`class_fusion_is_safe`, `rewrite.rs:661-739`) only constrains resolution when a `?x a Class` co-locates — it never fires here. But `?p` is bound by `edw:product`, a **RefObjectMap whose parent TriplesMap is provably `DIM_PRODUCT`** — that target is simply **not propagated** to constrain the `?p edw:name` resolution.

## (1) The fix — propagate the RefObjectMap target as a resolution constraint

When a variable `?p` is the OBJECT of a `RefObjectMap` POM (`edw:product` → parent TriplesMap `T` = DIM_PRODUCT), constrain the TriplesMap resolution of every downstream pattern `?p <pred> ?o` to `T` (and its subclass/partition set — see soundness). Concretely, mirror the existing **`class_prune_hint`** mechanism (rewrite records a resolution hint; the operator's `tm_passes_star_prune` filters the candidate maps, `operator.rs`, `star_tm_prune_enabled`), but derive the hint from the **ref target** rather than a class. This is the query-path generalization of the crawl-only `trust_fk_refs` prior art (`execute/runner.rs:687`, `r2rml/operator.rs:1042`, `rewrite.rs:99` — child-templated RefObjectMap target trusted to skip the parent scan; here we trust it to prune resolution). Effect: `?p edw:name` resolves to DIM_PRODUCT only → `load_table.n` **7 → 2**, fan-out re-scans collapse.

## (2) THE SOUNDNESS INVARIANT (load-bearing — two independent conditions)

The prune constrains `?p <pred>`'s resolution to `T` only when BOTH hold; if either fails, **DECLINE** (fall back to the full fan-out — always correct, just slow):

**(A) Join-var provenance — `?p`'s bindings are all provably `T` subjects.** Collect EVERY binding source of `?p` in the query scope. Allow the prune ONLY when every source is a RefObjectMap resolving to the **same** parent `T`. DECLINE if any source is:
- a **UNION** branch that binds `?p` (e.g. `{?inv edw:product ?p} UNION {?inv edw:supplier ?p}` — the supplier branch makes `?p` a DIM_SUPPLIER subject; pruning to DIM_PRODUCT drops those names);
- **another triple pattern** that binds `?p` as a subject/object from a non-ref source (e.g. `?p a edw:OtherClass`, or `?x edw:rel ?p`) that could produce non-`T` subjects;
- a **`VALUES ?p { … }`** (arbitrary IRIs, not guaranteed `T`);
- a **second RefObjectMap with a DIFFERENT parent** (`?inv edw:product ?p . ?x edw:store ?p` → parents DIM_PRODUCT ≠ DIM_STORE; constraining to one is wrong).
- *Allow* multiple ref sources IF all share the same parent `T`. Conservative first cut (recommended): allow ONLY the single-required-RefObjectMap case (exactly q031); DECLINE anything more complex, widen later behind the same switch + differential.
- Scope care: bindings inside an OPTIONAL/subquery vs the required part must be handled — treat any binding producer of `?p` reachable at the point `?p <pred>` is evaluated as a source; when in doubt, DECLINE.

**(B) Template-disjointness — `T` is the ONLY `<pred>`-bearing map whose subject template can match `?p`'s IRIs. MANDATORY, in-scope (lead-ruled 2026-07-14 — NOT a debug_assert).** This is the PR-3 (b')/F10 lesson (reuse `wildcard_class_fusion_is_safe`): if a **vertically-partitioned** map shares `T`'s subject template AND maps `<pred>` (the value lives in the partition, not `T`), pruning to `T` drops rows. So require every OTHER `<pred>`-bearing map to be subject-template prefix-**disjoint** from `T`. The SF01 corpus dims happen to be template-disjoint (`.../product/{k}` vs `.../supplier/{k}` …) — but the guard must NOT ride on that: a correctness guard resting on "this dataset's templates happen to be disjoint" is dataset coupling, the exact F10-class trap. So (B) is checked at runtime (reusing PR-3's predicate, cheap) and a hand-written vertically-partitioned mapping DECLINEs rather than silently drops. (Without (B), F20 would re-introduce exactly the unsoundness PR-3 corrected when it replaced raw (b) with (b').)

## (3) Expected-wall arithmetic (MEASURED — the loadTable term is real; ≤3 s is NOT guaranteed by the prune alone)

Post-prune, q031's plan = **2 loadTables** (FACT_INVENTORY_SNAPSHOT + DIM_PRODUCT) + one FACT_INVENTORY_SNAPSHOT scan (7,670 files / 300 K rows / 51 MB — file-count-bound) + one DIM_PRODUCT (1-file) hash-join + `FILTER(?oh<?rp)` (un-prunable, so the LIMIT can't cut the fact scan) + `LIMIT 5000` materialize.

**The arithmetic hole (lead's rider, now closed by measurement).** The earlier "≤3 s" priced the scan from q018/q044 — but those have **`load_table.n=0`** (cross-query-amortized), so they hid the loadTable term. Post-prune q031 still pays **two REAL loadTables** under the thrashed / first-ask protocol. **Measured** via **q032** — the exact post-prune shape (1 fact FACT_INVENTORY_SNAPSHOT + 1 single-file dim DIM_STORE), fresh in-memory TTL (new process) + warm disk catalog (steady-state):

| q032 (2-table proxy) | wall | `load_table` n / total | `oauth` | `parquet` n / total |
|---|---:|---:|---:|---:|
| rep 1 | 4.25 s | 2 / 6.61 s | 0.74 s | 7671 / 4.73 s |
| rep 2 | 2.81 s | 2 / 4.59 s | 0.68 s | 7671 / 1.36 s |

The `load_table` **total** (4.6–6.6 s) is the SUM of 2 loads that run **concurrently**, so the WALL contribution is ~2–3 s (the two overlap). q032's whole wall is **2.8–4.25 s** — and that IS the post-prune q031 shape (2 loadTables + a full 7,670-file fact scan + a 1-file dim join), except q032 folds to 500 rows (GROUP BY) where q031 materializes up to 5000 (LIMIT + var-var FILTER). So:

**Honest total: post-prune q031 ≈ loadTable ~2–3 s (wall, 2 concurrent) + fact scan ~1 s + 5000-row join/materialize ~1 s ≈ 3–5 s cache-thrashed, loadTable-DOMINATED.** That is **AT or slightly OVER the ≤3 s bar** — the prune alone does NOT guarantee it.

**Why the loadTable term does not amortize away here (verified).** PR-8 slice 2's disk catalog cache serves the metadata.json + the manifest-derived scan-file list from disk, but per its own contract (`disk_catalog_cache.rs`: *"a cold process still issues one loadTable GET for fresh vended credentials — this only removes the metadata + manifest S3 round-trips"*) it does **NOT** cover the loadTable **REST/OAuth credential GET**. That GET (~1–3 s/table) is paid per process per table, and it only amortizes via the **in-memory** 60 s moka cache when a PRIOR query touched the same table within 60 s. In the clean full-corpus baseline q031 showed `load_table.n=7` — i.e. it was NOT amortized (q031 is the near-sole InventorySnapshot consumer), so post-prune `load_table.n=2` will likewise be paid, not amortized. The loadTable term is real in AJ's actual protocol.

**Verdict + the named closer (per the rider — do not implement on inferred arithmetic).** The resolution-prune STILL ships on its own merit — it takes q031 from **72 s → ~3–5 s** (removes 5 dead dim loads incl. 390 K-row DIM_CUSTOMER + the fan-out re-scans), deterministic and sound. But it does **not** by itself clear a hard ≤3 s first-ask; the residual is the **2 loadTable credential GETs**, which the prune cannot touch. **Closer = a persistent loadTable/creds cache (a PR-8 slice extension)** — the exact residual PR-8 slice 2 explicitly leaves on the table (creds not persisted). This is a **separate follow-up item**, NOT part of the prune PR. So the prune's honest end-state is **"~3–5 s cache-thrashed, loadTable-bound"** (~1–2 s once loadTable-amortized), and the ≤3 s first-ask target is gated on the follow-up creds-cache — **PR-2a stays closed** (the 51 MB / 7,670-file scan is ~1 s, not the bottleneck; the bottleneck is the catalog round-trip, not the decode).

## (4) Kill switch

**Own switch** (new soundness surface — do NOT overload `FLUREE_R2RML_STAR_TM_PRUNE`): `FLUREE_R2RML_REF_TARGET_PRUNE` (default on). Off ⇒ today's full fan-out, byte-identical. The DECLINE path is also byte-identical to off for any query that fails (A) or (B).

## (5) Blast radius

- **q031** — the clear case (this PR). Any query with `?x <refPredicate> ?p . ?p <sharedPred> ?o` where `?p`'s sole binding is that ref benefits (fewer map loads/scans); result-identical by soundness.
- **Corpus scan for the shape:** other fact→dim-attribute joins (q032 `?inv edw:store ?st . ?st edw:name`, q016's dim attrs, etc.) — check which take the prune and confirm no regression; each is a co-benefit, none may change results.
- **DECLINE shapes must stay correct:** the differential's whole point (§gate). No native path change — `R2rmlScanOperator`/the R2RML rewrite never run on a native query.

## (6) DoD / gate (lead-specified)

1. **`load_table.n` 7 → 2** on q031 (deterministic, cache-independent — the crisp sentinel = the win) + **`scan_table` collapse** (the fan-out re-scans gone). This is the gating metric.
2. **Live q031 rows-parity vs oracle** (`rows_only` per manifest), cache-thrashed full-corpus order. **Wall: honest end-state ~3–5 s cache-thrashed, loadTable-bound (per §3 measurement) — NOT a hard ≤3 s.** The ≤3 s first-ask target is gated on the loadTable/creds-cache follow-up (§3 closer), NOT on this PR. Report the wall; do not fail the PR on the loadTable-bound residual.
3. **DECLINE cases as hermetic tests (all in-scope, incl. (B))** — one per unsound binding-source shape: UNION-bound `?p`, second non-ref binder, `VALUES ?p`, different-parent second ref, **and the (B) vertically-partitioned template-sharing map** (a synthetic mapping with a second map sharing `T`'s subject template AND mapping `<pred>` — the prune MUST DECLINE). Each shows the prune **NOT firing**, output byte-identical to switch-off.
4. **Full-corpus cache-thrashed baseline at head** — no other query's wall/hash regresses; the 42/50 ≤3 s set stays put.
5. Native 54/54 + W3C + unit sweeps green; kill-switch off = byte-identical.

## (7) Implementation trace — FINDINGS (injection point CONFIRMED, 2026-07-14; ready to implement)

The trace resolves all three questions — the fix **reuses the existing `class_prune_hint` machinery wholesale**; no new hint field, no operator change.

- **(a) Injection point + parent-class resolution.** `rewrite_patterns_for_r2rml` (`rewrite.rs:117`) collects same-subject `star_groups` and `class_groups` over one BGP scope, then per subject calls `fuse_class_if_safe` (`:293`). q031's `?p edw:name` is a star on `?p` with base predicate `edw:name`; today `fuse_class_if_safe` returns early (no `class_groups` entry for `?p` — no `?p a Class` in the query). **The parent class of a RefObjectMap IS resolvable:** a ref member's `rom.parent_triples_map` → `mapping.triples_maps.get(&rom.parent_triples_map).classes()` (prior art: `fused_aggregate.rs:1608`, `operator.rs:1144`). DIM_PRODUCT's TM declares `rr:class edw:Product` (verified in `enterprise-sf01-mapping.ttl`), so the ref target's class C = `edw:Product` is available.
- **(b) Reuse `class_prune_hint` — NO new field.** The existing hint (`rewrite.rs:706`, set by `fuse_class_if_safe`'s PR-3 (b') branch; consumed by `tm_passes_star_prune` in `operator.rs`) already means "constrain this star's TriplesMap resolution to class-C-declaring maps, resolution-only, class stays its own concern." F20 sets **the same `base.class_prune_hint = Some(C)`** where C is derived from `?p`'s RefObjectMap parent instead of a query `?p a C`. So the operator side needs **zero** change — the hint plumbing already prunes.
- **(c) Invariant (B) = `wildcard_class_fusion_is_safe(mapping, C)`** — the exact predicate `fuse_class_if_safe` already uses for the class case (`:705`). Reused verbatim.

**Implementation plan (rewrite.rs only; behind `FLUREE_R2RML_REF_TARGET_PRUNE`):**
1. **Pre-analysis over the scope's converted members** (after the collection loop, before the star-emit loop): build `ref_class: HashMap<VarId, Option<Iri>>` — for each star member with a constant predicate P and `object_var = Some(?o)` where P resolves (via the mapping) to a **RefObjectMap** with parent class C, record `?o → C`; if `?o` is the object of ≥2 refs with **different** parents, set `None` (DECLINE). (Resolve P→ref by scanning `mapping` POMs for a RefObjectMap whose predicate == P.)
2. **Invariant (A) pollution scan (conservative first cut, single-required-ref):** `?o` qualifies ONLY if its sole roles in this scope are (i) object of that one ref member and (ii) subject of its own star. Any other appearance DECLINES: scan the scope's non-star `result_patterns` (`Values`/`Union`/`Optional`/`Minus`/`Exists`/`Service`/`Subquery`) via `Pattern::referenced_vars` for `?o`; also DECLINE if `?o` is an object of a second (non-ref) member, or a `class_groups` subject with a different class. (These cover the UNION/VALUES/other-triple/different-parent-ref decline set.)
3. **Injection in the star-emit loop:** for the star on subject S, if `ref_class[S] == Some(C)`, S is not polluted, S has no fused/standalone class already, and `wildcard_class_fusion_is_safe(mapping, C)` — set `base.class_prune_hint = Some(C)`.
4. **Tests (DoD 3, all in-scope):** the 5 DECLINE hermetics (UNION-bound `?o`, second non-ref binder, `VALUES ?o`, different-parent second ref, (B) template-sharing map) each assert `class_prune_hint == None`; the positive q031-shape asserts `class_prune_hint == Some(edw:Product)`. Mirror the existing `class_prune_hint_set_when_disjoint_but_not_fusable` / `class_prune_hint_refused_under_vertical_partition` tests (`rewrite.rs:1381/1408`).
5. **Live gate:** q031 ON vs OFF — `load_table.n` 7→2, `scan_table` collapse, rows-parity; full-corpus baseline at head.

**Estimated blast:** ~80–120 lines in `rewrite.rs` (pre-analysis + injection) + ~6 hermetic tests; zero operator change. Ready to implement on the lead's go (already given); branch `perf/r2rml-q031-refprune` off `perf/r2rml-pr4d`.

## Open questions — RESOLVED (lead, 2026-07-14)

- **(i) Single-required-ref first cut — YES** (conservative-then-widen). This PR admits only the single-required-RefObjectMap binding of `?p`; anything more complex DECLINEs.
- **(ii) Condition (B) template-disjointness — YES, MANDATORY, in-scope** (not a debug_assert; a correctness guard must not ride on this corpus's templates being disjoint). Reuses `wildcard_class_fusion_is_safe`; its own DECLINE hermetic test (DoD 3).
- **(iii) Switch name — `FLUREE_R2RML_REF_TARGET_PRUNE`, approved.**
- **ARITHMETIC HOLE — CLOSED (§3, measured).** Post-prune q031 is loadTable-bound at ~3–5 s cache-thrashed, NOT a guaranteed ≤3 s. The prune ships on the 72 s → ~3–5 s win + the deterministic 7→2 load-count gate; the ≤3 s first-ask is a **named follow-up** (persistent loadTable/creds cache, a PR-8 slice extension — the residual PR-8 slice 2 explicitly leaves). PR-2a stays closed.

**STOP — awaiting the lead's go on the corrected, honest-arithmetic scope before implementation.**
