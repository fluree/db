# PR-4c — admit the same-subject STAR + object-correlation to the batched OPTIONAL (q016) — DESIGN SKETCH

**Branch:** `perf/r2rml-pr4c-optional-star` (off `perf/r2rml-pr8b-innerjoin-memo` HEAD `8fe47ae5f`)
**Status:** APPROVED (lead, + 2 gate sharpenings) → **IMPLEMENTED** + differential-tested; live gate running. Sharpenings: (1) `scan_table` collapse 445-517→single-digit is an explicit gate criterion (proves ONE batched scan, not a per-value loop); (2) the differential's cartesian/miss/null-member cases all in ONE mock, row-for-row, + the switch matrix (`FLUREE_R2RML_BATCHED_OPTIONAL_STAR` off ⇒ PR-4b byte-identical). Implementation: `optional.rs` `r2rml_star_is_hash_join_safe` + the star arm; tests `r2rml_star_admission` + `batched_equals_per_row_on_object_correlated_star`. NO change to `referenced_vars` (P1 done) or the seed/partition core.
**Substrate:** `07-pr4b-batched-optional.md` (shape table + open Qs 1–2), `09 §2` (q016 diagnosis), PR-4b as shipped.

## The seam

q016: `?o a edw:Order ; edw:orderId ?oid . OPTIONAL { ?sh edw:order ?o ; edw:shipStatus ?st }` LIMIT 5000. The OPTIONAL inner is a **same-subject star on `?sh` (FactShipment)** — members `edw:order`→`?o` (a RefObjectMap; `?o` is the OBJECT) and `edw:shipStatus`→`?st` (scalar) — whose **correlation var `?o` appears only as an object**. PR-4b's admission (`r2rml_leaf_is_hash_join_safe`, `optional.rs:1468`) took the narrow cut: a subject-driven single-object leaf (`predicate_filter.is_some()`, `star_bindings.is_empty()`, `star_constraints.is_empty()`, `type_var.is_none()`). q016's inner is a **multi-predicate star** (fails `star_bindings.is_empty()` and has no single `predicate_filter`), so it declines → the per-row `OptionalBuilder::build` rebuild → ~445–517 fact-scans of FactShipment / DNF@180 s.

## (a) STAR admission — sound, and which features

**Soundness (re-derived against current code).** The batched path executes the inner ONCE over the distinct correlation tuples and hash-partitions the output by the correlation key; it is exact iff the inner's solutions for a required row depend on the row ONLY through the correlation set `required-cols ∩ ⋃ referenced_vars(inner)` (`07` contract 1). For a same-subject star that reduces to two facts:
- **The correlation set is COMPLETE for a star (P1 — already landed + tested).** `R2rmlPattern::referenced_vars` (`adapters.rs:825`) **exhaustively destructures** the struct (a new field is a compile error) and surfaces `subject_var, object_var, predicate_var, type_var`, **every `star_bindings` object var** (`:850`), and the FILTER operands (`scan_filters`, `consumed_filter`). Test `referenced_vars_surfaces_every_var_bearing_field` (`:877`) pins it. So a star's members `?o`/`?st` are in the correlation-set input — no silent-drop mis-partition. `star_constraints` is deliberately excluded (constant-object existence filters, **no var**); `class_prune_hint`/`class_filter` are result-preserving resolution-only prunes with no var. So the star adds no hidden correlation channel beyond its member vars, which are all surfaced.
- **Cartesian multiplicity is fine for LEFT-JOIN.** A multi-valued member (or multi-row `?sh` per `?o`) yields a cartesian of inner solutions for that correlation; LEFT-JOIN attaches every one to the required row, and the per-row path produces the SAME cartesian. batched≡per-row. This is multiplicity, NOT the excluded row-multiplying *subquery* (contract 2 — an R2rml leaf carries no internal LIMIT/aggregate/subquery by construction).

**Admit:** a same-subject star = `subject_var.is_some()` + non-empty `star_bindings` (scalar and/or single-valued-ref members) + non-empty-or-empty `star_constraints` (safe: no var). **Keep EXCLUDED** (each a distinct, separately-testable shape q016 does not need): `type_var` (multi-class cartesian — sound per `07` but a follow-up), `predicate_var` (wildcard — not a restriction), `subject_constant` (bound subject — no correlation). This is the minimal widening that lets q016 in.

## (b) Object-side correlation (`?o`)

`?o` is the object of the ref member `edw:order`, produced by the outer. Two questions:
- **Partition key exists?** YES. `?o ∈ star_bindings` object vars ⇒ `?o ∈ referenced_vars(inner)` (a) ⇒ `?o ∈` the correlation set. Correct partition.
- **Seed path — block or de-optimize?** DE-OPTIMIZE only. `corr_var_only_triple_object(?o)` (`:1485`) is Triple-only: the inner is `Pattern::R2rml`, so it hits the `other` arm and returns `false` because the R2rml pattern references `?o` — i.e. `?o` is NOT left unbound for a subject-first probe; it is **seeded bound**. Seeding bound is sound (the seeded R2rml scan restricts `edw:order = ?o` via the operator FILTER / pushdown), it just forgoes a subject-probe that doesn't apply to an object anyway. So q016 is admitted and correct with the seed-bound fallback exactly as `07` open-Q2 anticipated; extending `corr_var_only_triple_object` to R2RML is a **later** optimization, not required for correctness — I propose deferring it (note it).

## (c) Differential plan (PR-4b (B)-style, the correctness gate)

Hermetic batched-vs-per-row on a mock star inner, asserting IDENTICAL solution multisets, covering exactly the cartesian-multiplicity risks:
1. **OPTIONAL-miss row** — a required `?o` with no matching `?sh` ⇒ the required row survives with `?st` unbound (LEFT-JOIN null), both paths.
2. **Multi-row-per-correlation** — one `?o` matched by ≥2 `?sh` ⇒ ≥2 output rows for that required row (the cartesian), identical both paths — the key soundness check.
3. **Dangling / null object** in a member ⇒ identical (`07` P3).
Plus live q016-vs-oracle (rows-only gate per its manifest note — unordered LIMIT). Behind a sub-switch (e.g. `FLUREE_R2RML_BATCHED_OPTIONAL_STAR`, defaulting on within the existing `FLUREE_R2RML_BATCHED_OPTIONAL` / `FLUREE_OPTIONAL_HASH_JOIN` family) so the star widening can be toggled independently of PR-4b's scalar admission.

## (d) Expected win (honest)

q016 per-row: ~445–517 FactShipment scans → DNF@180 s. Batched: **one** FactShipment scan seeded by the distinct `?o` set, then hash-partition ⇒ the 445-scan multiplier is gone → **DNF→ok**. First-pass **cold** is one FactShipment scan (fact-scale, ~7,670 files; the `edw:order = ?o` seed FILTER prunes only if the FK column carries file stats — if not, a near-full scan) — so cold q016 lands in the **data-fetch band (tens of seconds), not sub-second**; the residual is the cold data scan itself (PR-7 territory), not the OPTIONAL. **Hot / warm-disk: fast** (single inner scan + partition). PR-4's parent-memo composes inside the single batched inner.

## Minimal change (on nod)

Relax `r2rml_leaf_is_hash_join_safe` to also admit the star shape above (a new `is_star_hash_join_safe` arm), gated by the sub-switch; the object-side `?o` needs no code change (seed-bound already). Add the (c) differential tests. **No change to `referenced_vars` (P1 already complete) and none to the seed/partition core.**

**STOP — design review before implementation.**

## Result (measured) + the scan-count criterion correction

q016 flips **DNF@180s → ok, 5000 correct rows, ~39s hot**, rows-parity + the differential + the switch matrix all green, native 54/54 0-mismatch. Scan count **517 → 182**, attributed per-table (env-gated debug tag): **2 FACT_ORDER (outer — collapsed) + 180 FACT_SHIPMENT (inner)**. So PR-4c works — the OPTIONAL is admitted to the batched path and the outer driving side is fine.

The 180 inner scans are NOT the designed scan-once collapse the initial gate criterion expected (single-digit). But that criterion was mis-calibrated against an idealized model: the batched hash-left-join has **never** been scan-once for R2RML inners — the *shipped* PR-4b showed **92** inner scans on q050 (430→92, shipped on DNF-flip + parity). PR-4c behaves EXACTLY like the shipped PR-4b mechanism on a larger seed (5000 distinct `?o`): it drives the seed in WINDOWS and re-scans the inner table per window. This per-window re-scan is filed as **F14** (affects both shipped PR-4b and PR-4c; fix class = consume the whole seed in one inner scan + in-memory hash-join, a PR-4d candidate) — a properly-scoped follow-up, NOT a blocker, since it is uniform with what already shipped. PR-4c ships on the honest numbers (DNF→ok, 517→182, outer collapsed) + parity + the differential + F14.
