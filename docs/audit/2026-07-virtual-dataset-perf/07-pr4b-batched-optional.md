# PR-4b — Admit R2RML patterns to the batched correlated-OPTIONAL (soundness analysis)

**Date:** 2026-07-13
**Status:** ANALYSIS ONLY — design review required before any implementation (join-semantics territory).
**Companions:** `05-diagnosis.md` (q050, H3), `ROADMAP.md` PR-4/PR-4b. Follows PR-4 (#1485), which fixed the *within-operator* per-child-batch parent rebuild (q008) but explicitly deferred q050's *per-row operator rebuild*.

## The problem (recap)

q050 (`?p a Product ; isCurrent true ; name ?pn OPTIONAL { ?p edw:supplier ?s . ?s edw:rating ?r }`, native 95 ms) DNFs on virtual because the correlated OPTIONAL rebuilds the **entire inner operator tree per required row** — `OptionalBuilder::build` → `build_where_operators_seeded(SeedOperator::from_batch_row(...))` (`optional.rs:1130-1158`). Every correlation constructs a fresh `R2rmlScanOperator`, so **no** operator-scoped cache (neither `scan_cache` nor PR-4's `parent_lookup_cache`) can span it. PR-4 cannot help here; that is why q050 was cut from PR-4's DoD.

There already exists a path that eliminates the per-row rebuild: the **batched correlated OPTIONAL as a hash-left-join** (`build_batch`, `optional.rs:1191-1397`). It seeds the inner **once** with the distinct correlation tuples of the batch, executes it once, and hash-partitions the output back to each required row by correlation key ("collapses the per-driving-row subplan rebuild — the LDBC IC5 cliff — into a single inner scan"). q050 does **not** take it, because of one gate:

```rust
fn inner_pattern_is_hash_join_safe(p: &Pattern) -> bool {          // optional.rs:1418
    matches!(p, Pattern::Triple(_) | Pattern::Filter(_) | Pattern::PropertyPath(_))
}
```

The R2RML rewrite recurses into `Pattern::Optional` (`rewrite.rs:150`), so q050's OPTIONAL inner patterns are `Pattern::R2rml` by the time `build_batch` checks them → **not** in the allowlist → `Ok(None)` → the per-row `build` path. **PR-4b = admit the safe R2RML shapes here.** If sound, this eliminates the whole per-row rebuild (planning + operator setup + main-table re-scans + parent lookups), strictly more than any cache — and PR-4's parent-memo then rides along *inside* the single batched inner (the two compose).

## The soundness contract the batched path requires

From `build_batch` + its doc (`optional.rs:1168-1190`), an admitted inner pattern set must satisfy:

1. **Correlation-closed.** The inner's solutions for a required row depend on the row **only through the shared (correlation) variables** — the vars the required side and the inner both reference. The correlation set is computed as `required-columns ∩ ⋃ Pattern::referenced_vars(inner)` (`:1211-1215`). Partitioning the single execution by those vars then reproduces the per-row results exactly.
2. **Pure restriction.** Each inner pattern's per-seed evaluation is a restriction by the correlation tuple — **no internal LIMIT, no independent correlation, no row-multiplying subquery** (`:1173`).
3. **Same construction.** The batched inner is built by the SAME `build_where_operators_seeded(&self.inner_patterns, …)` (`:1306`) the per-row path uses — so operator semantics are identical; only the *seed* differs (distinct correlation tuples, once) and the results are partitioned. Two self-guards fall back to per-row rather than mis-partition: the inner output schema must expose every correlation var and every optional-only var (`:1317-1331`).

**Key structural fact for R2RML:** `Pattern::referenced_vars` already has an R2rml arm (`pattern.rs:693` → `R2rmlPattern::referenced_vars`), so the correlation-set computation is NOT blind to R2RML — admitting R2RML does not silently produce an empty/partial correlation set (which would be the classic mis-partition unsoundness). This must be **confirmed** (prereq P1 below), but the wiring exists.

## Shape-by-shape analysis (R2RML leaf patterns)

A `Pattern::R2rml` is always a **leaf scan** (a table scan through term maps) — it carries no internal LIMIT/aggregate/subquery, so contract (2)'s "no internal LIMIT / row-multiplying subquery" is satisfied by construction. The question is contract (1): correlation-closure per shape.

| R2RML shape | Correlation driver | Verdict | Why |
|---|---|---|---|
| **scalar POM** `?corr pred ?out` (?corr = subject) | subject | **SAFE** | Direct `Pattern::Triple` analog: scans the table restricted to ?corr, binds ?out per ?corr. Depends on the row only through ?corr. This is q050's `?s edw:rating ?r`. |
| **RefObjectMap** `?corr pred ?child` (?corr = subject) | subject | **SAFE (single-valued FK)** | A deterministic FK→parent lookup keyed on ?corr. Depends on the row only through ?corr. This is q050's `?p edw:supplier ?s`. Dangling-FK semantics are identical to the per-row path (same `build_where_operators_seeded`). |
| **same-subject star** `?corr a C ; p1 ?a ; p2 ?b` | subject | **SAFE for OPTIONAL, but differential-test** | Multi-valued POMs produce a cartesian of solutions per ?corr. For LEFT-JOIN semantics every such solution attaches to the required row, and the per-row path produces the SAME cartesian — so batched≡per-row. Multiplicity ≠ the excluded "row-multiplying *subquery*". |
| **type-var** `?corr a ?type` | subject | **SAFE for OPTIONAL, differential-test** | Multi-class subjects yield multiple ?type rows; same cartesian argument as star. |
| **object-only correlation** `?x pred ?corr` (?corr only as object) | object | **SOUND but UNoptimized** | `corr_var_only_triple_object` (`:1431`) is Triple-only, so it won't recognize an R2RML object-only corr var and will SEED it bound. Seeding bound is sound (the R2RML scan restricts to the object value via pushdown or the operator FILTER); it just forgoes the subject-first probe optimization. Acceptable initially; extend the helper later. |
| **bound/constant subject** `<iri> pred ?o` | none (no corr) | N/A | Not correlation-driven; irrelevant to the OPTIONAL correlation. |

**q050 verdict:** its inner is two subject-driven restrictions (`?p supplier ?s`, `?s rating ?r`) chained on ?p (the sole correlation). Both are SAFE rows above → q050 becomes batched-eligible → the per-row rebuild is eliminated.

## Prerequisites to verify before implementing (P1–P3)

- **P1 (soundness-critical).** Confirm `R2rmlPattern::referenced_vars` enumerates EVERY row-observable var: `subject_var`, `object_var`, `predicate_var`, `type_var`, all `star_bindings` object vars, and any `scan_filter`/`consumed_filter` operands. If any is omitted, the correlation set can be incomplete → mis-partition (wrong answers). This is the one place admitting R2RML could be unsound; it is a read-only audit of one function.
- **P2 (safety-net, already present).** The output-schema guards (`:1317-1331`) fall back to per-row if the inner doesn't expose a correlation var. R2RML operators carry seeded vars through, but confirm a subject-driven R2RML inner surfaces ?corr in `schema()` (else it silently — and safely — declines, giving no speedup but no wrong answer).
- **P3 (semantics parity).** R2RML dangling-FK / null-object semantics must be identical batched vs per-row. They should be (same operator construction), but this is the differential-test focus.

## Recommendation

Admit `Pattern::R2rml` to `inner_pattern_is_hash_join_safe`. Conservative first cut: a bare `Pattern::R2rml` leaf (no gating on shape needed for contract (2) — it has no internal ops), relying on P1 for contract (1). Land behind the existing `FLUREE_OPTIONAL_HASH_JOIN` kill switch (already gates `build_batch`) and a differential harness that runs q050's OPTIONAL both ways and asserts identical results, including: multi-valued POM stars, a dangling FK, a null-rating supplier (the OPTIONAL-miss row), and the object-only-correlation shape.

**Expected impact:** q050 DNF@120s → native-class (the per-row rebuild is gone; one inner scan of DIM_SUPPLIER/DIM_PRODUCT, then hash-partition). Composes with PR-4: inside the single batched inner, the parent lookup is memoized across the seed's batches by `parent_lookup_cache`.

**Risk:** MED–HIGH — join semantics + a correctness-sensitive partition. The soundness rests entirely on P1 (complete correlation set) and P3 (batched≡per-row semantics). Both are checkable/testable, but this is why the recommendation is *analysis first, review, then implement behind the differential harness* — not a drive-by allowlist edit.

## Open questions for design review

1. Do we admit ALL R2RML leaf shapes at once (relying on P1 + the differential harness), or start with scalar-POM + single-valued-RefObjectMap only (exactly q050) and widen later? The narrow cut is lower-risk but leaves star/type-var OPTIONAL inners on the per-row path.
2. Is the object-only-correlation "seed bound, skip the subject-probe" fallback acceptable indefinitely, or do we extend `corr_var_only_triple_object` to R2RML in the same PR?
3. Should P1 (the `referenced_vars` completeness audit) be a standalone precursor fix (with its own test) even independent of PR-4b, since an incomplete R2RML `referenced_vars` is a latent hazard for any correlation-based optimization?

**STOP — design review before implementation.**
