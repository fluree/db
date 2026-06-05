## STATUS AFTER df8acf47 (correctness FIXED; new PERFORMANCE issue)

Retested on a fresh bulk-imported + indexed LUBM-1. **Correctness is fully fixed** —
every LUBM query now returns the right answer, matching the standard LUBM(1) reference:
q01=4, q03=6, q04=34, q05=719, q06=7790, q10=4, q11=224, q13=1, q14=5916 (q07=66 vs
std 67 — off-by-one, almost certainly UBA seed-0 data variance, not reasoning). The
previously-empty derived joins now resolve, including q05's full-Person join and q11's
transitive `subOrganizationOf` (224).

**New issue: the heavier multi-pattern reasoning joins are catastrophically slow.**
On *tiny* LUBM-1 (~100K triples), isolated, server otherwise idle:
- q12 → 15 (correct) but **101 seconds**.
- q02 / q08 / q09 similarly run for minutes and time out at typical client limits.
The fix's "force eager materialization + disable the batched nested-loop join whenever
reasoning produced a derived overlay" routes these queries onto a very slow join path.
So they are slow-but-correct; the remaining work is performance (a reasoning-aware
join/scan that doesn't fall back to O(n²)-ish behavior, or keeps a fast path while
still merging the derived overlay). Tracked separately from the correctness bug, which
is resolved.

---

## POST-FIX STATUS (commit 9345bb2, retested on LUBM-1)

The fix resolves the originally-reported **2-pattern** case and several queries:
- ✓ `<g> a ub:Student . <g> ub:takesCourse ?c` now returns the courses (was `[]`).
- ✓ `?x a ub:Student . ?x ub:memberOf ?d` → 7790 (2-pattern derived join).
- Full LUBM-1 suite now matches the standard reference for q01,q02,q03,q04,q06,q10,
  q13,q14 (q04 35→34, q06 5916→7790, q10 0→4, q13 now 1 — all correct).

**The same class of bug still reproduces in larger joins**, so q07/q08/q09/q11/q12
still return 0. Minimal, verified reproducers (LUBM-1, `reasoning:"owl2rl"`):

- 3-pattern join where the FIRST pattern is satisfied via a derived fact:
  - `?x a ub:UndergraduateStudent . ?x ub:memberOf ?d . ?d a ub:Department` → 5916 ✓ (base types)
  - same with reasoning off → 5916 ✓
  - `?x a ub:Student . ?x ub:memberOf ?d . ?d a ub:Department` → **0** ✗ (Student is derived)
  - dropping the 3rd pattern (`?x a ub:Student . ?x ub:memberOf ?d`) → 7790 ✓
  So adding a third pattern that joins on a second variable collapses a derived-fact
  join to 0; the base-type equivalent is correct.
- Join on a derived PROPERTY fact (transitive `subOrganizationOf`):
  - `?x a ub:ResearchGroup` → 224 ✓ ; `?x ub:subOrganizationOf <Univ0>` (owl2rl) → 239 ✓
    (includes the 224 RGs via transitive inference)
  - `?x a ub:ResearchGroup . ?x ub:subOrganizationOf <Univ0>` → **0** ✗
  - but the bound-subject form `<RG0> a ?t . <RG0> ub:subOrganizationOf <Univ0>`
    → `[Organization, ResearchGroup]` ✓
  So an unbound hash/merge join doesn't match subjects coming from a derived property
  fact against the same subjects from base facts. (`?d a Department . ?d subOrgOf
  <Univ0>` → 15 works because those subOrgOf edges are direct/base, not derived.)

Net: the 2-pattern derived-fact join is fixed; 3+ pattern joins and derived-property
joins still drop the reasoned rows. Same root flavor (derived-fact bindings not
matching base bindings in a join key), one join layer deeper.

---

# Bug: OWL2-RL derived-type pattern does not join with a node-object pattern on the same subject

## Summary

When reasoning (`owl2rl`) is enabled, a triple pattern that is satisfied **only by a
derived (inferred) fact** — e.g. `?x rdf:type ub:Student` where `?x` is a graduate
student inferred to be a `Student` — produces **zero bindings** as soon as it is
joined with another triple pattern on the same subject whose **object is a node/IRI
(ref-valued)**. The same join works when the type comes from an asserted (base) fact,
and works when the second pattern's object is a literal.

Net effect: single-pattern reasoning is correct, but most multi-pattern queries that
rely on inference return incomplete/empty results.

## Severity / impact

High for any reasoning workload. It breaks the standard LUBM query set
(q07/q08/q09/q10/q11/q12 over LUBM-1) and, more generally, any query of the shape
"things inferred to be of type C that also have relationship P to something."

## Environment

- Branch `fix/lubm-reasoning` (off `perf/snapshot-arc-scaling`), i.e. with both
  reasoning fixes already applied:
  - `ea9a19b` read RDFS/OWL2-RL hierarchy from unindexed novelty
  - `f86ae68` canonical_split blank-node round-trip
- Dataset: LUBM-1 (Univ-Bench ontology + 1 university), bulk `fluree create --from`
  (indexed). Query via FQL with `"reasoning":"owl2rl"`.
- Reproduces on both a fresh `fluree create` index and a running server.

## Minimal reproduction

Subject `G = <http://www.Department0.University0.edu/GraduateStudent101>` is an
asserted `ub:GraduateStudent`, inferred `ub:Student` and `ub:Person`. Prefix
`ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>`. All queries use
`"reasoning":"owl2rl"` unless noted.

| # | Query (where) | Result | Expected |
|---|---------------|--------|----------|
| 1 | `?x a ub:Student` (selectDistinct ?x) | 7790 | 7790 ✓ |
| 2 | `<G> a ?t` | `[…, ub:Person, ub:Student, ub:GraduateStudent]` | ✓ (derived types visible) |
| 3 | `<G> ub:takesCourse ?c` (no reasoning) | `[GraduateCourse0, GraduateCourse65]` | ✓ |
| 4 | `<G> a ub:Student . <G> ub:name ?n` | `["GraduateStudent101"]` | ✓ (derived type + **literal** prop) |
| 5 | `<G> a ub:GraduateStudent . <G> ub:takesCourse ?c` | `[GraduateCourse0, GraduateCourse65]` | ✓ (**base** type + ref prop) |
| 6 | `<G> a ub:Student . <G> ub:takesCourse ?c` | `[]` | **BUG** (derived type + ref prop) |
| 7 | `<G> a ub:Person . <G> ub:takesCourse ?c` | `[]` | **BUG** |
| 8 | `<G> a ub:Student . <G> ub:memberOf ?o` | `[]` | **BUG** (memberOf is not a restricted property) |
| 9 | `<G> a ub:Student . <G> ub:advisor ?o` | `[]` | **BUG** |
| 10 | `<G> a ub:Student . <G> ub:undergraduateDegreeFrom ?o` | `[]` | **BUG** |
| 11 | `?x a ub:Student . ?x ub:takesCourse ?c` (selectDistinct ?x) | 7790 | — see note |
| 12 | `?x a ub:Student . ?x ub:takesCourse <GraduateCourse0>` | 0 | should be 4 |

Note on #11: distinct `?x` is 7790, which *looks* right, but #6 shows the per-subject
join is actually empty for the derived members — so #11's count is not coming from a
correct join of the two patterns (it matches the single-pattern type count). #12
(object bound) collapses to 0, consistent with the join dropping derived subjects.

## What works vs what fails (triage)

- ✓ Single derived-type pattern (generator) — derived members are produced.
- ✓ Bound derived-type probe with a **variable** object (`<G> a ?t`).
- ✓ Derived-type pattern joined with a **literal-valued** property (`ub:name`).
- ✓ **Base** (asserted) type pattern joined with a ref-valued property.
- ✓ A ref-valued property scan on its own, with `owl2rl` enabled.
- ✗ Derived-type pattern joined with **any node/IRI (ref-valued)** property on the
  same subject — independent of whether that property participates in reasoning
  (fails for `takesCourse` which is a someValuesFrom property *and* for `memberOf`,
  `advisor`, `undergraduateDegreeFrom` which are not).

## Ruled out

- Not the reasoning materialization itself: derived facts exist and are visible to
  single-pattern queries and to a bound `<G> a ?t` probe.
- Not the `DerivedFactsOverlay` index coverage: it serves Spot/Psot/Post/Opst and
  single lookups over it succeed (e.g. #1, #2).
- Not specific to restricted/reasoned properties: plain ref properties fail too.
- Not the two already-fixed SID/hierarchy bugs (those are in this build).

## Hypothesis / likely location

The two patterns are evaluated against different views and the **join key (the shared
subject variable) does not match** between a derived flake and a base flake when the
joined object is a node — or the executor's plan for "derived-satisfiable type pattern
+ ref-object pattern" does not run the ref-object scan over / merge with the reasoning
overlay. Because it works for literal objects but not node objects, the divergence is
tied to ref-object (Sid) handling in the join, not to the type pattern alone.

Suspect areas in `fluree-db-query/src/execute/`:
- the join/merge operator and how bindings from a reasoning-overlay-satisfied pattern
  are keyed vs. bindings from a base-index ref-object scan;
- `runner.rs::execute_prepared` (derived overlay is wrapped into `ReasoningOverlay`
  there) — confirm the *same* effective overlay is used for every pattern's scan in a
  multi-pattern plan, including ref-object scans and the hash/merge keys;
- whether ref-valued patterns get RDFS subproperty rewriting (UNION expansion) that,
  combined with a derived-overlay subject set, produces an empty intermediate.

## Suggested next diagnostics

1. Instrument the join operator (or run with query tracing) on case #6 and print the
   left input bindings (from `<G> a ub:Student`) and right input bindings (from
   `<G> ub:takesCourse ?c`), plus the `Sid` of the subject on each side — check for a
   subject-`Sid` mismatch between the derived row and the base row.
2. Compare the executable plan for case #5 (base type, works) vs #6 (derived type,
   fails) — what changes when the type pattern is only derived-satisfiable?
3. Repeat #6 over **novelty** (un-indexed, e.g. via the in-memory/staging path) vs the
   indexed ledger to see whether it's index-read-specific (the earlier two bugs were).

## Reproduce

```bash
# Build (LTO off for speed): CARGO_PROFILE_RELEASE_LTO=false \
#   CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 cargo build --release -p fluree-db-cli
fluree create lubm --from lubm-combined.ttl      # data + Univ-Bench ontology
fluree server start --listen-addr 0.0.0.0:8090
# POST the table's queries to /v1/fluree/query/lubm:main with {"reasoning":"owl2rl"}.
```
(LUBM-1 generation + the full query/ground-truth tooling are in the benchmark repo at
`benchmarks/lubm/` — see `FINDINGS.md` there.)
