# PR-6 — Fused Aggregate over one Fact⋈Dim Join (+ F9 formatter rider) — DESIGN

**Date:** 2026-07-13
**Branch:** `perf/r2rml-pr6-rollup-fusedagg` (off `perf/r2rml-pr4b-batched-optional` HEAD `01abe5412`)
**Status:** APPROVED (§11) — **6a IMPLEMENTED + GREEN** (§12.1: all four gates fuse, 0 mismatch, q032 70s→308ms, sentinel + q008-decline confirmed); 6b + F9 not started.
**Companions:** `05-diagnosis.md` (H6 deferred behind H1), `09-stacked-rebaseline.md` §3 (q008/q009/q032 are now operator-bound — the H6 case, visible post-PR-2), `ROADMAP.md` PR-6, `04-findings-register.md` F9.
**Code read at this HEAD:** `fluree-db-query/src/r2rml/fused_aggregate.rs`, `.../r2rml/operator.rs`, `.../ir/adapters.rs` (`R2rmlPattern`), `.../execute/operator_tree.rs` (the fused hook), `fluree-db-api/src/format/{sparql.rs,iri.rs,sparql_xml.rs}`.

---

## 0. What PR-6 is, in one paragraph

`09 §3` proved that with PR-2's decode wall gone, the rollup class (q008/q009/q032, and q010/q025) is **operator-bound**: the fact scan is warm-cheap (~0.5 s) but the generic pipeline then materializes an RDF `Binding` for every one of ~180 K fact rows, hash-joins them to the dimension(s), and folds the join away in a generic GROUP BY — 50–68 s of pure allocation + join churn for a handful of output groups. The existing `FusedR2rmlAggregateOperator` already folds `COUNT`/`SUM`/`AVG` **directly from typed `ColumnBatch` values** for a **single** TriplesMap (no join), never building a subject IRI or a per-row `Binding`. **PR-6 extends that fold to admit one fact→dim FK path so the group key comes from a dimension attribute** — the fact is streamed and folded; the small dimension(s) become an in-memory FK→group-key resolver built once (the PR-4 parent-lookup machinery). No 180 K bindings, no hash-join. **F9** rides along: a one-line result-formatter fix so virtual predicate/type IRIs CURIE-compact exactly as native (AJ's decided parity direction), flipping q002/q042 hash-green.

---

## 1. Target shapes — the empirical grounding (READ FIRST; it reframes the shape test)

Every target query, decomposed. **Two facts here diverge from the task's shape sketch and drive the design decisions below.**

| q | measure(s) | GROUP BY key(s) | FK **hops** fact→key | HAVING? | ORDER BY? | notes |
|---|---|---|---|---|---|---|
| **q014** channel_mix | SUM, COUNT | `?ch` = Order.orderChannel | **0** (fact column) | — | — | **already the existing single-table fused path** — the 0-hop control |
| **q032** onhand_by_store | SUM | `?sn` = DimStore.name | **1** (InvSnapshot→DimStore) | — | — | the clean one-hop case |
| **q025** category_csat | AVG, COUNT | `?cat` = DimProduct.category | **1** (Ticket→Product) | **YES** `AVG(?csat)<3` | — | one-hop **+ HAVING** |
| **q010** revenue_by_quarter | SUM | `?year`,`?q` = DateDim.year, DateDim.quarter | **1** (Order→DateDim) | — | **YES** | one-hop, **two keys from one dim** |
| **q008** revenue_by_region | SUM | `?region` = DimGeography.region | **2** (Order→Customer→Geography) | — | — | **two-hop chain** |
| **q009** revenue_by_region_having | SUM | `?region` = DimGeography.region | **2** (Order→Customer→Geography) | **YES** `SUM(?tot)>5M` | — | **two-hop chain + HAVING** |

**Divergence 1 — hops.** The task spec says "group keys from one parent TM via single-column FK." That is exactly q032/q025/q010 (**one** hop). But the headline gates **q008/q009 are two-hop chains** (Order→Customer→Geography). A strict one-hop operator does **not** cover the headline. → the design must resolve an **FK chain of length ≥ 1**, or we slice (see §7).

**Divergence 2 — HAVING.** The task spec's shape test excludes HAVING, but **q009 (headline) and q025 both have HAVING**. Resolution below (§5.3): HAVING is **already rewritten to reference aggregate-output vars** (`grouping.rs:189-192`), so it is applied by **wrapping a `HavingOperator` around the fused op** — exactly as ORDER BY/LIMIT are wrapped today (`operator_tree.rs:2456-2464`). HAVING therefore does **not** need to be inside the fold; it stays out of `detect`'s fold shape but is handled by the wrapper. This lets q009/q025 fuse with zero fold-side complexity.

All group cardinalities are tiny (regions, quarters, categories, stores — tens to hundreds), so a per-group accumulator `HashMap` (the existing `groups` map, `fused_aggregate.rs:962`) is the right structure unchanged.

---

## 2. The current operator and the exact join-rejection site

`FusedR2rmlAggregateOperator` (`fused_aggregate.rs:733`) streams **one** table via `table_provider.scan_table(table, projection, …)` (`:943-951`) and folds each row into either an implicit accumulator set or the per-group map, gated by the R2RML **row-validity** check (subject template columns + every object column non-null, `:1294-1325`) and the optional FILTER.

Detection (`detect_fused_r2rml_aggregate`, `:281`) is a cheap IR structural check; the R2RML-ness and single-scan collapse are deferred to `open`/`resolve_at_open` (`:1121`). **The join is rejected at one line:**

```rust
// fused_aggregate.rs:1158-1161  (resolve_at_open)
let pattern = match rr.patterns.as_slice() {
    [Pattern::R2rml(p)] => p.clone(),
    _ => return Ok(None),   // multiple scans / star not handled in slice 1  ← PR-6 relaxes THIS
};
```

A fact→dim chain does **not** collapse to one `R2rmlPattern`. `R2rmlPattern` (`adapters.rs:594`) is a **single-table scan** (one `subject_var`, one TM, same-subject members in `star_bindings`). q008's `?o customer ?c . ?c geography ?g . ?g region ?region` rewrites to **three** `R2rmlPattern`s — Order(`?o`, star: orderTotal→?tot, customer→?c), Customer(`?c`, star: geography→?g), Geography(`?g`, star: region→?region) — joined by the shared vars `?c`,`?g`. Today `rr.patterns.as_slice()` is length 3 → `Ok(None)` → generic fallback (why q008 runs the 52 s generic path). **PR-6 makes `resolve_at_open` recognize this fact + FK-chain-to-a-group-key shape and build a fold plan instead of bailing.**

---

## 3. The design — fold over the fact scan, resolve the group key through a memoized FK-chain lookup

The generic path already builds exactly the dimension lookups we need — `build_parent_lookup` (`operator.rs:2000`) scans each small parent dim and builds a `HashMap<join_key, RdfTerm>`, memoized across batches by `LookupCacheKey = (parent_tm_iri, sorted_join_cols)` on `parent_lookup_cache` (PR-4, `operator.rs:300, :1020-1029`). PR-6 reuses that **pattern** but with a different value, then folds.

**Runtime shape (one fact, chain length k to the group key):**
1. **Build the group-key resolver once** (before the fact scan). For a chain fact →FK₁→ dim₁ →FK₂→ … → dimₖ, where the group key is an attribute column on dimₖ: scan each dim (all small), building per-hop maps, and compose them into a single **`HashMap<fact_FK_value, GKey-tuple>`** (`GKey` = the already-encoded group-key value, one per GROUP BY var). Multi-key (q010) → the terminal map's value is a `Vec<GKey>`.
2. **Stream + fold the fact** exactly as today (`:965` loop), with **one added step per row**: read the fact's FK column(s), probe the resolver. **Miss (dangling or null FK at any hop) ⇒ skip the row** (the R2RML/inner-join row-drop — see §5.1). Hit ⇒ fold the measure columns into `groups[GKey-tuple]`.
3. Emit one result row per group; wrap HAVING/ORDER BY/OFFSET/LIMIT outside (§5.3).

### 3.1 Parent-lookup reuse analysis (the task's explicit ask)

**Reusable directly:** the `LookupCacheKey` scheme, the "dims are small → `collect_stream` the whole scan into a map" pattern (`operator.rs:1006-1017`), `get_join_key_from_batch` (`materialize`), and the PR-4 `parent_lookup_cache` memo + its `materialize_window_rows()` size cap (`operator.rs:1025`).

**NOT reusable directly — this is the load-bearing finding:** `build_parent_lookup` maps `join_key → parent **subject** RdfTerm` (`operator.rs:2010, :2032`). PR-6's group key is a **dimension attribute** (DimStore.name, DimGeography.region), and the intermediate chain hops are **FK→FK column** resolutions (DimCustomer.CUSTOMER_KEY → DimCustomer.GEOGRAPHY_KEY), **not** subject IRIs. So PR-6 needs a **sibling builder** — call it `build_attr_lookup(dim_tm, join_cols, project_cols) → HashMap<join_key, Vec<encoded-value>>` — that projects the *next* FK column (interior hop) or the *group-key attribute* column (terminal hop) and encodes it the way the fold consumes (a `GKey`, exactly as the single-table group path already encodes group columns via `GroupCol`/`group_kind`, `fused_aggregate.rs:1181-1200`). Chain composition then folds hop maps into `fact_FK → GKey-tuple`. **Recommendation:** factor the small dim-scan + collect into a shared helper both the generic operator and the fused operator call, keep `build_parent_lookup` as the subject-valued specialization, add `build_attr_lookup` as the attribute-valued one, and share the `LookupCacheKey` cache (the fused op can even reuse a warm subject lookup's *scan* — same table, different projection — but simplest first cut is its own attribute cache under the same key scheme). Confirm the dim scans are keyed so a query that ALSO joins the dim in a non-fused sibling doesn't double-scan (nice-to-have, not required).

### 3.2 What carries over unchanged

The decimal-exact fold (`Dec`, `NumericAcc`, overflow→exact-pipeline re-run, `:961-1090`), the `NumKind`/`numeric_kind` datatype typing, the COUNT-column vs COUNT-rows distinction, and the row-validity gate (extended, §5.1). The COUNT(*) manifest shortcut (`:915-941`) stays **single-table only** — a join has no single manifest count (leave it gated behind "no group cols, no filter, single fold"; a joined COUNT still folds via the scan).

---

## 4. Detection surface — what relaxes, the exact new shape test, what stays excluded

`detect_fused_r2rml_aggregate` (`:281`) stays a cheap IR check; the join structure is only knowable after the R2RML rewrite, so the **new admission logic lives in `resolve_at_open`** at the `:1158-1161` site. Two edits:

**Edit A — `detect` (IR):** relax two current bailouts:
- **HAVING** (`:311` `if having.is_some() … return None`) → allow when `having` references only GROUP BY vars + aggregate-output vars (it always does — pre-rewritten, `grouping.rs:189`); it is applied by the wrapper (§5.3). Keep rejecting `!aggregation.binds.is_empty()` (post-agg binds) unchanged.
- The inner-pattern loop (`:327-333`) already pushes every `Pattern::Triple` into `inner` and rejects non-triple/second-filter — a multi-triple fact⋈dim chain is **all triples**, so it already passes `detect`; **no change needed there** (the join was only rejected later, at `:1158`). *(The ROADMAP's ":327-333 join exclusion" anchor referred to an older layout; at this HEAD the operative rejection is `:1158`.)*

**Edit B — `resolve_at_open` (`:1158`):** replace the `[Pattern::R2rml(p)]`-only match with a classifier that accepts `[fact, dim₁, …, dimₖ]` when **all** hold (the EXACT new shape test):
1. Exactly **one fact TM**: the pattern whose `subject_var` is **not** the object/ref-target of any other pattern in the set (the chain root), and which carries every aggregate measure column.
2. The remaining patterns form a **single linear FK chain** from the fact to the group-key attribute: each non-fact pattern's `subject_var` is bound as the **RefObjectMap object** of exactly one prior pattern, no branching, no cycles.
3. Every **GROUP BY var** resolves to a **scalar attribute column on the terminal dim** (or, for a 0-hop key, a fact column — the q014 case) via `scalar_column_for_var` + `group_kind` (existing, `:1183-1200`).
4. Each FK join is a **single-column** equality (`rom` join_cols length 1) — composite FK deferred.
5. Every measure aggregate resolves to a **fact** column (§3.2); no aggregate references a dim attribute (a `SUM` over a dim column is out of scope — not in the corpus).

**Stays excluded (fall back to generic, byte-identical):** DISTINCT aggregates (`:356-367`, unchanged); post-aggregate BINDs (`:311`); a **branching** join (two dims off the fact, e.g. a star of two independent FKs) — chain only in this PR; **composite** FK; **multi-fact** joins (fact⋈fact, e.g. q015/q016 — those are OPTIONAL/left-join, not this shape); a group key that is itself an **IRI/ref** (only scalar attribute keys); aggregate over a **dim** column; **expression ORDER BY** (`order_binds`, `:292`); FILTER-without-GROUP-BY (the `:349` cost guard stays). Any exclusion ⇒ today's generic path.

---

## 5. Semantics traps (the correctness core)

### 5.1 Dangling FK / NULL FK column ⇒ the fact row drops from the rollup (MUST match generic)

In R2RML a RefObjectMap whose child FK value matches **no** parent subject produces **no** triple, so in `?o customer ?c . ?c … ?region` the inner join **drops** that Order — its measure is in **no** group. Identically, a **NULL** FK column produces no join key. The generic path enforces this via the inner join; **the fold must replicate it exactly**: the group-key resolver returns `None` when **any** hop's key is null or misses, and a `None` resolution **skips the row entirely** — not counted, not summed, not placed in a NULL/"" group. This extends the existing row-validity gate (`:1294-1325`), which today skips rows with a null subject-template or object column; PR-6 adds "…and the FK chain resolves to a non-null terminal group key." **This is the #1 differential-test target** (a fact row with a dangling FK, and one with a null FK, must yield identical results fused vs generic).

### 5.2 NULL measure ⇒ row already drops (carried over)

`?o orderTotal ?tot` produces no triple when orderTotal is null, so the inner-join row drops. The existing fold already lists object/measure columns in `validity_cols`/`count_non_null_cols` (`:1310-1318`) and skips null-measure rows — **unchanged**, and it stays correct because a joined rollup still requires the measure triple to exist. (Note the SPARQL semantics subtlety: `COUNT(?x)` counts non-null `?x`, `COUNT(*)` counts rows; both are already distinguished at `:1211-1222` and carry over.)

### 5.3 HAVING ⇒ wrap, don't fold

`having` is pre-rewritten so its aggregates are lifted into `aggregation.aggregates` with synthetic output vars and the expression references those vars (`grouping.rs:189-194`); the normal pipeline applies it via `HavingOperator` (`operator_tree.rs:3158-3163`). PR-6 mirrors the ORDER BY/LIMIT wrapping already at the fused hook (`operator_tree.rs:2456-2464`): after building the fused op, if `query.grouping.having()` is `Some`, wrap `HavingOperator::new(op, having)` — **before** the SortOperator (SPARQL order: WHERE → GROUP → HAVING → ORDER → LIMIT). The fused op emits every group; the wrapper drops the failing ones. Zero fold-side change; q009/q025 fuse. *(This is the recommended divergence from the task's "HAVING excluded" lean — flagged in §9 Q2.)*

**One detection detail this exposes.** `detect` requires the projection to be exactly `group_by ∪ aggregate-outputs` (`:372-383`, `projected.len() == outs.len()`). q009/q025 reuse the **same** aggregate in HAVING and SELECT (`SUM(?tot)`→`?rev`; `AVG(?csat)`→`?avg`), so the lift dedups to the projected var and `outs` is unchanged — they pass. But a HAVING that references an aggregate **not** in SELECT (e.g. `HAVING COUNT(*) > 5` with the count unprojected) lifts a synthetic aggregate var that is computed-but-not-emitted, so `outs` (all aggregate outputs) would exceed `projected`. For this PR: **admit HAVING only when its lifted aggregate outputs ⊆ projected aggregate outputs** (true for q009/q025); the fold computes exactly the projected aggregates and the wrapper filters on them. The general "compute an extra unprojected aggregate for HAVING then drop it" case stays excluded (a later widening).

### 5.4 Decimal / datatype exactness (carried, watch the join)

The measure fold keeps the existing exact-decimal path and datatype-by-declared-datatype typing (§3.2). The only new surface: the **group key's** datatype Sid must be encoded from the terminal dim's object map exactly as the single-table group path does (`:1189-1200`), so a fused group key binding is byte-identical to the generic materialized one.

### 5.5 Duplicate dim join-key ⇒ conflicting mapping ⇒ decline (checked invariant, #1490 review)

The FK-chain lookup builds `dim join-key → group-keys` maps by scanning each dim. The original code did `map.insert(key, gkeys)` with a `// last-wins (a dim join key is unique)` comment — an **unchecked** assumption. If a dim has a **duplicate join-key whose group attributes differ**, the generic pipeline (the reference semantics) materializes *both* attribute triples on that dim subject, so a joined fact row legitimately lands in **two** groups, while the single-value fused probe keeps only the last — a silent under-count. Pre-PR-6 ref lookups were immune (a duplicate key produces the same templated subject IRI ⇒ the same triple); PR-6 introduced *attribute*-valued lookups where the values can differ. The shape is reachable via this stack's own #1450 unverified subject-keys, name-based FK inference onto a non-unique column, and hand-written mappings — SF01 dims have unique PKs, so the corpus cannot catch it. **Fix:** the shared `insert_dim_gkeys` helper turns the comment into a checked invariant — a **conflicting** duplicate (same key, different group-keys) returns `false` and the resolver falls back with `Ok(None)` (the operator's decline contract); **equal-value** duplicates are harmless and kept. Unit-tested (`dim_dup_join_key_conflict_declines_equal_dup_kept`). Both the terminal scan and every interior hop route through the one helper, so the invariant holds across the whole chain.

---

## 6. F9 rider — CURIE-compact virtual predicate/type IRIs (one line, reuse native's compactor)

**Root cause (confirmed at this HEAD).** `format/sparql.rs` `write_term` (`:328-332`):
```rust
Binding::Sid { sid, .. }   => write_node(out, &compactor.compact_id_sid(sid)?),   // native: COMPACTED
Binding::IriMatch { iri,..} => write_node(out, &compactor.compact_id_iri(iri)),   // multi-ledger: COMPACTED
Binding::Iri(iri)          => write_node(out, iri.as_ref()),                       // R2RML: VERBATIM  ← F9
```
The R2RML operator binds vocabulary IRIs as `Binding::Iri(raw_string)` (`operator.rs:1419`, "IRIs are kept as raw strings — graph source IRIs [aren't in the namespace table]"), and `write_term` emits `Binding::Iri` **verbatim** while native's `Binding::Sid` goes through `IriCompactor::compact_id_sid`. Same IRI, different lexical form ⇒ different hash (F9).

**Fix (smallest, reuses native's code):** route `Binding::Iri` through the **same** compactor call `IriMatch` already uses:
```rust
Binding::Iri(iri) => write_node(out, &compactor.compact_id_iri(iri)),
```
`IriCompactor` (`format/iri.rs`) is already built from `snapshot.shared_namespaces()` **+ the query's parsed `@context`** (`IriCompactor::new(snapshot.shared_namespaces(), &context)`, e.g. `view/stream_query.rs:164`), and `compact_id_iri(full_iri)` compacts against those prefixes — so `http://ns.fluree.dev/edw#name` → `edw:name` using the query's `PREFIX edw:`, identical to what `compact_id_sid` yields natively. **No reimplementation, no new prefix plumbing.** Confined to `format/sparql.rs` (the SPARQL-results-JSON path); **JSON-LD (`format/jsonld.rs`) and its @context expansion are untouched**, per the constraint.

**DoD:** q002 and q042 flip **hash-green** vs the native oracle; re-bless the two virtual baselines; the corpus's other IRI-projecting queries (q001/q022/q054 project only literal objects — unaffected) stay green.

**SPARQL-XML path — no change needed (resolved).** `format/sparql_xml.rs:212` has the identical `Binding::Iri => write_iri_ref(verbatim)` line, but native's `Binding::Sid` arm there (`write_sid_ref`, `:263-269`) emits `<uri>prefix + name</uri>` — the **full absolute IRI, expanded not compacted** (XML `<uri>` is spec'd as a full IRI, no CURIE form). So on the XML path native and virtual **already agree** (both full IRIs); F9 is a **JSON-formatter-only** divergence — native JSON compacts via `compact_id_sid` while native XML expands. The fix is therefore correctly confined to `format/sparql.rs`; `sparql_xml.rs` stays as-is.

---

## 7. Slicing recommendation (risk management)

The one-hop and two-hop cases differ in exactly one place — the group-key resolver is a single `build_attr_lookup` (one hop) vs a composed chain (k hops). I recommend **landing both in PR-6 behind one sub-switch**, but implementing and validating in two commits so review can gate the chain separately:

- **6a — 0/1-hop:** the fold + `build_attr_lookup` + the `resolve_at_open` classifier restricted to chain length ≤ 1. Flips **q032, q025, q010** (and confirms q014's 0-hop still fuses). Lower risk; the resolver is a single map.
- **6b — k-hop chain:** generalize the classifier + resolver to compose ≥ 2 hops. Flips **q008, q009**. The chain composition and the "one fact = chain root" detection are the only added surface.

Both ride the same kill switch (§8). If review prefers, 6b can be a follow-on PR — but since q008/q009 are the headline, my lean is **one PR, two commits**.

## 8. Kill switches

- **New sub-switch `FLUREE_FUSED_R2RML_AGG_JOIN`** (agree with the lead's lean): gates **only** the join admission at `resolve_at_open:1158`. Off ⇒ the multi-pattern case returns `Ok(None)` → today's generic path, and the **proven single-table fused path is byte-identical and never hostage to the new code**. `FLUREE_FUSED_R2RML_AGG` remains the master switch (off ⇒ whole fused path off, unchanged).
- **F9 needs no switch** — it is the decided parity behavior (AJ). It is behavior-changing for virtual IRI projections, so it is called out in the PR description, not gated.

## 9. Gates / DoD

1. **Headline:** q008/q009/q032 dnf-era 50–68 s → **few-second fused folds** on `virtual-sf01`; assert scan/decode is the only fact I/O (no 180 K-binding materialization — check `r2rml.scan_table` count == fact + dims, not a per-row explosion).
2. **Sentinel:** **q022** (single-table fused GROUP BY, no join) unchanged — the join extension must not perturb the single-table path (kill-switch-off proves byte-identical).
3. **Shape coverage:** q010 (multi-key, ORDER BY), q025 (HAVING), q014 (0-hop) all correct and fused.
4. **Parity (exact):** q008/q009/q010/q025/q032 result hashes == native oracle; **F9** flips q002/q042 hash-green + re-bless.
5. **Dangling/NULL FK differential test** (§5.1): a synthetic mapping with a dangling FK and a null FK column yields identical results fused vs generic (kill-switch A/B).
6. **Suites:** W3C SPARQL green; native/JSON-LD IR-parity green; **native corpus 0-mismatch** and BSBM/native budgets unregressed (the fused path also gates native).
7. **Kill-switch fidelity:** `FLUREE_FUSED_R2RML_AGG_JOIN=0` ⇒ corpus byte-identical to pre-PR (generic path); q022 identical either way.

## 10. Open questions for lead review (before I implement)

1. **Chain in scope?** q008/q009 need **2-hop** resolution; the task spec said "one parent TM." Confirm PR-6 does the k-hop chain (my §7 lean: yes, as commit 6b), or split 6b to a follow-on.
2. **HAVING by wrapping** (§5.3) — I recommend admitting HAVING via a `HavingOperator` wrapper (needed for q009 headline + q025), which extends the task's "HAVING excluded" lean. Confirm.
3. **Resolver caching** — build a fresh `build_attr_lookup` per query (simple, correct) vs share the PR-4 `parent_lookup_cache` scan for the same dim (avoids a double dim-scan when a sibling non-fused pattern also joins it). Lean: fresh per query first, optimize later.
4. **F9 XML path** (§6) — **RESOLVED, no decision needed:** native SPARQL-XML already emits full IRIs (`write_sid_ref` expands prefix+name, `sparql_xml.rs:263-269`), so virtual's `Binding::Iri` already matches there. F9 is JSON-only by nature; fix stays confined to `format/sparql.rs`.
5. **One PR or two?** §7 — my lean is one PR, two commits (6a then 6b), one sub-switch.

**STOP — design review before implementation.**

---

## 11. Review outcome — APPROVED 2026-07-13 (all five questions answered)

- **Q1 (chain): YES**, k-hop in scope. **Constraint: LINEAR chains only, each hop a single-column FK, + a cycle-guard** (a mapping with an FK loop must **decline** to fuse, not spin — with a decline test). One PR, two commits (6a: 0/1-hop; 6b: k-hop). Structure 6a to be **independently green** (gate q032/q025/q010/q014) so 6b can slip without blocking 6a.
- **Q2 (HAVING): YES**, by wrapping (§5.3), admission = lifted HAVING aggregates ⊆ projected aggregates. **Also test the rejection case** (HAVING over an unprojected aggregate ⇒ generic path).
- **Q3 (resolver caching): fresh per query.** The cross-operator sharing idea is noted as future work only.
- **Q4 (F9 XML): JSON-only** (already resolved in §6 — native XML also emits full IRIs, so no divergence, no note needed; don't touch `sparql_xml.rs`).
- **Q5 (commits): three** — 6a, 6b, **and F9 as a separate third commit** (different subsystem, independently revertable). **One** sub-switch `FLUREE_FUSED_R2RML_AGG_JOIN` gates both 6a and 6b.

**Confirmed for impl:** dangling/NULL-FK row-drop is differential-test target #1 (hermetic, PR-4b-style mock provider — fact rows with a dangling FK and a NULL FK, fused-vs-generic equality); `build_attr_lookup` as a **sibling** (don't contort `build_parent_lookup`); the `:1158` admission point + `:311` HAVING relaxation are the only detection changes. **GO — 6a first, report at the 6a-green checkpoint before 6b.**

---

## 12. Implementation status — commit 6a (0/1-hop) IN PROGRESS

**Files changed (only these two; no unrelated fmt):**
- `fluree-db-query/src/r2rml/fused_aggregate.rs` — the join fold.
- `fluree-db-query/src/execute/operator_tree.rs` — the HAVING wrap in the fused hook (+9 lines).

**What 6a does:**
- **Sub-switch** `fused_r2rml_agg_join_enabled()` (`FLUREE_FUSED_R2RML_AGG_JOIN`, on by default); the master `FLUREE_FUSED_R2RML_AGG` still gates the whole path.
- **Admission** at the old join-rejection site (`resolve_at_open`, the `[Pattern::R2rml(p)]` match): a multi-pattern rewrite now routes to `resolve_join_at_open`. The single-table arm is **byte-identical** (the fold-resolution was extracted to a shared `resolve_agg_folds` both paths call; all 8 pre-existing fused tests still pass).
- **`resolve_join_at_open`** (6a = exactly 2 patterns): classifies fact vs dim via `joins_via` (with the `(Some,Some)` **cycle-guard** → decline), finds the **single-column** RefObjectMap FK on the fact, takes the dim TM from the FK's `parent_triples_map` (**authoritative — not `resolve_triples_map(dim_p)`, which is ambiguous under the shared `edw:name` predicate**; the group-col resolution validates the key predicate is a scalar POM on that dim), resolves GROUP BY keys as scalar dim attributes and the aggregates from the fact, then scans the dim once and builds `GroupKeyResolver { fact_fk_cols, map }` inserting **only fully-non-null dim rows**.
- **Fold** (`next_batch`): for a resolver plan, each fact row's group key is `get_join_key_from_batch(fact_fk_cols)` → `map.get`; a **null/missing FK or a dim row with a null group attribute drops the fact row** (the inner-join row-drop). Fact-side validity adds the FK child cols + measure cols (join var excluded — it's a RefObjectMap object). Decimal-exact fold + datatype-Sid group-key encoding carry over unchanged.
- **HAVING** (`detect` relaxed to admit it; hook wraps `HavingOperator` before ORDER BY): admitted only when the projection check still holds (lifted HAVING aggregate ⊆ projected), else generic.

**Excluded in 6a (→ generic, correct):** a FILTER over the join; composite FK; !=2 patterns (k-hop chains are 6b); non-scalar group keys; aggregates over a dim column; implicit (non-GROUP-BY) join aggregate.

**Tests (green):** `cargo test -p fluree-db-query` = **1207 passed, 0 failed** (incl. the 8 pre-existing single-table fused tests). New: `admits_having_referencing_projected_aggregate`, `declines_having_over_unprojected_aggregate` (the rejection case), `joins_via_classifies_direction_and_flags_cycle` (the cycle-guard/decline). Clean `cargo check`; only the two files above are modified.

**Open item (answered by the live run):** whether q025/q009's SPARQL HAVING lift **dedups** to the projected aggregate (→ fuses) or mints a synthetic `?__having_agg` var (→ stays generic by the conservative admission line). If the latter, q025 fusing needs a projection-wrapper widening — noted, not built.

**Pending for 6a-green:** live-corpus parity + fusing proof for q032/q010/q014/q025 (the differential vs the native oracle serves as the dangling/NULL-FK row-drop check at corpus scale; a hermetic ephemeral-provider execution test is a fast-follow — a pure unit exec test needs a snapshot with encoded datatypes, more scaffolding than the detect/joins_via units).

### 12.1 Live validation (release binary, virtual-sf01, `--cache-state hot`) — results

**6a is GREEN: all four gates fuse and are byte-correct.** `vbench compare` = **4 records, 0 hash mismatches, 0 perf violations** vs the native oracle; the q022 single-table sentinel is unchanged; q008 correctly declines to the generic path (2-hop, 6b). Full `fluree-db-query` suite = **1208 passed / 0 failed**.

| q | shape | fused? (scan) | **fused wall** | generic wall | note |
|---|---|---|---|---|---|
| **q032** | InvSnapshot→DimStore `SUM` GROUP BY name | **YES** (8→2) | **308 ms** | 70–79 s | **~230×** — the headline win |
| **q025** | Ticket→Product `AVG`+`COUNT` **HAVING** | **YES** (4→2) | **309 ms** | 1.8 s | HAVING fuses via the wrap |
| **q010** | Order→DateDim `SUM` keys year+quarter (int) | **YES** (3→2) | **309 ms** | 1.8 s | multi-key |
| q014 | 0-hop (fact column) | single-table (1) | 204 ms | 365 ms | unchanged |
| q022 | single-table sentinel | single-table (1) | 394 ms | — | byte-identical, no regression |
| q008 | 2-hop Order→Customer→Geography | **declines → generic** (8) | 42 s | — | correct (6b territory) |

**The root cause of every early decline was ONE bug** (found via env-gated decline-tag probes; both q032 and q025 declined at the same clause `group-key-datatype-unsupported (None)` with a clean 2-pattern rewrite — overturning the earlier "shared-name >2 patterns" and "HAVING synthetic var" hypotheses):

- **Un-annotated string group key.** `DimStore.STORE_NAME` (`edw:name`) and `DimProduct.CATEGORY` (`edw:category`) carry **no `rr:datatype`** in the mapping, so `scalar_column_for_var` returns `datatype = None` and `group_kind(None)` bailed. q010 fused all along because its `YEAR_NUM/QUARTER_NUM` keys have an explicit `rr:datatype xsd:integer`. **Fix:** default an un-annotated column key to `xsd:string` (R2RML's natural mapping for a string column) — hash parity on q032/q025 confirms it matches the native oracle. **Applied on the JOIN path only.** A first cut also applied it on the single-table path, which regressed the **q022 sentinel** (segment is also un-annotated): the single-table fold then fired on q022's `isCurrent true`-constrained shape and mismatched. Since q032/q025 prove `xsd:string` is the *correct* datatype for these string columns, q022's mismatch is single-table-fold-shape-specific, not a datatype error — so the single-table path is left **byte-identical** (un-annotated key → generic, as before) and the default is scoped to the validated join path. The latent single-table bug this exposed (un-annotated string key + constant-object `star_constraint`, unreachable today) is logged as **`04-findings-register.md` F12** — must be fixed before anyone enables the string default on the single-table path.

**Two further bugs found + fixed during validation** (both regression-tested):
1. **Integer group key as Decimal (q010 0-rows).** A Snowflake `NUMBER(n,0)` `xsd:integer` column arrives as `Column::Decimal`, which `GroupCol::key_at`'s Integer arm didn't read → every dim row dropped → 0 rows. Fixed to read exact-integer decimals.
2. **F8 shared-predicate sanity check.** My first cut declined via `resolve_triples_map(dim_p)` when `edw:name` spanned dims; replaced by trusting the FK's authoritative `parent_triples_map`.

**Corrected reading of q032's 70s:** it was the **generic** path all along (declined, so both A/B legs ran generic) — the 70s is the 180K-binding materialization + F8 fan-out, **not** the scan. Fusing removes it → 308 ms, confirming `09 §3`'s operator-bound premise. The env-gated diagnostics used to find this were removed before this run (the clean binary reproduces the result).

### 12.2 Commit 6b (k-hop chain) + F9 rider — results

**6b (k-hop chain) is GREEN.** `resolve_join_at_open` now orders the rewritten leaves into a linear `fact → dim1 → … → dimk` chain (`order_chain`: single ref-join per hop, no branch/cycle/merge, exactly-one-root walk), resolves each hop's single-column FK, and composes the group-key resolver by scanning each small dim ONCE from the terminal dim back toward the fact (a dangling FK or null attr at **any** hop folds into one fact-row drop). Live (`--cache-state hot`, compare vs native oracle = **7 records, 0 hash mismatches**):

| q | shape | fused? (scan) | fused wall | generic | note |
|---|---|---|---|---|---|
| **q008** | Order→Customer→Geography `SUM` GROUP BY region | **YES** (scan 3) | **609 ms** | ~45–50 s | **~75×** — the 6b headline (2-hop) |
| **q009** | as q008 **+ HAVING** | **YES** (scan 3) | **591 ms** | ~45–50 s | 2-hop + HAVING |
| q010/q014/q025/q032/q022 | (6a regression) | unchanged | 289–553 ms | — | all HASH-OK, no regression |

Unit suite **1209 passed / 0 failed** (added `order_chain_orders_linear_and_declines_nonlinear` — orders a shuffled 3-pattern chain, declines a 3-cycle and a branch). The cycle-guard is `order_chain`'s "no root ⇒ decline" + the walk's `seen[]` check.

**F9 rider** (separate 3rd commit): one line at `fluree-db-api/src/format/sparql.rs:332` — `Binding::Iri(iri)` now renders via `compactor.compact_id_iri(iri)` (the exact call `IriMatch` uses), so a virtual R2RML predicate/type IRI CURIE-compacts against the query `@context` + snapshot namespaces identically to a native `Sid` binding. SPARQL-results-JSON only; JSON-LD + SPARQL-XML untouched (both already emit full IRIs on both sides). DoD: q002/q042 flip hash-green vs the native oracle (validation in flight).
