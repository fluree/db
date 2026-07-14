# Virtual-Dataset (Iceberg / R2RML) — Findings Register (first SF01 parity run)

**Date:** 2026-07-10
**Branch:** `bench/virtual-dataset-corpus` (worktree `db-vbench`)
**Source runs:** native `results/runs/run-20260710T180017Z-01KX6JZY909RBB9MXJWHA59Z8T.jsonl`; virtual `results/runs/run-20260710T180521Z-01KX6K975N0TWK4ZG75TT430MX.jsonl` (target `virtual-sf01`, `smoke` subset, 16 queries).
**Companions:** `01-pathway-inventory.md` (strategies §N, code anchors), `02-hypothesis-map.md` (H1–H8), `03-corpus-design.md` (query lineage `qNNN`).

This is the first native-vs-virtual parity comparison on SF01. Each finding below is one row of the smoke run, classed and grounded in the exact record data (rows / result-hash equality / wall-ms both targets / pathway span counts), with a suspected mechanism anchored to the inventory and a hypothesis linkage. **Correctness findings rank above perf findings** — a wrong answer is worse than a slow one.

**Headline:** 7 of 16 smoke queries are **correct and hash-equal** to native (q001, q003, q006, q007, q011, q022, q054 — see F6). The rest split into **two silent-empty correctness bugs** (F1, F2), **one silent-divergence** (F3, root-caused), **two corpus-determinism defects** (F4, engine exonerated), and **four perf-DNFs** (F5), plus a **harness span-coverage note** (F7).

---

## 0. Evidence table (all 16 smoke queries)

`hash`: EQ = native and virtual result-hashes identical; NE = both ok but hashes differ; — = not comparable (a DNF/error side). `v_scan` = count of `r2rml.scan_table` spans on the virtual rep. `pruned/sel` = `files_pruned`/`files_selected`.

| q | native rows | virt rows | hash | native ms | virt ms | v_scan | v_load | pruned/sel | virt status | finding |
|---|---|---|---|---|---|---|---|---|---|---|
| q001 | 500 | 500 | **EQ** | 1 | 2271 | 7 | 0 | 0/0 | ok | F6 |
| q003 | 9 | 9 | **EQ** | 16 | 365 | 1 | 0 | 0/0 | ok | F6 |
| q005 | 20 | 20 | **NE** | 2 | 3707 | 12 | 0 | 0/0 | ok | **F4** |
| q006 | 3593 | 3593 | **EQ** | 16 | 2498 | 7 | 0 | 0/1 | ok | F6 |
| q007 | 10 | 10 | **EQ** | 10 | 350 | 1 | 0 | 0/0 | ok | F6 |
| q011 | 2136 | 2136 | **EQ** | 323 | 4020 | 1 | 0 | **7579/91** | ok | F6 (H4✓) |
| q022 | 3 | 3 | **EQ** | 390 | 593 | 1 | 0 | 0/0 | ok | **F6 (1.52×)** |
| q025 | 5 | 0 | — | 33 | 180000 | 1 | 1 | 0/0 | **dnf** | **F5** |
| q034 | 4514 | **0** | NE | 175 | 2 | **0** | 0 | 0/0 | ok | **F1** |
| q040 | 1100 | 0 | — | 385 | 180000 | 1 | 1 | 0/0 | **dnf** | **F5** |
| q042 | 24 | **21** | **NE** | 0 | 2991 | 9 | 0 | 0/3 | ok | **F3** |
| q049 | 5000 | 5000 | **NE** | 64 | 5085 | 14 | 0 | 0/0 | ok | **F4** |
| q050 | 30000 | **0** | — | 95 | 120000 | **377** | 6 | 0/0 | **dnf** | **F5** |
| q051 | 247 | **0** | NE | 474 | 0 | **0** | 0 | 0/0 | ok | **F2** |
| q053 | 5000 | 0 | — | 399 | 180000 | 3 | 3 | 0/0 | **dnf** | **F5** |
| q054 | 9 | 9 | **EQ** | 2 | 2567 | 8 | 0 | 0/0 | ok | F6 |

---

## F1 — Transitive property path silently returns zero rows *(correctness-silent-empty)*

**Query.** q034 `?e a edw:Employee ; edw:name ?en ; edw:manager+ ?boss` (BI-17, transitive `+` path).

**Evidence.** native **4514 rows** / 175 ms; virtual **0 rows** / 2 ms, `status=ok`, hashes NE. The virtual rep fired **no `r2rml.scan_table` span** (`spans_missing` = `[r2rml.scan_table, iceberg.scan_plan]`) — the engine returned instantly without touching Iceberg. A wrong answer delivered as success, with no error and no scan.

**Suspected mechanism.** A `PropertyPath` pattern is **not converted** to an R2RML leaf — it is preserved as-is by the rewriter `[verified rewrite.rs:162-179]` and evaluated by the generic path operator, which resolves against the graph source's (empty) native index and yields nothing. Critically, **no loud error fires**: the whole-GRAPH-scope guard `if rewrite_result.unconverted_count > 0` `[graph.rs:245-253]` counts only unconverted **top-level `Pattern::Triple`s**; a `PropertyPath` is a different pattern variant, so it never increments `unconverted_count` and the scope is not failed.

> **Refines inventory §13.** The claim "unconvertible triples fail the whole GRAPH scope" is correct *for triples*, but the guard **does not cover sub-scope pattern types** (`PropertyPath`, `Subquery`, …). Those route to generic operators over an empty index and return silently empty. §13 should be amended: the error is triple-scoped, not scope-scoped.

**Nuance (reconciles the two field reports).** *Sequence* paths (`a/b`, e.g. the CLI `edw:geography/edw:region` probe) **do** work on virtual — SPARQL lowering decomposes `a/b` into two ordinary triples, which convert normally (but run the generic join above the scans — slow, ~18.75 s, an H8 perf case). *Transitive* paths (`+`/`*`) cannot decompose into a fixed triple set, stay a `PropertyPath`, and hit this silent-empty bug. So q034 (`manager+`) is a **correctness** bug; q035 (`manager/manager`, sequence) is a **perf** case. **Action:** q034's manifest header note (currently "works but slow") is wrong and should read "transitive path returns 0 rows on virtual — silent-empty"; q035's note is correct.

**Hypothesis linkage.** H8 (non-lowered forms) — but escalated from "slow" to "silently wrong" for the transitive sub-case.

---

## F2 — Subquery scope silently returns zero rows *(correctness-silent-empty)*

**Query.** q051 `… { SELECT ?s (COUNT(?o) AS ?cnt) … } { SELECT (AVG(?c2) …) } FILTER(?cnt > ?avg)` (BI-24, stores above average order count). **Also q013** (`products_above_avg_units`, BI-08 — same subquery shape): masked at WP6 time by its own DNF (the query never completed, so the silent-empty was unobservable) and surfaced only when the perf stack made it runnable; now `expected_status: {native: ok, virtual: error}` alongside q034/q051 (`09-stacked-rebaseline.md` §4).

**Evidence.** native **247 rows** / 474 ms; virtual **0 rows** / 0 ms, `status=ok`, hashes NE, **no `r2rml.scan_table` span**. Same signature as F1: instant, scanless, silently empty.

**Suspected mechanism.** Identical to F1: `Pattern::Subquery` is preserved-as-is by the rewriter `[verified rewrite.rs:162-179]`, its inner triples are never converted to R2RML leaves, and the sub-scope escapes the top-level `unconverted_count` guard `[graph.rs:245-253]`. The generic subquery operator evaluates against the empty native index.

**Hypothesis linkage.** H8. Same root as F1 — a single fix (recurse the rewriter into `Subquery`/`PropertyPath` sub-scopes, or fail the scope when a sub-scope contains unconverted triples) closes both F1 and F2.

---

## F3 — Bound-subject wildcard drops the `rdf:type` triple *(correctness-divergence — CONFIRMED, root-caused)*

**Query.** q042 `{ <edw/store/1> ?p ?o } UNION { <edw/store/2> ?p ?o } UNION { <edw/store/3> ?p ?o }` (BI-19, three-store detail).

**Evidence.** native **24 rows** vs virtual **21 rows** — exactly **3 rows lost**, hashes NE. `files_selected=3` (the prefix-prune correctly hit one file per store). Per bound subject, **virtual emits exactly the 7 POM predicates** (`edw:channel`, `edw:geography`, `edw:name`, `edw:openDate`, `edw:regionManager`, `edw:storeId`, `edw:storeType`) and **omits `rdf:type`**; native emits **8** (those 7 + `rdf:type → edw:Store`). So each of the 3 UNION arms loses exactly its one type row: `24 = 3 × 8` → `21 = 3 × 7`. Confirmed by native `--keep-heads` (three `rdf:type → edw:Store` rows present natively).

**Root cause (confirmed).** The bound-subject wildcard scan (`<iri> ?p ?o`) **never emits the SubjectMap's `rr:class` triple**. The class is carried on the SubjectMap (`rr:class edw:Store`, `enterprise.ttl:85`), not as a POM, so the POM-iterating materialization skips it. The `predicate_var`/`type_var` binding added in **#1450 (`81b0ec601`)** covers the **subject-VAR** wildcard path (`?s ?p ?o`); the **bound-subject variant** — `new_bound_subject` → the operator's bound-subject scan on the **`a5528e880` prefix-prune path** — was not given the same class-emission, so it returns POMs only. The two wildcard paths diverged on class semantics.

**User-visible impact.** The **solo UI subject inspector** (which reads `<entity> ?p ?o`) is **missing `@type` for every virtual-dataset subject** — entity detail views silently lack the class. Systematic, deterministic, on the exact shape the prefix-prune (§7) was built for.

**Hypothesis linkage.** Not a perf hypothesis — a correctness gap in the bound-subject wildcard materialization (adjacent to §2/§7). **Fix surface is small and deterministic:** emit the `rr:class` `rdf:type` row(s) for the matching TriplesMap(s) in the bound-subject wildcard branch, honoring the same class semantics as the subject-var path (#1450).

---

## F4 — Hash mismatches with equal row counts *(corpus-defect — nondeterministic-selection; engine EXONERATED)*

Two queries return the **same row count** as native but a **different result-hash**. Both are **corpus determinism defects, not engine bugs** — resolved by head-row diff (same values, different tied/arbitrary subset). The engine is exonerated on both.

**q005** — supplier scorecard, 20/20 rows, NE. `ORDER BY DESC(?r) LIMIT 20` over many suppliers **tied at `rating = 4.99`**: each target returns a different *but equally correct* subset of the tied rows (head-row diff: identical values, different tied suppliers selected at the LIMIT boundary). A top-k over ties with no unique tiebreaker is inherently nondeterministic. **Not a float-canonicalization bug** (the earlier suspicion) — the values match; only *which* tied rows survive the cut differs.

**q049** — CONSTRUCT customer→region, 5000/5000 nodes, NE. `LIMIT 5000` over **~300K current customers with NO `ORDER BY`**: any 5000 are a valid answer, and the two engines pick different (equally correct) 5000. Not the CONSTRUCT node-serialization concern flagged earlier — it is the same unordered-LIMIT nondeterminism.

**Fix (corpus, not engine).** These drive the follow-up **corpus determinism amendment** (see `03-corpus-design.md` §5): q005 (ORDER BY + LIMIT) gets a **unique tiebreaker** appended to its sort key so hash gating stays exact and its perf shape is unchanged (still top-k over a full scan, H2 intact); q049 (pure unordered LIMIT, any-k valid) is gated on **row count + invariants** via a new manifest `hash_gate: "rows_only"` rather than an exact hash.

**Hypothesis linkage.** None (corpus hygiene). This class matters because it would otherwise mask real divergences: an unordered-LIMIT or untie-broken top-k **cannot** be hash-gated, so every such query in the corpus must be either tiebroken or `rows_only`-gated before its parity is trusted — the amendment audits all LIMIT-bearing queries for exactly this.

---

## F5 — Perf DNFs: virtual hits the deadline where native is sub-second *(perf-dnf)*

Four smoke queries **did not finish** on virtual (capped at their `timeout_s`) while native answered in ≤ 500 ms. Ranked by how damning the gap is:

| q | shape | native ms | virt | v_scan | mechanism (inventory) | H |
|---|---|---|---|---|---|---|
| **q050** | dims-only `Product ⋉ Supplier` OPTIONAL | **95** | DNF@120s | **377** | **Correlated-join rebuild.** 377 `r2rml.scan_table` spans for a *two-small-dimension* query ⇒ the parent (Supplier) dim is re-scanned per child batch; the OPTIONAL routes to an R2RML leaf (§13) whose scan is not memoized across batches (§8/§9, `operator.rs:889-897`). A dims-only query has no business issuing 377 scans. | **H3** |
| q025 | `Ticket ⋈ Product` GROUP BY HAVING | 33 | DNF@180s | 1 | Single fact scan of `FACT_SUPPORT_TICKET` + join, agg over a join declines the fused path (§11) → full materialize; 1 scan but the decode+join wall exceeds 180 s. | H6, H1, H3 |
| q040 | `VALUES ?store … ⋈ Order` | 385 | DNF@180s | 1 | VALUES not lowered (§13) ⇒ the store constraint never becomes a scan filter; full 180K-order scan + generic join. | H8, H1 |
| q053 | Customer `NOT EXISTS` Order | 399 | DNF@180s | 3 | Negation over a fact scan; the correlated NOT-EXISTS re-probes Order. | H1, H3 |

**q050 is the alarm.** It is dims-only, both tables are single-file dimensions, native does it in 95 ms — and virtual issues **377 scans** and times out. This is the cleanest existence proof of the H3 correlated-rebuild / no-cross-batch-parent-memoization cost on the whole run, and it is on the *cheapest* class of query. It should anchor the H3 line of the roadmap.

**Hypothesis linkage.** H3 (q050, q053), H1 (all), H6 (q025), H8 (q040).

---

## F6 — Positive controls: correctness parity and a tolerable ratio *(positive-control)*

- **Correctness parity (hash EQ):** q001, q003, q006, q007, q011, q022, q054 all return **hash-identical** results to native. The core dims + simple fact aggregates are **correct** on virtual — the silent-empty/divergence bugs are confined to non-lowered forms (F1/F2) and the wildcard class triple (F3).
- **q022 — fused-agg ratio existence proof:** current-customers-by-segment, virtual **593 ms** vs native **390 ms = 1.52×**, hash EQ. A single-table GROUP BY that takes the fused aggregate path (§11) achieves a *tolerable* native-to-virtual ratio — proof the pathway can be fast when the right strategy fires. This is the ratio floor the roadmap should push the rest of the corpus toward.
- **q011 — H4 date pushdown works at scale:** orders-in-Q1-2024, `files_pruned=7579 / files_selected=91` — the date-range FILTER on a physically-`date` column **pruned 98.8% of files** (inventory §6/§7, the `*_DATE` literals kept in the mapping precisely for this, `enterprise.ttl:5-6`). Hash EQ, 12× wall. This is the **positive control for H4-date** and the contrast partner for the decimal-blind case (H4, q019).

---

## F7 — `iceberg.scan_plan` span fires only on the pushdown branch *(harness-note)*

`iceberg.scan_plan` appears in `spans_missing` for almost every virtual query; it fired on only **2 of 16** (q006 `0/1`, q011 `7579/91`) — exactly the two with a pushed FILTER that produced a scan filter. So the span is **conditional on the predicate-pushdown branch**, not emitted on every scan. Two consequences for WP6:
1. The `EXPECTED_FOR_VIRTUAL` span set flags `iceberg.scan_plan` as "missing" on every non-filtered scan, which is noise, not signal — **tune the expected-span list** so `scan_plan` is only expected when a scan filter is present (or make the reader emit a `scan_plan` span unconditionally with `files_pruned=0`).
2. `files_pruned`/`files_selected` counters are therefore **only populated on filtered scans** — the H1 decode-wall analysis (bytes/rows decoded) needs the separate reader-span counter called out as a bench-gap in `02-hypothesis-map.md` (H1 confirm), which this run confirms is still missing.

---

## F8 — Cold q001 loads six tables for a single-table query *(perf-ratio / over-scan — investigate at WP7)*

**Query.** q001 `?s a edw:Store ; edw:name ?n ; edw:channel ?ch ; edw:storeType ?t` (BI-01, single-class single-table star, 500 rows).

**Evidence.** WP6 cold exec-one record (`cache_state=cold`, virtual-sf01): wall **20,760 ms**, of which **`r2rml.load_table` n=6, ~12.04 s** across six tables; `files_selected=6`; `estimated_row_count=450,000`. A query that needs **one** `DIM_STORE` scan (500 rows) loaded **six** tables and planned for 450K rows. (The warm smoke rep — §0 evidence table — shows q001 at 7 `scan_table` spans / 2271 ms; the cold penalty is the 6× `loadTable` OAuth/catalog cost, ~2 s each.)

**Suspected mechanism (candidate, not yet root-caused).** This is the **tier-2 over-scan class** the inventory §2 covers — the subject-only prune / class fusion not firing for a *star-with-column-members* shape. q001 is `?s a Store` fused with three column-POM members (name/channel/storeType); it is **not** `is_subject_only_pattern` (§2, `operator.rs:382` — that requires no predicate members), so it takes the star path. Two candidates for the 6-table fan-out: (a) the class filter is not pruning TriplesMap resolution to `DIM_STORE` (class fusion `fuse_class_if_safe`/`class_fusion_is_safe` not applied — e.g. mapping-unavailable at plan time, `rewrite.rs:572-624`), so the star resolves against multiple maps; or (b) ref-POM **parent prefetch** — `DIM_STORE` carries `edw:geography`→Geography and `edw:regionManager`→Employee ref POMs, and if the star's `build_progress` builds parent lookups for POMs beyond the queried predicates (`operator.rs:769-903`), it would scan parent dims that q001 never projects. Six ≈ Store + a fan-out of parents/maps; the `estimated_row_count=450,000` (≫ 500) points at maps/tables other than `DIM_STORE` being planned.

**Why it matters.** If 5 of the 6 `loadTable`s are dead work, cold latency on the *simplest* dimension query is ~3× worse than necessary — and this shape (typed single-dimension list) is the single most common BI-tool query. High-ROI cold-latency target.

**Hypothesis linkage.** H7 (cold/warm structure — the `loadTable` fixed cost) amplified by an over-scan (§2). **WP7 action:** confirm which 6 tables load (Jaeger/`scan_table` span names) and which of candidate (a)/(b) fires; the fix is to make the class fusion + subject-projection prune fire for the star-with-column-members shape so only `DIM_STORE` loads.

---

## F9 — Predicate/type IRIs serialize as CURIEs on native, full IRIs on virtual *(correctness-divergence — cosmetic-lexical; CONFIRMED, root-caused)*

**Queries.** q002 `SELECT ?p ?o { <edw/store/1> ?p ?o }` (bound-subject inspector, 8 rows) and q042 (three-store UNION detail, 24 rows). Both hash-MISMATCH with **equal row counts** to native.

**Evidence.** Head-row diff (`exec-one --keep-heads` vs `baselines/expected/q00N.json`): after folding the namespace, **every cell is byte-identical** — the only difference is the lexical form of IRI-valued bindings. Native compacts them to CURIEs using the query's `PREFIX edw: <http://ns.fluree.dev/edw#>` — `<edw:name>`, `<edw:channel>`, `<edw:storeId>`, `<edw:storeType>`, `<edw:openDate>`, `<edw:geography>`, `<edw:regionManager>`, and the `rdf:type` object `<edw:Store>`. Virtual (R2RML) emits the **full IRI** — `<http://ns.fluree.dev/edw#name>`, …, `<http://ns.fluree.dev/edw#Store>`. The literal object values (`2023-06-27`, `Retail`, `Store 1`, `STORE-00001`, `Warehouse`) and reference IRIs (`employee/604`, `geography/3603`) are identical on both sides. It is exclusively the **projected predicate `?p`** and the **`rdf:type` object** — i.e. IRIs the engine draws from the vocabulary — that diverge, which is why every corpus query projecting only literal *object* values (q001, q022, q054) is hash-OK.

**Root cause (SHARPENED 2026-07-14 — a NAMESPACE-MAP gap, not a formatter-dispatch gap).** The PR-6 attempt (route `Binding::Iri` through `IriCompactor::compact_id_iri` — the call `IriMatch` already uses) was implemented and **did NOT flip q002/q042**: virtual still renders full IRIs (`<http://ns.fluree.dev/edw#name>`) vs native's `<edw:name>`. The real divergence is upstream of the formatter: `IriCompactor` compacts against the **snapshot's namespace map**, and the **virtual graph-source snapshot never registers the R2RML mapping's vocabulary namespaces** (`edw:` …), so the compactor has nothing to compact against; a native ledger gets `edw:` from its **ingested data**, which is why native compacts and virtual does not. (Note: native compacts from **ledger** namespaces, **not** the query `@context` — so "compact against the query prefixes" is the wrong mechanism; parity means matching native's mechanism, not just the output string.)

**PR-F9 scope (reframed, tail queue).** Register the R2RML mapping's own `@prefix` declarations (`enterprise.ttl` carries `edw:`) into the virtual snapshot's namespace map at graph-source resolution (likely `catalog_session` / compile time). Then the shared formatter compacts virtual IRIs naturally against those namespaces — **possibly with zero formatter change**. The `Binding::Iri → compact_id_iri` one-liner was **reverted from PR-6** as a no-op (its DoD, the q002/q042 flip, was not met). AJ's parity direction stands: virtual aligns to native CURIE-compaction.

> **⚠️ CORRECTION (2026-07-14) — the "SHARPENED" root cause above and the namespace-map scope are REFUTED.** Full analysis: [`16-pr-f9-curie-alignment.md`](16-pr-f9-curie-alignment.md). The vbench hash is `sparql_json`; compaction there is done by `ContextCompactor::compact_id` (`fluree-graph-json-ld/src/compact.rs:60`), which keys **exclusively on the parsed query `@context`/`PREFIX`** and **never reads the namespace-code map**. So the "compact against the snapshot namespace map / native compacts from ingested data" claim is wrong — proven by this entry's OWN evidence: `rdf:type` (reserved code 3) and the reference IRIs `http://data.fluree.dev/edw/employee/604` are BOTH in a native ledger's namespace map yet render FULL, because the query declares no prefix for them; `edw#name` compacts ONLY because the query declares `PREFIX edw:`. Existing unit tests confirm it (`format::iri::tests::test_compact_id_does_not_apply_vocab`; `fluree-graph-json-ld compact::tests`). The real gap is a **formatter dispatch**: native predicates are `Binding::Sid` → `compact_id_sid → compact_id_iri` (compacts); virtual predicates are `Binding::Iri`, written **raw** at `format/sparql.rs:333` (streaming) and `:487` (DOM). The "PR-6 one-liner no-op" is explained: vbench hashes the streaming path; a fix touching only one arm no-ops there. **Fix = route `Binding::Iri` through `compact_id_iri`, but SCOPED to graph-source origin** — because native-reachable `Binding::Iri` sites exist (`BIND(IRI)`/`SERVICE`/`GRAPH ?g`/BM25/vector — see F16), an unconditional formatter change would alter native output (AJ's deferred question, filed as **fluree/db#1496**). Chosen vehicle: a query-level graph-source flag (`GraphDb.graph_source_id`). Namespace-map seeding (old "Option B") is dead — it cannot move the hash.

**Why it surfaced now (relationship to F3).** F3 was the *row-count* half of this shape (virtual dropped the `rdf:type` row entirely: q042 21 vs 24, q002 7 vs 8). PR-0's bound-subject `rr:class` emission (`4a878e33d`/#1476) fixed the count (q042→24, q002→8) — which **unmasked** this value-level serialization divergence that the count mismatch had been hiding. So F9 is the residual of F3 after the count was repaired.

**Not PR-3.** Confirmed via the `FLUREE_R2RML_STAR_TM_PRUNE` discriminator (2026-07-13): both q002 and q042 mismatch **identically with PR-3's star pruning ON and OFF**. Mechanically expected — q002 is a bound-subject wildcard (no star members, no class), so PR-3's fix (a)/(b') never engage.

**Product decision (escalated to AJ, not decided here).** SPARQL 1.1 Query Results JSON serializes an `iri` binding as its **full string value** (`{"type":"uri","value":"http://…"}`) — there is no CURIE form in the spec — so on a strict reading the **virtual** side conforms and the **native** CURIE-compaction is the non-conforming one. But changing native output to full IRIs would change results every existing native user already depends on. Which side changes (or whether the harness canonicalizer namespace-folds both before hashing) is a **product/compat decision for AJ**, not an engine bug to "fix" unilaterally. **Do NOT** allowlist/mask q002/q042 in the harness — they stay honestly red until parity is decided.

**Record-integrity note (low priority).** q042 was recorded as "hash-parity restored" in the PR-0 era, yet today it mismatches at **both** switch states. Either the PR-0-era bless predated the `?p`-projection serialization path, or a PR-1/PR-2-era formatter change altered it — both pre-existing to PR-3. A bisect against an older binary would settle it; offered but **not run** (out of PR-3 scope, low priority).

---

## F10 — Split-member same-subject star yields zero rows under vertical partitioning *(correctness-silent-empty; PRE-EXISTING, recorded 2026-07-13 from the PR-3 review)*

**Shape.** A same-subject star whose required members are split across TriplesMaps sharing a subject template — the data-data analog of PR-3's (b') vertical-partition counterexample (ROADMAP §PR-3): `TM_A` (subj `store:{k}`, carries `edw:name`) + `TM_B` (subj `store:{k}`, carries `edw:channel`); query `?s edw:name ?n ; edw:channel ?ch`. No corpus query exercises it (the SF01 emitter mapping is one-TriplesMap-per-table with no shared subject template across data TMs), so it is synthetic today — but hand-written multi-TM-per-subject mappings hit it directly.

**Behavior.** Virtual returns **zero rows** as a silent success; native answers the same data via a subject join. Mechanism: the rewrite fuses same-subject members into one star **unconditionally** (`rewrite.rs` star loop) and materialization is per-map — no cross-map member join (`operator.rs` `tm_passes_star_prune` doc) — so when no single map supplies every member, no map produces a complete star row.

**Pre-existing, and preserved (not caused) by PR-3.** Pre-PR-3, base-predicate resolution scanned every base-predicate-bearing map, but each map still lacked some member ⇒ zero star rows. PR-3's intersection prune (fix (a)) empties the resolution set instead ⇒ identically zero rows. The prune is provably result-preserving *given* this formation behavior; the gap lives in star **formation**, not resolution. (Independently re-derived by the PR-3 reviewer; recorded here so it is a registered residual rather than folklore.)

**Fix owner (future, rewrite — not PR-3).** Refuse to fuse a star when no TriplesMap covers all members, falling back to per-member scans joined on subject (the always-correct pre-fusion path). Once formation refuses that shape, fix (a)'s "provably empty" argument holds unconditionally rather than relative to formation behavior.

---

## F11 — Ad-hoc `run --out` JSONL parsing must skip the `meta` header *(harness-note; recorded during PR-4 as F10, renumbered F11 at the PR-3 restack — F10 was taken by the star-formation gap above)*

`vbench run --out <file>` writes a `{"kind":"meta",...}` header as **line 1**, then one RunRecord per query. An ad-hoc single-value extraction like `jq -r .result_hash "$file"` therefore returns **two lines** (the meta record's `null` + the real hash), and a string compare against the single blessed hash **always fails** — a false MISMATCH. This produced a spurious "q008 hash mismatch" alarm during PR-4 (chased as a suspected F9/float-order divergence before it was traced to the header). `exec-one` emits a single record with no meta line, so it is unaffected, and the real `vbench compare` gate reads per-record and is unaffected. **Rule for any ad-hoc `run --out` parsing:** filter to real records first — `jq 'select(.query_id)'` (or iterate per-record and skip null `query_id`). q008 itself is deterministically correct (hash-stable, byte-identical to the native oracle across four clean runs); there is no q008 divergence.

---

## F12 — Single-table fused aggregate mishandles an un-annotated string GROUP BY key under a constant-object star constraint *(latent — unreachable today; DO NOT extend the string default to the single-table path without fixing this first)*

**Discovery.** During PR-6 6a, a first cut applied the "un-annotated column key → `xsd:string`" default (the R2RML natural mapping) to **both** the join and the single-table group-key resolution in `fused_aggregate.rs`. That regressed the **q022 sentinel** (`SELECT ?seg (COUNT(?c) AS ?n) WHERE { ?c a Customer ; edw:isCurrent true ; edw:segment ?seg } GROUP BY ?seg`, hash-MISMATCH), while the same default on the **join** path was hash-correct on q032 (`DimStore.STORE_NAME`) and q025 (`DimProduct.CATEGORY`). So `xsd:string` is provably the *correct* datatype for these un-annotated string columns (q032/q025 pass); the q022 mismatch is therefore **single-table-fold-shape-specific, not a datatype error**.

**Suspected mechanism (not yet root-caused — deferred).** q022 is the first shape to combine, on the *single-table* fused path, an **un-annotated string GROUP BY key** (`edw:segment` → `rr:column "SEGMENT"`, no `rr:datatype`) with a **constant-object star constraint** (`edw:isCurrent true` → a `star_constraint`). The single-table fold appears to mishandle that combination (wrong row set or wrong key binding) — it had simply never been exercised, because `group_kind(None)` declined every un-annotated key before PR-6.

**Why it is unreachable today.** PR-6 leaves the single-table path **byte-identical**: an un-annotated key still hits `group_kind(None)` → `Ok(None)` → the generic pipeline (correct). The `None → xsd:string` default is scoped to `resolve_join_at_open` only. So the bug is masked by the datatype decline and cannot fire.

**Constraint for the future.** Anyone extending the un-annotated-string default (or otherwise enabling an un-annotated string key) to the **single-table** fused path MUST first root-cause and fix this shape (un-annotated string key + `star_constraints`), gated by re-running the q022 sentinel for byte-identical parity. Add a fixture with a constant-object constraint + a string key with no `rr:datatype`.

**Hypothesis linkage.** None (a fused-operator correctness gap, adjacent to the PR-6 join fold; surfaced, contained, and documented rather than fixed — out of PR-6 scope).

---

## F15 — Latent NaN over-prune in the Float bounds compare *(latent — unreachable pre-PR-7; armed AND fixed in-PR)*

**Discovery.** While widening pruning to double/decimal (PR-7, H4), the recon found that `bounds_can_contain` reasons via `TypedValue::lt`/`le` (NOT `partial_cmp`), and the `Float32`/`Float64` arms used a raw `<`/`<=`. A NaN operand therefore yielded `Some(false)`, which in `bounds_can_contain` can PRUNE: e.g. `column >= v` against a row group whose upper bound is NaN evaluates `lit.le(NaN) = Some(false)` → `false` → prune, even though NaN rows exist. That is an **over-prune** — a strict-superset violation (the pushdown may only ever over-KEEP; the in-engine FILTER is the sole authority).

**Why it was unreachable pre-PR-7.** No float/double predicate ever reached the Iceberg reader: `to_scan_value` returned `None` for `Double`/`Decimal`, so `build_iceberg_filter` never emitted a `LiteralValue::Float64`, so `stat_bounds` never produced a `Float*` bound and the raw-`<` arms were never exercised. PR-7's `ScanValue::Double` push is exactly what ARMS the hazard — so its fix must ship in the same PR (the F12 pattern: a latent hazard gets a register entry, not just a silent fix).

**Fix (in-PR).** The `Float32`/`Float64` arms of `lt`/`le` now return `None` when either operand is NaN (`(!a.is_nan() && !b.is_nan()).then(|| a < b)`), so `bounds_can_contain`'s `unwrap_or(true)` KEEPS the group. `±0.0` collapses to one bound (`-0.0 == +0.0`, neither `<` the other) with no ordering hazard. Guarded by `bounds_can_contain_keeps_on_nan_bound` (pruning.rs) + `nan_float_compare_is_incomparable` (value_codec.rs).

**Hypothesis linkage.** H4 (numeric pruning). Same "latent hazard armed by the widening → register entry + in-PR fix" shape as F12.

---

## F16 — Non-SPARQL-JSON formatters render virtual graph-source IRIs raw (CURIE-alignment follow-up) *(consistency — follow-up, not in PR-F9)*

**Context.** PR-F9 aligns the **`sparql_json`** formatter so virtual (graph-source) `Binding::Iri` predicate/type IRIs CURIE-compact like native, scoped to graph-source origin via the query-level `GraphDb.graph_source_id` flag (routing `Binding::Iri → compact_id_iri` at `format/sparql.rs:333` streaming + `:487` DOM). It fixes q002/q042.

**The follow-up.** The **same raw-`Binding::Iri` arms** exist in the other output formatters, so virtual results in those formats still render **full IRIs** (no CURIE alignment): `format/jsonld.rs:206,409`, `format/sparql_xml.rs:212`, `format/typed.rs:189,389`, `format/delimited.rs:382`. Extending the graph-source compaction to these is a **consistency** change that needs its **own per-format test gates** — the vbench corpus only exercises `sparql_json` (and `jsonld` for CONSTRUCT/DESCRIBE), so these arms are un-gated by the current suite. Deferred out of PR-F9 to keep that PR corpus-gated and native byte-identical.

**Related native-side question (separate, product decision).** The provenance-dependent rendering this all stems from — native compacts STORED IRIs (`Binding::Sid`) but renders CONSTRUCTED/FEDERATED/GRAPH/search IRIs (`Binding::Iri`) raw, even when a prefix matches — is filed for the team as **fluree/db#1496**. Five native-reachable `Binding::Iri` sites: `eval/value.rs:522` (`BIND(IRI)`/`UUID`), `sparql_results.rs:84,88` (SERVICE), `graph.rs:343,440` (`GRAPH ?g`), `bm25/operator.rs:615`, `vector/operator.rs:350`. If PR-F9 lands the **query-level** flag (view-scoped, not per-binding), a virtual query's OWN `BIND(IRI)`/`SERVICE`/`GRAPH` IRIs would also compact (differing from native's raw) — the same provenance question, virtual-side; tracked under #1496.

**Fix owner.** engine (formatter consistency) — deferred; gated by its own per-format tests. Full analysis: [`16-pr-f9-curie-alignment.md`](16-pr-f9-curie-alignment.md).

---

## F17 — q029 scan-bound / re-scan-amplified: UNION absorbs the LIMIT budget *(perf-residual — known since PR-2; the F9 gate's deadline exposed it)*

**Shape.** q029 = `UNION(WebEvent eventType="purchase", ="add_to_cart") LIMIT 100`, `hash_gate=rows_only`. Completes ~142s at PR-2 (`09-stacked-rebaseline.md` §1/§3), ~150s on the 2026-07-14 healthy-net rerun (within variance). Manifest `timeout_s = 180`.

**Mechanism (`09-stacked-rebaseline.md` §3).** `scan_n = 253`, `pqrd_n = 1,940,510` — **1.94M file reads for a 100-row LIMIT**. The UNION **absorbs the LIMIT budget** (hypothesis H2): the two branch scans of `FACT_WEB_EVENT` are **re-driven ~253×** (files_selected 15,340 ≈ 2×7,670; reads ≈ 253× that). The `eventType` string-equality does not prune (WebEvent isn't partitioned by it). Warm-cheap when the fact files are cached, so the ~142–150s is the cold/eviction path.

**Remedy — BUDGET PROPAGATION THROUGH UNION** (roadmap mechanism-class **D**, never shipped — PR-5 shipped only the ORDER BY top-k leg for q012/q046). Mirror PR-5's wrapper directive-forwarding pattern (`graph.rs` threads the budget into inner subplans; `project.rs`/offset forward to children): a `UnionOperator` forwards each branch the LIMIT/row budget so a branch can stop at k rather than scan the whole fact table. **Overlap:** the ~253× re-drives are the same batched-re-scan family as **F14 / PR-4d** (per-window inner re-scan), so PR-4d may fix most of q029 independently — present the UNION-budget PR as a candidate **alongside** PR-4d, AJ's call.

**Harness fix.** The PR-F9 gate applied a **blanket 120s deadline** < q029's manifest `timeout_s = 180`, producing a spurious DNF (see the gate observation above). Gate scripts must forward each query's manifest `timeout_s` (vbench `run`/`exec-one` accept `--timeout-s`; `run --timeout-s` already forwards its cold override per-query) instead of one blanket `--timeout-s 120`. Fixed in `pf9_gate.sh` going forward.

**Fix owner.** engine (UNION budget forwarding — candidate, AJ's call; likely subsumed by PR-4d) + harness (per-query manifest timeout).

---

## F18 — q031 parent-memo does not engage on the LIMIT-pushdown driving path *(perf — PRE-EXISTING, cache-sensitive; NOT a regression)*

**Shape.** q031 = `?inv a InventorySnapshot ; edw:onHandQty ?oh ; edw:reorderPoint ?rp ; edw:product ?p . ?p edw:name ?pn . FILTER(?oh<?rp) LIMIT 5000`. Clean north-star re-baseline (#1499 head): **72.1 s** cache-thrashed. `scan_table.n = 1448`, `load_table.n = 7` (~21 s), `files_selected = 0` (dim `loadTable` lookups, no scan_plan).

**Mechanism.** PR-8b's query-scoped parent-memo (`R2rmlParentMemo`, `r2rml/operator.rs:77`; unit tests `pr8b::parent_lookup_survives_operator_rebuild` PASS) is **not reached** by q031's plan → the DimProduct RefObjectMap parent lookup rebuilds per driving batch (~1448 un-memoized re-scans; the pre-PR-8b 1306 shape). The seam is the **LIMIT-pushdown driving path**: `FLUREE_R2RML_LIMIT_PUSHDOWN=0` → `scan_table 1448→8` (memo engages) **but 88 s** (LIMIT no longer cuts the fact scan). So it's a memo × limit-pushdown interaction: neither current setting gives both.

**NOT a regression (forensics).** Two-point bisect: A @ `2a07bbbc4` (PR-7 head, pre-PR-5) and B @ `b1cb988c2` (PR-4c head, pre-PR-7) **both = 1448 / ~70 s**. PR-8b's OWN gate `pf8b_virtual.jsonl` recorded q031 at **65 s / scan_table 1448** — identical to now. Seven-switch A/B refutes every candidate (BATCHED_OPTIONAL(_STAR)=PR-4c, PARENT_MEMO, NUMERIC_STATS=PR-7, TOPK, STAR_TM_PRUNE, FILTER_CONSUMPTION, SCAN_CACHE). No q031 corpus drift in `118a25587..HEAD`. So the 1448-scan plan is **unchanged since PR-8b**; PR-4c/PR-5/PR-7/PR-8b all exonerated. **Cache-sensitive:** 188 ms fully-warm ↔ 65–72 s cache-thrashed — the "188 ms PR-8b sentinel" was a fully-warm reps=3 artifact (q029, 125 s cold, ran **156 ms** in that same phase; whole corpus sub-second, zero I/O).

**Remedy.** Make the query-scoped parent-memo engage on the LIMIT-pushdown batched driving path — get BOTH the memo (8 scans, not 1448) AND the LIMIT cut on the fact scan. **Expected end-state: low-single-digit s cache-thrashed / sub-second warm** (partial LIMIT-cut fact scan + one memoized dim load; meets the ≤~3 s north-star bar — NOT the 0.2 s warm-only artifact). The differential MUST include a **q031-shaped hermetic case** (N driving batches under LIMIT pushdown ⇒ 1 parent scan) so this interaction cannot silently regress.

**Gate-protocol lesson.** The 188 ms sentinel was warm-cache, hiding the 65 s cold cost in the SAME gate (~350× optimism) → sentinel numbers must be cache-thrashed (or report warm AND cold); consider a full-corpus baseline per stacked-PR head (q031 wasn't in PR-5's sentinel set; only the full corpus caught it).

**Fix owner.** engine (parent-memo × limit-pushdown) — pre-existing perf, north-star slate.

**REFRAMED (2026-07-14, F18 sketch trace).** F18's "memo × limit-pushdown" premise is wrong for the WALL — see the F18 sketch (`17-pr-f18-q031-memo-limit.md`) and F18-note below. q031's 72 s is **cold-floor / cache-residency** (`load_table` 21.2 s cold + cold reads + materialize; 188 ms fully warm; 72 s budget ≈ 76 s no-budget so the dim re-scans are near-free). F18 is re-scoped to a **cold-floor design-scope** (the PR-8 tail: persistent loadTable/creds pin + fact-residency), NOT a memo/limit PR. The memo non-engagement is split out as **F19**.

**SUPERSEDED AGAIN → F20 (2026-07-14, measurement; the FOURTH flip — kept as a case study).** The cold-floor "loadTable pin LEAK" reading is ALSO wrong. Measurement (fresh-cache q031 run + disk `metadata.json` snapshot summaries; `18-pr8tail-coldfloor-loadtable-pin.md` MEASUREMENT ADDENDUM): q031 loads **7 DISTINCT tables, each exactly ONCE** — the per-query pin held across the whole 72–115 s query. No leak; no residency issue (FACT_INVENTORY_SNAPSHOT is 51 MB). The 21.2 s `load_table` is a **RefObjectMap-target resolution FAN-OUT** — `?p edw:name ?pn` resolves against all 6 `edw:name`-bearing dims (the 6 loaded dims match the mapping exactly) because the `edw:product` RefObjectMap parent (DIM_PRODUCT) is not propagated to constrain it. **q031's real fix is F20 (resolution-prune), not a memo, not a pin, not residency.** The verdict-flip chain — (1) suspected-PR-5-regression → bisect-refuted → (2) memo × limit-pushdown → A/B-refuted → (3) cold-floor loadTable-pin/residency → measurement-refuted → (4) **RefObjectMap-target resolution fan-out (F20)** — is preserved deliberately: it is the audit's clearest "measure before you build" case study.

---

## F19 — PR-8b query-scoped parent-memo silently does NOT engage across correlated sub-executions *(latent correctness/efficiency; wall-neutral on the current corpus)*

**Symptom.** For q031 (correlated inner-join to DIM_PRODUCT + non-pushable `FILTER(?oh<?rp)` + LIMIT), `FLUREE_R2RML_PARENT_MEMO` on/off is IDENTICAL (1448 scan_table either way). The PR-8b query-scoped memo (`R2rmlParentMemo` on `ExecutionContext.r2rml_parent_memo`; consult `operator.rs:1135`, publish `:1242`; unit test `pr8b::parent_lookup_survives_operator_rebuild` PASSES in isolation) **never hits in production** across the ~1447 per-driving-batch `build_progress` invocations. The insert gate (`:1223`, `lookup.len() ≤ 512K window`) passes (DIM_PRODUCT ≪ 512K), so the memo IS published on batch 1 — yet consult misses on every later batch.

**Suspected cause (exact site TBD — the F19 fix's first step).** The memo lives on `ExecutionContext`; `ctx.r2rml_parent_memo` is CLONED (Arc-shared) by the GRAPH-scoped derivations (`context.rs` with_active_graph/with_default_graph/with_graph_ref, 1103/1161) but re-created FRESH (`::default()`) by the base constructors (`new`/`with_time`/`with_overlay`/`with_time_and_overlay`, 337/392/451/499/546/595). If the correlated inner-join driver derives the inner scan's context via a fresh-memo path (or otherwise doesn't share the parent's memo Arc), the "query-scoped" memo is populated-and-discarded per driving batch → never reused. Confirm which derivation the correlated non-GRAPH inner join uses; remedy = clone the parent's `r2rml_parent_memo` into it.

**Wall impact — NONE on the current corpus.** q031's wall is cold-floor-dominated (F18), so fixing the memo is wall-neutral there (the dim re-scans are near-free: DIM_PRODUCT is one cached file, `load_table` shared at 7). No other current corpus tail query loses the WALL to this: q028 (the only sibling correlated dim-join) has `scan_table.n=13` (few driving batches, no re-scan blowup) at 3.9 s. So F19 is a **latent** gap — it would bite a future/other query with a correlated inner-join to a dim whose re-scan is NOT near-free (a larger dim, or a dim needing a cold `loadTable` per re-scan). It also means PR-8b's query-scoped memo has been effectively inert in production since it shipped.

**Remedy.** Confirm the correlated inner-join context-derivation site; share (clone) `r2rml_parent_memo`; add a q031-shaped hermetic test (N correlated driving batches ⇒ 1 parent scan WITH a shared query ctx, mirroring `pr8b::parent_lookup_survives_operator_rebuild` but through the real correlated driver, not fresh operators against one hand-built ctx). Small, isolated — do NOT fold into the cold-floor PR (no scope mixing).

**Fix owner.** engine (context memo-derivation) — standalone, low priority (latent).

---

## F20 — RefObjectMap target class not propagated to constrain downstream shared-predicate resolution *(perf — over-scan; F8/PR-3 family, joined-dim-attribute variant; the real q031 root)*

**Shape.** q031 = `?inv a InventorySnapshot ; edw:onHandQty ?oh ; edw:reorderPoint ?rp ; edw:product ?p . ?p edw:name ?pn . FILTER(?oh<?rp) LIMIT 5000`. The second triple `?p edw:name ?pn` is a **variable-subject, single shared-base-predicate** pattern.

**Evidence (measurement, 2026-07-14).** A fresh-catalog-cache q031 run loaded **7 DISTINCT tables, each exactly once**: `FACT_INVENTORY_SNAPSHOT` + `DIM_ACCOUNT, DIM_CUSTOMER, DIM_EMPLOYEE, DIM_PRODUCT, DIM_STORE, DIM_SUPPLIER` (`load_table.n = 7`, wall 114.8 s cold). The 6 dims are **exactly** the 6 that map `edw:name` (verified against `enterprise-sf01-mapping.ttl`: SUPPLIER_NAME / ACCOUNT_NAME / FULL_NAME(emp) / STORE_NAME / FULL_NAME(cust) / PRODUCT_NAME). So the per-query pin is NOT leaking (each table once, held across 72–115 s > the 60 s TTL — `cache.rs:37`'s contract holds); the 21.2 s of `load_table` is a **resolution fan-out**.

**Mechanism.** TriplesMap resolution is base-predicate-driven (`rewrite.rs:690` "the base predicate drives TriplesMap selection"). `edw:name` resolves to **every** map bearing it. The `class_fusion` machinery (`fuse_class_if_safe` / `class_fusion_is_safe`, `rewrite.rs:661-739`) constrains resolution to a class ONLY when a `?x a Class` co-locates with the predicate — but q031's `?p edw:name` carries **no class assertion**; `?p` is bound by `edw:product`, a **RefObjectMap whose parent TriplesMap is provably DIM_PRODUCT**. That target is **not propagated** to constrain the `?p edw:name` resolution, so it fans out to all 6 name-dims. (The 1447 `scan_table` re-scans previously attributed to "DIM_PRODUCT" in F18/F19 are ≈ `241 driving batches × 6 fan-out dims` — the fan-out drives them too, not a single dim.)

**Prior art.** `ExecutionContext::trust_fk_refs` (`execute/runner.rs:687`, `context.rs:154`, `r2rml/operator.rs:1042`, `rewrite.rs:99`) already trusts a child-templated RefObjectMap's target to render the parent IRI **without a parent scan** — but it is **crawl-path-only** (browse "list instances"). F20 generalizes the same "trust the RefObjectMap target" idea to the **query path's resolution** (constrain which maps resolve `?p <pred>`), a distinct but adjacent lever ([[fk-templated-ref-fusion]]).

**Remedy (F20 = the q031 PR).** Propagate the RefObjectMap parent (DIM_PRODUCT, from `?p ← edw:product`) to constrain the resolution of downstream patterns on `?p` → `load_table.n` **7 → 2**, killing ~15 s of dead dim loads (incl. 390 K-row DIM_CUSTOMER) and collapsing the fan-out re-scans. **SOUNDNESS INVARIANT (load-bearing):** the prune may constrain `?p <pred>`'s resolution to the ref target ONLY when **EVERY binding source of `?p` in the query scope is provably that one RefObjectMap** — if `?p` can also bind from a UNION branch, another triple pattern, a `VALUES`, or a second RefObjectMap with a **different** parent, the prune MUST DECLINE and fall back to the full fan-out. Standalone PR with its **own kill switch** (new soundness surface — do not overload `STAR_TM_PRUNE`). Full design: `20-pr-q031-refprune.md`.

**Expected wall (arithmetic).** Post-prune residual = one 7,670-file `FACT_INVENTORY_SNAPSHOT` decode (the un-prunable `FILTER(?oh<?rp)` blocks any LIMIT cut) + one DIM_PRODUCT (1-file) join + LIMIT-5000 materialize. Empirical reference from the SAME baseline: a single full 7,670-file fact scan is **~1 s** (q018 1.06 s / 7,670 files / 200 K rows; q044 0.96 s / 7,670 files / 250 K rows). So q031 is expected to land **low-single-digit s cache-thrashed WITHOUT PR-2a** — confirmed at the gate; PR-2a (the decode-wall) opens only if the gate misses.

**Fix owner.** engine (rewrite: RefObjectMap-target resolution constraint) — north-star slate, **the q031 PR** (supersedes the F18 cold-floor framing).

---

## Summary & routing

| Finding | Class | Query | Root | Fix owner |
|---|---|---|---|---|
| **F1** | correctness-silent-empty | q034 | `PropertyPath` (transitive) not converted; sub-scope escapes GRAPH-error guard | engine (rewrite/graph) |
| **F2** | correctness-silent-empty | q051, q013 (DNF-masked until the perf stack) | `Subquery` not converted; same guard gap | engine (same fix as F1) |
| **F3** | correctness-divergence | q042 | wildcard omits `rr:class` `rdf:type` triple | engine (R2RML materialize) |
| **F4** | corpus-defect (nondeterministic-selection) | q005, q049 | untie-broken top-k over rating ties (q005); unordered `LIMIT` over 300K (q049) — **engine exonerated** | corpus (determinism amendment §5) |
| **F5** | perf-dnf | q050, q025, q040, q053 | H3 correlated rebuild (q050 377 scans), H1/H6/H8 | roadmap (WP8) |
| **F6** | positive-control | q001/03/06/07/11/22/54 | correctness parity; fused-agg 1.52×; H4-date prunes 98.8% | — |
| **F7** | harness-note | (all) | `scan_plan` conditional on pushdown branch | harness (WP6 span tuning) |
| **F8** | perf-ratio / over-scan | q001 (cold) | 6 `loadTable`s for a 1-table query — class fusion / subject-prune not firing on star-with-column-members (§2); or ref-POM parent prefetch | WP7 investigate → engine (FIXED by PR-3: 6→1) |
| **F9** | correctness-divergence (cosmetic-lexical) | q002, q042 | predicate/type IRIs render as CURIEs on native, full IRIs on virtual — data identical after namespace-folding; residual of F3 after PR-0 fixed the row count. **Root cause CORRECTED 2026-07-14** (see F9 addendum): `@context`/PREFIX-driven `compact_id`, NOT a namespace-map gap; the divergence is the raw `Binding::Iri` formatter arm. | engine — PR-F9 (graph-source-scoped formatter fix); native-side provenance question → fluree/db#1496 |
| **F10** | correctness-silent-empty (pre-existing) | (synthetic — no corpus query) | same-subject star members split across template-sharing TMs → zero star rows; formation fuses unconditionally, materialization is per-map | engine (rewrite: refuse to fuse when no TM covers all members) — future, not PR-3 |
| **F11** | harness-note | (any `run --out` parse) | ad-hoc `jq .result_hash` reads the `kind:"meta"` header line → false MISMATCH; filter `jq 'select(.query_id)'`. q008 is actually deterministically correct | harness (parsing hygiene) |
| **F12** | correctness-latent (unreachable today) | q022 | single-table fused agg mishandles an un-annotated string GROUP BY key + a constant-object `star_constraint`; masked by the `group_kind(None)` decline. **Must fix before extending the string default to single-table** | engine (deferred — not PR-6) |
| **F13** | harness-note / baseline-drift | q034, q050 (q009/q010 class) | native micro-query blessed baselines drift with machine state → recurring false `SLOW` alarms. PR-8b: q034 1.84×/q050 2.98× confirmed at 5 reps, then a base A/B (`c4a9b799e`, no PR-8b) reproduced 1.83×/2.93× — i.e. the ratio pre-dates the change. Chronic (twice now). **Re-bless these micro-query baselines on a quiet machine before the next gating cycle.** | harness (baseline re-bless) |
| **F14** | perf-residual (post-PR-4b/4c) | q050, q016 | the batched R2RML OPTIONAL hash-left-join drives the seed in WINDOWS and **re-scans the main (inner) table per window** — it was never scan-once. Attributed on q016 (PR-4c): 182 `scan_table` = 180 FACT_SHIPMENT (inner, per-window) + 2 FACT_ORDER (outer, collapsed); q050 (PR-4b, shipped): 92 scans. So it flips DNF→ok (q016 39s hot, q050 9.3s) but not to seconds. Fix class: **consume the WHOLE seed in one inner scan + in-memory hash-join** (IN-set/probe extension — the real successor to `07` open-Q2); prize q016 ~39s→seconds, q050 ~9.3s→~1s. **PR-4d candidate — not a blocker** (uniform with shipped PR-4b). | engine (batched-OPTIONAL seed-windowing) |
| **F15** | correctness-latent (armed + fixed in-PR by PR-7) | (synthetic — NaN float bounds) | `TypedValue::lt`/`le` `Float` arms used a raw `<`, so a NaN bound returns `Some(false)` → `bounds_can_contain` could over-prune a row group holding NaN rows (strict-superset violation). **Unreachable until PR-7's `ScanValue::Double` push produces a float bound.** Fixed in-PR: NaN operand → `None` → keep; guarded by two unit tests. | engine (fixed in PR-7) |
| **F16** | consistency (follow-up, not in PR-F9) | (virtual, non-`sparql_json` formats) | the other output formatters render virtual graph-source `Binding::Iri` raw (no CURIE alignment): `jsonld.rs:206,409`, `sparql_xml.rs:212`, `typed.rs:189,389`, `delimited.rs:382`; needs own per-format test gates (corpus only covers `sparql_json`). Related native provenance-rendering question → **fluree/db#1496**. | engine (formatter consistency) — deferred |
| **F17** | perf-residual (known since PR-2) | q029 | `UNION(purchase, add_to_cart) LIMIT 100` re-drives both `FACT_WEB_EVENT` branch scans ~253× → **1.94M file reads for 100 rows**; the UNION absorbs the LIMIT budget (H2). ~142–150s (manifest `timeout_s=180`); the F9 gate's blanket 120s deadline caused a spurious DNF. Remedy: **budget propagation through UNION** (mechanism-class D, never shipped; mirror PR-5 wrapper-forwarding) — likely subsumed by F14/PR-4d. Harness: honor per-query `timeout_s`. | engine (UNION budget — AJ's call) + harness (per-query timeout) |
| **F18** | perf (**pre-existing, NOT a regression**) — **root SUPERSEDED → F20** | q031 | **RE-SCOPED 2026-07-14 → COLD-FLOOR, then SUPERSEDED AGAIN → F20 (resolution fan-out).** The cold-floor row below is the (refuted) third reading; q031's real fix is F20. Kept for the flip-chain case study. Historical: 72.1s cache-thrashed = cold-residency, NOT memo/limit: budget-on 72s ≈ budget-off 76s so the 1448 dim re-scans are near-free; the wall is `load_table` 21.2s cold (7× catalog OAuth+manifest) + cold parquet reads + ~18s materialize (188ms fully warm). Cache-sensitive (188ms warm ↔ 65–72s thrashed); ALL PRs exonerated (bisect A@2a07bbbc4 + B@b1cb988c2 both 1448/~70s; PR-8b gate recorded 65s/1448); the 188ms "sentinel" was warm-cache (~350× optimism). Remedy: persistent loadTable/creds pin (beyond the 60s moka TTL) + fact-residency under corpus pressure — see `17-pr-f18-q031-memo-limit.md`. The memo non-engagement splits out as **F19**. | engine (cold-floor: loadTable pin + residency) — north-star slate |
| **F19** | latent correctness/efficiency (wall-neutral today) | q031 (latent; no current corpus query loses the wall) | PR-8b query-scoped parent-memo (`R2rmlParentMemo` on `ExecutionContext`) does NOT engage across correlated sub-executions: PARENT_MEMO on/off ≡ 1448 scan_table. Suspected cause: base `ExecutionContext` constructors re-create a FRESH `::default()` memo (`context.rs` 337/392/451/499/546/595/1222) vs the GRAPH derivations that CLONE the Arc (1103/1161) — the correlated inner-join driver likely derives a fresh-memo ctx per driving batch → memo published-and-discarded, never reused. Wall-neutral now (q031 is cold-floor-dominated; q028, the only sibling correlated dim-join, is `scan_table.n=13`/3.9s). Remedy: confirm the derivation site, clone the memo Arc, add a q031-shaped hermetic test through the REAL correlated driver. Do NOT fold into the cold-floor PR. | engine (context memo-derivation) — standalone, low priority (latent) |
| **F20** | perf — over-scan (F8/PR-3 family) — **the real q031 root** | q031 | RefObjectMap target class not propagated to constrain downstream shared-predicate resolution. `?p edw:name ?pn` (variable-subject, shared `edw:name`) resolves against all 6 name-bearing dims even though `?p` is bound by `edw:product` (RefObjectMap → provably DIM_PRODUCT). Measured: 7 DISTINCT tables loaded once each (pin NOT leaking; F18's pin/residency framing REFUTED). Remedy: propagate the ref target to prune resolution (7→2 loads) — **SOUNDNESS: prune only when every binding source of `?p` is provably that one RefObjectMap; else DECLINE** (UNION/other-triple/VALUES/different-parent-ref). Standalone PR, own kill switch. Residual = one 7,670-file fact decode ≈ ~1s (q018/q044 ref) → ≤3s cache-thrashed without PR-2a. Prior art: `trust_fk_refs` (crawl-only). Full design: `20-pr-q031-refprune.md`. | engine (rewrite: RefObjectMap-target resolution constraint) — the q031 PR, supersedes F18 |

**Gate observation (non-blocking, PR-F9 gate 2026-07-14) — q029 DNF was a harness deadline mismatch, NOT a regression.** During the F9 gate q029 (`purchase_or_cart_events`, `hash_gate=rows_only`) DNF'd in both switch phases because the gate script applied a **blanket 120s deadline**, but q029's **manifest `timeout_s` is 180** and it completes at ~142–150s (§3 classifies it: **scan-bound / re-scan-amplified**, 142s at PR-2; a healthy-network rerun here = ok/rows=100/~150s, within variance). It is F9-neutral (formatter-only). Two fixes: the mechanism is now filed as **F17** (below), and the gate scripts must honor per-query `timeout_s` (see F17). The earlier "healthy at corpus close / upstream scan variance" framing was wrong — q029 has been a known ~142s scan-bound completer since PR-2.

**The two correctness bugs (F1/F2) share one root** and are the highest priority — they deliver wrong answers as silent successes. **F3 is a second, independent correctness gap** (root-caused) on the most common inspector shape — small fix surface, high user-visible impact (missing `@type` in the solo subject inspector). **F5-q050** is the sharpest perf signal (dims-only, 377 scans). **F4 is corpus hygiene, not an engine bug** — but it must be fixed (the determinism amendment) so nondeterministic-selection queries stop masking real divergences. F1/F2/F3/F5 feed the WP7 diagnosis and WP8 roadmap; F4 feeds the corpus determinism amendment; F7 feeds back into WP6 harness tuning.
