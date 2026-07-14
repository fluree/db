# PR-F9 — virtual formatter CURIE alignment — DESIGN SKETCH

**Branch:** `fix/f9-virtual-curie` (off `perf/r2rml-pr5-scan-topk`)
**Status:** SKETCH — **STOP for lead RE-review** (the approved Option B is refuted by the code; corrected mechanism + fix below, no engine code until re-approved).
**Substrate:** `04-findings-register.md` F9 + AJ's product decision (VIRTUAL aligns to NATIVE CURIE-compaction; native output must NOT change; SPARQL-1.1 results-JSON conformance deferred).

## Correction notice (2026-07-14, rider-1 mechanism investigation)

The lead's rider 1 asked me to pin down the compaction mechanism in the doc before blessing the flip, because the evidence held a latent tension (native reference IRIs render full even though a native ledger's namespace map contains their namespaces). Chasing that tension down through the actual formatter code **refuted the approved Option B and the register's "namespace-map gap" root cause.** The corrected mechanism and fix are below; both the register F9 entry and my original sketch were wrong on the mechanism. This section is the STOP-for-re-review.

## (1)+(rider-1) Mechanism — pinned down in code, position-independent, `@context`-driven

The vbench hash is computed over `FormatterConfig::sparql_json()` (`exec.rs:226/284`). In that formatter, every IRI-valued binding is compacted (or not) as follows (`fluree-db-api/src/format/sparql.rs`, streaming `write_term` and DOM `format_binding`):

- **`Binding::Sid { sid }`** (NATIVE emits this for predicates/types/refs) → `compactor.compact_id_sid(sid)` → `decode_sid` (Sid→full IRI via the namespace map) **then** `compact_id_iri(iri)`.
- **`Binding::IriMatch { iri }`** (multi-ledger) → `compact_id_iri(iri)`.
- **`Binding::Iri(iri)`** (VIRTUAL / R2RML emits this — the code comment literally says *"Raw IRI string (from graph source, not in namespace table)"*) → **written RAW** (`write_node(out, iri.as_ref())` at line 333; the DOM twin at line 487), **bypassing the compactor entirely.**

`compact_id_iri` → `ContextCompactor::compact_id` (`fluree-graph-json-ld/src/compact.rs:60`) keys **exclusively on the parsed `@context`** (`reverse_context(context, include_vocab=false, include_base=true)` → exact-term then longest-prefix match). **It never reads the namespace-code map.** The namespace map (`IriCompactor.namespace_codes`) is consulted only by `decode_sid` (Sid→IRI) and by the *display-only* `compact_for_display`/`try_fallback` path — which the SPARQL/JSON/XML result formatters do **not** call.

**Why native compacts and virtual does not** is therefore NOT a namespace-map gap. Both native and virtual carry the identical query context: `query_view_with_r2rml_options` parses the SPARQL the same way native does (`parse_sparql_to_ir`, `query.rs:269`) and builds the result via the same `build_query_result(... context: parsed.context ...)` (`helpers.rs:403`), so `result.context` holds `PREFIX edw: <http://ns.fluree.dev/edw#>` on **both** paths. The *only* divergence is the binding variant: native's `Binding::Sid` runs through `compact_id_sid → compact_id_iri` (compacts against the context), while virtual's `Binding::Iri` is written raw.

**Position-independence (the boundary the lead asked me to state honestly).** In SPARQL-results JSON, every `uri` binding is an `@id`-position node identifier, so ALL of them go through `compact_id` (context explicit-prefixes + `@base`, never `@vocab`). Compaction is therefore **uniform and position-independent** — it depends only on whether the IRI's namespace matches a **declared query prefix**, not on predicate-vs-reference position. This is proven directly by the expected q002 output and by hand-tracing `compact_id`:

| binding IRI | declared prefix match? | renders |
|---|---|---|
| `http://ns.fluree.dev/edw#name` (predicate) | yes (`edw:`) | `edw:name` |
| `http://ns.fluree.dev/edw#Store` (rdf:type object) | yes (`edw:`) | `edw:Store` |
| `http://www.w3.org/1999/02/22-rdf-syntax-ns#type` (predicate) | **no** (no `rdf:` prefix declared) | **full IRI** |
| `http://data.fluree.dev/edw/employee/604` (reference object) | **no** (`edw/` ≠ `edw#`) | **full IRI** |

This table also **falsifies the register's "native compacts from ledger namespaces" claim**: `rdf:` (reserved code 3) and `http://data.fluree.dev/edw/` (minted at ingestion) are both in a native ledger's namespace map, yet both render **full** — because they are absent from the *query context*. Compaction tracks the query prefixes, full stop.

**The boundary, stated honestly:** because the fix makes virtual call the **exact same `compact_id_iri`** native already uses, parity holds **unconditionally** — for predicates, types, AND references, in every namespace. The lead's hypothetical — a future subject template `http://ns.fluree.dev/edw#order123` under a query that declares `edw:` — would compact to `edw:order123` on **both** sides (native Sid and virtual Iri hit the same `compact_id`), so parity is preserved, not broken. There is no fragile position/namespace boundary under this fix; that fragility was an artifact of the (wrong) namespace-map approach.

## Refutation of Option B (the approved approach)

Option B was "seed the virtual snapshot's namespace map with the mapping's vocabulary namespaces via `encode_iri`." Since `compact_id` never consults the namespace map, seeding it **cannot change the sparql_json hash** — it would be a no-op for q002/q042, exactly like the earlier PR-6 attempt the register records. (The likely reason that PR-6 one-liner "did nothing": vbench hashes the *streaming* `write_term` path (line 333); a fix touching only the DOM `format_binding` arm (line 487), or only a non-sparql formatter, would leave the streaming path raw — and the two are parity-tested to be byte-identical, so a partial change would break that parity test rather than flip the hash.)

## Corrected fix — route `Binding::Iri` through `compact_id_iri` (native's mechanism)

Change the `Binding::Iri` arm in **both** `sparql.rs` sites to compact via the context, mirroring the `IriMatch` arm one line above each:

```rust
// streaming write_term (line 333) and DOM format_binding (line 487)
Binding::Iri(iri) => write_node(out, &compactor.compact_id_iri(iri)),   // was: iri.as_ref()
```

`compact_id_iri` is a pure context lookup; on an IRI whose namespace matches no declared prefix it returns the IRI unchanged, so blank-node (`_:`) handling and all non-matching IRIs are byte-identical to today. Hand-traced against q002's context: `edw#name→edw:name`, `edw#Store→edw:Store`, `rdf:type`/`edw/employee/604`→full — i.e. **byte-identical to native**.

**Scope decision for the lead:** the minimal fix for the DoD (q002/q042 in sparql_json) is the two `sparql.rs` arms. The same raw-`Binding::Iri` behavior exists in `jsonld.rs` (206/409), `sparql_xml.rs` (212), `typed.rs` (189/389), and `delimited.rs` (382). Those affect *other* output formats, not the vbench sparql_json hash. Recommendation: fix the two `sparql.rs` arms now (DoD-scoped, corpus-gated); treat the other formatters as an optional consistency follow-up (a broader "virtual compacts uniformly across formats" change) so we don't widen the blast radius under a perf-branch stack. Your call.

## (4) Hash blast radius — still exactly q002 + q042

Unchanged from the enumeration, and now with a tighter argument: the fix compacts a virtual `Binding::Iri` **only** when its namespace matches a declared query prefix. Across the corpus that is exactly q002 (`?p`) and q042 (`?p` UNION) — the only queries projecting vocabulary IRIs that a declared prefix covers. Virtual reference objects (`data.fluree.dev/edw/…`) match no declared prefix and stay full (as today, as native). The full virtual+native corpus compare is the evidence; the enumeration says none other can move.

## (2) Collision policy — N/A under the corrected fix

There is no namespace-map seeding, so there is no code/namespace collision surface. Compaction uses the query context's prefixes, which are already collision-resolved by the parser (identically for native and virtual).

## (3) Scope — `Binding::Iri`-only; native untouched by construction

Native predicates/types/refs are `Binding::Sid`, never `Binding::Iri`; the changed arm is unreachable for native inputs. The W3C SPARQL suite runs on native ledgers (Turtle/RDF → `Binding::Sid`), so it emits no `Binding::Iri` and is unaffected. Proof obligation stands: native 54/54 + W3C byte-identical.

## (5) Vehicle — unchanged: standalone stacked PR on `perf/r2rml-pr5-scan-topk`

Still self-contained and independent of PR-4d/F14; if anything the corrected fix is *smaller* (a two-line formatter change vs a namespace-seeding site). Branch `fix/f9-virtual-curie` is correct.

## Gates / DoD (unchanged)

- q002 + q042 flip red→hash-green vs the native oracle; re-bless `expected/q002.json` + `q042.json` **only after** inspecting the row diff — every changed cell must be exactly full-IRI→CURIE with identical row counts/order (rider 3); keep old hashes in the commit message.
- Full virtual + native corpus compare: NO other query's hash changes.
- Hermetic unit test: `IriCompactor::new(namespaces_WITHOUT_edw, context_WITH edw prefix).compact_id_iri("http://ns.fluree.dev/edw#name") == "edw:name"` — proves the namespace map is unnecessary and the context suffices (this is the decisive, Snowflake-free proof that the fix works and Option B was misdirected). Plus a `Binding::Iri` write_term/format_binding parity assertion.
- Unit + W3C green; clippy/fmt clean.
- Zero native-path touched (native 54/54 byte-identical).

## Follow-up: correct the F9 findings-register entry

`04-findings-register.md` F9 (line 148, "SHARPENED — a NAMESPACE-MAP gap") is refuted by `compact.rs` + the existing `iri.rs` tests + its own cited evidence. I'll correct it on your nod (or you may want to; flagging rather than unilaterally rewriting a canonical entry mid-reversal).

## PROOF OBLIGATION (rider, pre-code) — `Binding::Iri` construction-site enumeration — BLOCKS the unconditional fix

The lead required, before code, that every `Binding::Iri` construction site be enumerated workspace-wide and each confirmed graph-source-only reachable — because if any native-reachable constructor exists, routing `Binding::Iri` through `compact_id_iri` in the SHARED formatter is a **native-visible** behavior change. **Result: native-reachable constructors DO exist.** The "native untouched by construction" scope claim is therefore false for the unconditional formatter fix.

**Native-reachable `Binding::Iri` construction sites (would change native output under the shared-formatter fix):**

| site | trigger | native-reachable via |
|---|---|---|
| `fluree-db-query/src/eval/value.rs:522` | `ComparableValue::Iri` not encodable to a SID — the code comment names `UUID`, `IRI()` | `BIND(IRI("…"))` / `UUID()` / constructed IRIs on any query |
| `fluree-db-query/src/sparql_results.rs:84,88` (via `fluree-db-api/src/remote_service.rs:153`) | remote SPARQL-Results `uri`/`bnode` term | `SERVICE <…> { }` federation (the lead's named hazard) |
| `fluree-db-query/src/graph.rs:343,440` (via `Binding::iri()`) | the `?g` graph-variable value | `GRAPH ?g { }` projection |
| `fluree-db-query/src/bm25/operator.rs:615` | full-text hit IRI not encodable to a SID | native BM25 search (cross-ledger/unencodable hit) |
| `fluree-db-query/src/vector/operator.rs:350` | vector hit IRI not encodable to a SID | native vector search (same) |

**Virtual/graph-source-only (the intended target, safe to compact):** `fluree-db-query/src/r2rml/operator.rs` — 1628/1629 (predicate/blank), 2117/2163/2169/2193/2200/2225/2226 (class / rdf:type / predicate emission). These go through the `Binding::iri()` constructor from the R2RML materialization only.

**Test-only (not production):** `graph.rs:958`, `optional.rs:2528+`, `eval/types.rs:150-151`, `eval/rdf.rs:451`, `format/sparql_xml.rs:782`, `format/delimited.rs:850-862`.

### Consequence — the unconditional shared-formatter fix violates the DoD's "zero native-path touched"

Compacting `Binding::Iri` in `sparql.rs` would compact native GRAPH/SERVICE/BIND(IRI)/search-hit IRIs whenever they match a declared query prefix. This is arguably a *consistency* improvement (a native STORED IRI (`Binding::Sid`) already compacts via `compact_id`; a constructed/federated/graph IRI with the identical value currently does not — same value, different rendering by provenance), but it IS a native-visible change and W3C-relevant (GRAPH/SERVICE/BIND-IRI are all in the W3C suite). Per the rider, back to the lead before code.

### Options for the lead (+ likely AJ)

- **(C) Own it as an intentional native consistency fix.** Route `Binding::Iri → compact_id_iri` unconditionally; accept that native constructed/federated/graph IRIs now compact identically to stored ones. Simplest code; widest validation — must re-run the full W3C suite (and confirm how `testsuite-sparql` compares results: if it canonicalizes to full IRIs, the CURIE display is invisible to it and native W3C stays green; if it compares raw sparql_json, some expected files re-bless) + native 54/54 + AJ sign-off on the native-output change.
- **(A) Provenance-scoped fix.** Compact only graph-source-originated `Binding::Iri`. `Binding::Iri` is a bare `Arc<str>` with no provenance, so this needs a marker (a new binding variant or a flag) — an enum-touching change that ripples across every `Binding` match, but it keeps native byte-identical by construction.
- **(B) Encode-at-operator.** Have the R2RML operator emit `Binding::Sid` (encode the IRI into a seeded snapshot namespace) so virtual rides native's existing `Binding::Sid → compact_id_sid` arm with no shared-formatter change. This resurrects namespace seeding (the refuted Option B's machinery, but now for *decode*, not compaction) and is more complex than (C); native paths untouched.

### Test-gate exposure of (C) — measured, not assumed: ZERO regression

- **W3C: unaffected.** The runner formats every actual result with an EMPTY context — `format::format_results(&query_result, &ParsedContext::new(), …)` at `testsuite-sparql/src/query_handler.rs:256/258`, `308/310`, `392/395`. `compact_id_iri` keys only on the context, so an empty context means it returns FULL IRIs for BOTH `Binding::Sid` and `Binding::Iri` — the (C) fix is a NO-OP in the W3C runner. Second independent safety net: the harness EXPANDS any compact IRI back to absolute form (`result_format.rs:1060-1246`, `expand_id`/`expand_vocab`) and compares as isomorphic RdfTerms (`result_comparison.rs:16`), so even a compacted result would match.
- **Native 54/54: byte-identical.** No corpus query uses GRAPH ?g / SERVICE / BIND(IRI|UUID) (grep of `corpus/queries/` — the only two hits, q043/q044, match the substring "GRAPH" in *comments*; both are plain single-subject SELECTs returning 0 rows). So no corpus query reaches a native `Binding::Iri` construction path.
- **Real native-user exposure (the only real change):** narrow — a real *API* query (non-W3C, non-corpus) that declares a prefix AND projects a GRAPH/SERVICE/BIND(IRI)/search IRI matching it would render a CURIE where it renders a full IRI today. This is the provenance-consistency change (stored IRIs already compact; constructed/federated/graph IRIs would now too) and is the only thing needing AJ sign-off. It regresses NO test gate.
- **Implementation note:** format-module unit tests that construct `Binding::Iri` under a NON-empty matching context (e.g. any that assert full-IRI output) would need expectation updates — that's expected fix churn, caught by `cargo test`, not a native regression.

**Recommendation (now measured): (C).** It is the smallest code (two-line `sparql.rs` change), regresses ZERO test gate (W3C no-op + corpus byte-identical, both proven above), and its only real effect is a narrow, arguably-correct native consistency improvement that AJ signs off on. (A) — the provenance variant — is an enum-touching change to avoid an output change that doesn't even touch the test gates, so it is over-engineered unless AJ vetoes the native consistency change outright. Not my final call — routing to you + AJ for the sign-off.

**STOP — the unconditional fix is blocked by the native-reachability finding. Lead (+AJ) scope decision required before any code.** (Mechanism + corrected-fix direction from the prior sections stand; only the *unconditional vs scoped* application is now in question.)
