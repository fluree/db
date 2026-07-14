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

**STOP — design RE-review before implementation.** The approach changed materially from the approved Option B.
