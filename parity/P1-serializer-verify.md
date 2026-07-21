# P1 — Serializer defect chain verification (graph-source crawl drops the IRI term type)

**Rev verified:** `733cb2c8eca45aa85428a4083fab053fbc1e3ea5` (exact pin in the handoff; worktree `wt-parity`, detached HEAD). READ-ONLY; no code changed.

**Source:** the solo-side virtual-dataset browse handoff, Part 1 + "Notes for whoever picks this up".

## Bottom-line verdicts

**(a) IRI term type lost in node-document object position — CONFIRMED (narrowed to object position).** For a virtual dataset, an FK-ref object serializes as a bare JSON string, not `{"@id": …}`. The root `@id` and `@type` values are rendered by dedicated, correct code; only the general object position (FK refs and any IRI-valued non-type object) is wrong. So the defect is real but *partial on the path*: it bites object-position IRIs, not `@id` or `@type`.

**(b) `format: "typed-json"` silently ignored on the entire crawl path — CONFIRMED.** `expand_wildcard_crawl` reads exactly one field off `FormatterConfig` — `normalize_arrays` — and never inspects `.format`. It hardcodes the JSON-LD flat-select `format_binding_with_result`. A typed-json request against a virtual dataset gets default-shaped output with no error. Refined nuance below: for *literals* this only diverges from native under typed-json (default-format literals already match native); for *refs* it diverges under both formats.

## Receipt-by-receipt (every claim CONFIRMED)

Every line reference in Part 1 lands on the exact code claimed. No DRIFTED, no WRONG. Two trivial range nits noted.

| Receipt | Claim | Verdict |
|---|---|---|
| `emit/render.rs:82-106` | `render_pom` emits `rr:parentTriplesMap` + `rr:joinCondition` when `foreign_key: Some(_)` | CONFIRMED (fn at :82, FK branch :85, parentTriplesMap emit :87-94) |
| `emit/heuristic.rs:647` | `infer_foreign_keys` | CONFIRMED (fn signature at :647) |
| `emit/heuristic.rs:717` | `ForeignKey { … }` constructed | CONFIRMED (`let fk = ForeignKey {` at :717) |
| `loader/extractor.rs:305-334` | `rr:parentTriplesMap` parses to `ObjectMap::RefObjectMap` | CONFIRMED (find PARENT_TRIPLES_MAP :306; returns `RefObjectMap::with_conditions` :331-334) |
| `ledger_info.rs:1187` | `ObjectMap::RefObjectMap(_) => "@id"` (datatype display) | CONFIRMED (exact) |
| `ledger_info.rs:1252-1290` | ref-class resolution behind the About-tab arrow | CONFIRMED (`ref_targets` from RefObjectMap → parent `classes()`, :1262-1296) |
| `mapping/term_map.rs:281` | `ObjectMap::RefObjectMap(_) => TermType::Iri  // Refs always produce IRIs` | CONFIRMED (exact, comment included) |
| `query/r2rml/operator.rs:140-199` | `RefShortcut` + `build_ref_shortcut` (crawl fast path) | CONFIRMED (struct :155, builder :170-199) |
| `operator.rs:1786` | `RdfTerm::Iri(iri) => Binding::iri(iri.as_str())` | CONFIRMED (inside `encode`; comment: graph-source IRIs kept as raw strings) |
| `operator.rs:1972-2035` | `materialize_pom_object`: both ref paths yield an IRI term | CONFIRMED (fn :1974; RefShortcut → `RdfTerm::iri` :2017-2019; parent-lookup → cached term :2021-2024 — see caveat) |
| `graph_source/crawl.rs:34` | imports `format_binding_with_result` from `crate::format` | CONFIRMED |
| `crawl.rs:194` | `execution.with_trust_fk_refs(true)` | CONFIRMED |
| `crawl.rs:271` / `:305` | object formatted via `format_binding_with_result` (wildcard / predicates branch) | CONFIRMED (both) |
| `crawl.rs:328` | only `format_config.normalize_arrays` is read | CONFIRMED (sole read; see coverage table) |
| `format/mod.rs:89` | that import resolves to the JSON-LD flat-select formatter | CONFIRMED (`pub(crate) use jsonld::format_binding_with_result;`) |
| `format/jsonld.rs:402` | `Binding::Sid` renders bare string | CONFIRMED |
| `format/jsonld.rs:409` | `Binding::Iri(iri) => Ok(JsonValue::String(iri.to_string()))` (bare) | CONFIRMED (exact) |
| `format/hydration.rs:1513` | native ref: `None => json!({ "@id": compact_id_sid(ref_sid)? })` | CONFIRMED (exact) |
| `format/typed.rs:389` | `Binding::Iri(iri) => Ok(json!({"@id": iri.as_ref()}))` (would be correct if reached) | CONFIRMED (exact) |
| weak test `crawl.rs:1284-1355` | substring `.contains("account/10")` guard can't distinguish the shapes | CONFIRMED (asserts `.contains("account/10")` :1337 and `.contains("account/99")` :1343 — both substrings of both shapes) |

**Full defect chain, end to end (all links confirmed):** loader parses `rr:parentTriplesMap` → `ObjectMap::RefObjectMap` (extractor:331) → `term_type()` = `TermType::Iri` (term_map:281) → `materialize_pom_object` yields `RdfTerm::Iri` (operator:2017/2024) → `encode` → `Binding::iri` = raw `Binding::Iri` (operator:1786) → crawl calls `format_binding_with_result` (crawl:271/305) → resolves to `jsonld::format_binding_with_result` (mod:89, jsonld:624) → delegates to `jsonld::format_binding` (jsonld:634) → `Binding::Iri` arm = **bare string** (jsonld:409). The typed serializer that would wrap it (`typed.rs:389`, reached only via the `format` dispatch at `mod.rs:246`) is never called because the crawl path forks off *before* `format_results`'s dispatch.

**Two receipt nits (non-substantive):** the weak-test fn actually spans :1284-1357 (closing brace :1357, not :1355). And the operator "parent-lookup path returns `RdfTerm::Iri`" is true by construction, not by a literal `RdfTerm::Iri` at that line: line :2021-2024 returns whatever `ParentLookup` cached, which is the parent subject term — an IRI because the parent subject map is IRI-templated (always true for auto-generated Iceberg mappings). The RefShortcut arm (:2017-2019) is an explicit `RdfTerm::iri`. Both reach formatting as `Binding::Iri`.

## Adversarial pass

**(i) Is the defect total or partial? — PARTIAL / NARROWED to object-position IRIs.** Three IRI-bearing positions in the crawl document are rendered by *different* code, and only one is broken:
- Root `@id`: `doc.insert("@id", json!(compactor.compact_id_iri(&key)))` (crawl:334) — a string under the `@id` key, which is the correct JSON-LD `@id` representation. Correct.
- `@type` values: `acc.add_type(compactor.compact_vocab_iri(class_iri))` (crawl:267, and :312 for the predicates branch), emitted as an array of bare compacted strings (crawl:336-337). Native emits `@type` identically — bare compacted strings (`hydration.rs:1493` pushes `compact_sid` string, not `{"@id"}`). **Parity on `@type`.**
- Object position (everything else, incl. FK refs): `format_binding_with_result` (crawl:271/305) → bare string. **This is the sole break.**

So the reported symptom (FK refs render as inert text) is the complete blast radius; `@id`/`@type` are unaffected. The defect is specifically "object-position `Binding::Iri` is not `@id`-wrapped."

**(ii) Does the `jsonld.rs:402` Sid arm matter here? — NO (unreachable on pure virtual); fix should still cover it defensively.** The R2RML operator's `encode` keeps graph-source IRIs as *raw* `Binding::Iri`, never `Binding::Sid` (operator:1780-1786, explicit comment "graph source IRIs are independent of any Fluree namespace table"). `format_binding_with_result` (jsonld:624) only materializes-to-Sid when `binding.is_encoded()`, which raw Iri/Lit bindings are not. On a **hybrid** dataset the crawl expander still only regroups the *R2RML operator's* output for the virtual graph source (native subjects go through native hydration, a different path), so Sid objects don't reach this formatter either. Conclusion: `jsonld.rs:402` is effectively dead on the crawl path; the defect is entirely via the `Binding::Iri` arm (:409). The handoff's proposed fix nonetheless special-cases `Sid`/`IriMatch` too — cheap, matches `typed.rs` (which wraps all three), and future-proofs against an encoded-binding path, so keep it.

**(iii) Dispatch-gap audit — CONFIRMED and stronger than claimed: `crawl.rs` is the ONLY graph-source path that consumes `FormatterConfig` at all, and it reads only `normalize_arrays`.** A grep of `fluree-db-api/src/graph_source/` for `format_config` / `FormatterConfig` hits *only* `crawl.rs`. The other graph-source paths don't read `FormatterConfig` *partially* — they don't read it *at all*, because they emit bespoke JSON (metadata/preview structs), not binding-table formatting. So "no other graph-source path reads `format_config` partially" is true by the strongest reading. Coverage table:

| Graph-source path | File | Consumes `FormatterConfig`? | Honors `format`? | Honors `normalize_arrays`? | Notes |
|---|---|---|---|---|---|
| Wildcard subgraph crawl (`expand_wildcard_crawl`) | `graph_source/crawl.rs` | **Yes** (param :151/:409) | **NO** (never reads `.format`) | Yes (:328) | **The defect.** Hardcodes `jsonld::format_binding_with_result`. |
| Flat `SELECT ?o` over a virtual dataset | normal `format::format_results` (`format/mod.rs`) | Yes | **Yes** (`mod.rs:243-248`) | Yes | Correct — full `OutputFormat` dispatch. Handoff's own note: a flat select "would have gone through typed.rs:389 and been correct." |
| Native binary-index subgraph crawl (hydration) | `format/hydration.rs` | Yes | **Yes** (`typed = format == TypedJson`, :690/:1029) | Yes | The parity reference. |
| Iceberg catalog preview / sample | `graph_source/iceberg_catalog.rs`, `iceberg_sample.rs` | No | N/A | N/A | Bespoke JSON (schema/rows preview), not a binding formatter. |
| `/info` for a virtual dataset (`build_virtual_ledger_info`) | `ledger_info.rs` | No | N/A | N/A | Builds a `LedgerInfo` metadata struct; not a formatter. |
| Ephemeral / catalog-session | `graph_source/ephemeral.rs`, `catalog_session.rs` | No | N/A | N/A | No `FormatterConfig`. |

(The two extra `FormatterConfig::default()` occurrences in `crawl.rs` at :915/:1198 are inside the in-crate test helper `run_crawl`, not a production dispatch.)

**(iv) Does the native crawl/select-map path honor `format: typed-json`? — YES. So virtual honoring it is PARITY, not novelty.** Native's wildcard crawl is a *hydration* query; it routes through `hydration::format_async` (`mod.rs:451`) which sets `typed = config.format == OutputFormat::TypedJson` (`hydration.rs:690`, :1029) and switches literal rendering on it (:1544-1548). The native *flat* select honors it via the top-level `match config.format` dispatch (`mod.rs:243-248`). The virtual crawl is the *only* select-map path in the engine that drops this — it never reaches either dispatch.

## Native parity anchor (the fix target — pinned to native's ACTUAL behavior)

Same select-map query (`{"select":{"?s":["*"]}}`) on a **native** dataset, from code (`hydration.rs`):

| Object | Native — default format | Native — typed-json | Mechanism |
|---|---|---|---|
| **FK ref** | `{"@id": <compacted>}` | `{"@id": <compacted>}` (identical) | `hydration.rs:1513` — ref is `@id`-wrapped *before* and *independent of* the `typed` flag. |
| **Literal, inferable dt** (xsd:string / integer / boolean / double…) | **bare** (`"foo"`, `42`, `true`) | `{"@value": …, "@type": …}` | default: `format_literal_value` :2046-2064 (inferable ⇒ bare); typed: `format_typed_literal_value` :2186+ (always `@type`). |
| **Literal, non-inferable dt** | `{"@value": …, "@type": …}` | `{"@value": …, "@type": …}` | `format_literal_value` falls through to typed form for non-inferable dt. |
| **Lang-tagged literal** | `{"@value": …, "@language": …}` | `{"@value": …, "@language": …}` | :2030-2043 / :2170-2184. |

Two load-bearing facts for the fix: **(1)** native's FK-ref shape is `{"@id": compacted}` under *both* formats — so the ref fix is format-independent; **(2)** the virtual crawl's *current* literal rendering (`jsonld::format_binding`, jsonld:412-469) already byte-matches native's *default* literal shaping (same "inferable ⇒ bare, else typed, lang ⇒ @language" logic). Therefore defect (b)'s literal impact is *only* under typed-json: virtual gives bare where native gives `{"@value","@type"}`. Refs are wrong under both formats (defect a). The native-untouched rule holds trivially: the fix touches only the virtual crawl formatter; native `hydration.rs` is the *reference*, unchanged.

## Design sketch (no code)

**Where the dispatch belongs — a format-module facade, not inline per-object in `crawl.rs`.** There are two identical call sites (crawl:271 wildcard, crawl:305 predicates) that must not drift; a shared `format::` helper — e.g. `format_node_object_binding(result, binding, compactor, format_config)` — keeps format knowledge in the format module (where `OutputFormat`, `typed`, and the compactor semantics already live), is unit-testable in isolation, and mirrors what `hydration.rs` already does at the object position. It should:
- Ref/IRI bindings (`Binding::Iri`, and defensively `Sid`/`IriMatch`) → `{"@id": compactor.compact_id_iri(iri)}`.
- Literal bindings → dispatch on `format_config.format`: `OutputFormat::TypedJson` ⇒ `typed::format_binding_with_result`; else `jsonld::format_binding_with_result` (current behavior). This closes (b) for literals with zero change to default-format output.
- Preserve the `is_encoded()` materialization guard already inside both `format_binding_with_result`s.

**Compaction — the `{"@id"}` value MUST be `compact_id_iri`, not raw and not `compact_vocab_iri`.** The correctness requirement is not "compact vs not" but *byte-identity*: a ref's `@id` string has to equal the target subject's own `@id` string (crawl:334 uses `compact_id_iri(&key)`) or FK navigation in the browser still can't match them — which is the entire point of the fix. Using the same `compact_id_iri` for the root `@id` and for ref objects guarantees this. **F9 CURIE-align interaction (see `[[pr-f9-virtual-curie]]`):** CURIE alignment is `@context`-driven via the same compactor and gated by kill switch `FLUREE_R2RML_CURIE_ALIGN`; because subject `@id` and ref `@id` would share one compactor + one `@context`, alignment (on or off) applies uniformly to both — no new divergence. Do **not** route ref objects through `compact_vocab_iri` (the `@type`/predicate compactor) — that uses `@vocab` and would produce a different string than the subject `@id`, silently re-breaking navigation.

**Risk surface.** (1) `@id`/`@type`/root untouched — no regression there. (2) Default-format literal output is byte-identical (same jsonld path) — the only behavioral change under default format is refs gaining `{"@id"}`, which is the fix. (3) typed-json output changes shape for literals *and* refs — desired, but any downstream Solo parser must accept it (handoff §"Out of scope" #5 says referenced-by/count parsing now accepts both shapes, so this is safe to land). (4) Ensure `normalize_arrays` collapse still runs after the object is built (crawl:328/337/339) — the facade returns a single value; array-collapse stays where it is.

**Test design — the handoff's "shape-parity test between `/info` datatype and crawl serialization" is feasible and the right invariant.** Reuse the in-crate `MockCrawlProvider` + `genesis_view` + `run_crawl` harness already in `crawl.rs`'s test module. Minimum: replace the two `.contains(...)` substring asserts with `serde_json::from_str::<Value>` + structural assertions — for the ref property assert the value is an object with an `"@id"` key equal to the expected parent IRI; for a literal property assert the datatype-appropriate shape. Stronger (the handoff's actual proposal): drive `build_virtual_ledger_info` on the *same* `CompiledR2rmlMapping`, read each property's reported datatype (`"@id"` for a RefObjectMap via ledger_info:1187, else the literal datatype), then assert the crawl serialization of that property matches — every property `/info` calls `"@id"` must serialize as `{"@id": …}`. That pins the cross-path invariant that actually failed (the two paths disagreeing about `account`) and generalizes past this one case. Add a typed-json variant: run `run_crawl` with `FormatterConfig { format: OutputFormat::TypedJson, .. }` and assert refs are `{"@id"}` and literals are `{"@value","@type"}` — this guards (b), which no current test exercises. Feasibility caveat: `build_virtual_ledger_info` needs a `GraphSourceRecord` + `VirtualSourceMeta` + `table_row_counts`; that's a bit more fixture than the current test carries, but all types are in-crate and the row counts can be a small stub map — no live Iceberg/Snowflake needed.

## Surprises

**1. The break point is architecturally a *fork-before-dispatch*, not a wrong arm in the dispatch.** Both defects have the same single root cause: `expand_wildcard_crawl` bypasses `format::format_results` (and its `match config.format` at `mod.rs:243`) entirely and hand-rolls object formatting with one hardcoded formatter. A correct `typed.rs:389` arm exists and is simply never reached. Fixing the fork (route through a format-aware facade) closes (a) and (b) together.

**2. Defect (b) is narrower than "literals affected too" implies — under *default* format, virtual literals already match native.** The virtual crawl's literal rendering (`jsonld::format_binding`) is byte-identical to native's default literal rendering. So (b)'s literal divergence is *exclusively* a typed-json phenomenon; refs are the only thing wrong under default format. Worth stating precisely so the fix's default-format blast radius is understood to be "refs only."

**3. `@type` is already at parity with native** (bare compacted strings both sides) — so the crawl path already has *some* node-document awareness (`@id` and `@type` are special-cased correctly); it's only the generic object slot that fell back to the flat-select formatter. The fix is completing an existing pattern, not introducing one.
