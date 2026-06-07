# Edge Annotations

Edge annotations let you attach properties to a *relationship* — the connection between two subjects — without modeling an intermediate node by hand. A property graph user calls these "edge properties" or "relationship properties." A SPARQL user calls them "annotations on a quoted triple." A Fluree user gets one ergonomic surface that reads correctly from either side.

```text
ex:alice ──[ ex:worksFor: { role: "Engineer", since: 2024-01-01, confidence: 0.97 } ]──▶ ex:acme
```

Annotations are first-class RDF data: properties on the annotation subject are stored as ordinary triples and participate in policy, history, indexing, and query like everything else. The only thing that's special is the *attachment*: a sidecar relation that records which annotation subject belongs to which edge. The fact indexes (and queries that don't ask for annotations) are unchanged.

For the storage-internals view — how the attachment sidecar is laid out, how the `f:reifies*` bundle encodes an edge, and how the index root carries the annotation arena — see the [Edge annotations design doc](../design/edge-annotations.md).

## When to use edge annotations

Reach for `@annotation` when you need any of these:

- **Property-graph-shaped edges.** A `worksFor` relationship needs `role` and `since`. Modeling that as a separate `Employment` node works but distorts the graph.
- **Provenance / quality on a fact.** "This `ex:hasAuthor` claim has confidence 0.97 from source X." Classic RDF reification with quoted triples.
- **Multiple parallel relationships** between the same two subjects — e.g. Alice was both an Engineer and later a Manager at Acme. Plain RDF can't distinguish two `ex:worksFor` triples; annotations can.
- **Cypher / LPG imports.** Relationship properties round-trip without forcing every edge through an intermediate `:Relationship` node.

If a fact is naturally about a *node* (Alice's birthdate, Acme's industry), put it on the node — not on the edge. Annotations are for facts about the *relationship*.

## Where can I write edge annotations?

| Surface | How | Notes |
|---|---|---|
| **JSON-LD insert / upsert / update** | `@annotation` (or alias `@edge`) on a value object, `@reifies` on a node | Most ergonomic. Full coverage including literal-valued edges, named graphs, parallel annotations, named reifiers, body cascades. |
| **SPARQL 1.2 UPDATE** | `INSERT DATA { :s :p :o {\| ... \|} }`, `~ <reifier>`, optional `INSERT { } WHERE { }` templates | Use this when integrating with SPARQL pipelines or when porting from RDF 1.2 / SPARQL-star. See [SPARQL 1.2 surface](#sparql-12--rdf-12-surface) below for the per-operation rules. |
| **Turtle / TriG / N-Quads file ingest** | Not natively (today) | The Turtle ingest path is RDF 1.1 + Fluree extensions; it does **not** parse RDF 1.2 annotation tails (`{\| ... \|}`) or the `~` reifier. Two routes work: (a) convert your `.ttl` to JSON-LD before ingesting, or (b) ingest the plain edges first and then add annotations with a follow-up SPARQL `INSERT DATA { :s :p :o {\| ... \|} }` transaction. Either route ends up with the same on-disk shape. |
| **Cypher / LPG import** | Relationship-property syntax (`-[:T {p:v}]->`) | The storage primitive is in place; the full Cypher front-end (paths, `MERGE`, pattern comprehensions) is a separate workstream. Relationship properties round-trip today through the JSON-LD surface that imports emit. |

The reserved-predicate firewall rejects user-authored `f:reifies*` predicates on normal write surfaces (JSON-LD and SPARQL UPDATE). The way to mint annotations through application writes is `@annotation` / `@reifies` (JSON-LD) or the RDF 1.2 annotation tail (`~`, `{| |}`) in SPARQL — not direct `f:reifiesSubject` triples. Bulk import is an administrative bootstrap path: it may ingest already-lowered `f:reifies*` bundles, marks the ledger as annotation-bearing, and lets the indexer seal the annotation arena from those durable facts.

## The surface

### Inserting an annotated edge

The annotation block lives under the value object — `@id` is the edge target, `@annotation` carries the relationship's properties.

```json
{
  "@context": {
    "ex": "http://example.org/",
    "xsd": "http://www.w3.org/2001/XMLSchema#"
  },
  "insert": {
    "@id": "ex:alice",
    "ex:worksFor": {
      "@id": "ex:acme",
      "@annotation": {
        "ex:role": "Engineer",
        "ex:since": { "@value": "2024-01-01", "@type": "xsd:date" },
        "ex:confidence": 0.97
      }
    }
  }
}
```

Internally this commits four things atomically:

1. The base edge `ex:alice ex:worksFor ex:acme`.
2. A fresh annotation subject (a blank node by default).
3. An attachment row recording that the annotation belongs to that edge.
4. The annotation properties (`ex:role`, `ex:since`, `ex:confidence`) as ordinary triples on the annotation subject.

`@edge` is accepted as an alias for users coming from LPG; it normalizes to `@annotation`.

### Naming the annotation explicitly

You can give the annotation an IRI when you need stable identity — for updates, external references, signatures, or "the contract for Alice's 2024 employment."

```json
{
  "@id": "ex:alice",
  "ex:worksFor": {
    "@id": "ex:acme",
    "@annotation": {
      "@id": "ex:employment/alice-acme-2024",
      "ex:role": "Engineer",
      "ex:since": { "@value": "2024-01-01", "@type": "xsd:date" }
    }
  }
}
```

Two inserts that target the same explicit `@id` reattach to the same annotation subject — idempotent. Two inserts with no explicit `@id` mint two distinct annotations on the same edge (see *Parallel annotations* below).

### Annotating literal-valued edges

RDF 1.2 permits annotations on triples whose object is a literal — `:alice :name "Alice" {| :source :hr |}` in Turtle-star, equivalently:

```json
{
  "@id": "ex:alice",
  "ex:name": {
    "@value": "Alice",
    "@annotation": { "ex:source": "ex:hr-system" }
  }
}
```

Because JSON scalars can't carry sibling metadata, an annotated literal **must** be written as a JSON-LD value object — the expanded form with `@value`. The same applies to typed and language-tagged literals:

```json
{
  "@id": "ex:alice",
  "ex:joinedAt": {
    "@value": "2024-01-01",
    "@type": "xsd:date",
    "@annotation": { "ex:source": "ex:hr-system" }
  },
  "ex:label": {
    "@value": "chat",
    "@language": "fr",
    "@annotation": { "ex:source": "ex:lexicon" }
  }
}
```

A few rules that keep the annotation's identity in sync with the base flake:

- **The value object must carry its `@type` / `@language` explicitly when the predicate's `@context` would otherwise coerce them.** When `@annotation` is present, the lowering pass rejects two coercion paths that the JSON-LD value-object expander applies: a term-level `@type` on the predicate's context entry, and a default `@language` on the active context. (Per-term `@language` overrides are intentionally ignored, mirroring the value-object expander's own behavior — it reads `context.language` directly, not the per-term entry.) The non-annotated form continues to use context coercion normally; this stricter rule applies only to annotated literals so the reified `f:reifies*` bundle's `EdgeKey` cannot silently diverge from the base flake's.
- **Language-tagged literals are language-pinned.** Two annotations on `"chat"@fr` and `"chat"@en` are independent; selector-form retracts and hydration both match on language.
- **Hydration promotes annotated literals to value-object form.** A subject expansion (`select: {"?s": ["*"]}`) renders unannotated `ex:name "Alice"` as the scalar `"Alice"`, but renders the annotated form as `{"@value": "Alice", "@annotation": {...}}` so the annotation has somewhere to attach.

The deferred shapes from "Current limits" below (list occurrences, multi-triple reifiers, triple terms as object values) still apply on the literal path.

### Querying inline: edge first, metadata second

The query shape mirrors the insert shape. Match the base edge, then constrain or project annotation metadata.

```json
{
  "@context": { "ex": "http://example.org/" },
  "select": ["?person", "?org", "?role", "?since"],
  "where": {
    "@id": "?person",
    "ex:worksFor": {
      "@id": "?org",
      "@annotation": {
        "ex:role": "?role",
        "ex:since": "?since"
      }
    }
  }
}
```

This binds one row per `(edge, annotation)` pair currently asserted.

### Querying annotation-rooted: metadata first, edge second

When you start from the metadata — "find every employment with `role = Engineer`" — use `@reifies` to walk back to the edge.

```json
{
  "@context": { "ex": "http://example.org/" },
  "select": ["?person", "?org", "?since"],
  "where": {
    "ex:role": "Engineer",
    "ex:since": "?since",
    "@reifies": {
      "@id": "?person",
      "ex:worksFor": { "@id": "?org" }
    }
  }
}
```

`@reifies` is the same idea as `rdf:reifies` in RDF 1.2 — given an annotation subject, walk to the edge it reifies. Fluree resolves it through the reverse attachment index, so it's cheap regardless of how many annotations exist in the ledger.

### Subject expansion

Graph-crawl projection preserves the annotation block in the output:

```json
{
  "select": {
    "?person": [
      "@id",
      {
        "ex:worksFor": [
          "@id",
          { "@annotation": ["ex:role", "ex:since", "ex:confidence"] }
        ]
      }
    ]
  },
  "where": { "@id": "?person", "ex:worksFor": { "@id": "?org" } }
}
```

Output:

```json
{
  "@id": "ex:alice",
  "ex:worksFor": {
    "@id": "ex:acme",
    "@annotation": {
      "ex:role": "Engineer",
      "ex:since": "2024-01-01",
      "ex:confidence": 0.97
    }
  }
}
```

## Cardinality: the multiplicity contract

This is the rule to internalize:

> **A bare triple pattern returns one row per `(s, p, o)`. Binding an annotation variable returns one row per `(edge, annotation)`.**

Concretely:

- `?s ex:worksFor ?o` returns the same rows whether the edge has zero, one, or twenty annotations attached. RDF set semantics are preserved; existing queries don't change behavior just because a ledger started using annotations.
- `?s ex:worksFor ?o, @annotation { ?ann }` (or any `@annotation` body that binds a variable / matches a property) returns one row per annotation occurrence on each matching edge.

This is what lets a Cypher `MATCH (a)-[r]->(b)` faithfully return parallel-edge rows while leaving plain RDF queries undisturbed.

`select: "*"` follows the same rule — it does not multiply by occurrence count unless the WHERE binds an annotation variable.

## Parallel annotations on one edge

Two annotation blocks on the same `(s, p, o)` mint two distinct annotation subjects (anonymous case) or attach to the same subject (explicit-`@id` case).

```json
{
  "@graph": [
    {
      "@id": "ex:alice",
      "ex:worksFor": {
        "@id": "ex:acme",
        "@annotation": {
          "@id": "ex:emp/2020",
          "ex:role": "Engineer"
        }
      }
    },
    {
      "@id": "ex:alice",
      "ex:worksFor": {
        "@id": "ex:acme",
        "@annotation": {
          "@id": "ex:emp/2024",
          "ex:role": "Manager"
        }
      }
    }
  ]
}
```

Querying with an `@annotation` binding returns two rows:

```text
?person     ?org     ?role
ex:alice    ex:acme  Engineer
ex:alice    ex:acme  Manager
```

Querying without binding the annotation (`?person ex:worksFor ?org`) returns one row.

## Anonymous vs explicit annotation IDs

The two forms have deliberately different lifecycle behavior. The default is conservative: anonymous annotations behave like LPG edge properties; explicit-IRI annotations behave like ordinary RDF resources.

| | Anonymous (no `@id`) | Explicit `@id` |
|---|---|---|
| Visible in `select: "*"` | No — hidden from wildcard subject expansion | Yes |
| Visible in graph crawl | Only via `@annotation` projection | Yes, like any subject |
| Retract base edge → owned facts cascade | Yes (the annotation is intrinsic to the edge) | No, by default — explicit IRIs are not deleted surprisingly |

The anonymous-hide rule means a user wildcard query against Alice doesn't suddenly start returning a sea of internal annotation SIDs once you adopt edge metadata. Annotations participate in queries that ask for them and stay out of the way otherwise.

The explicit-ID-doesn't-cascade rule protects user-named resources from accidental deletion when an edge gets retracted. Opt out via *LPG mode* (below) when you actually want Cypher's "delete the relationship deletes its properties" semantics.

## Retraction semantics

### RDF mode (default)

Retracting a base edge removes the attachment and any owned facts on **anonymous** annotations. Explicit-IRI annotations keep their non-attachment facts — only the attachment row is retracted.

```json
{
  "delete": {
    "@id": "ex:alice",
    "ex:worksFor": { "@id": "ex:acme" }
  }
}
```

After this:
- Anonymous `_:annN` subjects attached to the edge: gone (attachment + body).
- Explicit `ex:employment/alice-acme-2024`: attachment retracted, but `ex:role`, `ex:since`, etc. are still in the graph as ordinary RDF.

History preserves both events — query at the pre-retract `t` and the annotation comes back, unchanged.

### LPG mode (opt-in per transaction)

For Cypher fidelity — "deleting the relationship deletes the relationship's properties" — set `lpgEdgeLifecycle: true` in transaction options:

```json
{
  "delete": {
    "@id": "ex:alice",
    "ex:worksFor": { "@id": "ex:acme" }
  },
  "opts": { "lpgEdgeLifecycle": true }
}
```

Now explicit-IRI annotations cascade their owned metadata too. Cypher imports default to this mode automatically.

### Updating annotation properties

Updating metadata is normal RDF update against the annotation subject. Once you've bound the occurrence by `@id` or by selector, treat it like any other subject:

```json
{
  "where": {
    "@id": "ex:alice",
    "ex:worksFor": {
      "@id": "ex:acme",
      "@annotation": { "@id": "?edge", "ex:role": "Engineer" }
    }
  },
  "delete": { "@id": "?edge", "ex:confidence": "?old" },
  "insert": { "@id": "?edge", "ex:confidence": 0.99 }
}
```

## Empty annotation blocks

In RDF mode, `"@annotation": {}` is a no-op: no annotation subject is minted, no attachment row is written. Inserts stay idempotent at the `(s, p, o)` level.

In LPG mode, an empty block mints a fresh annotation subject — a property-less relationship still has identity, the way Cypher relationships do.

## SPARQL 1.2 / RDF 1.2 surface

`@annotation` lowers to the same on-disk model as the RDF 1.2 annotation tail. All four equivalent forms below produce identical storage; pick whichever is ergonomic for your input.

### Equivalent forms

JSON-LD `@annotation`:

```json
{
  "@id": "ex:alice",
  "ex:worksFor": {
    "@id": "ex:acme",
    "@annotation": { "ex:role": "Engineer" }
  }
}
```

JSON-LD `@reifies` (annotation-rooted form):

```json
{
  "@id": "ex:ann1",
  "@reifies": {
    "@id": "ex:alice",
    "ex:worksFor": { "@id": "ex:acme" }
  },
  "ex:role": "Engineer"
}
```

SPARQL 1.2 / RDF 1.2 annotation block (anonymous reifier):

```sparql
PREFIX ex: <http://example.org/>
INSERT DATA {
  ex:alice ex:worksFor ex:acme {| ex:role "Engineer" |} .
}
```

SPARQL 1.2 / RDF 1.2 named reifier (`~`):

```sparql
PREFIX ex: <http://example.org/>
INSERT DATA {
  ex:alice ex:worksFor ex:acme ~ ex:emp1 {| ex:role "Engineer" |} .
}
```

### Grammar reference (subset)

The annotation tail attaches to the triple — not the object — per the RDF 1.2 grammar mirrored by SPARQL 1.2:

```text
annotation       ::= ( reifier | annotationBlock )*
reifier          ::= '~' ( iri | BlankNode | Var )?
annotationBlock  ::= '{|' predicateObjectList '|}'
tripleTerm       ::= '<<(' ttSubject verb ttObject ')>>'
```

Notes:

- An `annotationBlock` without a preceding `~` mints a fresh anonymous reifier.
- A bare `~` (no identifier) is equivalent to `~` + a fresh blank node — useful when you want a reifier variable bound in WHERE but don't care about its IRI.
- `tripleTerm` (the parenthesized `<<( s p o )>>` form) is accepted **only** as the object of `rdf:reifies`. Other uses error at parse time.
- Property-path triples cannot carry an annotation tail. `?s ex:p1/ex:p2 ?o {| ... |}` is rejected — write a simple-predicate triple instead.

### SPARQL UPDATE rules by operation

Different UPDATE operations place different constraints on reifier identity per SPARQL Update §3.1 and §4.1. The contract below is what Fluree enforces.

| Operation | `~ <iri>` (named) | `~ ?var` (variable) | `~ _:label` (blank) | `{\| \|}` with no `~` (anonymous) | `?ann rdf:reifies <<( ... )>>` |
|---|---|---|---|---|---|
| `INSERT DATA` | ✅ resolved via nameservice | ❌ vars not allowed in DATA | ✅ minted as fresh Sid for the operation | ✅ fresh blank reifier minted | ❌ DATA accepts only the `~ {\| \|}` form (semantically equivalent) |
| `DELETE DATA` | ✅ addresses an existing reifier by stable IRI | ❌ vars not allowed in DATA | ❌ rejected per SPARQL §3.1.3 — blank nodes have no addressable identity in `DELETE DATA` | ❌ rejected (same reason) — use `DELETE WHERE` with a binding instead | ❌ same as `INSERT DATA` |
| `INSERT { } WHERE { }` template | ✅ resolved | ✅ var bound by WHERE; resolves per solution | ✅ per-solution fresh blank (same label across the template = same per-solution blank) | ✅ per-solution fresh blank | ❌ INSERT templates accept only `~ {\| \|}` |
| `DELETE { } WHERE { }` template | ✅ resolved | ✅ required — the only addressable identity in a DELETE template | ❌ blank nodes forbidden in DELETE templates per SPARQL §3.1.3 | ❌ rejected — anonymous reifier has no addressable identity. Use a named `~ ?ann` bound by WHERE. | ❌ same |
| `DELETE { } INSERT { } WHERE { }` | DELETE clause follows DELETE rules; INSERT clause follows INSERT rules; WHERE follows query rules | Variable bound by WHERE; usable in both clauses | DELETE: rejected. INSERT: per-solution blank | DELETE: rejected. INSERT: per-solution blank | ❌ |

The reserved-predicate firewall fires across all UPDATE entry points: `INSERT DATA { _:a f:reifiesSubject ex:b }` is rejected at parse time with an error pointing at `@annotation` / the `~ {| |}` syntax. Mint annotations only through the supported surface forms.

### Querying annotations from SPARQL

Three query shapes, each backed by a different sidecar lookup:

**Inline anonymous** — match base edge, match metadata, no reifier identity needed:

```sparql
PREFIX ex: <http://example.org/>
SELECT ?role WHERE {
  ex:alice ex:worksFor ex:acme {| ex:role ?role |} .
}
```

**Inline with bound reifier** — one row per parallel annotation on the edge:

```sparql
PREFIX ex: <http://example.org/>
SELECT ?ann ?role WHERE {
  ?p ex:worksFor ex:acme ~ ?ann {| ex:role ?role |} .
}
```

**Annotation-rooted via `rdf:reifies`** — filter by metadata, return reified-edge endpoints:

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ex:  <http://example.org/>
SELECT ?person ?org WHERE {
  ?ann rdf:reifies <<( ?person ex:worksFor ?org )>> .
  ?ann ex:role "Engineer" .
}
```

Sibling triples about the reifier (here `?ann ex:role "Engineer"`) live in the surrounding scope and join via the standard executor — they do **not** need to live inside the `<<( ... )>>` term.

#### Blank nodes in `WHERE` clauses

Per SPARQL §4.1.4, a blank-node label in a `WHERE` clause is a **non-distinguished variable** — bindable inside the BGP but not exposable via `SELECT`. The same rule applies to reifiers: `?p ex:worksFor ex:acme ~ _:ann { ... }` lets `_:ann` join across the BGP but does not surface in the result.

Anonymous annotation blocks (`{| |}` without `~`) lower to a fresh non-distinguished variable internally (the formatter hides it from `SELECT *` so query output stays clean).

#### Lifecycle coupling

Fluree's annotation is *lifecycle-coupled* to an asserted edge: the annotation describes a triple that's currently in the graph. RDF 1.2 also allows reifiers for unasserted propositions ("X claims Alice works for Acme without us asserting it"). That mode is **not supported in v1** — see *Current limits* below.

### Deferred SPARQL shapes (rejected at parse time)

These produce a clear error with a span pointing at the offending construct:

- **Triple terms outside `rdf:reifies`.** `ex:doc ex:mentions <<( :s :p :o )>>` is rejected. Use a separate annotation subject with `rdf:reifies` if you need to refer to a triple.
- **Nested triple terms.** `<<( :s :p <<( :a :b :c )>> )>>` is rejected.
- **Multi-triple reifiers.** One reifier identifier reifying more than one triple term in the same scope is rejected. A reifier corresponds to one edge occurrence.
- **Annotation on a property-path triple.** `?s ex:p1/ex:p2 ?o {| ... |}` is rejected — the grammar only attaches annotations to simple-predicate triples.
- **SPARQL `CONSTRUCT` template projecting annotation metadata.** Until the Turtle-star vs RDF 1.2 reifier output decision lands, a CONSTRUCT template containing an annotation tail or `rdf:reifies` returns `UnsupportedFeature`. CONSTRUCT *without* annotation in the template still works even when the WHERE pattern uses annotations to filter.

Annotations on literal-valued objects (plain, typed, and language-tagged) are supported on **both** the JSON-LD and SPARQL UPDATE write surfaces — the SPARQL path emits `f:reifiesLang` for language-tagged objects so the stored reifier matches the base edge.

### Legacy Fluree-specific `<< s p ?o >>` syntax

Fluree predates RDF 1.2. The bare `<< s p o >>` SPARQL-star quoted-triple form (without parens) remains valid for the **Fluree-specific** `f:t` / `f:op` flake-metadata extraction:

```sparql
PREFIX f:  <https://ns.flur.ee/db#>
PREFIX ex: <http://example.org/>
SELECT ?age ?t ?op WHERE {
  << ex:alice ex:age ?age >> f:t ?t ; f:op ?op .
}
```

This binds `?t` to the transaction time and `?op` to the assert/retract flag of the matched flake. It is **not** edge annotations and is unrelated to the RDF 1.2 reifier surface above. Use `<<( ... )>>` (parenthesized) and `{| ... |}` for edge annotations; use bare `<< ... >>` only for `f:t` / `f:op`.

The bare-quoted-triple form combined with an annotation tail (`<< :s :p :o >> :pred :obj {| ... |}`) is rejected at parse time — the two surfaces don't compose.

## Current limits

Today's surface covers the common LPG / RDF-star use cases. The following are not yet supported and produce a clear validation error rather than silent partial behavior:

- **Annotations on list-occurrence triples.** `@list` membership is in scope as a future extension; the on-disk format already reserves space for it. Today, annotating a list element is rejected at parse time.
- **Reifiers for unasserted triples.** `@reifies` must point at an asserted edge. Pure-proposition reification (claims about triples that are not in the graph) is deferred.
- **Reifiers for multiple triples.** One annotation subject corresponds to one edge. Reifying several unrelated triples from a single annotation isn't allowed.
- **Triple terms as object values.** `ex:doc ex:mentions << ex:s ex:p ex:o >>` is not yet a representable value. Use a separate annotation subject.
- **Non-JSON-LD output.** v1 emits annotations in JSON-LD output. Turtle, TriG, N-Quads, and SPARQL CONSTRUCT need a separate surface-form decision (Turtle-star vs RDF 1.2 reifier vs other) and currently return `UnsupportedFeature` when a CONSTRUCT against those targets projects annotation metadata.
- **SPARQL 1.2 triple-term functions and constructor.** `TRIPLE`, `SUBJECT`, `PREDICATE`, `OBJECT`, `isTRIPLE`, and the `BIND(<<( ?s ?p ?o )>> AS ?t)` triple-term constructor are deferred — they presuppose triple terms as first-class values, which v1's LPG model does not represent. The compact reifier delimiter `<< s p o ~ r >>` is also deferred (it collides with the legacy `f:t`/`f:op` extraction form above); use the supported `s p o ~ r {| ... |}` annotation tail instead.

The mandated SPARQL 1.2 `VERSION "1.2"` prologue declaration is **accepted** (lex-and-skipped): the RDF 1.2 surface runs ungated, so a conformant 1.2 client that emits the declaration parses normally.

## Storage and indexing — the short version

- A ledger with no annotations creates no annotation artifacts. The annotation arena is `Option<...>` on the index root and is omitted entirely when unused.
- Plain triple queries take exactly the same plan they did before annotations existed; the planner only routes through the attachment indexes when the query mentions `@annotation`, `@reifies`, or RDF-star quoted triples.
- Annotation properties are stored as ordinary RDF facts. Time travel, policy, history, export, and reasoning all work on them without special cases.
- Retraction cascade has a fast path: when both the index root and current novelty know the ledger has no annotations, base-edge retracts skip the attachment lookup entirely.
- Branch fork, pack/sync, and ledger drop all walk annotation arena artifacts as part of the index reachability set, so annotated ledgers round-trip cleanly across these operations.

For the index format, sidecar layout, sort orders, and garbage-collection treatment, see the [Edge annotations design doc](../design/edge-annotations.md).

## Cypher / LPG compatibility

The storage primitive supports the property-graph shape directly:

```cypher
CREATE
  (:Person {id: "alice"})
    -[:WORKS_FOR {role: "Engineer", confidence: 0.97}]->
  (:Org {id: "acme"})
```

lowers to the same shape as the JSON-LD insert above. Parallel relationships round-trip:

```cypher
MATCH (a)-[r:WORKS_FOR]->(b) RETURN a, b, r.role
```

returns one row per relationship occurrence. Relationship variable `r` corresponds to the annotation subject.

A full Cypher front-end is its own workstream — path values, `MERGE`, pattern comprehensions, and so on still need language and runtime work — but the storage layer is in place and the JSON-LD `@annotation` surface is the same primitive Cypher imports use.

## See also

- [Edge annotations design](../design/edge-annotations.md) — storage internals (EdgeKey, sidecar arena, sticky-bit state machine, GC reachability).
- [Datasets and named graphs](datasets-and-named-graphs.md) — annotations work in named graphs as well as the default graph.
- [Time travel](time-travel.md) — annotation events live in history like every other fact.
- [Policy enforcement](policy-enforcement.md) — annotation properties pass through normal policy checks.
