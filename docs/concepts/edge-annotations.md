# Edge Annotations

Edge annotations let you attach properties to a *relationship* — the connection between two subjects — without modeling an intermediate node by hand. A property graph user calls these "edge properties" or "relationship properties." A SPARQL user calls them "annotations on a quoted triple." A Fluree user gets one ergonomic surface that reads correctly from either side.

```text
ex:alice ──[ ex:worksFor: { role: "Engineer", since: 2024-01-01, confidence: 0.97 } ]──▶ ex:acme
```

Annotations are first-class RDF data, stored as RDF 1.2 defines them: the annotation subject's `rdf:reifies` link to the triple it describes, `ex:claim1 rdf:reifies <<( ex:alice ex:knows ex:bob )>>`, plus its properties as ordinary triples. All of it participates in policy, history, indexing, and query like everything else. The fact indexes (and queries that don't ask for annotations) are unchanged.

For the storage-internals view — how the link is written, indexed through the term dictionary, and kept consistent with its edge — see the [Edge annotations design doc](../design/edge-annotations.md).

## When to use edge annotations

Reach for `@annotation` when you need any of these:

- **Property-graph-shaped edges.** A `worksFor` relationship needs `role` and `since`. Modeling that as a separate `Employment` node works but distorts the graph.
- **Provenance / quality on a fact.** "This `ex:hasAuthor` claim has confidence 0.97 from source X." Classic RDF reification with quoted triples.
- **Multiple parallel relationships** between the same two subjects — e.g. Alice was both an Engineer and later a Manager at Acme. Plain RDF can't distinguish two `ex:worksFor` triples; annotations can.
- **Property-graph imports.** Relationship properties round-trip without forcing every edge through an intermediate `:Relationship` node.

If a fact is naturally about a *node* (Alice's birthdate, Acme's industry), put it on the node — not on the edge. Annotations are for facts about the *relationship*.

## Where can I write edge annotations?

| Surface | How | Notes |
|---|---|---|
| **JSON-LD insert / upsert / update** | `@annotation` (or alias `@edge`) on a value object | Most ergonomic. Covers literal-valued edges (with explicit `@type` / `@language`), parallel annotations, named reifiers, and **named-graph edges** (an annotation on an edge inside a named graph is written into that same graph, keeping the edge's graph identity). A node's `@reifies` (`{"@id": s, p: o}`, or an array of them) makes the node a reifier of that triple **without asserting it**, as `r rdf:reifies <<( s p o )>>` does. |
| **SPARQL 1.2 UPDATE** | `INSERT DATA { :s :p :o {\| ... \|} }`, `~ <reifier>`, optional `INSERT { } WHERE { }` templates | Use this when integrating with SPARQL pipelines or when porting from RDF 1.2 / SPARQL-star. Works inside `GRAPH { }` blocks and under `WITH <g>`; `<< :s :p :o ~ :r >>` reifies without asserting. See [SPARQL 1.2 surface](#sparql-12--rdf-12-surface) below for the per-operation rules. |
| **Turtle / N-Triples / TriG / N-Quads ingest** (`insert`, `upsert`, bulk `import`, graph sync (`fluree sync`, `/sync`), memory import; TriG via `insert` / `upsert` / `import` / `fluree sync` / `/sync`) | RDF 1.2 forms: the annotation syntax `:s :p :o ~ <reifier> {\| ... \|}`, `<< :s :p :o ~ :r >>` in subject or object position, and the canonical `:r rdf:reifies <<( :s :p :o )>>` (the only star spelling N-Triples and N-Quads have) — in the default graph and inside TriG `GRAPH { }` blocks alike | Same stored link as `@annotation`. An annotation inside a `GRAPH { }` block, or on an N-Quads statement with a graph label, is written into that graph with the edge's graph identity, exactly as JSON-LD `@graph` + `@annotation` does. As in RDF 1.2, only the annotation syntax asserts the triple: `<< s p o >>` and `rdf:reifies <<( s p o )>>` reify it without asserting it, so N-Triples and N-Quads state an annotated triple as the triple plus its reifier's link. Rejected with a specific error: `<<( ... )>>` as a subject, star constructs inside an annotation body, annotations on collections, and annotations in a TriG `<#txn-meta>` block. Paths that convert to JSON-LD first (`upsert`, `graph sync`, memory import) also reject an annotation on an `rdf:type` edge. See [Turtle ingest](../transactions/turtle.md#edge-annotations-rdf-12--turtle-star). |

Mint annotations through `@annotation` / `@edge` (JSON-LD) or the RDF 1.2 forms (`~`, `{| |}`, `<< >>`, `rdf:reifies <<( )>>`) in SPARQL UPDATE and Turtle. The [`f:reifies*` predicates](../reference/vocabulary.md#edge-annotation-predicates-reserved) earlier releases stored annotations under are reserved, and the write surfaces reject them; bulk import reads them, as an export written before links carries them, and stores each annotation's link instead.

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

`@edge` is accepted as an alias for `@annotation`; the two are interchangeable.

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

### Minting an annotation in an update

The same `@annotation` form works inside an update's `insert` clause, so you can annotate edges selected by a `WHERE` pattern. Variables bound in `WHERE` are usable as the edge subject/object, and each solution mints its own annotation:

```json
{
  "@context": { "ex": "http://example.org/" },
  "where":  { "@id": "?person", "ex:worksFor": "?org" },
  "insert": {
    "@id": "?person",
    "ex:worksFor": { "@id": "?org", "@annotation": { "ex:role": "Staff" } }
  }
}
```

This is distinct from *editing* an existing annotation's metadata (below), which addresses the annotation subject directly by `@id`.

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

- **The value object must carry its `@type` / `@language` explicitly when the predicate's `@context` would otherwise coerce them.** When `@annotation` is present, the lowering pass rejects two coercion paths that the JSON-LD value-object expander applies: a term-level `@type` on the predicate's context entry, and a default `@language` on the active context. (Per-term `@language` overrides are intentionally ignored, mirroring the value-object expander's own behavior — it reads `context.language` directly, not the per-term entry.) The non-annotated form continues to use context coercion normally; this stricter rule applies only to annotated literals so the annotation's stored edge identity cannot silently diverge from the base flake's.
- **Language-tagged literals are language-pinned.** Two annotations on `"chat"@fr` and `"chat"@en` are independent; selector-form retracts and hydration both match on language.
- **Hydration promotes annotated literals to value-object form.** A subject expansion (`select: {"?s": ["*"]}`) renders unannotated `ex:name "Alice"` as the scalar `"Alice"`, but renders the annotated form as `{"@value": "Alice", "@annotation": {...}}` so the annotation has somewhere to attach.

The deferred shape from "Current limits" below (list occurrences) still applies on the literal path.

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

`@reifies` is the same idea as `rdf:reifies` in RDF 1.2 — given an annotation subject, walk to the edge it reifies. Fluree reads it, as it reads SPARQL's `?r rdf:reifies <<( s p o )>>` and `<< s p o ~ ?r >>`, from the reifier's `rdf:reifies` link, whose object is a triple term the index resolves through its term dictionary, so it's cheap regardless of how many annotations exist in the ledger. A policy that hides an edge hides its link too: the link is visible only when the triple it names would be.

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

This is what lets a property-graph traversal faithfully return parallel-edge rows while leaving plain RDF queries undisturbed.

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

The two forms differ in visibility: an anonymous annotation is an edge property you reach through its edge, while an explicit-IRI annotation is an ordinary RDF resource.

| | Anonymous (no `@id`) | Explicit `@id` |
|---|---|---|
| Visible in `select: "*"` | No — hidden from wildcard subject expansion | Yes |
| Visible in graph crawl | Only via `@annotation` projection | Yes, like any subject |
| Retract base edge → link and body removed | Only in LPG mode | Only in LPG mode |
| Re-assert a retracted edge → earlier claims return | Yes, outside LPG mode; remove them through `@reifies` or `<< s p o ~ ?r >>` | Yes, outside LPG mode; delete the reifier's triples |

The anonymous-hide rule means a user wildcard query against Alice doesn't suddenly start returning a sea of internal annotation SIDs once you adopt edge metadata. Annotations participate in queries that ask for them and stay out of the way otherwise.

## Retraction semantics

A transaction retracts the triples it names and nothing else, as RDF 1.2 defines it: deleting a triple does not delete the statements of a reifier that reifies it. A reifier's link (`r rdf:reifies <<( s p o )>>`) is a triple of its own, so it outlives the edge, and so does its body. Two read surfaces then disagree, on purpose:

- The **annotation syntax** (`s p o {| … |}`, `~`, JSON-LD `@annotation`, a Cypher relationship) asserts its triple, so it stops matching a claim once the edge is gone.
- The **reified-triple form** (`<< s p o ~ ?r >>`, `?r rdf:reifies <<( … )>>`, JSON-LD `@reifies`) reads the link alone and still finds it.

Opt into the property-graph lifecycle with [LPG mode](#lpg-mode-opt-in-per-transaction) when deleting an edge should delete its claims.

### Which spelling does what

**The annotation form asserts the base triple, so deleting it retracts the base edge.** Both `s p o ~ :r {| … |}` and the bare `s p o ~ :r` expand to the base triple *plus* the reification: RDF 1.2 Turtle §2.11.1 defines the syntax as one that both reifies **and asserts** a triple, and SPARQL 1.2 Update §3.1.2 admits the same production into `DELETE DATA`. So `DELETE DATA { :alice :knows :bob ~ :claim1 {| … |} }` retracts the edge, `:claim1`'s link and its body.

Seeded with one edge and two independent claims about it:

```turtle
:alice :knows :bob ~ :claim1 {| :confidence 0.8 ; :source :sourceA |} .
:alice :knows :bob ~ :claim2 {| :confidence 0.6 ; :source :sourceB |} .
```

| you write | edge | `:claim1` body | `:claim2` body | still linked | annotation syntax matches |
| --- | --- | --- | --- | --- | --- |
| `DELETE DATA { :alice :knows :bob ~ :claim1 {\| :confidence 0.8 ; :source :sourceA \|} }` | **gone** | gone | survives | `:claim2` | none |
| `DELETE DATA { :alice :knows :bob ~ :claim1 }` | **gone** | survives | survives | `:claim2` | none |
| `DELETE WHERE { :alice :knows :bob ~ ?c {\| :confidence ?f \|} }` | **gone** | `:source` only | `:source` only | none | none |
| `DELETE DATA { :alice :knows :bob }` | **gone** | survives | survives | both | none |
| `upsert` restating `:claim1` on a different object | object replaced | survives | survives | `:claim2` | none |
| `DELETE DATA { :claim1 :confidence 0.8 ; :source :sourceA }` | survives | gone | survives | both | `:claim2` |
| JSON-LD `delete` with `"@annotation": {"@id": ":claim1"}` | survives | survives | survives | `:claim2` | `:claim2` |
| `DELETE DATA { :alice :knows :bob {\| … \|} }` | *refused* — an anonymous block has no addressable identity to delete | | | | |

Three rows deserve calling out:

- **`~ :claim1` with no body block deletes the edge.** It reads like "detach claim1", but the annotation form asserts the triple, so the delete retracts it too, and the annotation syntax stops matching `:claim2` as well.
- **A variable reifier matches every claim on the edge.** `~ ?c {| :confidence ?f |}` retracts the link and the body properties the block names, from *all* of them, and the edge with them.
- **An `upsert` that changes the object retracts the old edge** with no delete written anywhere, and the claims left on it stop matching the annotation syntax.

So:

- To **withdraw one claim** and keep the edge, retract its link with the JSON-LD `@annotation` delete; retract its body as well if it should go.
- To **remove the edge and everything about it**, delete the edge in [LPG mode](#lpg-mode-opt-in-per-transaction).

History preserves every event — query at the pre-retract `t` and the annotation comes back, unchanged.

### LPG mode (opt-in per transaction)

For property-graph relationship lifecycle — "deleting the relationship deletes the relationship's properties" — set `lpgEdgeLifecycle: true` in transaction options. Cypher `DELETE` sets it.

```json
{
  "delete": {
    "@id": "ex:alice",
    "ex:worksFor": { "@id": "ex:acme" }
  },
  "opts": { "lpgEdgeLifecycle": true }
}
```

Retracting the edge now retracts every link naming it, and a reifier left with no link loses its body, whether it is anonymous or has an explicit `@id`.

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

In LPG mode, an empty block mints a fresh annotation subject — a property-less relationship still has identity, the way property-graph relationships do.

## SPARQL 1.2 / RDF 1.2 surface

`@annotation` lowers to the same on-disk model as the RDF 1.2 annotation tail. The equivalent **write** forms below produce identical storage; pick whichever is ergonomic for your input. Each asserts the triple and links its reifier to it; `@reifies` and `rdf:reifies <<( … )>>` write the link alone (see [Reifiers of unasserted triples](#reifiers-of-unasserted-triples)).

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
- `tripleTerm` (the parenthesized `<<( s p o )>>` form) is a value in object position: a reifier's triple under `rdf:reifies`, a stored value under any other predicate, or another triple term's object (see [Triple terms as values](#triple-terms-as-values)). As a subject it errors at parse time.
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

#### Annotations in `CONSTRUCT` output

A `CONSTRUCT` template can carry annotations into its result, written the same two ways:

```sparql
PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

CONSTRUCT { ?person ex:worksFor ?org ~ ?ann }
WHERE {
  ?person ex:worksFor ?org
  OPTIONAL { ?ann rdf:reifies <<( ?person ex:worksFor ?org )>> }
}
```

Every result format carries the link: `o ~ r` in Turtle and TriG, an `r rdf:reifies <<( s p o )>>` line in N-Triples and N-Quads, `@annotation` in JSON-LD, the `rdf:annotation` attribute in RDF/XML. A [Graph Store `GET`](../api/graph-store.md) returns a graph's annotations the same way, so a graph read and put back unchanged commits nothing. See [CONSTRUCT](../query/construct.md#edge-annotations-in-the-template) for template blocks and fresh reifiers.

#### Blank nodes in `WHERE` clauses

Per SPARQL §4.1.4, a blank-node label in a `WHERE` clause is a **non-distinguished variable** — bindable inside the BGP but not exposable via `SELECT`. The same rule applies to reifiers: `?p ex:worksFor ex:acme ~ _:ann { ... }` lets `_:ann` join across the BGP but does not surface in the result.

Anonymous annotation blocks (`{| |}` without `~`) lower to a fresh non-distinguished variable internally (the formatter hides it from `SELECT *` so query output stays clean).

#### One annotation, several edges

As in RDF 1.2, an annotation subject may reify several triples: a given `@id` on two edges is linked to both, and its properties describe each. To move an explicit-IRI annotation from one edge to another, retract the old attachment and assert the new one; a JSON-LD upsert of the annotation does that for you.

#### Reifiers of unasserted triples

A reifier may describe a triple that is not in the graph ("X claims Alice works for Acme", without asserting it). Write one with a reified triple — Turtle `<< :alice :worksFor :acme ~ :claim >> :source :x`, `:claim rdf:reifies <<( :alice :worksFor :acme )>>`, or JSON-LD:

```json
{
  "@id": "ex:claim",
  "ex:source": {"@id": "ex:x"},
  "@reifies": {"@id": "ex:alice", "ex:worksFor": {"@id": "ex:acme"}}
}
```

Reified-triple patterns (`<< :alice :worksFor ?o ~ ?r >>`, `?r rdf:reifies <<( … )>>`, JSON-LD `@reifies`) find such a reifier; the annotation syntax (`:alice :worksFor ?o {| … |}`, JSON-LD `@annotation`, a Cypher relationship) matches only asserted triples, as RDF 1.2 defines it. Export writes the reifier as its link (`@reifies` in JSON-LD), so it round-trips.

#### Triple terms as values

A triple term is also an ordinary value under any predicate. `ex:doc ex:mentions <<( ex:s ex:p ex:o )>>` stores the term itself: it neither asserts `ex:s ex:p ex:o` nor reifies it, so `?r rdf:reifies ?t` does not find `ex:doc`. Every write surface takes one in object position:

- Turtle, TriG, N-Triples and N-Quads: `<<( s p o )>>`.
- SPARQL UPDATE: `INSERT DATA`, `DELETE DATA`, `DELETE WHERE` and templates.
- JSON-LD: the triple's node as the value's `@id`, `"ex:mentions": {"@id": {"@id": "ex:s", "ex:p": {"@id": "ex:o"}}}`.

A query matches one as a constant (`?d ex:mentions <<( ex:s ex:p ex:o )>>`, or a `VALUES` row) or by its components (`?d ex:mentions <<( ex:s ?p ?o )>>`; in JSON-LD, `{"@id": {"@id": "ex:s", "ex:p": "?o"}}`), and `SUBJECT`, `PREDICATE` and `OBJECT` take a bound one apart (JSON-LD names them `subject`, `predicate` and `object`; see [JSON-LD query](../query/jsonld-query.md#triple-term-functions)). Results, CONSTRUCT output and exports write it back in the same forms (see [Output formats](../query/output-formats.md#triple-terms)), so it round-trips.

A triple term's object may itself be a triple term — `<<( ex:alice ex:says <<( ex:s ex:p ex:o )>> )>>` — on every surface above, and a triple whose object is a triple term can be annotated like any other. `sameTerm` compares two triple terms as terms; `=` compares them as values, so `<<( :a :b 123 )>> = <<( :a :b 123.0 )>>` holds while `sameTerm` does not.

### Deferred SPARQL shapes (rejected at parse time)

These produce a clear error with a span pointing at the offending construct:

- **Annotation on a property-path triple.** `?s ex:p1/ex:p2 ?o {| ... |}` is rejected — the grammar only attaches annotations to simple-predicate triples.
- **Property paths in a `CONSTRUCT` template's annotation.** A template annotation block (`{| ... |}`) takes simple predicates only.

Annotations on literal-valued objects (plain, typed, and language-tagged) are supported on **both** the JSON-LD and SPARQL UPDATE write surfaces — the SPARQL path records the language tag for language-tagged objects so the stored annotation matches the base edge.

### Legacy Fluree-specific `<< s p ?o >>` syntax

Fluree predates RDF 1.2. The bare `<< s p o >>` SPARQL-star quoted-triple form (without parens) remains valid for the **Fluree-specific** `f:t` / `f:op` flake-metadata extraction:

```sparql
PREFIX f:  <https://ns.flur.ee/db#>
PREFIX ex: <http://example.org/>
SELECT ?age ?t ?op WHERE {
  << ex:alice ex:age ?age >> f:t ?t ; f:op ?op .
}
```

This binds `?t` to the transaction time and `?op` to the assert/retract flag of the matched flake. It is **not** edge annotations and is unrelated to the RDF 1.2 reifier surface above.

The legacy reading is selected only by the predicate: a reifier-less `<< s p o >>` whose predicate is `f:t` or `f:op`. Any other predicate, or a `~ reifier`, gives the bare form its RDF 1.2 *reified triple* reading — `<< :s :p :o ~ ?r >> .` and `<< :s :p :o >> ?q ?z` denote the reifier node, exactly as `?r rdf:reifies <<( :s :p :o )>>` does. This applies to query `WHERE` patterns, Turtle ingest and SPARQL UPDATE alike.

The bare-quoted-triple form combined with an annotation tail (`<< :s :p :o >> :pred :obj {| ... |}`) is rejected at parse time — the two surfaces don't compose.

## Current limits

Today's surface covers the common LPG / RDF-star use cases. The following are not yet supported and produce a clear validation error rather than silent partial behavior:

- **Annotations on list-occurrence triples.** `@list` membership is in scope as a future extension; the on-disk format already reserves space for it. Today, annotating a list element is rejected at parse time.

The mandated SPARQL 1.2 `VERSION "1.2"` prologue declaration is **accepted** (lex-and-skipped): the RDF 1.2 surface runs ungated, so a conformant 1.2 client that emits the declaration parses normally.

## Storage and indexing — the short version

- An annotation is its `rdf:reifies` link plus its properties; a ledger with no annotations stores none and pays nothing for them.
- Plain triple queries take exactly the same plan they did before annotations existed; the planner only reads links when the query mentions `@annotation`, `@reifies`, `rdf:reifies` or a reified triple.
- Annotation properties are stored as ordinary RDF facts. Time travel, policy, history, export, and reasoning all work on them without special cases.
- The LPG-mode retraction cascade has a fast path: when both the index root and current novelty know the ledger has no annotations, base-edge retracts skip the link lookup entirely.
- An index built by an earlier Fluree version holds annotations in the `f:reifies*` form and no links, so on an annotated ledger annotation queries fail with an error asking for a rebuild until it is reindexed (`fluree reindex <ledger>`). The next index build after a new annotation is written is a full rebuild, which links them all.

For the term dictionary and the transaction-time rules, see the [Edge annotations design doc](../design/edge-annotations.md).

## Property-graph (LPG) model

Edge annotations are the storage primitive for the labeled-property-graph shape: a relationship that carries its own properties, and parallel relationships between the same two nodes. A property-graph relationship with properties maps directly onto an annotated edge —

- the relationship's properties become the `@annotation` body,
- the relationship's identity is the annotation subject (anonymous, or pinned with an explicit `@id`),
- two relationships of the same type between the same endpoints become two parallel annotations (see *Parallel annotations* above),
- *LPG mode* (`lpgEdgeLifecycle: true`) gives the "delete the relationship deletes its properties" lifecycle property graphs expect.

[Cypher](../query/cypher.md) is the property-graph query/write front-end: a Cypher relationship `(a)-[r:T {p: v}]->(b)` lowers to exactly this annotated-edge shape, so property-graph users get LPG ergonomics while the data stays first-class RDF. The JSON-LD `@annotation` surface and the SPARQL 1.2 annotation tail read and write the same shape.

## See also

- [Cypher](../query/cypher.md) — the property-graph front-end; Cypher relationships map onto edge annotations.
- [Edge annotations design](../design/edge-annotations.md) — storage internals (the link, the term dictionary, cascade, ledgers written before links).
- [Datasets and named graphs](datasets-and-named-graphs.md) — annotations work in named graphs as well as the default graph, on every write surface.
- [Time travel](time-travel.md) — annotation events live in history like every other fact.
- [Policy enforcement](policy-enforcement.md) — annotation properties pass through normal policy checks.
