# Edge Annotations

Edge annotations let you attach properties to a *relationship* — the connection between two subjects — without modeling an intermediate node by hand. A property graph user calls these "edge properties" or "relationship properties." A SPARQL user calls them "annotations on a quoted triple." A Fluree user gets one ergonomic surface that reads correctly from either side.

```text
ex:alice ──[ ex:worksFor: { role: "Engineer", since: 2024-01-01, confidence: 0.97 } ]──▶ ex:acme
```

Annotations are first-class RDF data: properties on the annotation subject are stored as ordinary triples and participate in policy, history, indexing, and query like everything else. The only thing that's special is the *attachment*: a sidecar relation that records which annotation subject belongs to which edge. The fact indexes (and queries that don't ask for annotations) are unchanged.

For the storage-internals view of how annotations are indexed, see [`EDGE_ANNOTATIONS.md`](https://github.com/fluree/db/blob/main/EDGE_ANNOTATIONS.md) at the repository root.

## When to use edge annotations

Reach for `@annotation` when you need any of these:

- **Property-graph-shaped edges.** A `worksFor` relationship needs `role` and `since`. Modeling that as a separate `Employment` node works but distorts the graph.
- **Provenance / quality on a fact.** "This `ex:hasAuthor` claim has confidence 0.97 from source X." Classic RDF reification with quoted triples.
- **Multiple parallel relationships** between the same two subjects — e.g. Alice was both an Engineer and later a Manager at Acme. Plain RDF can't distinguish two `ex:worksFor` triples; annotations can.
- **Cypher / LPG imports.** Relationship properties round-trip without forcing every edge through an intermediate `:Relationship` node.

If a fact is naturally about a *node* (Alice's birthdate, Acme's industry), put it on the node — not on the edge. Annotations are for facts about the *relationship*.

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

## Relationship to RDF-star and RDF 1.2

`@annotation` lowers to the same model as RDF 1.2 reifiers. The following equivalent forms all produce the same storage shape:

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

JSON-LD `@reifies`:

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

SPARQL 1.2 / RDF 1.2 annotation block:

```sparql
PREFIX ex: <http://example.org/>
INSERT DATA {
  ex:alice ex:worksFor ex:acme {| ex:role "Engineer" |} .
}
```

SPARQL 1.2 / RDF 1.2 named reifier:

```sparql
PREFIX ex: <http://example.org/>
INSERT DATA {
  ex:alice ex:worksFor ex:acme ~ ex:emp1 {| ex:role "Engineer" |} .
}
```

SPARQL 1.2 / RDF 1.2 explicit reifier with `rdf:reifies` — **query only** in v1:

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ex:  <http://example.org/>
SELECT ?role WHERE {
  ?ann rdf:reifies <<( ex:alice ex:worksFor ex:acme )>> .
  ?ann ex:role ?role .
}
```

The `rdf:reifies` + triple-term form is accepted in SPARQL `WHERE` clauses only; SPARQL UPDATE (`INSERT DATA`, `DELETE DATA`, `INSERT WHERE` / `DELETE WHERE` templates) accepts only the `~ {| |}` annotation-tail form. Both forms are semantically equivalent — use `~` for inserts and updates.

(For DATA operations: anonymous `{| |}` and bare `_:` reifiers are allowed in `INSERT DATA` but rejected in `DELETE DATA` per SPARQL §3.1.3.)

### Querying annotations from SPARQL

The same three surface forms work in `WHERE` clauses. Inline:

```sparql
PREFIX ex: <http://example.org/>
SELECT ?role WHERE {
  ex:alice ex:worksFor ex:acme {| ex:role ?role |} .
}
```

With a bound reifier variable (one row per parallel annotation):

```sparql
PREFIX ex: <http://example.org/>
SELECT ?ann ?role WHERE {
  ?p ex:worksFor ex:acme ~ ?ann {| ex:role ?role |} .
}
```

Annotation-rooted via `rdf:reifies` — filter by metadata, return reified-edge endpoints:

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ex:  <http://example.org/>
SELECT ?person ?org WHERE {
  ?ann rdf:reifies <<( ?person ex:worksFor ?org )>> .
  ?ann ex:role "Engineer" .
}
```

Sibling triples about the reifier (here `?ann ex:role "Engineer"`) live in the surrounding scope and join via the standard executor — they do **not** need to live inside the `<<( ... )>>` term.

Fluree's annotation is *lifecycle-coupled* to an asserted edge: the annotation describes a triple that's currently in the graph. RDF 1.2 also allows reifiers for unasserted propositions ("X claims Alice works for Acme, without us asserting it"). That mode is not supported in v1 — see *Current limits* below.

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

## Current limits

Today's surface covers the common LPG / RDF-star use cases. The following are not yet supported and produce a clear validation error rather than silent partial behavior:

- **Annotations on literal-valued objects.** `@annotation` is only valid on `@id`-valued objects (asserted relationship triples). Annotating a string or number is rare in practice and has tricky datatype/language semantics that are still being worked out.
- **Annotations on list-occurrence triples.** `@list` membership is in scope as a future extension; the on-disk format already reserves space for it. Today, annotating a list element is rejected at parse time.
- **Reifiers for unasserted triples.** `@reifies` must point at an asserted edge. Pure-proposition reification (claims about triples that are not in the graph) is deferred.
- **Reifiers for multiple triples.** One annotation subject corresponds to one edge. Reifying several unrelated triples from a single annotation isn't allowed.
- **Triple terms as object values.** `ex:doc ex:mentions << ex:s ex:p ex:o >>` is not yet a representable value. Use a separate annotation subject.
- **Non-JSON-LD output.** v1 emits annotations in JSON-LD output. Turtle, TriG, N-Quads, and SPARQL CONSTRUCT need a separate surface-form decision (Turtle-star vs RDF 1.2 reifier vs other) and currently return `UnsupportedFeature` when a CONSTRUCT against those targets projects annotation metadata.

## Storage and indexing — the short version

- A ledger with no annotations creates no annotation artifacts. The annotation arena is `Option<...>` on the index root and is omitted entirely when unused.
- Plain triple queries take exactly the same plan they did before annotations existed; the planner only routes through the attachment indexes when the query mentions `@annotation`, `@reifies`, or RDF-star quoted triples.
- Annotation properties are stored as ordinary RDF facts. Time travel, policy, history, export, and reasoning all work on them without special cases.
- Retraction cascade has a fast path: when both the index root and current novelty know the ledger has no annotations, base-edge retracts skip the attachment lookup entirely.
- Branch fork, pack/sync, and ledger drop all walk annotation arena artifacts as part of the index reachability set, so annotated ledgers round-trip cleanly across these operations.

For the index format, sidecar layout, sort orders, and garbage-collection treatment, read [`EDGE_ANNOTATIONS.md`](https://github.com/fluree/db/blob/main/EDGE_ANNOTATIONS.md). For the implementation milestones, see `EDGE_ANNOTATIONS_IMPL_PLAN.md`.

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

- [`EDGE_ANNOTATIONS.md`](https://github.com/fluree/db/blob/main/EDGE_ANNOTATIONS.md) — design plan and storage internals.
- [Datasets and named graphs](datasets-and-named-graphs.md) — annotations work in named graphs as well as the default graph.
- [Time travel](time-travel.md) — annotation events live in history like every other fact.
- [Policy enforcement](policy-enforcement.md) — annotation properties pass through normal policy checks.
