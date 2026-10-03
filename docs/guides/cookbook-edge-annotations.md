# Cookbook: Edge Annotations

Edge annotations attach properties to a *relationship* — the connection between two subjects — without hand-modeling an intermediate node. They cover three overlapping needs on one surface: property-graph relationship properties, RDF-star / RDF 1.2 statement-level provenance, and parallel relationships between the same two subjects.

This guide is a set of working patterns. For the model, the cardinality contract, and the full boundary list, read the [Edge annotations concept doc](../concepts/edge-annotations.md) first; for the on-disk representation, see the [storage-internals design doc](../design/edge-annotations.md).

Throughout, the running example is employment: a `worksFor` edge that needs a `role`, a `since` date, and a `confidence`.

## Picking a surface

| You have… | Use | Why |
|---|---|---|
| JSON-LD writes, or you need named-graph edges, or literal-valued edges | **JSON-LD `@annotation`** | Most complete surface — covers everything below. |
| A SPARQL 1.1/1.2 pipeline, or you're porting RDF-star data | **SPARQL 1.2 annotation tail** (`{\| \|}`, `~`, `rdf:reifies`) | Standards syntax, in the default graph and named graphs. |
| A Turtle / N-Triples / TriG / N-Quads file with RDF 1.2 annotations | **Ingest it as-is** — `insert`, `upsert`, `import` and `graph sync` all accept `{\| \|}`, `~`, `<< >>` and `rdf:reifies <<( )>>`, and TriG `GRAPH { }` blocks accept them too (TriG via `insert` / `upsert` / `import` / `/sync`) | Same on-disk shape as `@annotation`; the annotation syntax asserts its triple, `<< >>` and `rdf:reifies` do not; re-`upsert` the file to update claim bodies (see [Turtle ingest](../transactions/turtle.md#edge-annotations-rdf-12--turtle-star)). |

## Attach metadata to a relationship

The `@annotation` block sits on the object node-map of the edge it describes. Everything inside is ordinary RDF stored on a fresh annotation subject.

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

This commits the base edge `ex:alice ex:worksFor ex:acme`, mints an (anonymous) annotation subject, attaches it to that edge, and writes `ex:role` / `ex:since` / `ex:confidence` on it. `@edge` is an interchangeable alias for `@annotation`.

## Query inline: edge first, metadata second

The query shape mirrors the write shape — match the edge, then pull or constrain its metadata.

```json
{
  "@context": { "ex": "http://example.org/" },
  "select": ["?person", "?org", "?role", "?since"],
  "where": {
    "@id": "?person",
    "ex:worksFor": {
      "@id": "?org",
      "@annotation": { "ex:role": "?role", "ex:since": "?since" }
    }
  }
}
```

One row per `(edge, annotation)` pair. **The plain edge pattern is undisturbed:** `?person ex:worksFor ?org` with *no* `@annotation` block still returns one row per edge, regardless of how many annotations hang off it. Annotations only multiply cardinality through the `@annotation` / `@reifies` keywords.

## Query annotation-rooted: metadata first, edge second

When you start from the metadata — "find every employment where `role = Engineer`" — walk back to the edge with `@reifies`.

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

`@reifies` resolves through the reverse attachment index, so it stays cheap no matter how many annotations the ledger holds. It is **query-side only** — using `@reifies` on a write is rejected; write with `@annotation`.

## Parallel relationships between the same two subjects

Plain RDF can't distinguish two `ex:worksFor` triples between Alice and Acme. Annotations can — Alice was an Engineer, then a Manager:

```json
{
  "@context": { "ex": "http://example.org/" },
  "insert": {
    "@graph": [
      { "@id": "ex:alice", "ex:worksFor": {
          "@id": "ex:acme",
          "@annotation": { "@id": "ex:emp/2020", "ex:role": "Engineer" }
      }},
      { "@id": "ex:alice", "ex:worksFor": {
          "@id": "ex:acme",
          "@annotation": { "@id": "ex:emp/2024", "ex:role": "Manager" }
      }}
    ]
  }
}
```

The inline query returns two rows:

```text
?person     ?org      ?role
ex:alice    ex:acme   Engineer
ex:alice    ex:acme   Manager
```

while `?person ex:worksFor ?org` (no annotation binding) still returns one.

## Provenance on a fact (including literal-valued edges)

The classic RDF-star use case: record where a claim came from and how confident you are. Annotations work on literal-valued edges too — write the literal as a JSON-LD value object so the annotation has a sibling key to attach to.

```json
{
  "@context": { "ex": "http://example.org/" },
  "insert": {
    "@id": "ex:alice",
    "ex:name": {
      "@value": "Alice",
      "@annotation": { "ex:source": "ex:hr-system", "ex:confidence": 0.92 }
    }
  }
}
```

A scalar (`"ex:name": "Alice"`) can't carry sibling metadata — the value-object form is required when annotating a literal. Typed and language-tagged literals follow the same rule (`@type` / `@language` plus `@annotation`), and language-tagged annotations are language-pinned: `"chat"@fr` and `"chat"@en` annotate independently.

## Stable annotation identity

Give an annotation an explicit `@id` when you need to reference, sign, or update it later — "the contract for Alice's 2024 employment":

```json
{
  "@id": "ex:alice",
  "ex:worksFor": {
    "@id": "ex:acme",
    "@annotation": {
      "@id": "ex:employment/alice-acme-2024",
      "ex:role": "Engineer"
    }
  }
}
```

Two inserts targeting the same explicit `@id` reattach to the same subject (idempotent). Two with no `@id` mint two distinct annotations. Explicit-IRI annotations are visible in `select: "*"` and graph crawls like any resource; anonymous ones stay hidden from wildcards and surface only through `@annotation`.

## Update annotation metadata

Once you've bound the annotation — by `@id` or by selector — it's an ordinary RDF subject. Bump Alice's confidence:

```json
{
  "@context": { "ex": "http://example.org/" },
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

## Retract an edge — and understand what stays

Retracting the base edge retracts that triple and nothing else, as RDF 1.2 defines it: the annotation's link and body stay, and the annotation syntax (`@annotation`, `{| |}`) stops matching it because it joins the edge. `@reifies` still finds it.

```json
{
  "delete": {
    "@id": "ex:alice",
    "ex:worksFor": { "@id": "ex:acme" }
  }
}
```

To delete the edge's annotations with it — the property-graph "delete the relationship deletes its properties" lifecycle — set LPG mode:

```json
{
  "delete": { "@id": "ex:alice", "ex:worksFor": { "@id": "ex:acme" } },
  "opts": { "lpgEdgeLifecycle": true }
}
```

History preserves every event either way — query at the pre-retract `t` and the annotation comes back. See [Retractions](../transactions/retractions.md#edge-annotation-cascade).

## The same patterns in SPARQL 1.2

The SPARQL 1.2 annotation tail lowers to the identical on-disk shape. A conformant `VERSION "1.2"` prologue is accepted (lexed and skipped).

**Write** (anonymous, or named with `~`):

```sparql
PREFIX ex: <http://example.org/>
INSERT DATA {
  ex:alice ex:worksFor ex:acme {| ex:role "Engineer" ; ex:since "2024-01-01" |} .
}
```

```sparql
PREFIX ex: <http://example.org/>
INSERT DATA {
  ex:alice ex:worksFor ex:acme ~ ex:emp1 {| ex:role "Engineer" |} .
}
```

**Query inline:**

```sparql
PREFIX ex: <http://example.org/>
SELECT ?role WHERE {
  ex:alice ex:worksFor ex:acme {| ex:role ?role |} .
}
```

**Query annotation-rooted** with `rdf:reifies` and a parenthesized triple term:

```sparql
PREFIX ex:  <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?person ?org WHERE {
  ?ann rdf:reifies <<( ?person ex:worksFor ?org )>> ;
       ex:role "Engineer" .
}
```

The triple term `<<( s p o )>>` is accepted **only** as the object of `rdf:reifies`. The parenthesis-free `<< s p o ~ :r >>` is a reified triple: it stands for its reifier and does not assert `s p o` (a reifier-less `<< s p o >>` under `f:t` / `f:op` is the separate flake-metadata construct). Per-operation reifier rules (variables are template-only; blank/anonymous reifiers are rejected in `DELETE DATA`) are tabulated in the [concept doc](../concepts/edge-annotations.md#sparql-update-rules-by-operation).

## Annotate an edge inside a named graph

Edge annotations live in the same graph as the edge they reify. On the JSON-LD surface, name the target graph with a node-level `@graph` selector — the annotation is written into that same graph and carries the graph identity automatically:

```json
{
  "@context": { "ex": "http://example.org/" },
  "insert": {
    "@id": "ex:alice",
    "@graph": "ex:hr-graph",
    "ex:worksFor": {
      "@id": "ex:acme",
      "@annotation": { "ex:role": "Engineer" }
    }
  }
}
```

> **Named graphs.** An annotation tail inside a `GRAPH { }` block or under a `WITH <g>` template writes the link and body into that graph, beside the triple.

## Keep a Turtle claims file in sync

The Turtle parser accepts RDF 1.2 annotation tails on every write path, so a claims file needs no conversion. `upsert` is the verb for keeping it in sync: each claim's body is replaced (the way upsert replaces any predicate value) while the edge and the reifier attachment stay put.

```bash
# claims.ttl
# @prefix ex: <http://example.org/> .
# ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 ; ex:source ex:hr |} .

curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
  -H "Content-Type: text/turtle" \
  --data-binary '@claims.ttl'
```

Name your reifiers (`~ ex:claim1`) when the file will be re-sent and you want the claim replaced. An anonymous `{| |}` block has no identity to replace by, so re-sending an unchanged file is a no-op on both `upsert` and `fluree sync`, but re-sending it with a *changed* annotation body replaces the claim on sync and adds a second claim on upsert. See the table in [Turtle transactions](../transactions/turtle.md#edge-annotations-rdf-12--turtle-star) for why the two differ.

The same claim can be layered on afterwards with SPARQL UPDATE when the annotations come from a separate process:

```bash
curl -X POST "http://localhost:8090/v1/fluree/update?ledger=mydb:main" \
  -H "Content-Type: application/sparql-update" \
  --data-binary @- <<'SPARQL'
PREFIX ex: <http://example.org/>
INSERT DATA {
  ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 ; ex:source ex:hr |} .
}
SPARQL
```

## Retract one claim and keep the edge

The natural-looking SPARQL form deletes more than the claim:

```sparql
# Retracts the base edge ex:alice ex:knows ex:bob — so the annotation
# syntax stops matching EVERY claim on that edge, not only ex:claim1.
DELETE DATA { ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 |} . }
```

`DELETE DATA` / `DELETE WHERE` with an annotation tail always retract the base edge ([Retractions](../transactions/retractions.md#edge-annotation-cascade)). To retract one claim and keep the edge, retract its link — the JSON-LD by-id retract, which removes exactly one attachment:

```json
{
  "@context": { "ex": "http://example.org/" },
  "delete": {
    "@id": "ex:alice",
    "ex:knows": { "@id": "ex:bob", "@annotation": { "@id": "ex:claim1" } }
  }
}
```

The edge and every other claim on it stay live. The named claim's body (`ex:confidence`, `ex:source`) survives as ordinary RDF about `ex:claim1` — retract it in the same transaction if it should go too; in LPG mode (`opts.lpgEdgeLifecycle: true`) the body is removed with the attachment.

## Gotchas

- **One annotation `@id` may reify several triples**, and a single edge carries many parallel annotations. To re-home an explicit-IRI annotation, retract the old attachment and assert the new one in the same transaction; a JSON-LD upsert of the annotation does that for you.
- **Deleting a claim with `DELETE DATA { … ~ :claim {| … |} }` deletes the edge**, so the annotation syntax stops matching every other claim on it. Retract one claim with the JSON-LD by-id form (see [above](#retract-one-claim-and-keep-the-edge)).
- **Don't write `f:reifies*` predicates by hand.** They're reserved and rejected on every write surface; they're also hidden from `?p` scans and `select: "*"`. Use `@annotation` / the annotation tail. (See [Vocabulary](../reference/vocabulary.md#edge-annotation-predicates-reserved).)
- **Empty `@annotation: {}`** is a no-op in RDF mode (no subject minted); in LPG mode it mints a property-less relationship with identity.
- **Not yet supported** (all reject cleanly, no silent partial results): annotations on `@list` elements, triple terms as object values, annotation output in Turtle/CONSTRUCT, and the SPARQL 1.2 triple-term functions (`TRIPLE`, `isTRIPLE`, …). See [Current limits](../concepts/edge-annotations.md#current-limits).

## See also

- [Edge annotations](../concepts/edge-annotations.md) — the full model and contract
- [Edge annotations storage internals](../design/edge-annotations.md) — on-disk representation
- [Insert](../transactions/insert.md#edge-annotations) and [Retractions](../transactions/retractions.md#edge-annotation-cascade) — write/lifecycle reference
- [JSON-LD query](../query/jsonld-query.md#edge-annotations) and [SPARQL](../query/sparql.md#edge-annotations-sparql-12--rdf-12) — query reference
