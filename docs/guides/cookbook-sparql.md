# Cookbook: SPARQL

Practical recipes for querying and writing Fluree with [SPARQL](../query/sparql.md).
SPARQL 1.1 works exactly as you already know it, so this cookbook skips the
basics and focuses on what's specific to Fluree: **time travel**, **fact
history**, **edge annotations** (RDF 1.2 / SPARQL 1.2), and **cross-ledger**
queries. SPARQL runs on the same engine as JSON-LD and Cypher, so data written
through any surface is queryable here.

The running example is employment: an `ex:worksFor` edge that carries a `role`,
a `since` date, and a `confidence`. Examples declare prefixes explicitly; via the
CLI (or HTTP with `?default-context=true`) a query with no `PREFIX` line inherits
the ledger's [default context](../query/sparql.md#default-prefixes), so you can
often drop the common ones. For how to send these over CLI / HTTP, see
[Endpoint usage](../query/sparql.md#endpoint-usage).

## Travel back in time

Pin the default graph to a past state with a time specifier in `FROM` — no
snapshot to restore:

```sparql
PREFIX ex: <http://example.org/>
SELECT ?title
FROM <mydb:main@t:100>
WHERE {
  ex:alice ex:title ?title .
}
```

The specifier can be a transaction number (`@t:100`), an ISO datetime
(`@iso:2024-01-15T10:30:00Z`), a commit id (`@commit:bafy…`), or `@t:latest` for
the current head. Everything else about the query is unchanged — you're just
reading an earlier snapshot.

## Query the history of a fact

`FROM … TO` over a time range returns every assertion and retraction, with the
RDF-star term `<< s p o >>` exposing the per-flake metadata `f:t` (the
transaction) and `f:op` (`true` = asserted, `false` = retracted):

```sparql
PREFIX ex: <http://example.org/>
PREFIX f:  <https://ns.flur.ee/db#>
SELECT ?salary ?t ?op
FROM <mydb:main@t:1>
TO   <mydb:main@t:latest>
WHERE {
  << ex:alice ex:salary ?salary >> f:t ?t .
  << ex:alice ex:salary ?salary >> f:op ?op .
}
ORDER BY ?t
```

Each row is one change to Alice's salary over the range. Filter to just the
retractions with `FILTER(?op = false)`, or bound the window with ISO datetimes
(`FROM <mydb:main@iso:2024-01-01T00:00:00Z> TO <mydb:main@iso:2024-12-31T23:59:59Z>`).

> **Two `<<` forms, different jobs.** The bare `<< s p o >>` above is Fluree's
> flake-metadata term for `f:t` / `f:op`. The parenthesized `<<( s p o )>>` is
> the RDF 1.2 *triple term* used with `rdf:reifies` (next section). They don't
> compose.

## Annotate an edge

Attach metadata to a specific `(subject, predicate, object)` edge with the
SPARQL 1.2 annotation tail `{| … |}`. A conformant `VERSION "1.2"` prologue is
accepted (lexed and skipped):

```sparql
PREFIX ex: <http://example.org/>
INSERT DATA {
  ex:alice ex:worksFor ex:acme {| ex:role "Engineer" ; ex:since "2024-01-01" |} .
}
```

This commits the base edge and mints an anonymous annotation subject carrying
`ex:role` / `ex:since`. Name the reifier with `~` when you want to reference it
later:

```sparql
PREFIX ex: <http://example.org/>
INSERT DATA {
  ex:alice ex:worksFor ex:acme ~ ex:emp1 {| ex:role "Engineer" |} .
}
```

Annotation tails are accepted in `INSERT DATA`, `DELETE DATA`, and
`INSERT`/`DELETE … WHERE` templates.

## Query edge annotations

Match an edge, then pull or constrain its metadata with the same tail:

```sparql
PREFIX ex: <http://example.org/>
SELECT ?role WHERE {
  ex:alice ex:worksFor ex:acme {| ex:role ?role |} .
}
```

> **The plain edge is undisturbed.** `ex:alice ex:worksFor ?org` with *no* tail
> still returns one row per edge, no matter how many annotations hang off it.
> Annotations multiply cardinality only through the `{| |}` / `rdf:reifies`
> surface — so two parallel `worksFor` edges (Engineer, then Manager) between the
> same pair are distinguishable here, where plain RDF would collapse them.

Start from the metadata instead — "find every employment where `role =
Engineer`" — and walk back to the edge with `rdf:reifies` and a triple term:

```sparql
PREFIX ex:  <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?person ?org WHERE {
  ?ann rdf:reifies <<( ?person ex:worksFor ?org )>> ;
       ex:role "Engineer" .
}
```

The triple term `<<( s p o )>>` is accepted **only** as the object of
`rdf:reifies`.

## Update annotation metadata

Once an annotation is bound — by its `~` name or by selecting it through the
edge — it's an ordinary RDF subject. Bump Alice's confidence with a
`DELETE`/`INSERT … WHERE`:

```sparql
PREFIX ex:  <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
DELETE { ?ann ex:confidence ?old }
INSERT { ?ann ex:confidence 0.99 }
WHERE  {
  ?ann rdf:reifies <<( ex:alice ex:worksFor ex:acme )>> .
  OPTIONAL { ?ann ex:confidence ?old }
}
```

## Add annotations to data you already ingested

Turtle / N-Triples / TriG / N-Quads ingest paths don't parse annotation tails.
Load the plain edges first, then layer provenance onto the edges they already
created with a SPARQL UPDATE that repeats the same `(s, p, o)`:

```sparql
PREFIX ex: <http://example.org/>
INSERT DATA {
  ex:alice ex:worksFor ex:acme {| ex:source ex:hr-import ; ex:confidence 0.92 |} .
}
```

## Query across ledgers

`FROM` takes any number of ledgers to union as the default graph:

```sparql
PREFIX ex: <http://example.org/>
SELECT ?name
FROM <hr:main>
FROM <payroll:main>
WHERE { ?person ex:name ?name }
```

Keep each ledger addressable with `FROM NAMED` + a `GRAPH` block:

```sparql
PREFIX ex: <http://example.org/>
SELECT ?g ?name
FROM NAMED <hr:main>
FROM NAMED <payroll:main>
WHERE { GRAPH ?g { ?person ex:name ?name } }
```

> **On the ledger-scoped endpoint** (`POST /query/{ledger}`), `GRAPH` patterns
> resolve the ledger's registered named graphs with no `FROM NAMED` needed —
> `SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }` discovers them. Supplying
> `FROM NAMED` narrows resolution to exactly the graphs listed. The reserved
> system graphs (`#txn-meta`, `#config`) stay addressable only via explicit
> `FROM NAMED`.

## Cross-surface round-trip

Because all three surfaces store edge annotations the same way, an annotation
written here reads back through JSON-LD `@annotation` and Cypher relationship
properties as the same edge with the same metadata:

```sparql
PREFIX ex: <http://example.org/>
INSERT DATA {
  ex:alice ex:worksFor ex:acme ~ ex:emp1 {| ex:role "Engineer" |} .
}
```

is the same edge a Cypher `(:Person)-[:worksFor {role: "Engineer"}]->(:Org)` read
returns — see [Edge annotations](../concepts/edge-annotations.md).

## Gotchas

- **Annotations are default-graph only.** A tail inside an explicit `GRAPH { }`
  block or under `WITH <g>` is rejected — use the JSON-LD `@annotation` surface
  to annotate an edge inside a named graph.
- **Simple-predicate edges only.** A tail on a property-path edge
  (`?s ex:p1/ex:p2 ?o {| … |}`) is rejected.
- **No annotations in `CONSTRUCT` templates** (output form deferred); a
  `CONSTRUCT` whose `WHERE` *filters* on annotations still works.
- **SPARQL 1.2 triple-term functions** (`TRIPLE`, `isTRIPLE`, `SUBJECT`, … and
  the `BIND(<<( ?s ?p ?o )>> AS ?t)` constructor) are deferred.
- **Don't write the `f:reifies*` system predicates by hand** — they're reserved
  and rejected on every write surface. Mint annotations only through `~` / `{| |}`.

## See also

- [SPARQL (reference)](../query/sparql.md) — the full query/UPDATE surface.
- [Edge annotations](../concepts/edge-annotations.md) — the model behind the
  annotation tail, shared across all three query surfaces.
- [Cypher cookbook](cookbook-cypher.md) and [Query patterns](cookbook-query-patterns.md)
  — the same recipes from the property-graph and JSON-LD angles.
- [Time travel patterns](cookbook-time-travel.md) — point-in-time and history
  across surfaces.
