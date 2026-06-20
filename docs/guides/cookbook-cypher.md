# Cookbook: Cypher

Practical recipes for querying and writing Fluree with [openCypher](../concepts/cypher.md).
Cypher runs on the same engine as JSON-LD and SPARQL, and Cypher relationships
are stored as [edge annotations](../concepts/edge-annotations.md) — so data
written here is readable from every surface.

Examples assume a ledger whose default `@context` maps the bare names
(`Person`, `KNOWS`, `name`, …) — see [IRI mapping](../concepts/cypher.md#iri-mapping-for-bare-identifiers).
For how to send these statements (Rust / CLI / HTTP), see
[Running Cypher](../concepts/cypher.md#running-cypher).

## Model a property graph

Create nodes with labels and properties, and a relationship that carries its own
properties:

```cypher
CREATE (alice:Person {name: "Alice", age: 34})
CREATE (acme:Org {name: "Acme"})
```

```cypher
MATCH (a:Person {name: "Alice"}), (o:Org {name: "Acme"})
CREATE (a)-[:WORKS_FOR {role: "Engineer", since: 2024}]->(o)
```

The relationship's properties (`role`, `since`) live on the edge, not on a
hand-modeled intermediate node.

## Query relationships

```cypher
MATCH (p:Person)-[r:WORKS_FOR]->(o:Org)
RETURN p.name, r.role, o.name
```

> **Plain vs. reified edges.** Binding a relationship variable (`-[r:T]->`) or
> filtering on its properties reads the *reified* edge (bag semantics, one row
> per relationship). Without the variable (`-[:T]->`) you get the plain triple
> (set semantics) and see every base edge, including ones not written through
> Cypher/`@annotation`. See [relationship lowering](../concepts/cypher.md#relationship-lowering--three-shapes-three-behaviors).

Optional matches and filters:

```cypher
MATCH (p:Person)
OPTIONAL MATCH (p)-[:WORKS_FOR]->(o:Org)
WHERE p.age > 30
RETURN p.name, o.name
```

## Find-or-create with MERGE

`MERGE` creates the pattern only if it doesn't already exist, with optional
`ON CREATE` / `ON MATCH` branches:

```cypher
MERGE (p:Person {email: "alice@example.com"})
ON CREATE SET p.created = 2024
ON MATCH SET p.lastSeen = 2024
RETURN p
```

`MERGE` also works on a relationship. Standalone, the whole path is the match
key and missing endpoints are created:

```cypher
MERGE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})
```

With a leading `MATCH` binding the endpoints it is a per-row find-or-create —
the edge is added only for matched pairs that don't already have it (idempotent
on re-run):

```cypher
MATCH (a:Person), (b:Person) WHERE a.name <> b.name
MERGE (a)-[:KNOWS]->(b)
```

## Update and delete

```cypher
MATCH (p:Person {name: "Alice"}) SET p.age = 35, p:Verified
MATCH (p:Person {name: "Alice"}) REMOVE p.age
MATCH (p:Person {name: "Bob"}) DETACH DELETE p
```

`SET p += {a: 1, b: 2}` merges a map of properties; `SET p:Label` adds a label;
`DETACH DELETE` removes a node together with its relationships.

## Paths

Variable-length traversal (name the relationship type):

```cypher
MATCH (a:Person {name: "Alice"})-[:KNOWS*1..3]->(b:Person)
RETURN DISTINCT b.name
```

Shortest path between two people, and its length:

```cypher
MATCH (a:Person {name: "Alice"}), (b:Person {name: "Dan"})
MATCH p = shortestPath((a)-[:KNOWS*]->(b))
RETURN length(p), nodes(p)
```

`allShortestPaths(...)` returns one row per minimal-length path; `nodes(p)` and
`pathPairs(p)` are list-valued, so they feed `UNWIND` and the list functions.

## Aggregate and collect

```cypher
MATCH (p:Person)-[:WORKS_FOR]->(o:Org)
RETURN o.name, count(p) AS headcount, collect(p.name) AS people
ORDER BY headcount DESC
```

`UNWIND` expands a list back into rows — e.g. the nodes along a path:

```cypher
MATCH p = shortestPath((a:Person {name: "Alice"})-[:KNOWS*]->(b:Person {name: "Dan"}))
UNWIND nodes(p) AS person
RETURN person.name
```

## Cross-surface round-trip

Because Cypher relationships are edge annotations, a relationship written in
Cypher is visible to JSON-LD and SPARQL, and vice versa:

```cypher
CREATE (a:Person {name: "Alice"})-[:WORKS_FOR {role: "Engineer"}]->(o:Org {name: "Acme"})
```

reads back through the SPARQL 1.2 annotation tail or the JSON-LD `@annotation`
surface as the same edge with the same `role` metadata — see
[Edge annotations](../concepts/edge-annotations.md).

## See also

- [Cypher (concept)](../concepts/cypher.md) — supported surface, RDF mapping,
  and what's [not yet supported](../concepts/cypher.md#not-yet-supported).
- [Edge annotations](../concepts/edge-annotations.md) — the storage model behind
  Cypher relationships.
- [Query patterns](cookbook-query-patterns.md) — the generic operators
  (`unwind`, `collect`, shortest path) in JSON-LD.
