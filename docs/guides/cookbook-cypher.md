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

## Shape rows as maps

Build a structured row with a map literal, or dump a node's properties:

```cypher
MATCH (p:Person {name: "Alice"})
RETURN {name: p.name, age: p.age} AS person

MATCH (p:Person {name: "Alice"})
RETURN properties(p) AS props, keys(p) AS fields
```

Map projection is shorthand for building a map from a node — `.key` selectors,
a computed entry, or `.*` for every property:

```cypher
MATCH (p:Person {name: "Alice"})
RETURN p{.name, .age, nextYear: p.age + 1} AS person

MATCH (p:Person {name: "Alice"})
RETURN p{.*} AS allProps
```

In the default cypher-json output a map is a native JSON object
(`{"name": "Alice", "age": 30}`). `properties(n)` returns only data properties
(not labels or relationships); `keys(n)` is their names. An object parameter is a
map value too — `$filter = {city: "NYC"}` can be passed and returned as-is.

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

## Compute or filter before a write with WITH

A `WITH` between the match and the write can carry a computed value into the
write or gate which rows are written:

```cypher
MATCH (a:Person {name: "Alice"})
WITH a, a.birthYear + 30 AS adultAt
SET a.adultAt = adultAt

MATCH (p:Person)
WITH p, p.age AS age WHERE age >= 30
SET p.adult = true
```

`WITH` narrows scope to its projection (Cypher semantics) — only the listed
names are visible to the write. Aggregation, `DISTINCT`, and `ORDER BY` /
`SKIP` / `LIMIT` on a write-side `WITH` are not yet supported.

## Transform lists with comprehensions, reduce, and predicates

```cypher
// Project + filter a list inline.
MATCH (p:Person)
RETURN [x IN range(1, 10) WHERE x % 2 = 0 | x * x] AS evenSquares

// Fold a list to a scalar.
RETURN reduce(total = 0, x IN [3, 5, 7] | total + x) AS sum

// Collect nodes, then map a property over them (loop-local property access).
MATCH (p:Person)
RETURN [x IN collect(p) | x.name] AS names

// Quantify over a list.
MATCH (p:Person)
WHERE all(t IN p.tags WHERE t <> "banned")
RETURN p.name
```

The loop variable is scoped to the body, and property access on it works for
both node elements (`x.name` scans the graph) and map elements
(`row.email` for `[row IN $people | row.email]`). A null or non-list input
yields null.

## Paths

Variable-length traversal (name the relationship type):

```cypher
MATCH (a:Person {name: "Alice"})-[:KNOWS*1..3]->(b:Person)
RETURN DISTINCT b.name
```

Untyped traversal — follow **any** relationship type per hop (handy for "who is
reachable from Alice within 3 hops, over any edge"):

```cypher
MATCH (a:Person {name: "Alice"})-[*1..3]->(b:Person)
RETURN DISTINCT b.name
```

Untyped paths follow only node→node relationships — they skip data properties,
`:Label` membership, and the edge-annotation sidecar — and use reachability
semantics (each node reachable within the hop range). Give the path a direction
(`-[*]->` or `<-[*]-`); undirected untyped paths aren't supported.

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
