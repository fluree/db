# Graph Crawl

Graph crawl enables recursive traversal of relationships — following links between entities to discover connected data. This is built on **property paths**, which provide operators for transitive, inverse, and multi-predicate traversal.

## Overview

Graph crawl queries traverse relationships in the graph, following links from one entity to another. Common use cases:

- **Social networks** — Find friends-of-friends, influence chains
- **Organizational hierarchies** — Traverse reporting structures
- **Knowledge graphs** — Follow related concepts across multiple hops
- **Dependency graphs** — Trace transitive dependencies
- **Bill of materials** — Recursive part containment

## Property path operators

Property paths are the foundation of graph crawl. They let you follow relationships beyond a single hop.

| Operator | Syntax | Description | Example |
|---|---|---|---|
| One or more (`+`) | `ex:knows+` | Follow 1+ times (transitive closure) | Friends of friends |
| Zero or more (`*`) | `ex:knows*` | Follow 0+ times (includes self) | Self and all reachable |
| Inverse (`^`) | `^ex:reportsTo` | Follow in reverse direction | Who reports to me? |
| Alternative (`\|`) | `ex:knows\|ex:colleague` | Match any of several predicates | Social or professional connections |
| Sequence (`/`) | `ex:knows/ex:name` | Chain of predicates | Names of friends |

### JSON-LD Query syntax

Property paths are defined using `@path` in the `@context`:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "allReports": { "@path": "^ex:reportsTo+" }
  },
  "select": ["?name"],
  "where": [
    { "@id": "ex:ceo", "allReports": "?person" },
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

Two syntax forms are available:

**String form** (SPARQL-style):
```json
"knowsTransitive": { "@path": "ex:knows+" }
```

**Array form** (S-expression):
```json
"knowsTransitive": { "@path": ["+", "ex:knows"] }
```

### SPARQL syntax

Property paths are native SPARQL syntax:

```sparql
PREFIX ex: <http://example.org/>

# All people reachable through ex:knows (1+ hops)
SELECT ?person WHERE {
  ex:alice ex:knows+ ?person .
}
```

## Patterns

### Friend-of-friend network

Find everyone Alice knows, directly or transitively:

**SPARQL:**
```sparql
PREFIX ex: <http://example.org/>
PREFIX schema: <http://schema.org/>

SELECT ?name WHERE {
  ex:alice ex:knows+ ?person .
  ?person schema:name ?name .
}
```

**JSON-LD:**
```json
{
  "@context": {
    "ex": "http://example.org/",
    "schema": "http://schema.org/",
    "knowsTransitive": { "@path": "ex:knows+" }
  },
  "select": ["?name"],
  "where": [
    { "@id": "ex:alice", "knowsTransitive": "?person" },
    { "@id": "?person", "schema:name": "?name" }
  ]
}
```

### Organizational hierarchy

Find all people who report to a manager (at any level):

```sparql
PREFIX ex: <http://example.org/>
PREFIX schema: <http://schema.org/>

SELECT ?name WHERE {
  ?person ex:reportsTo+ ex:vp-engineering .
  ?person schema:name ?name .
}
```

Or use inverse path to start from the top:

```sparql
SELECT ?name WHERE {
  ex:vp-engineering ^ex:reportsTo+ ?person .
  ?person schema:name ?name .
}
```

### Class hierarchy (RDFS)

Find all subclasses of a class:

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?subclass WHERE {
  ?subclass rdfs:subClassOf+ ex:Vehicle .
}
```

### Path chaining (sequence)

Follow a chain of different predicates:

```sparql
PREFIX ex: <http://example.org/>
PREFIX schema: <http://schema.org/>

# Names of Alice's friends' managers
SELECT ?managerName WHERE {
  ex:alice ex:knows/ex:reportsTo ?manager .
  ?manager schema:name ?managerName .
}
```

In JSON-LD:
```json
{
  "@context": {
    "ex": "http://example.org/",
    "schema": "http://schema.org/",
    "friendManager": { "@path": "ex:knows/ex:reportsTo" }
  },
  "select": ["?managerName"],
  "where": [
    { "@id": "ex:alice", "friendManager": "?manager" },
    { "@id": "?manager", "schema:name": "?managerName" }
  ]
}
```

### Multi-relationship traversal

Follow any of several relationship types:

```sparql
PREFIX ex: <http://example.org/>
PREFIX schema: <http://schema.org/>

# People connected by friendship OR professional relationship
SELECT ?name WHERE {
  ex:alice (ex:knows|ex:colleague)+ ?person .
  ?person schema:name ?name .
}
```

### Self-inclusive traversal (zero or more)

Use `*` to include the starting node:

```sparql
PREFIX ex: <http://example.org/>
PREFIX schema: <http://schema.org/>

# Alice and everyone she knows (transitively)
SELECT ?name WHERE {
  ex:alice ex:knows* ?person .
  ?person schema:name ?name .
}
```

With `*`, Alice herself is included in results (zero hops). With `+`, only her connections are returned.

### Inverse relationships

Find who links to a given entity:

```sparql
PREFIX ex: <http://example.org/>
PREFIX schema: <http://schema.org/>

# Who has Alice as a friend?
SELECT ?name WHERE {
  ?person ex:knows ex:alice .
  ?person schema:name ?name .
}

# Same thing using inverse path syntax
SELECT ?name WHERE {
  ex:alice ^ex:knows ?person .
  ?person schema:name ?name .
}
```

Inverse paths are especially useful in transitive queries:

```sparql
# All ancestors in a taxonomy
SELECT ?ancestor WHERE {
  ex:goldRetriever rdfs:subClassOf+ ?ancestor .
}

# All descendants (inverse)
SELECT ?descendant WHERE {
  ex:animal ^rdfs:subClassOf+ ?descendant .
}
```

## Performance considerations

### Property path cost

| Operator | Cost | Notes |
|---|---|---|
| Simple predicate | O(log n) | Single index lookup |
| Sequence (`/`) | O(k * log n) | k joins, each indexed |
| One-or-more (`+`) | O(reachable * log n) | Breadth-first expansion |
| Zero-or-more (`*`) | O(reachable * log n) | Same as `+` plus start node |
| Alternative (`\|`) | O(sum of alternatives) | Each alternative evaluated |
| Inverse (`^`) | O(log n) | Uses OPST index |

Transitive operators (`+`, `*`) expand breadth-first and track visited nodes to detect cycles. The cost is proportional to the number of reachable nodes, not the total graph size.

### Optimizing traversals

1. **Start from the specific side** — If you know one endpoint, start there. `ex:alice ex:knows+ ?person` is faster than `?person ex:knows+ ex:alice` because it anchors the traversal.

2. **Add filters after traversal** — Filter the results of a traversal rather than trying to filter during:
   ```sparql
   SELECT ?name WHERE {
     ex:alice ex:knows+ ?person .
     ?person schema:name ?name .
     ?person ex:department "Engineering" .
   }
   ```

3. **Use `+` over `*` when possible** — `*` includes the start node and typically has one more step to evaluate.

4. **Prefer sequence over transitive for known depth** — If you know the relationship is exactly 2 hops, use a sequence (`ex:a/ex:b`) or two explicit patterns instead of `ex:a+`.

5. **Combine with LIMIT** — For exploration, limit results to avoid materializing the full reachable set:
   ```sparql
   SELECT ?person WHERE {
     ex:alice ex:knows+ ?person .
   } LIMIT 100
   ```

### Cycle handling

Fluree's property path engine tracks visited nodes during transitive expansion. If a cycle is encountered (e.g., A knows B knows C knows A), the traversal stops at the already-visited node. This prevents infinite loops without requiring user intervention.

## Property paths vs. explicit patterns

For fixed-depth queries, explicit patterns are equivalent and sometimes clearer:

**Property path (2 hops):**
```sparql
SELECT ?fof WHERE {
  ex:alice ex:knows/ex:knows ?fof .
}
```

**Explicit patterns (2 hops):**
```sparql
SELECT ?fof WHERE {
  ex:alice ex:knows ?friend .
  ?friend ex:knows ?fof .
}
```

Both produce the same results. Use property paths when:
- The depth is variable or unknown (transitive closure)
- You want compact syntax for chains
- You need alternative or inverse traversal

Use explicit patterns when:
- The depth is fixed and small
- You need to bind intermediate variables (e.g., `?friend` above)
- You want maximum clarity

## Related documentation

- [JSON-LD Query — Property Paths](jsonld-query.md#property-paths) — Full JSON-LD property path syntax
- [SPARQL — Property Paths](sparql.md) — SPARQL property path reference
- [Datasets](datasets.md) — Multi-graph traversal
- [Explain Plans](explain.md) — Understand query execution
