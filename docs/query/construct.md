# CONSTRUCT Queries

CONSTRUCT queries generate RDF graphs from query results, enabling you to transform and reshape data into new graph structures.

## Overview

CONSTRUCT queries return RDF graphs instead of variable bindings. They're useful for:
- Extracting subgraphs
- Transforming data structures
- Creating new graph views
- Generating RDF for export

## Basic CONSTRUCT

### SPARQL CONSTRUCT

```sparql
PREFIX ex: <http://example.org/ns/>

CONSTRUCT {
  ?person ex:displayName ?name .
}
WHERE {
  ?person ex:name ?name .
}
```

This generates a new graph with `ex:displayName` properties from `ex:name` values.

### Shorthand Form (`CONSTRUCT WHERE`)

When the template is identical to the WHERE pattern, omit the template:

```sparql
PREFIX ex: <http://example.org/ns/>

CONSTRUCT WHERE { ?s ex:name ?o }
```

Per the SPARQL 1.1 grammar, the shorthand WHERE block is a **basic graph
pattern of triple patterns only**. `FILTER`, `GRAPH`, `OPTIONAL`, `BIND`,
`UNION`, and sub-`SELECT` are rejected as syntax errors in this position — use
the explicit-template form (`CONSTRUCT { ... } WHERE { ... }`) when you need
them.

### JSON-LD Query

A JSON-LD query builds a graph with a `construct` template in place of `select`: node maps
shaped like the `where` clause, whose variables are filled from each solution.

```json
{
  "@context": {"ex": "http://example.org/ns/"},
  "where": [{"@id": "?person", "ex:name": "?name"}],
  "construct": [{"@id": "?person", "ex:displayName": "?name"}]
}
```

`"construct": true` uses the `where` clause's triples as the template, like SPARQL's
`CONSTRUCT WHERE`.

### Multiple Triples

Construct multiple triples per solution:

```sparql
PREFIX ex: <http://example.org/ns/>

CONSTRUCT {
  ?person ex:displayName ?name .
  ?person ex:hasAge ?age .
}
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
}
```

## Complex Patterns

### Conditional Construction

Use filters to conditionally construct triples:

```sparql
PREFIX ex: <http://example.org/ns/>

CONSTRUCT {
  ?person ex:status ex:Adult .
}
WHERE {
  ?person ex:age ?age .
  FILTER (?age >= 18)
}
```

### Transitive Relationships

Construct inferred relationships:

```sparql
PREFIX ex: <http://example.org/ns/>

CONSTRUCT {
  ?person ex:knows ?friendOfFriend .
}
WHERE {
  ?person ex:friend ?friend .
  ?friend ex:friend ?friendOfFriend .
}
```

## CONSTRUCT with Aggregation

Construct triples from aggregated data:

```sparql
PREFIX ex: <http://example.org/ns/>

CONSTRUCT {
  ?category ex:productCount ?count .
}
WHERE {
  {
    SELECT ?category (COUNT(?product) AS ?count)
    WHERE {
      ?product ex:category ?category .
    }
    GROUP BY ?category
  }
}
```

## Named Graphs in the Template

A `GRAPH` block in the template writes its triples into a named graph, so the result is a
dataset rather than a single graph. The graph can be a constant or a variable bound by the
`WHERE` clause. This is an extension to SPARQL 1.1 (Apache Jena ARQ supports the same form).

```sparql
PREFIX ex: <http://example.org/ns/>

# Copy every named graph, keeping each triple in its graph
CONSTRUCT { GRAPH ?g { ?s ?p ?o } }
WHERE { GRAPH ?g { ?s ?p ?o } }
```

```sparql
PREFIX ex: <http://example.org/ns/>

# Mix the default graph and a named graph
CONSTRUCT {
  ?person a ex:Named .
  GRAPH ex:names { ?person ex:name ?name }
}
WHERE { ?person ex:name ?name }
```

In a JSON-LD query, the block has the `where` clause's graph form, `["graph", <graph IRI or
?var>, node-map, ...]`:

```json
{
  "@context": {"ex": "http://example.org/ns/"},
  "where": [{"@id": "?person", "ex:name": "?name"}],
  "construct": [
    {"@id": "?person", "@type": "ex:Named"},
    ["graph", "ex:names", {"@id": "?person", "ex:name": "?name"}]
  ]
}
```

Only formats that can express named graphs carry a dataset: TriG, N-Quads and JSON-LD (see
[Output Formats](#output-formats)). A row that leaves the graph variable unbound writes nothing.

## Edge Annotations in the Template

A template triple can carry an RDF 1.2 annotation, so the result links a reifier to the edge
(see [Edge annotations](../concepts/edge-annotations.md)). Write it as an annotation tail,
`~ reifier` and an optional `{| ... |}` body, or as `?r rdf:reifies <<( s p o )>>`:

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

# Carry each edge's stored reifiers into the result
CONSTRUCT { ?s ex:worksFor ?o ~ ?r }
WHERE {
  ?s ex:worksFor ?o
  OPTIONAL { ?r rdf:reifies <<( ?s ex:worksFor ?o )>> }
}
```

```sparql
PREFIX ex: <http://example.org/ns/>

# Describe each edge with a new, per-row reifier
CONSTRUCT { ?s ex:worksFor ?o {| ex:source ex:hrExport |} }
WHERE { ?s ex:worksFor ?o }
```

A reifier variable that a row leaves unbound attaches nothing; the triple is still written.
A `{| ... |}` block without a reifier, or a blank-node reifier (`~ _:r`), mints a fresh blank
node for each solution, like `[ ]`. The block's properties become ordinary triples about the
reifier.

In a JSON-LD query, put `@annotation` on the object, as when writing an annotation. An
`@annotation` without an `@id` mints a fresh reifier per solution:

```json
{
  "@context": {"ex": "http://example.org/ns/"},
  "where": [{"@id": "?s", "ex:worksFor": {"@id": "?o", "@annotation": {"@id": "?r"}}}],
  "construct": [{"@id": "?s", "ex:worksFor": {"@id": "?o", "@annotation": {"@id": "?r"}}}]
}
```

Every output format carries the annotations: `o ~ r` in Turtle and TriG,
`r rdf:reifies <<( s p o )>>` lines in N-Triples and N-Quads, `@annotation` in JSON-LD, and
the RDF 1.2 `rdf:annotation` attribute in RDF/XML.

## Use Cases

### Extract Subgraph

Extract a subgraph for a specific entity:

```sparql
PREFIX ex: <http://example.org/ns/>

CONSTRUCT {
  ?s ?p ?o .
}
WHERE {
  ex:alice ?p ?o .
  BIND (ex:alice AS ?s)
}
```

### Transform Data Structure

Transform data into a different structure:

```sparql
PREFIX ex: <http://example.org/ns/>

CONSTRUCT {
  ?order ex:hasItem [
    ex:product ?product ;
    ex:quantity ?quantity
  ] .
}
WHERE {
  ?order ex:item ?item .
  ?item ex:product ?product .
  ?item ex:quantity ?quantity .
}
```

### Generate Inferred Facts

Generate inferred relationships:

```sparql
PREFIX ex: <http://example.org/ns/>

CONSTRUCT {
  ?person ex:ancestor ?ancestor .
}
WHERE {
  ?person ex:parent+ ?ancestor .
}
```

## Solution Multiplicity: Blank Nodes and LIMIT

A CONSTRUCT template is instantiated once per **solution**, and a WHERE clause
is a bag — a subject with three `ex:tag` values contributes three solutions,
not one (SPARQL 1.1 §16.2). Because the result is an RDF graph, identical
triples built from different solutions collapse into one, so this is usually
invisible. It becomes visible in exactly two places:

**Blank nodes in the template mint one blank node per solution.** A template
blank node (`[ ... ]` or `_:b`) is fresh for every solution, so each solution
produces distinct triples:

```sparql
PREFIX ex: <http://example.org/ns/>

CONSTRUCT { ?s ex:note [ ex:v "seen" ] }
WHERE { ?s a ex:Gadget . ?s ex:tag ?o }
```

A gadget with three tags yields three matched solutions and therefore three
distinct `ex:note` blank nodes — one per solution, not one per gadget. If you
want one node per subject, make the WHERE clause produce one solution per
subject (for example, drop the `?s ex:tag ?o` pattern, or move it into a
subquery with `SELECT DISTINCT ?s`).

**`LIMIT` counts solutions, not output triples.** The slice is applied to the
solution sequence *before* the template is instantiated, and duplicate triples
from the surviving solutions still collapse afterward. So
`CONSTRUCT { ?s ex:flag "y" } WHERE { ?s a ex:Gadget . ?s ex:tag ?o } LIMIT 10`
can return far fewer than 10 triples: the first 10 solutions may cover only a
few distinct subjects (a single gadget with 10+ tags covers them all), and
without an `ORDER BY` which solutions those are is not defined. If you are
using `LIMIT` to preview *n* subjects, limit the subjects rather than the
solutions:

```sparql
PREFIX ex: <http://example.org/ns/>

CONSTRUCT { ?s ex:flag "y" }
WHERE {
  { SELECT DISTINCT ?s WHERE { ?s a ex:Gadget . ?s ex:tag ?o } LIMIT 10 }
}
```

## Output Formats

A CONSTRUCT result is returned as JSON-LD unless you ask for another graph
format. Over HTTP, send `Accept: text/turtle`, `application/n-triples`,
`application/rdf+xml`, `application/trig` or `application/n-quads` to
`POST /v1/fluree/query/{ledger}`; in Rust, pass `FormatterConfig::turtle()`, `ntriples()`,
`rdf_xml()`, `trig()` or `nquads()` and call `execute_formatted_string()`. Turtle and TriG
output use the query's `PREFIX`es:

```bash
curl -X POST http://localhost:8090/v1/fluree/query/mydb:main \
  -H "Content-Type: application/sparql-query" \
  -H "Accept: text/turtle" \
  --data 'PREFIX ex: <http://example.org/ns/>
CONSTRUCT { ?person ex:displayName ?name } WHERE { ?person ex:name ?name }'
```

```turtle
@prefix ex: <http://example.org/ns/> .

ex:alice ex:displayName "Alice" .
```

A template with `GRAPH` blocks produces a dataset, which Turtle, N-Triples and RDF/XML cannot
express: over HTTP such a query negotiates only among TriG, N-Quads and JSON-LD, and an `Accept`
that admits none of them is a `406`. Without `GRAPH` blocks, TriG and N-Quads output are plain
Turtle and N-Triples.

DESCRIBE results take the same formats. See
[Graph Formats](output-formats.md#graph-formats-construct--describe) for the
details of each.

## Best Practices

1. **Specific Patterns**: Construct specific patterns rather than wildcards
2. **Filter Early**: Apply filters in WHERE clause, not CONSTRUCT
3. **Avoid Duplicates**: Use DISTINCT if needed
4. **Performance**: CONSTRUCT can be expensive for large result sets

## Current Limitations

- `GRAPH` blocks cannot nest, and the `CONSTRUCT WHERE` shorthand has no `GRAPH` form (its
  template is a basic graph pattern, per SPARQL 1.1).
- A triple term in a template is accepted only as the object of `rdf:reifies`; nested triple
  terms and property paths inside a template annotation block are rejected.
- A SPARQL datalog rule whose head (the template) annotates an edge or writes into a named
  graph is rejected: rules infer default-graph triples only.

## Related Documentation

- [SPARQL](sparql.md): SPARQL query language
- [JSON-LD Query](jsonld-query.md): JSON-LD Query language
- [Output Formats](output-formats.md): Result formats
