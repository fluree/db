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

## Best Practices

1. **Specific Patterns**: Construct specific patterns rather than wildcards
2. **Filter Early**: Apply filters in WHERE clause, not CONSTRUCT
3. **Avoid Duplicates**: Use DISTINCT if needed
4. **Performance**: CONSTRUCT can be expensive for large result sets

## Current Limitations

- **RDF collection syntax** (`( ?a ?b )`) is not yet supported in CONSTRUCT
  templates — list the `rdf:first`/`rdf:rest`/`rdf:nil` triples explicitly.
- **No annotations in CONSTRUCT templates** (the template output form is
  deferred); a `CONSTRUCT` whose `WHERE` uses annotations to filter still works.

## Related Documentation

- [SPARQL](sparql.md): SPARQL query language
- [JSON-LD Query](jsonld-query.md): JSON-LD Query language
- [Output Formats](output-formats.md): Result formats
