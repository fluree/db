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

## Best Practices

1. **Specific Patterns**: Construct specific patterns rather than wildcards
2. **Filter Early**: Apply filters in WHERE clause, not CONSTRUCT
3. **Avoid Duplicates**: Use DISTINCT if needed
4. **Performance**: CONSTRUCT can be expensive for large result sets

## Related Documentation

- [SPARQL](sparql.md): SPARQL query language
- [JSON-LD Query](jsonld-query.md): JSON-LD Query language
- [Output Formats](output-formats.md): Result formats
