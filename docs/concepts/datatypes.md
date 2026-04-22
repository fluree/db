# Datatypes and Typed Values

Fluree enforces strong typing for all literal values, ensuring data consistency and enabling efficient indexing and querying. Every literal value has an explicit datatype, following RDF and XSD standards.

## Core Principle: No Untyped Literals

Unlike some databases that allow "plain" strings, Fluree requires every literal to have a datatype. This design provides:

- **Type Safety**: Prevents type confusion in queries and applications
- **Consistent Comparisons**: Typed values compare predictably
- **Standards Compliance**: Follows RDF and SPARQL specifications
- **Query Optimization**: Enables efficient indexing and query planning

## XSD Datatypes

Fluree supports the core XML Schema Definition (XSD) datatypes:

### String Types

```json
{
  "@context": {
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "ex": "http://example.org/ns/"
  },
  "@graph": [
    {
      "@id": "ex:book1",
      "ex:title": "The Great Gatsby",
      "ex:author": {
        "@value": "F. Scott Fitzgerald",
        "@type": "xsd:string"
      }
    }
  ]
}
```

**xsd:string** is the default for plain string literals when no type is specified.

### Numeric Types

```json
{
  "@graph": [
    {
      "@id": "ex:product1",
      "ex:price": {
        "@value": "29.99",
        "@type": "xsd:decimal"
      },
      "ex:quantity": {
        "@value": "100",
        "@type": "xsd:integer"
      },
      "ex:rating": {
        "@value": "4.5",
        "@type": "xsd:double"
      }
    }
  ]
}
```

Supported numeric types:
- **xsd:integer**: Whole numbers (-∞, ∞)
- **xsd:long**: 64-bit integers
- **xsd:int**: 32-bit integers
- **xsd:short**: 16-bit integers
- **xsd:byte**: 8-bit integers
- **xsd:decimal**: Arbitrary precision decimals
- **xsd:double**: 64-bit floating point
- **xsd:float**: 32-bit floating point

### Boolean Type

```json
{
  "@graph": [
    {
      "@id": "ex:user1",
      "ex:isActive": {
        "@value": "true",
        "@type": "xsd:boolean"
      },
      "ex:hasVerifiedEmail": {
        "@value": "false",
        "@type": "xsd:boolean"
      }
    }
  ]
}
```

**xsd:boolean** accepts: `true`, `false`, `1`, `0`.

### Date and Time Types

```json
{
  "@graph": [
    {
      "@id": "ex:event1",
      "ex:startDate": {
        "@value": "2024-01-15",
        "@type": "xsd:date"
      },
      "ex:startTime": {
        "@value": "14:30:00Z",
        "@type": "xsd:time"
      },
      "ex:createdAt": {
        "@value": "2024-01-15T14:30:00Z",
        "@type": "xsd:dateTime"
      }
    }
  ]
}
```

Temporal types:
- **xsd:date**: Dates without time (e.g., `2024-01-15`)
- **xsd:time**: Times without date (e.g., `14:30:00Z`)
- **xsd:dateTime**: Full timestamps (e.g., `2024-01-15T14:30:00Z`)

### Other XSD Types

```json
{
  "@graph": [
    {
      "@id": "ex:resource1",
      "ex:homepage": {
        "@value": "https://example.com",
        "@type": "xsd:anyURI"
      },
      "ex:duration": {
        "@value": "PT1H30M",
        "@type": "xsd:duration"
      }
    }
  ]
}
```

Additional types include:
- **xsd:anyURI**: Web addresses and identifiers
- **xsd:duration**: Time periods (ISO 8601 format)
- **xsd:gYear**, **xsd:gMonth**, **xsd:gDay**: Partial date components

## RDF Datatypes

Beyond XSD, Fluree supports RDF-specific datatypes:

### Language-Tagged Strings

```json
{
  "@graph": [
    {
      "@id": "ex:book1",
      "ex:title": {
        "@value": "The Great Gatsby",
        "@language": "en"
      },
      "ex:titel": {
        "@value": "Der große Gatsby",
        "@language": "de"
      }
    }
  ]
}
```

**rdf:langString** represents strings with language tags. This is distinct from plain strings and enables language-aware queries.

### JSON Data

```json
{
  "@graph": [
    {
      "@id": "ex:config1",
      "ex:settings": {
        "@value": "{\"theme\": \"dark\", \"notifications\": true}",
        "@type": "@json"
      }
    }
  ]
}
```

**rdf:JSON** stores JSON data as typed literals. This is useful for storing complex structured data that doesn't fit the RDF model.

### Geographic Data

```json
{
  "@context": {
    "geo": "http://www.opengis.net/ont/geosparql#",
    "ex": "http://example.org/"
  },
  "@graph": [
    {
      "@id": "ex:location1",
      "ex:coordinates": {
        "@value": "POINT(2.3522 48.8566)",
        "@type": "geo:wktLiteral"
      }
    }
  ]
}
```

**geo:wktLiteral** stores geographic data in Well-Known Text (WKT) format. POINT geometries are automatically converted to an optimized binary encoding, while other geometry types (polygons, lines) are stored as strings.

See [Geospatial](../indexing-and-search/geospatial.md) for complete documentation.

### Vector Data

```json
{
  "@context": {
    "ex": "http://example.org/"
  },
  "@graph": [
    {
      "@id": "ex:doc1",
      "ex:embedding": {
        "@value": [0.1, 0.2, 0.3, 0.4],
        "@type": "@vector"
      }
    }
  ]
}
```

**@vector** (full IRI: `https://ns.flur.ee/db#embeddingVector`, prefix form: `f:embeddingVector`) stores numeric arrays as embedding vectors. Values are quantized to IEEE-754 f32 at ingest for compact storage and SIMD-accelerated similarity computation. In Turtle/SPARQL, use `f:embeddingVector` with the `^^` typed-literal syntax.

Without this type annotation, plain JSON arrays are decomposed into individual RDF values where duplicates may be removed and ordering is lost.

See [Vector Search](../indexing-and-search/vector-search.md) for complete documentation.

### Fulltext Data

```json
{
  "@context": {
    "ex": "http://example.org/"
  },
  "@graph": [
    {
      "@id": "ex:article-1",
      "ex:content": {
        "@value": "Rust is a systems programming language focused on safety and performance",
        "@type": "@fulltext"
      }
    }
  ]
}
```

**@fulltext** (full IRI: `https://ns.flur.ee/db#fullText`, prefix form: `f:fullText`) marks a string value for full-text search indexing. Values annotated with `@fulltext` are automatically analyzed (tokenized, stemmed, stopword-filtered) and indexed into per-predicate fulltext arenas during background index builds. This enables BM25-ranked relevance scoring via the `fulltext()` query function.

Without this type annotation, strings are stored as plain `xsd:string` values and support only exact matching and prefix queries -- not relevance-ranked full-text search.

See [Inline Fulltext Search](../indexing-and-search/fulltext.md) for complete documentation.

## Type Coercion and Compatibility

### Automatic Type Promotion

Fluree handles type compatibility intelligently:

```sparql
# This works - integer can be used where decimal is expected
SELECT ?price
WHERE {
  ?product ex:price ?price .
  FILTER(?price > 10.0)  # decimal comparison
}
```

### Comparisons Between Incompatible Types

When a filter compares values of incompatible types (e.g., a number and a string), the behavior depends on the operator:

- **Equality** (`=`) returns `false` — values of different types are never equal
- **Inequality** (`!=`) returns `true` — values of different types are never equal
- **Ordering** (`<`, `<=`, `>`, `>=`) raises an error — ordering between incompatible types is undefined

Numeric types (long, double, bigint, decimal) are mutually comparable via automatic promotion, so cross-numeric comparisons work as expected. Similarly, temporal types can be compared with string representations that parse to the same temporal type.

### Type Casting in Queries

SPARQL provides functions for type conversion:

```sparql
SELECT ?name (xsd:string(?id) AS ?idString)
WHERE {
  ?person ex:name ?name ;
          ex:id ?id .
}
```

## Best Practices

### Choosing Datatypes

1. **Be Specific**: Use the most appropriate type for your data
   - Use `xsd:integer` for whole numbers that will be used in calculations
   - Use `xsd:string` for identifiers and labels
   - Use `xsd:dateTime` for timestamps

2. **Consider Query Patterns**: Choose types that support your intended queries
   - Numeric types enable range queries and aggregations
   - Date types enable temporal queries
   - String types support text search

3. **Standards Alignment**: Use standard datatypes where possible
   - Prefer XSD types over custom types
   - Use established vocabularies with well-defined ranges

### Type Consistency

1. **Consistent Usage**: Use the same datatype for equivalent properties across your data
2. **Change Planning**: Plan for type changes as your data model evolves
3. **Validation**: Validate data types at ingestion time

### Performance Considerations

1. **Index Efficiency**: Different types have different indexing characteristics
   - Numeric types support efficient range queries
   - String types support prefix and substring matching
   - Date types enable temporal range queries

2. **Storage Size**: Some types are more storage-efficient than others
   - `xsd:integer` is more compact than `xsd:string`
   - `xsd:boolean` is more efficient than string representations

## Type System Architecture

### Internal Representation

Fluree stores all typed values with their datatype information:

- **Value Storage**: The literal value as a string
- **Type Metadata**: The datatype IRI
- **Comparison Logic**: Type-aware comparison functions

### Query Processing

The type system affects query processing:

- **Type Checking**: Ensures type compatibility in filters and joins
- **Index Selection**: Chooses appropriate indexes based on types
- **Result Formatting**: Formats results according to datatype rules

### Standards Compliance

Fluree's type system is fully compliant with:

- **RDF 1.1 Concepts**: Literal typing requirements
- **SPARQL 1.1**: Type promotion and compatibility rules
- **XSD 1.1**: Datatype definitions and constraints
- **JSON-LD 1.1**: Typed value syntax

This strong typing foundation ensures data consistency, enables optimization, and maintains interoperability with the broader semantic web ecosystem.