# Turtle and TriG Ingest

Fluree supports ingesting RDF data in **Turtle** (Terse RDF Triple Language) and **TriG** formats. Turtle is a compact, human-readable format for RDF triples, while TriG extends Turtle to support named graphs.

## What is Turtle?

Turtle is a W3C standard format for writing RDF triples. It's more readable than XML-based formats and commonly used in the Semantic Web community.

**Example Turtle:**
```turtle
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .

ex:alice a schema:Person ;
  schema:name "Alice" ;
  schema:email "alice@example.org" ;
  schema:age 30 .

ex:bob a schema:Person ;
  schema:name "Bob" ;
  schema:email "bob@example.org" .
```

## Transaction Endpoints

Fluree supports Turtle and TriG on different endpoints with different semantics:

| Endpoint | Turtle (`text/turtle`) | TriG (`application/trig`) |
|----------|------------------------|---------------------------|
| `/insert` | Supported (fast direct path) | Not supported (400 error) |
| `/upsert` | Supported | Supported |

- **Insert** (`/insert`): Pure insert semantics. Uses fast direct flake parsing. Will fail if subjects already exist with conflicting data. TriG is not supported because named graphs require the upsert path for GRAPH block extraction.
- **Upsert** (`/upsert`): For each (subject, predicate) pair, existing values are retracted before new values are asserted. Supports TriG with GRAPH blocks for named graph ingestion.

## Basic Turtle Transaction

Submit Turtle data via HTTP API:

```bash
# Insert (pure insert, fast path)
curl -X POST "http://localhost:8090/v1/fluree/insert?ledger=mydb:main" \
  -H "Content-Type: text/turtle" \
  --data-binary '@data.ttl'

# Or upsert (replace existing values)
curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
  -H "Content-Type: text/turtle" \
  --data-binary '@data.ttl'
```

**File: data.ttl**
```turtle
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .

ex:alice a schema:Person ;
  schema:name "Alice" ;
  schema:email "alice@example.org" .
```

## Turtle Syntax

### Prefixes

Define namespace prefixes:

```turtle
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
```

### Basic Triples

```turtle
ex:alice schema:name "Alice" .
ex:alice schema:age 30 .
ex:alice schema:email "alice@example.org" .
```

### Semicolon Shorthand

Share subject across predicates:

```turtle
ex:alice schema:name "Alice" ;
         schema:age 30 ;
         schema:email "alice@example.org" .
```

Equivalent to three separate triples.

### Comma Shorthand

Share subject and predicate:

```turtle
ex:alice schema:email "alice@example.org" ,
                      "alice@work.com" ,
                      "alice@personal.net" .
```

Creates three triples with same subject and predicate.

### Type Shorthand

```turtle
ex:alice a schema:Person .
```

Equivalent to:
```turtle
ex:alice rdf:type schema:Person .
```

### Literals

**Plain String:**
```turtle
ex:alice schema:name "Alice" .
```

**Typed Literal:**
```turtle
ex:alice schema:age "30"^^xsd:integer .
ex:alice schema:price "29.99"^^xsd:decimal .
ex:alice schema:birthDate "1994-05-15"^^xsd:date .
```

**Language-Tagged:**
```turtle
ex:alice schema:name "Alice"@en .
ex:alice schema:name "アリス"@ja .
```

**Boolean:**
```turtle
ex:alice schema:active true .
```

**Numbers:**
```turtle
ex:alice schema:age 30 .
ex:alice schema:height 1.68 .
```

### IRIs

**Full IRI:**
```turtle
<http://example.org/ns/alice> schema:name "Alice" .
```

**Prefixed IRI:**
```turtle
ex:alice schema:name "Alice" .
```

### Blank Nodes

**Anonymous:**
```turtle
ex:alice schema:address [
  a schema:PostalAddress ;
  schema:streetAddress "123 Main St" ;
  schema:addressLocality "Springfield"
] .
```

**Labeled:**
```turtle
ex:alice schema:address _:addr1 .

_:addr1 a schema:PostalAddress ;
  schema:streetAddress "123 Main St" .
```

### Collections

**RDF Lists:**
```turtle
ex:alice schema:favoriteColors ( "red" "blue" "green" ) .
```

Equivalent to linked list structure in RDF.

## Bulk Import

### From File

```bash
curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
  -H "Content-Type: text/turtle" \
  --data-binary '@large-dataset.ttl'
```

### From URL

```bash
curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
  -H "Content-Type: text/turtle" \
  -d "@https://example.org/data.ttl"
```

### Streaming Large Files

For very large files, split into batches:

```bash
# Split large file
split -l 10000 large-dataset.ttl batch-

# Import batches
for file in batch-*; do
  curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
    -H "Content-Type: text/turtle" \
    --data-binary "@$file"
  sleep 1  # Allow indexing time
done
```

## Complete Example

```turtle
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

# Company
ex:company-a a schema:Organization ;
  schema:name "Acme Corp" ;
  schema:url <https://acme.example.com> ;
  schema:foundingDate "2000-01-15"^^xsd:date .

# People
ex:alice a schema:Person ;
  schema:name "Alice" ;
  schema:email "alice@example.org" , "alice@work.com" ;
  schema:age 30 ;
  schema:worksFor ex:company-a ;
  schema:address [
    a schema:PostalAddress ;
    schema:streetAddress "123 Main St" ;
    schema:addressLocality "Springfield" ;
    schema:postalCode "12345"
  ] .

ex:bob a schema:Person ;
  schema:name "Bob" ;
  schema:email "bob@example.org" ;
  schema:age 25 ;
  schema:worksFor ex:company-a ;
  schema:knows ex:alice .

ex:carol a schema:Person ;
  schema:name "Carol" ;
  schema:email "carol@example.org" ;
  schema:knows ex:alice , ex:bob .
```

## Format Conversion

### From JSON-LD to Turtle

Many tools can convert between formats:

```bash
# Using rapper (from Redland)
rapper -i json-ld -o turtle data.jsonld > data.ttl

# Using riot (from Apache Jena)
riot --output=turtle data.jsonld > data.ttl
```

### From RDF/XML to Turtle

```bash
rapper -i rdfxml -o turtle data.rdf > data.ttl
```

### From N-Triples to Turtle

```bash
rapper -i ntriples -o turtle data.nt > data.ttl
```

## Validation

Validate Turtle syntax before importing:

```bash
# Using rapper
rapper -i turtle -c data.ttl

# Using riot
riot --validate data.ttl
```

## Error Handling

### Syntax Errors

```json
{
  "error": "ParseError",
  "message": "Invalid Turtle syntax at line 5",
  "code": "TURTLE_PARSE_ERROR",
  "details": {
    "line": 5,
    "column": 12,
    "token": "unexpected EOF"
  }
}
```

### Invalid IRIs

```json
{
  "error": "ValidationError",
  "message": "Invalid IRI: not a valid URI",
  "code": "INVALID_IRI",
  "details": {
    "iri": "not a uri",
    "line": 8
  }
}
```

## Performance Tips

### 1. Use Batch Import

Import large datasets in batches of 10,000-100,000 triples.

### 2. Optimize Prefixes

Use short prefixes for efficiency:

Good:
```turtle
@prefix ex: <http://example.org/ns/> .
ex:alice ex:name "Alice" .
```

Less efficient:
```turtle
<http://example.org/ns/alice> <http://example.org/ns/name> "Alice" .
```

### 3. Monitor Memory

Large Turtle files consume memory during parsing. Split very large files.

### 4. Allow Indexing Time

After large imports, wait for indexing:

```bash
# Import
curl -X POST ... --data-binary '@batch.ttl'

# Wait for indexing
sleep 5

# Import next batch
curl -X POST ... --data-binary '@batch2.ttl'
```

## Best Practices

### 1. Use Standard Vocabularies

Prefer well-known vocabularies:

```turtle
@prefix schema: <http://schema.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix dc: <http://purl.org/dc/terms/> .
```

### 2. Include Types

Always specify entity types:

```turtle
ex:alice a schema:Person ;
  schema:name "Alice" .
```

### 3. Use Typed Literals

Be explicit about datatypes:

```turtle
ex:alice schema:birthDate "1994-05-15"^^xsd:date ;
         schema:age "30"^^xsd:integer ;
         schema:height "1.68"^^xsd:decimal .
```

### 4. Document Namespaces

Comment your prefixes:

```turtle
# Schema.org vocabulary for general entities
@prefix schema: <http://schema.org/> .

# Application-specific namespace
@prefix ex: <http://example.org/ns/> .

# Standard XSD datatypes
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
```

### 5. Validate Before Import

Always validate Turtle syntax:

```bash
rapper -i turtle -c data.ttl
```

### 6. Split Large Files

For files > 100MB, split into smaller batches.

### 7. Include Provenance

Add metadata about the import:

```turtle
ex:dataset-import-2024-01-22 a ex:DatasetImport ;
  schema:dateCreated "2024-01-22T10:00:00Z"^^xsd:dateTime ;
  schema:author <https://example.org/users/admin> ;
  ex:sourceFile "data-2024-01.ttl" ;
  ex:recordCount 1234567 .
```

## Comparing Formats

### JSON-LD vs Turtle

**JSON-LD:**
- Native to Fluree
- Easy for JavaScript applications
- Verbose for large datasets

**Turtle:**
- More compact
- Standard in RDF community
- Better for bulk imports
- Requires conversion for JavaScript apps

### When to Use Turtle

Use Turtle for:
- Large bulk imports
- Integration with RDF tools
- Data from Semantic Web sources
- Data exchange with RDF systems

Use JSON-LD for:
- Application integration
- Real-time transactions
- JavaScript/TypeScript apps
- REST API interactions

## TriG Format (Named Graphs)

TriG extends Turtle to support **named graphs**. Each named graph groups triples under a graph IRI.

### What is TriG?

TriG (TriG RDF Triple Graph) is a W3C standard format that adds named graph support to Turtle syntax. It allows you to partition data into logical groups that can be queried independently.

### Basic TriG Syntax

```trig
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .

# Default graph triples (no GRAPH block)
ex:company a schema:Organization ;
    schema:name "Acme Corp" .

# Named graph for products
GRAPH <http://example.org/graphs/products> {
    ex:widget a schema:Product ;
        schema:name "Widget" ;
        schema:price "29.99"^^xsd:decimal .

    ex:gadget a schema:Product ;
        schema:name "Gadget" ;
        schema:price "49.99"^^xsd:decimal .
}

# Named graph for inventory
GRAPH <http://example.org/graphs/inventory> {
    ex:widget schema:inventory 42 ;
        schema:warehouse "main" .

    ex:gadget schema:inventory 15 ;
        schema:warehouse "secondary" .
}
```

### Submitting TriG Data

TriG is only supported on the **upsert** endpoint (or transact). Use the `application/trig` content type:

```bash
# TriG requires upsert (for named graph support)
curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
  -H "Content-Type: application/trig" \
  --data-binary '@data.trig'
```

TriG on the `/insert` endpoint will return a 400 error because named graph extraction requires the upsert path.

### Querying Named Graphs

After ingesting TriG data, query specific graphs using JSON-LD with the structured `from` object:

```json
{
  "@context": { "schema": "http://schema.org/" },
  "from": {
    "@id": "mydb:main",
    "graph": "http://example.org/graphs/products"
  },
  "select": ["?name", "?price"],
  "where": [
    { "@id": "?product", "schema:name": "?name" },
    { "@id": "?product", "schema:price": "?price" }
  ]
}
```

For cross-graph queries, use `fromNamed` with aliases:

```json
{
  "@context": { "schema": "http://schema.org/" },
  "from": "mydb:main",
  "fromNamed": [
    { "@id": "mydb:main", "alias": "products", "graph": "http://example.org/graphs/products" },
    { "@id": "mydb:main", "alias": "inventory", "graph": "http://example.org/graphs/inventory" }
  ],
  "select": ["?name", "?inventory", "?warehouse"],
  "where": [
    ["graph", "products", { "@id": "?product", "schema:name": "?name" }],
    ["graph", "inventory", { "@id": "?product", "schema:inventory": "?inventory", "schema:warehouse": "?warehouse" }]
  ]
}
```

### Graph IDs

Fluree assigns internal graph IDs to named graphs:

| Graph ID | Purpose |
|----------|---------|
| 0 | Default graph (triples without GRAPH block) |
| 1 | txn-meta (commit metadata) |
| 2+ | User-defined named graphs |

### TriG with Transaction Metadata

You can combine named graphs with transaction metadata using the special `#txn-meta` graph fragment:

```trig
@prefix ex: <http://example.org/ns/> .
@prefix f: <https://ns.flur.ee/db#> .

# Transaction metadata (stored in txn-meta graph)
GRAPH <#txn-meta> {
    fluree:commit:this ex:jobId "batch-import-001" ;
        ex:source "warehouse-export" ;
        ex:operator "system-admin" .
}

# User data in named graph
GRAPH <http://example.org/graphs/products> {
    ex:widget a ex:Product ;
        ex:name "Widget" .
}
```

### Limits

- Maximum 256 named graphs per transaction
- Maximum 8KB per graph IRI
- Named graphs are queryable after indexing completes

### When to Use TriG

Use TriG when you need to:
- Partition data into logical groups
- Separate data by source, tenant, or domain
- Maintain provenance at the graph level
- Integrate with RDF quad stores

Use plain Turtle when:
- All data belongs in the default graph
- Graph partitioning isn't needed
- Working with simpler data models

## Bulk import (Rust API)

For high-throughput ingest of large Turtle datasets into a **fresh ledger**, prefer the bulk import
pipeline exposed by `fluree-db-api`:

- See: [Using Fluree as a Rust library → Bulk import Turtle chunks](../getting-started/rust-api.md#bulk-import-turtle-chunks-high-throughput)

This pipeline:
- Parses Turtle in parallel, but **writes commits serially** (hash-linked commit chain).
- Streams run generation during import and builds multi-order binary indexes (SPOT/PSOT/POST/OPST).
- Writes an index root to CAS and publishes it to the nameservice so queries can use the normal
  `db()` / `query()` path.
  
Temporary `tmp_import/` session files are cleaned up on success (configurable).

## Tools and Libraries

### Command-Line Tools

**Rapper (Redland):**
```bash
# Install on macOS
brew install redland

# Parse Turtle
rapper -i turtle data.ttl
```

**Riot (Apache Jena):**
```bash
# Install
# Download from https://jena.apache.org/

# Validate
riot --validate data.ttl
```

### Programming Libraries

**JavaScript/TypeScript:**
```javascript
import { Parser } from 'n3';

const parser = new Parser();
const quads = parser.parse(turtleString);
```

**Python:**
```python
from rdflib import Graph

g = Graph()
g.parse('data.ttl', format='turtle')
```

**Java:**
```java
import org.apache.jena.rdf.model.*;

Model model = ModelFactory.createDefaultModel();
model.read("data.ttl", "TURTLE");
```

## Related Documentation

- [Insert](insert.md) - Adding data via JSON-LD
- [Overview](overview.md) - Transaction overview
- [Datasets and Named Graphs](../concepts/datasets-and-named-graphs.md) - Named graph concepts
- [Data Types](../concepts/datatypes.md) - Supported datatypes
- [API Headers](../api/headers.md) - Content-Type specifications
