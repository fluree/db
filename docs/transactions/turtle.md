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
| `/sync` | Supported | Supported (one graph per request) |
| `/data` ([Graph Store](../api/graph-store.md) `PUT` / `POST`) | Supported | Supported (one graph per request) |

- **Insert** (`/insert`): Pure insert semantics. Uses fast direct flake parsing. Will fail if subjects already exist with conflicting data. TriG is not supported because named graphs require the upsert path for GRAPH block extraction.
- **Upsert** (`/upsert`): For each (subject, predicate) pair, existing values are retracted before new values are asserted. Supports TriG with GRAPH blocks for named graph ingestion.
- **Sync** (`/sync?graph=<iri>`): The body becomes the named graph's entire contents, committing only the difference; an unchanged body commits nothing. N-Triples (`application/n-triples`) is accepted too. See [Sync](sync.md#payload-formats) for the TriG rules.

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

### Parser Limits

The parser enforces two resource limits; input past either is rejected with a
parse error rather than ingested:

- Blank-node property lists (`[ ... ]`), collections (`( ... )`), and reified
  triples (`<< ... >>`) may nest at most 128 levels deep. Wide structures are
  unaffected — a flat list of any length counts as one level; only the nesting
  chain is limited.
- A single parse call accepts at most 4 GiB (`u32::MAX` bytes) of input. Bulk
  import splits files into chunks well below this automatically.

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

## Edge annotations (RDF 1.2 / Turtle-star)

The Turtle parser (which also reads N-Triples) accepts the RDF 1.2 *asserting* forms on every Turtle write path — `insert`, `upsert`, bulk `import`, `fluree graph sync`, and the memory importer. All of them produce the same on-disk `f:reifies*` bundle that the JSON-LD `@annotation` and SPARQL 1.2 `{| |}` surfaces write, so cascade retracts, hydration, and the annotation arena treat every surface as one, and the annotations are queryable from every query surface:

```turtle
@prefix ex:  <http://example.org/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

# Annotation block — fresh anonymous reifier
ex:alice ex:worksFor ex:acme {| ex:role "Engineer" ; ex:since "2024-01-01"^^xsd:date |} .

# Named reifier, with or without a block
ex:alice ex:knows ex:bob ~ ex:friendship1 {| ex:since 2019 |} .
ex:alice ex:knows ex:carol ~ ex:friendship2 .

# Reified triple in subject or object position (the reifier is the node)
<< ex:alice ex:worksFor ex:acme ~ ex:emp1 >> ex:confidence 0.97 .
ex:doc ex:cites << ex:alice ex:worksFor ex:acme ~ ex:emp1 >> .

# The canonical RDF 1.2 spelling every form above desugars to — and the only
# star construct N-Triples has
ex:emp1 rdf:reifies <<( ex:alice ex:worksFor ex:acme )>> .
```

Two rules to know:

- **The reified triple is asserted.** RDF 1.2 says `<< s p o >>` and `r rdf:reifies <<( s p o )>>` do *not* put `s p o` in the graph; Fluree's annotations describe a live edge, so ingest asserts the base triple as well and attaches the reifier to it. The reifier's own triples (the annotation body) are ordinary RDF about the reifier. Each anonymous `<< s p o >>` / `{| |}` occurrence mints a fresh reifier — two textual occurrences are two annotations.
- **`<<( ... )>>` is accepted only as the object of `rdf:reifies`.** As a plain value (`ex:doc ex:mentions <<( ... )>>`), nested inside another triple term, or inside an annotation body, it is rejected with a specific "deferred" error rather than silently dropped.

TriG and N-Quads accept the same forms inside `GRAPH { }` blocks (and on N-Quads statements with a graph label). The annotation is written into that graph and carries the edge's graph identity, exactly as JSON-LD `@graph` + `@annotation` does:

```trig
@prefix ex: <http://example.org/> .

GRAPH ex:hr {
  ex:alice ex:worksFor ex:acme {| ex:role "Engineer" |} .
  << ex:alice ex:knows ex:bob ~ ex:f1 >> ex:confidence 0.9 .
}
```

Sending a claims file through `upsert` replaces each claim's body (`ex:confidence`) the way upsert replaces any other predicate value, while the edge and its attachment stay put — the natural way to keep a claims file in sync with a ledger.

```turtle
@prefix ex: <http://example.org/> .

ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 ; ex:source ex:hr |} .
ex:alice ex:knows ex:carol {| ex:source ex:linkedin |} .
```

**The same syntax in a *delete* removes the edge.** Because the annotation form asserts the base triple, `DELETE DATA { ex:alice ex:knows ex:bob ~ ex:claim1 {| … |} }` — and the bare `~ ex:claim1` with no block — retract `ex:alice ex:knows ex:bob` itself, which detaches every claim on that edge rather than just the one named. This is RDF 1.2 / SPARQL 1.2 behavior, not a Fluree choice. The forms above are documented for *insert*, where nothing about them hints at that; see [Which spelling does what](../concepts/edge-annotations.md#which-spelling-does-what) for the delete side and the spellings that withdraw or detach a single claim.

**Write the reifier before the annotation block.** `s p o ~ ?claim {| … |}` binds `?claim` to the reifier of the very claim the block matches. Reversing them — `s p o {| … |} ~ ?claim` — is legal but means something else: two *independent* annotation units on the same edge, one matching the body and one binding a reifier, joined. On an edge with two claims that returns four rows rather than two, silently, because each unit matches every claim.

This one is documented rather than refused, and the line is worth stating because Fluree draws it elsewhere too. The reversed form is well-formed SPARQL-star with defined semantics: four rows is the *correct* answer to what was written, and no parser can know the author meant the other thing. Fluree refuses a construct only when there is no correct answer to give — a property read on an enumerated variable-length relationship is refused (see `docs/query/cypher.md`) because the enumeration operator does not retain per-hop edge identity, so every answer, nulls included, would be a fiction. A right answer to the wrong question gets a warning in the docs; no right answer gets an error.

**TriG goes through `upsert` or `/sync`, not `insert`.** `fluree insert` routes a file to the streaming Turtle parser, which has no `GRAPH` keyword and reports `expected subject, found 'GRAPH'`. Named-graph blocks are read by `fluree upsert -f file.trig`, or by `POST /sync` for one graph.

**Anonymous reifiers have no identity you can refer to, and the two re-send paths differ.** `~ ex:claim1` is an identity: re-ingesting the file finds the same claim and replaces its body, on every path. A bare `{| … |}` block has no such handle, so what happens on a re-send depends on where the path scopes blank-node identity.

| re-sending the same file | sync (`fluree sync`, `/sync`) | `upsert` |
| --- | --- | --- |
| unchanged payload | no-op | no-op |
| changed annotation body | the claim's body is replaced | a second claim is added |

Sync scopes blank-node identity to the target graph, so the same source label names the same reifier across payloads and a changed body lands on the claim already there. `upsert` scopes it to the payload, so a changed body is a different payload, mints a different reifier, and leaves the first claim in place. Name the reifier when you want replacement on both.

Rejected with a clear parse or stage error, never silently dropped:

- the parenthesized triple term `<<( :s :p :o )>>` anywhere other than the object of `rdf:reifies` (RDF 1.2 triple terms as values are not representable yet), and a triple term nested inside another;
- an annotation block nested inside an annotation body (`{| :q :v {| … |} |}`), and an annotation tail on an `rdf:reifies <<( … )>>` statement (it would annotate the reification itself);
- an annotation on a collection object (`( :a :b ) {| … |}`);
- one named reifier on two different triples — a reifier denotes exactly one edge (see [the single-target invariant](../concepts/edge-annotations.md#one-annotation-one-edge-single-target-invariant));
- an annotation on an `rdf:type` edge (`:s a :C {| … |}`) on the paths that convert Turtle to JSON-LD first (`upsert`, `graph sync`, memory import) — JSON-LD has no place to hang an annotation on a `@type` value. `insert` and SPARQL UPDATE accept it;
- TriG: annotations in a `<#txn-meta>` block — its triples become commit metadata, not edges.

The RDF 1.2 version directive — `VERSION "1.2"` or `@version "1.2" .` — is accepted anywhere a directive may appear and ignored: the RDF 1.2 surface is always on. Base-direction language tags (`"…"@en--ltr`) are accepted; a direction other than `ltr` / `rtl` is a syntax error. They are stored as an `rdf:langString` whose language is the whole `en--ltr` string, not yet as `rdf:dirLangString` with a separate direction — so `LANG()` returns `en--ltr` and `langMatches(?l, "en")` will not match it.

Annotations are written back out in RDF 1.2 syntax (`~ r` in Turtle and TriG, `rdf:reifies <<( … )>>` in N-Triples and N-Quads) by export (see [export](../cli/export.md#edge-annotations-rdf-12)), by a SPARQL CONSTRUCT whose template carries them (see [CONSTRUCT](../query/construct.md#edge-annotations-in-the-template)), and by the Graph Store `GET`. For the SPARQL 1.2 UPDATE equivalents see [the cookbook](../guides/cookbook-edge-annotations.md#the-same-patterns-in-sparql-12); for the full model — `rdf:reifies` for annotation-rooted queries, the per-operation rules for SPARQL UPDATE templates, and the deferred shapes — see the [Edge annotations concept doc](../concepts/edge-annotations.md).

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

TriG is supported on the **upsert** endpoint, and on **sync** for replacing one named graph's contents ([Sync](sync.md#payload-formats)). Use the `application/trig` content type:

```bash
# TriG requires upsert (for named graph support)
curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
  -H "Content-Type: application/trig" \
  --data-binary '@data.trig'
```

TriG on the `/insert` endpoint will return a 400 error because named graph extraction requires the upsert path.

**Known limitation ([#1930](https://github.com/fluree/db/issues/1930)):** on `/upsert` and bulk import, a `GRAPH` block's contents are read by a smaller parser that rejects anonymous blank nodes (`[ … ]`) and collections (`( … )`) with `expected object, found '['`. Triples outside blocks are unaffected. `/sync` and the [Graph Store Protocol](../api/graph-store.md) read block contents with the full Turtle parser, so they accept both; for `/upsert`, use labeled blank nodes (`_:b1`) inside blocks.

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

- See: [Using Fluree as a Rust library → Bulk import Turtle chunks](../getting-started/rust-api.md#bulk-import-high-throughput)

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
