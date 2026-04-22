# Glossary

Definitions of key terms and concepts used throughout Fluree documentation.

## Core Concepts

### Ledger

A versioned graph database instance in Fluree, equivalent to a database in traditional systems. Ledgers are identified by ledger IDs like `mydb:main`.

Example: `customers:main`, `inventory:prod`

### Branch

A variant of a ledger, allowing multiple independent versions of the same logical database. Branches are part of the ledger ID after the colon.

Example: In `mydb:dev`, "dev" is the branch.

### Transaction Time (t)

A monotonically increasing integer assigned to each transaction, representing the logical time of the transaction.

Example: `t=42` is transaction number 42.

### Flake

Fluree's internal representation of an RDF triple with temporal information. A flake is a tuple: (subject, predicate, object, transaction-time, operation, metadata).

### Novelty Layer

The set of transactions that have been committed but not yet indexed. The gap between `commit_t` and `index_t`.

Example: If `commit_t=150` and `index_t=145`, the novelty layer contains transactions 146-150.

### Nameservice

Fluree's metadata registry that tracks ledger state, including commit and index locations. Enables discovery and coordination across distributed deployments.

## RDF Terminology

### IRI (Internationalized Resource Identifier)

A globally unique identifier for resources, predicates, and graphs. The internationalized version of URI supporting Unicode.

Example: `http://example.org/alice`, `http://例え.jp/人物/アリス`

### Triple

The fundamental unit of RDF data: a subject-predicate-object statement.

Example: `ex:alice schema:name "Alice"`

### Subject

The entity being described in a triple (first position).

Example: In `ex:alice schema:name "Alice"`, `ex:alice` is the subject.

### Predicate

The property or relationship in a triple (second position).

Example: In `ex:alice schema:name "Alice"`, `schema:name` is the predicate.

### Object

The value or target entity in a triple (third position).

Example: In `ex:alice schema:name "Alice"`, `"Alice"` is the object.

### Literal

A data value in a triple (string, number, date, etc.), as opposed to an IRI reference.

Example: `"Alice"`, `30`, `"2024-01-22"^^xsd:date`

### Blank Node

An anonymous resource without an explicit IRI.

Example: `[ schema:streetAddress "123 Main St" ]`

### Named Graph

A set of triples identified by an IRI, allowing data partitioning within a ledger.

Example: `ex:graph1` containing specific triples.

### Dataset

A collection of graphs (one default graph and zero or more named graphs) used for query execution.

## Transaction Terms

### Assertion

Adding a new triple to the database.

Example: Asserting `ex:alice schema:age 30` adds this triple.

### Retraction

Removing an existing triple from the current database state.

Example: Retracting `ex:alice schema:age 30` removes this triple.

### Commit

A persisted transaction with assigned transaction time and cryptographic signature.

### Commit ContentId

Content-addressed identifier (CIDv1) for a commit, providing storage-agnostic identity and integrity verification. The SHA-256 digest is embedded in the CID.

Example: `bafybeig...commitT42`

### Replace Mode

Transaction mode where all properties of an entity are replaced, enabling idempotent writes.

Also called: Upsert mode

### WHERE/DELETE/INSERT

Update pattern for targeted modifications: match data (WHERE), remove old data (DELETE), add new data (INSERT).

## Index Terms

### SPOT Index

Subject-Predicate-Object-Time index, optimized for retrieving all properties of a subject.

### POST Index

Predicate-Object-Subject-Time index, optimized for finding subjects with specific property values.

### OPST Index

Object-Predicate-Subject-Time index, optimized for finding subjects that reference specific objects.

### PSOT Index

Predicate-Subject-Object-Time index, optimized for scanning all values of a predicate.

### Index Snapshot

A complete, query-optimized snapshot of the database at a specific transaction time.

### Background Indexing

Asynchronous process that builds index snapshots from committed transactions.

## Query Terms

### Variable

A placeholder in a query pattern that matches actual values in the data, prefixed with `?`.

Example: `?person`, `?name`, `?age`

### Binding

The association of a variable with a specific value during query execution.

Example: `?name` binds to `"Alice"`

### Pattern

A triple template with variables that matches actual triples in the database.

Example: `{ "@id": "?person", "schema:name": "?name" }`

### Filter

A condition that restricts which variable bindings are included in query results.

Example: `"filter": "?age > 25"`

### CONSTRUCT

A SPARQL query form that generates RDF triples rather than variable bindings.

### Graph Crawl

Following relationships recursively to explore connected entities.

## Graph Source Terms

### Graph Source

An addressable query source that participates in execution and can be named in SPARQL via `FROM`, `FROM NAMED`, and `GRAPH <…>`.

Graph sources include:
- Ledger graph sources (default graph and named graphs stored in a ledger)
- Index graph sources (BM25 and vector/HNSW indexes)
- Mapped graph sources (R2RML and Iceberg-backed graph mappings)

### Graph Source (Non-Ledger)

A non-ledger graph source is a queryable data source that appears in graph queries but is backed by specialized storage (BM25 index, vector index, Iceberg table, SQL database).

Example: `products-search:main`, `products-vector:main`

### BM25

Best Matching 25, a ranking algorithm for full-text search. Scores documents by relevance to query terms.

### Vector Embedding

A numerical representation of data (text, images, etc.) as a high-dimensional vector, enabling similarity search.

Example: 384-dimensional vector for text embeddings

### HNSW

Hierarchical Navigable Small World, a graph-based algorithm for approximate nearest neighbor search in high-dimensional spaces.

### R2RML

RDB to RDF Mapping Language, a W3C standard for mapping relational databases to RDF.

### Iceberg

Apache Iceberg, an open table format for huge analytical datasets with ACID guarantees.

## Security Terms

### Policy

A rule specifying who can perform what operations on which data.

### DID (Decentralized Identifier)

A globally unique identifier that doesn't require a central authority, used for cryptographic identity.

Example: `did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK`

### JWS (JSON Web Signature)

An IETF standard (RFC 7515) for representing digitally signed content as JSON.

### Verifiable Credential (VC)

A W3C standard for cryptographically verifiable digital credentials.

### Public Key

Cryptographic key used to verify signatures, shared publicly.

### Private Key

Cryptographic key used to create signatures, kept secret.

## Storage Terms

### ContentId

A CIDv1 (multiformats) value that uniquely identifies any immutable artifact in Fluree. Encodes the content kind (multicodec) and a SHA-256 digest. The canonical string form is base32-lower multibase (e.g., `bafybeig...`).

See [ContentId and ContentStore](../design/content-id-and-contentstore.md) for details.

### ContentKind

An enum identifying the type of content a ContentId refers to: `Commit`, `Txn`, `IndexRoot`, `IndexBranch`, `IndexLeaf`, `DictBlob`, or `DefaultContext`. Encoded as a multicodec tag within the CID.

### ContentStore

The content-addressed storage trait providing `get(ContentId)`, `put(ContentKind, bytes)`, and `has(ContentId)` operations. All immutable artifacts are stored and retrieved via ContentStore.

### Commit ID

A ContentId identifying a committed transaction. Derived by hashing the canonical commit bytes with SHA-256.

Example: `bafybeig...commitT42`

### Index ID

A ContentId identifying an index root snapshot. Derived by hashing the index root descriptor bytes with SHA-256.

Example: `bafybeig...indexRootT145`

### Storage Backend

The underlying system storing Fluree data (memory, file system, AWS S3/DynamoDB).

### Nameservice Record

Metadata about a ledger stored in the nameservice, including commit and index ContentIds.

## Time Travel Terms

### Time Specifier

A suffix on a ledger reference indicating which point in time to query.

Examples: `@t:100`, `@iso:2024-01-22`, `@commit:bafybeig...`

### Point-in-Time Query

A query executed against database state at a specific transaction time.

### History Query

A query that returns changes to entities over a time range, showing assertions and retractions.

### Temporal Database

A database that maintains complete history of all changes, enabling queries at any past state.

## JSON-LD Terms

### @context

JSON-LD mechanism for defining namespace prefixes and term mappings.

Example:
```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  }
}
```

### @id

JSON-LD property for specifying the IRI of a resource.

Example: `"@id": "ex:alice"`

### @type

JSON-LD property for specifying the type(s) of a resource.

Example: `"@type": "schema:Person"`

### @graph

JSON-LD property containing an array of entities.

Example:
```json
{
  "@graph": [
    { "@id": "ex:alice", "schema:name": "Alice" }
  ]
}
```

### @value

JSON-LD property for specifying a literal value explicitly.

Example: `{"@value": "30", "@type": "xsd:integer"}`

### Compact IRI

A shortened IRI using namespace prefix.

Example: `ex:alice` (compact) vs `http://example.org/ns/alice` (full)

### IRI Expansion

Converting compact IRIs to full IRIs using @context mappings.

Example: `ex:alice` expands to `http://example.org/ns/alice`

### IRI Compaction

Converting full IRIs to compact form using @context.

Example: `http://schema.org/name` compacts to `schema:name`

## Query Execution Terms

### Fuel

A measure of query/transaction execution cost. One unit of fuel is consumed for each item processed (flakes matched, items expanded during graph crawl, etc.). Used to prevent runaway queries from consuming excessive resources.

Example: `"opts": {"max-fuel": 10000}` limits query to 10,000 fuel units.

### Tracking

Query/transaction execution monitoring that provides visibility into performance metrics. When enabled, returns time (execution duration), fuel (items processed), and policy statistics.

Example: `"opts": {"meta": true}` enables all tracking metrics.

### TrackingTally

The result of tracking, containing time (formatted as "12.34ms"), fuel (total count), and policy stats (`{policy-id: {executed, allowed}}`).

## Acronyms

- **ANN:** Approximate Nearest Neighbor
- **API:** Application Programming Interface
- **CORS:** Cross-Origin Resource Sharing
- **CAS:** Compare-And-Swap
- **CID:** Content Identifier (multiformats)
- **DID:** Decentralized Identifier
- **HTTP:** Hypertext Transfer Protocol
- **HNSW:** Hierarchical Navigable Small World
- **IRI:** Internationalized Resource Identifier
- **JSON:** JavaScript Object Notation
- **JSON-LD:** JSON for Linked Data
- **JWT:** JSON Web Token
- **JWS:** JSON Web Signature
- **RDF:** Resource Description Framework
- **REST:** Representational State Transfer
- **SHA:** Secure Hash Algorithm
- **SPARQL:** SPARQL Protocol and RDF Query Language
- **SSL/TLS:** Secure Sockets Layer / Transport Layer Security
- **URI:** Uniform Resource Identifier
- **URL:** Uniform Resource Locator
- **VC:** Verifiable Credential
- **W3C:** World Wide Web Consortium
- **XSD:** XML Schema Definition

## Related Documentation

- [Standards and feature flags](compatibility.md) - Standards compliance and feature flags
- [Crate Map](crate-map.md) - Code architecture
- [Concepts](../concepts/README.md) - Core concepts
