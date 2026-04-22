# Crate Map

Fluree is organized into multiple Rust crates, each with a specific purpose. This document provides an overview of the crate architecture and dependencies.

## Crate Organization

```text
fluree-db/
├── Core
│   ├── fluree-vocab/              # RDF vocabulary constants and namespace codes
│   ├── fluree-db-core/            # Runtime-agnostic core types and queries
│   └── fluree-db-novelty/         # Novelty overlay and commit types
│
├── Graph Processing
│   ├── fluree-graph-ir/           # Format-agnostic RDF intermediate representation
│   ├── fluree-graph-json-ld/      # JSON-LD processing
│   ├── fluree-graph-turtle/       # Turtle parser
│   └── fluree-graph-format/       # RDF formatters (JSON-LD, Turtle, etc.)
│
├── Query & Transaction
│   ├── fluree-db-query/           # Query engine (JSON-LD Query)
│   ├── fluree-db-sparql/          # SPARQL parser and lowering
│   └── fluree-db-transact/        # Transaction processing
│
├── Storage & Connection
│   ├── fluree-db-connection/      # Storage backends and connection management
│   ├── fluree-db-storage-aws/     # AWS storage (S3, S3 Express, DynamoDB)
│   ├── fluree-db-nameservice/     # Nameservice implementations
│   └── fluree-db-nameservice-sync/# Git-like remote sync for nameservice
│
├── Indexing
│   ├── fluree-db-binary-index/    # Binary index formats + read-side runtime
│   ├── fluree-db-indexer/         # Index building
│   └── fluree-db-ledger/          # Ledger state (indexed DB + novelty)
│
├── Security & Validation
│   ├── fluree-db-policy/          # Policy enforcement
│   ├── fluree-db-credential/      # JWS/VerifiableCredential verification
│   ├── fluree-db-crypto/          # Storage encryption (AES-256-GCM)
│   └── fluree-db-shacl/           # SHACL validation engine
│
├── Reasoning
│   └── fluree-db-reasoner/        # OWL2-RL reasoning engine
│
├── Graph Sources
│   ├── fluree-db-tabular/         # Tabular column batch types
│   ├── fluree-db-iceberg/         # Apache Iceberg integration
│   └── fluree-db-r2rml/           # R2RML mapping support
│
├── Search
│   ├── fluree-search-protocol/    # Search service protocol types
│   ├── fluree-search-service/     # Search backend implementations
│   └── fluree-search-httpd/       # Standalone HTTP search server
│
├── Networking
│   ├── fluree-sse/                # Server-Sent Events parser
│   └── fluree-db-peer/            # SSE protocol for peer mode
│
└── Top-Level
    ├── fluree-db-api/             # Public API and high-level operations
    └── fluree-db-server/          # HTTP server (binary)
```

## Foundation Crates

### fluree-vocab

**Purpose:** RDF vocabulary constants and namespace codes

**Responsibilities:**
- Standard RDF namespace definitions (rdf:, rdfs:, xsd:, owl:, etc.)
- Fluree-specific namespace codes
- IRI constants for common predicates

**Dependencies:** None (foundation crate)

### fluree-db-core

**Purpose:** Runtime-agnostic core library for Fluree DB

**Responsibilities:**
- Core types (Flake, Sid, IndexType, etc.)
- Index structures (SPOT, POST, OPST, PSOT)
- Range query operations
- Database snapshot representation
- Statistics and cardinality tracking
- Content-addressed identity (`ContentId`, `ContentKind`)
- Content store trait (`ContentStore`)

**Key Types:**
- `Flake` - Indexed triple representation
- `Sid` - Subject identifier
- `LedgerSnapshot` - Database snapshot at a point in time
- `IndexType` - Index selection enum
- `StatsView` - Query statistics
- `ContentId` - CIDv1 content-addressed identifier
- `ContentKind` - Content type enum (Commit, Txn, IndexRoot, etc.)
- `ContentStore` - Content-addressed storage trait
- `BranchedContentStore` - Recursive content store with namespace fallback for branches

**Dependencies:**
- fluree-vocab

### fluree-db-novelty

**Purpose:** Novelty overlay and commit types

**Responsibilities:**
- In-memory novelty (uncommitted/unindexed flakes)
- Commit metadata and structure
- Novelty application and slicing

**Key Types:**
- `Novelty` - In-memory flake overlay
- `Commit` - Commit metadata
- `FlakeId` - Novelty flake identifier

**Dependencies:**
- fluree-db-core
- fluree-db-binary-index
- fluree-vocab

## Graph Processing Crates

### fluree-graph-ir

**Purpose:** Format-agnostic RDF intermediate representation

**Responsibilities:**
- Generic graph IR for RDF data
- Triple/quad representation
- Format-independent graph operations

**Dependencies:**
- fluree-vocab

### fluree-graph-json-ld

**Purpose:** Minimal JSON-LD processing

**Responsibilities:**
- JSON-LD expansion
- JSON-LD compaction
- @context handling
- IRI resolution

**Dependencies:**
- fluree-graph-ir
- fluree-vocab

### fluree-graph-turtle

**Purpose:** Turtle (TTL) parser

**Responsibilities:**
- Turtle syntax parsing
- Triple generation from Turtle

**Dependencies:**
- fluree-graph-ir
- fluree-vocab

### fluree-graph-format

**Purpose:** RDF graph formatters

**Responsibilities:**
- Output formatting (JSON-LD, Turtle, N-Triples)
- Serialization utilities

**Dependencies:**
- fluree-graph-ir

## Query & Transaction Crates

### fluree-db-query

**Purpose:** Query engine for JSON-LD Query

**Responsibilities:**
- Query parsing and planning
- Statistics-driven pattern reordering across all WHERE-clause pattern types
  (triples, UNION, OPTIONAL, MINUS, search patterns, Graph, Service, etc.)
- Bound-variable-aware selectivity estimation using HLL-derived property
  statistics (with heuristic fallbacks)
- Query execution
- Filter pushdown (index-level range filters, inline join/BIND evaluation,
  dependency-based placement, compound pattern nesting)
- Aggregations
- BM25 and vector search integration
- Explain plan generation for optimization debugging

**Key Types:**
- `Query` - Parsed query
- `VarRegistry` - Variable management
- `Pattern` - Query patterns
- `TriplePattern` - Subject–predicate–object pattern with optional `DatatypeConstraint`
- `Ref` - Variable or constant in subject/predicate position (no literals)
- `Term` - Variable or constant in object position (includes literals)
- `DatatypeConstraint` - Explicit datatype (`Explicit(Sid)`) or language tag
  (`LangTag`; implies `rdf:langString` datatype)
- `PatternEstimate` - Cardinality classification (Source, Reducer, Expander, Deferred)

**Dependencies:**
- fluree-db-core

### fluree-db-sparql

**Purpose:** SPARQL parsing and execution

**Responsibilities:**
- SPARQL lexing and parsing
- AST construction
- Lowering to internal IR
- Diagnostic reporting

**Key Types:**
- `Query` - SPARQL query AST
- `Pattern` - Graph pattern
- `Diagnostic` - Parse/validation errors

**Dependencies:**
- fluree-db-query
- fluree-db-core

### fluree-db-transact

**Purpose:** Transaction processing

**Responsibilities:**
- JSON-LD transaction parsing
- RDF triple generation
- Flake generation
- Commit creation

**Dependencies:**
- fluree-graph-json-ld
- fluree-db-core

## Storage & Connection Crates

### fluree-db-connection

**Purpose:** Storage backends and connection management

**Responsibilities:**
- Storage abstraction trait
- Memory, file, and cloud storage
- Address resolution
- Commit storage and retrieval

**Key Types:**
- `Storage` trait
- `MemoryStorage`
- `FileStorage`

**Dependencies:**
- fluree-db-core
- fluree-graph-json-ld
- fluree-db-storage-aws (optional)
- fluree-db-nameservice

### fluree-db-storage-aws

**Purpose:** AWS storage backends

**Responsibilities:**
- S3 storage implementation
- S3 Express One Zone support
- DynamoDB integration

**Dependencies:**
- fluree-db-core
- fluree-db-nameservice

### fluree-db-nameservice

**Purpose:** Nameservice implementations

**Responsibilities:**
- Nameservice abstraction
- Ledger metadata management
- Publish/lookup operations
- Branch creation and listing
- File and DynamoDB backends

**Key Types:**
- `NameService` trait (includes `list_branches`, `create_branch`, `drop_branch`)
- `Publisher` trait (commit/index publishing)
- `NsRecord` - Nameservice record (includes `source_branch` for ancestry and `branches` child count for reference counting)
- `FileNameService`

**Dependencies:**
- fluree-db-core

### fluree-db-nameservice-sync

**Purpose:** Git-like remote sync for nameservice

**Responsibilities:**
- Remote nameservice synchronization (fetch/push refs)
- Multi-origin CAS object fetching with integrity verification
- Pack protocol client (streaming binary transport for clone/pull)
- SSE-based change streaming
- Sync driver (fetch/pull/push orchestration)

**Key Types:**
- `MultiOriginFetcher` - Priority-ordered HTTP origin fallback
- `HttpOriginFetcher` - Single-origin CAS object + pack fetcher
- `SyncDriver` - Orchestrates fetch/pull/push with remote clients
- `PackIngestResult` - Result of streaming pack import

**Dependencies:**
- fluree-db-core
- fluree-db-nameservice
- fluree-db-novelty
- fluree-sse

## Indexing Crates

### fluree-db-binary-index

**Purpose:** Binary index wire formats and read-side runtime

**Responsibilities:**
- Binary index format codecs (FIR6 root, FBR3 branch, FLI3 leaf, leaflet layout)
- Dictionary artifacts and readers (inline dicts, dict trees, arenas)
- Query-time read types (`BinaryIndexStore`, `BinaryGraphView`, cursors)

**Dependencies:**
- fluree-db-core

### fluree-db-indexer

**Purpose:** Index building for Fluree DB

**Responsibilities:**
- Incremental index updates
- Full reindexing
- Index refresh orchestration

**Dependencies:**
- fluree-db-core
- fluree-db-binary-index
- fluree-db-novelty
- fluree-db-nameservice
- fluree-vocab

### fluree-db-ledger

**Purpose:** Ledger state management

**Responsibilities:**
- Combining indexed DB with novelty overlay
- Ledger snapshot creation
- State transitions
- Building `BranchedContentStore` trees from branch ancestry

**Key Types:**
- `LedgerState` - Complete ledger snapshot

**Dependencies:**
- fluree-db-core
- fluree-db-novelty
- fluree-db-nameservice

## Security & Validation Crates

### fluree-db-policy

**Purpose:** Policy enforcement

**Responsibilities:**
- Policy parsing and evaluation
- Query augmentation for policy
- Transaction authorization

**Dependencies:**
- fluree-db-query
- fluree-db-core

### fluree-db-credential

**Purpose:** Credential verification

**Responsibilities:**
- JWS signature verification
- VerifiableCredential processing
- DID resolution

**Dependencies:** None (standalone)

### fluree-db-crypto

**Purpose:** Storage encryption

**Responsibilities:**
- AES-256-GCM encryption/decryption
- Key management
- Encrypted storage layer

**Dependencies:**
- fluree-db-core

### fluree-db-shacl

**Purpose:** SHACL validation engine

**Responsibilities:**
- SHACL shapes parsing
- Constraint validation
- Validation reports

**Dependencies:**
- fluree-db-core
- fluree-db-query
- fluree-vocab

## Reasoning

### fluree-db-reasoner

**Purpose:** OWL2-RL reasoning engine

**Responsibilities:**
- OWL2-RL rule application
- Inference generation
- Materialization

**Dependencies:**
- fluree-db-core
- fluree-vocab

## Graph Source Crates

### fluree-db-tabular

**Purpose:** Tabular column batch types

**Responsibilities:**
- Arrow-compatible column batches
- Graph source data abstraction

**Dependencies:** None (foundation for graph sources)

### fluree-db-iceberg

**Purpose:** Apache Iceberg integration

**Responsibilities:**
- Iceberg REST catalog support
- Iceberg table scanning
- Parquet file reading

**Dependencies:**
- fluree-db-core
- fluree-db-tabular

### fluree-db-r2rml

**Purpose:** R2RML mapping support

**Responsibilities:**
- R2RML mapping parsing
- Relational-to-RDF mapping
- Graph source generation

**Dependencies:**
- fluree-graph-ir
- fluree-graph-turtle (optional)
- fluree-db-tabular
- fluree-vocab

## Search Crates

### fluree-search-protocol

**Purpose:** Search service protocol types

**Responsibilities:**
- Request/response structs
- Error model and codes
- Protocol version constants
- BM25 and vector query definitions

**Dependencies:** serde, thiserror

### fluree-search-service

**Purpose:** Search backend implementations

**Responsibilities:**
- `SearchBackend` trait
- BM25 backend (tantivy)
- Vector backend (usearch, feature-gated)
- Index caching with TTL

**Dependencies:**
- fluree-search-protocol
- fluree-db-query
- fluree-db-core

### fluree-search-httpd

**Purpose:** Standalone HTTP search server

**Responsibilities:**
- HTTP API for search queries
- Index loading from storage
- Health and capabilities endpoints

**Dependencies:**
- fluree-search-protocol
- fluree-search-service
- axum, tokio

## Networking Crates

### fluree-sse

**Purpose:** Lightweight SSE parser

**Responsibilities:**
- Server-Sent Events parsing
- Event stream handling

**Dependencies:** None (foundation)

### fluree-db-peer

**Purpose:** SSE protocol for peer mode

**Responsibilities:**
- Peer protocol types
- SSE client for peer communication

**Dependencies:**
- fluree-sse

## Top-Level Crates

### fluree-db-api

**Purpose:** Public API and orchestration

**Responsibilities:**
- Ledger lifecycle (create, load, drop, branch)
- Query execution coordination
- Transaction execution
- Time travel resolution
- Policy application
- Dataset and view composition

**Key Types:**
- `Fluree` - Main entry point
- `Graph` - Lazy handle for chaining
- `GraphSnapshot` - Materialized snapshot
- `LedgerState` - Loaded ledger state
- `QueryResult` - Query results
- `TransactResult` - Commit receipt

**Dependencies:**
- fluree-db-query
- fluree-db-sparql
- fluree-db-transact
- fluree-db-connection
- fluree-db-nameservice
- fluree-db-policy
- fluree-db-reasoner
- fluree-db-shacl

### fluree-db-server

**Purpose:** HTTP server (binary)

**Responsibilities:**
- HTTP API endpoints
- Request routing
- Response formatting
- TLS/SSL, CORS handling

**Dependencies:**
- fluree-db-api
- axum

## Dependency Layers

```text
Layer 5 (Top)        fluree-db-server
                            │
                     fluree-db-api
                            │
Layer 4 (Features)   ┌──────┼──────┬──────────┬───────────┐
                     │      │      │          │           │
                  policy  shacl reasoner  credential  crypto
                     │      │      │
Layer 3 (Query)      └──────┴──────┴──────────┐
                                              │
                     fluree-db-query ←── fluree-db-sparql
                            │
Layer 2 (Data)       ledger, binary-index, indexer, novelty, connection
                            │
Layer 1 (Core)       fluree-db-core
                            │
Layer 0 (Foundation) fluree-vocab, fluree-sse, fluree-db-tabular
```

## External Dependencies

### Key External Crates

**Web Framework:**
- `axum` - HTTP server framework
- `tokio` - Async runtime
- `tower` - Service abstractions

**Serialization:**
- `serde` - Serialization framework
- `serde_json` - JSON support

**RDF:**
- `oxiri` - IRI parsing and validation

**Storage:**
- `aws-sdk-s3` - AWS S3 client
- `aws-sdk-dynamodb` - AWS DynamoDB client

**Search:**
- `tantivy` - BM25 full-text search
- `usearch` - Vector similarity search (HNSW indexes)

**Analytics:**
- `iceberg-rust` - Apache Iceberg support
- `parquet` - Parquet file reading

**Cryptography:**
- `ed25519-dalek` - Ed25519 signatures
- `ring` - Cryptographic operations

## Building

### Build All

```bash
cargo build --release
```

### Build Server Only

```bash
cargo build --release --bin fluree-db-server
```

### Run Tests

```bash
cargo test
```

### Build with Features

```bash
cargo build --features native,vector
```

## Crate Versions

All crates use synchronized versioning and are updated together.

Check versions:

```bash
cargo tree | grep fluree
```

## Related Documentation

- [Contributing: Dev Setup](../contributing/dev-setup.md) - Development environment
- [Contributing: Tests](../contributing/tests.md) - Testing guide
- [Glossary](glossary.md) - Term definitions
