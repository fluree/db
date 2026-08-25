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
├── Consensus
│   ├── fluree-raft-core/          # Generic Raft substrate (storage, node/group identity, ownership)
│   └── fluree-db-consensus/       # Committer traits + the Raft-replicated nameservice
│
└── Top-Level
    ├── fluree-db-api/             # Public API and high-level operations
    ├── fluree-db-bolt/            # Bolt protocol codec + session machine
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

## Consensus Crates

### fluree-raft-core

**Purpose**: Application-agnostic Raft substrate — the generic half of what
began inside `fluree-db-consensus::raft`.

**Key modules**:

- `storage` — durable log/vote/snapshot traits, with filesystem
  (atomic write → fsync → rename) and in-memory backends
- `node` — `NodeId` and `ClusterNode`, the raft/client address pair that
  travels through membership changes
- `group` — `GroupId`, the validated name of one group within a process
  (a group's storage lives at `<root>/<group_id>/`)
- `ownership` — rendezvous (HRW) hashing for assigning work to members
  without a consensus round
- `http` — hop-by-hop header classification for request forwarding

Under the `raft` feature, which gates `openraft`:

- `config` — `FlureeRaftConfig`, the constrained openraft profile every
  group shares (pins `NodeId`, `Node`, `Entry`, `SnapshotData`,
  `Responder`, `AsyncRuntime`, leaving only `D`/`R` open).
  Blanket-implemented; applications still write their own
  `declare_raft_types!`.
- `state_machine` — the application seam: `AppStateMachine` for
  deterministic reduction, `StateMachineObserver` for effects captured
  under the state lock and published after it drops, a versioned
  snapshot codec, and the adapter that drives openraft from the pair
- `runtime` — `RaftGroup::bootstrap`, `RaftGroupConfig`, and the
  leader-only task lifecycle (cancellation with bounded graceful
  shutdown, then abort)
- `log_adapter` — `LogAdapter<C, S>`, openraft's `RaftLogStorage` over
  the storage traits
- `network` — `RaftTransportConfig`, the HTTP+postcard RPC client, and a
  **relative** router for `append-entries` / `vote` / `install-snapshot`
- `admin` — `RaftAdmin<C>` and a relative router for `initialize`,
  `add-learner`, `change-membership`, `status`
- `forward` — follower→leader middleware, generic over a `LeaderView`
  source rather than tied to `Raft` directly

And under `kv` (independent of `raft` — pure state plus a pure
reduction, so a consumer can hold the semantics without linking
openraft):

- `kv` — a replicated key/value *fragment* an application embeds in its
  own state machine, not a service and not its own group. A lease fences
  the work it guards only if both are ordered by the same log. An
  entry's version is the **Raft log index** of the write that created
  it, so a fencing token can never repeat; expiry is logical absence,
  kept invisible across partial sweeps by a monotonic logical-time floor
  that every reclamation raises; every CAS failure returns the current
  record, which is also the recovery path for a lost response. TTLs are
  rejected rather than clamped, and a fragment's expiry index and byte
  total are rebuilt on snapshot decode rather than trusted. Tenancy is
  the application's
  composition — `BTreeMap<Tenant, KvFragment>` keyed by an
  **append-only** enum, because postcard is positional: appending a
  struct field breaks every existing snapshot, while appending an enum
  variant does not.
- `kv::sweep` (`kv` + `raft`) — the leader-only eviction driver.
  `Evict` is bounded on purpose, so something has to notice
  `more_expired` and come back. Two details live here rather than in
  each consumer: re-propose **immediately with the same cutoff** (a
  fresh clock read per round lets a steadily-expiring fragment outrun
  the sweep), and **propose nothing when nothing has expired** (an idle
  ticker that still writes grows the log on every node forever). Spawn
  `run_sweep` from `spawn_leader_watcher`'s task factory.

Full design rationale: `docs/design/raft-core.md`.

And under `testing`:

- `testing` — a conformance fixture any openraft state-machine adapter
  can be run through (snapshot persist-before-swap, boot restore,
  membership bookkeeping, one response per entry). Deliberately not
  specific to this crate's adapter: any openraft `RaftStateMachine` can
  be held to the same contract. Both consumers run it — the toy counter
  in `fluree-raft-core/tests/state_machine_seam.rs` and the nameservice
  in `fluree-db-consensus/tests/it_adapter_conformance.rs`.

**Depends on**: nothing in the workspace. Without the `raft` feature
there is no `openraft` dependency either: storage payloads are opaque
bytes, and `ClusterNode` satisfies openraft's blanket `Node` bound
through its derives alone. That keeps monolithic Fluree builds — which
reach this crate through `fluree-db-consensus` for `http::is_hop_by_hop`
— from compiling or linking openraft.

**Note**: `ownership`'s hash is effectively a wire format — nodes compute
ownership locally and independently, so two nodes that disagree can both
claim the same key. See the module docs before touching it.

**Routing**: the `network` and `admin` routers carry no prefix of their
own. The host nests them — at `/raft` and `/cluster` for a single group,
or under a `GroupId` when several share a process — which is what lets an
existing group keep the paths already recorded in its replicated
membership.

### fluree-db-consensus

**Purpose**: The `Committer` abstraction for submitting transactions, plus
the Raft-replicated nameservice state machine.

**Key types**: `Committer`, `LocalCommitter`, `CachingCommitter`,
`Command`/`Response`, `NameServiceState`, `NameServiceApp`,
`NameServiceObserver`, `RaftNameService`, `QueuedTransactor`,
`commit_worker::Worker`.

**Raft state machine**: `raft::app` holds both halves of the
nameservice's contribution — `NameServiceApp` (the pure reduction) and
`NameServiceObserver` (event bus, waiters, staged receipts, releases,
ledger-cache watermark). The generic bookkeeping is
`fluree_raft_core::state_machine::StateMachineAdapter`, and
`raft::state_machine_adapter` is just their composition, kept at its
historical path. `publish` runs in two phases: every commit-head
watermark reaches the ledger cache before any event reaches the bus.

**Feature flags**: `raft` (non-default) gates `openraft` so monolithic
users don't compile or link it. `testing` (implies `raft`) pulls in
`fluree-raft-core`'s conformance fixture and runs the nameservice's
state machine through it; test-only, so it is off outside
`--all-features`.

**Depends on**: fluree-raft-core, fluree-db-api, fluree-db-core,
fluree-db-nameservice, fluree-db-transact, fluree-db-ledger

**See also**: `docs/design/raft-command-queue.md`,
`docs/operations/raft-clusters.md`

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

### fluree-db-bolt

**Purpose:** Bolt protocol (Neo4j wire protocol) server-side codec

**Responsibilities:**
- PackStream encode/decode
- Chunked message framing + handshake version negotiation
- Autocommit session state machine (pure — no IO, no Fluree deps)

**Dependencies:**
- (none beyond `tracing`; the server crate owns TCP + execution glue)

### fluree-db-server

**Purpose:** HTTP server (binary)

**Responsibilities:**
- HTTP API endpoints
- Request routing
- Response formatting
- TLS/SSL, CORS handling
- Bolt protocol listener (feature `bolt`, via fluree-db-bolt)

**Dependencies:**
- fluree-db-api
- fluree-db-bolt (optional)
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
