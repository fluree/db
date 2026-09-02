# Design

Architecture and design documents for Fluree's internal systems. These documents describe the rationale behind key design decisions, wire formats, and trait architectures.

## Documents

### [Performance architecture](performance.md)

Why Fluree is fast, layer by layer: integer-ID execution, columnar leaflets with region-selective decompression, directory-only aggregates, the cost model and its tested invariants, the specialized join operators, the 16 fast-path operators and their fallback contract, batched frontier traversal, and where parallelism is and isn't applied. Includes measured head-to-head results and a frank account of current limits.

### [Query execution and overlay merge](query-execution.md)

How queries run through a single preparation/execution pipeline, how scan operators select the binary-cursor path vs the range fallback, and where overlay novelty merges with indexed data (including graph scoping boundaries).

### [Row-returning multi-fact virtual joins (late-materialization corridor)](virtual-multifact-row-corridor.md)

Sized design (not yet implemented) for a row-returning multi-fact join over the virtual/R2RML path: why it needs a new row-emitting columnar operator rather than a widening of the fused aggregate, and how it reuses the semi-join membership + FK→IRI resolver + budget-forwarding primitives to prune columnar-first and late-materialize only surviving rows. Captured out of db #1589 (which shipped the fused-aggregate S1/S2 generality).

### [Auth Contract (CLI ↔ Server)](auth-contract.md)

Wire-level contract between the Fluree CLI and any Fluree-compatible server, covering OIDC device auth, token refresh, and storage proxy authentication.

### [Nameservice Schema v2](nameservice-schema-v2.md)

Design of the nameservice schema: ledger records, graph source records, configuration payloads, and the ref/config/tracking store abstractions.

### [Storage-agnostic Commits and Sync](storage-agnostic-commits-and-sync.md)

How ContentId (CIDv1) values decouple the commit chain from storage backends, enabling replication across filesystem, S3, and IPFS. Includes the pack protocol wire format for efficient bulk transfer.

### [Remote mounts and serving tiers](remote-mounts.md)

How a Fluree server exposes ledgers to other instances (query tier vs raw-block tier, per-ledger `f:servingDefaults` posture, token scoping) and how a consumer mounts a remote's ledgers read-only under an alias prefix: `CompositeNameService`, `StorageBackend::Routed`, `ProxyStorage` raw/filtered modes, and the CID-verified cache-forever integrity model.

### [ContentId and ContentStore](content-id-and-contentstore.md)

The content-addressed identity layer: `ContentId` type, `ContentStore` trait, multicodec content kinds, and the bridge between CID-based identity and storage-backend addressing.

### [Index Format](index-format.md)

Binary columnar index format: branch/leaf/leaflet hierarchy, dictionary artifacts, SPOT/PSOT/POST/OPST layout, and encoding details.

### [Edge annotations (storage internals)](edge-annotations.md)

On-disk representation of RDF 1.2 edge annotations: the durable `f:reifies*` flake bundle, the derived `EdgeKey ↔ subject` annotation arena, the indexer state machine, and garbage-collection reachability. (User-facing contract lives in [Edge annotations (concept doc)](../concepts/edge-annotations.md).)

### [Spatial Index](spatial-index.md)

Geospatial indexing internals: inline GeoPoint encoding (packed 60-bit lat/lng for POINT geometries with latitude-band scans) and the S2 cell-based index for complex geometries, plus the query pipeline and time-travel semantics.

### [Namespace allocation and fallback modes](namespace-allocation.md)

How Fluree assigns `ns_code` values for IRIs (prefix trie matching, fallback split modes), including bulk-import preflight mitigation and how the “host-only” fallback persists for future transactions.

### [Ontology imports (`f:schemaSource` + `owl:imports`)](ontology-imports.md)

How the reasoner consumes schema from a named `f:schemaSource` graph and transitively resolves `owl:imports`: resolution order, the `SchemaBundleOverlay` projection, schema-triple whitelist, and caching.

### [Cross-ledger model enforcement](cross-ledger-model-enforcement.md)

How a single **model ledger** can hold the ontology, SHACL shapes, policy rules, datalog rules, and uniqueness constraints that govern many **data ledgers** that reference it via `f:GraphRef` (`f:ledger`, `f:atT`). Specifies the shared resolver contract, term-space translation, policy IR identity split, failure variants, caching, and phasing.

### [Storage Traits](storage-traits.md)

Storage trait architecture: `StorageRead`, `StorageWrite`, `ContentAddressedWrite`, `Storage`, and `NameService` trait design with guidance for implementing new backends.

### [Raft substrate (`fluree-raft-core`)](raft-core.md)

The application-agnostic half of Raft: storage, node/group identity, rendezvous
ownership, transport, membership admin, forwarding, the `AppStateMachine` /
`StateMachineObserver` seam, group bootstrap, and the optional replicated
key/value fragment. Read this before building a new replicated group, or before
touching the fence, expiry, or eviction semantics of `kv`.

### [Raft command queue and replicated state machine](raft-command-queue.md)

How `fluree-db-consensus` replicates writes across a cluster: the queue → stage → apply flow, log entry types, snapshot model, and the rationale behind splitting "decisions" (in the Raft log) from "bytes" (in the shared content-addressed store). Operator-facing recipe lives in [Raft clusters (replicated writes)](../operations/raft-clusters.md).

## Related Documentation

- [Crate Map](../reference/crate-map.md) - Workspace architecture
- [Contributing](../contributing/README.md) - Development guidelines
- [Graph Identities and Naming](../reference/graph-identities.md) - Naming conventions (user-facing and internal)
