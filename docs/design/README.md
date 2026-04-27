# Design

Architecture and design documents for Fluree's internal systems. These documents describe the rationale behind key design decisions, wire formats, and trait architectures.

## Documents

### [Query execution and overlay merge](query-execution.md)

How queries run through a single preparation/execution pipeline, how scan operators select the binary-cursor path vs the range fallback, and where overlay novelty merges with indexed data (including graph scoping boundaries).

### [Auth Contract (CLI ↔ Server)](auth-contract.md)

Wire-level contract between the Fluree CLI and any Fluree-compatible server, covering OIDC device auth, token refresh, and storage proxy authentication.

### [Nameservice Schema v2](nameservice-schema-v2.md)

Design of the nameservice schema: ledger records, graph source records, configuration payloads, and the ref/config/tracking store abstractions.

### [Storage-agnostic Commits and Sync](storage-agnostic-commits-and-sync.md)

How ContentId (CIDv1) values decouple the commit chain from storage backends, enabling replication across filesystem, S3, and IPFS. Includes the pack protocol wire format for efficient bulk transfer.

### [ContentId and ContentStore](content-id-and-contentstore.md)

The content-addressed identity layer: `ContentId` type, `ContentStore` trait, multicodec content kinds, and the bridge between CID-based identity and storage-backend addressing.

### [Index Format](index-format.md)

Binary columnar index format: branch/leaf/leaflet hierarchy, dictionary artifacts, SPOT/PSOT/POST/OPST/TSPO layout, and encoding details.

### [Namespace allocation and fallback modes](namespace-allocation.md)

How Fluree assigns `ns_code` values for IRIs (prefix trie matching, fallback split modes), including bulk-import preflight mitigation and how the “host-only” fallback persists for future transactions.

### [Ontology imports (`f:schemaSource` + `owl:imports`)](ontology-imports.md)

How the reasoner consumes schema from a named `f:schemaSource` graph and transitively resolves `owl:imports`: resolution order, the `SchemaBundleOverlay` projection, schema-triple whitelist, and caching.

### [Storage Traits](storage-traits.md)

Storage trait architecture: `StorageRead`, `StorageWrite`, `ContentAddressedWrite`, `Storage`, and `NameService` trait design with guidance for implementing new backends.

### [Custom Merge Commits](merge-custom.md)

`MergePlan` design for the four-step merge flow (preview → query → validate → commit): per-conflict resolutions, plan-level patches, staleness guards, SHACL validation against the staged merge state, and idempotent retry semantics.

## Related Documentation

- [Crate Map](../reference/crate-map.md) - Workspace architecture
- [Contributing](../contributing/README.md) - Development guidelines
- [Graph Identities and Naming](../reference/graph-identities.md) - Naming conventions (user-facing and internal)
