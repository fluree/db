# Standards and Feature Flags

This document covers Fluree's compliance with standards and feature flags.

## Standards Compliance

### RDF 1.1

**Status:** Fully compliant

Fluree implements the W3C RDF 1.1 specification:
- RDF triples (subject-predicate-object)
- IRI identifiers
- Typed literals
- Language tags
- Blank nodes
- RDF datasets

**Specification:** https://www.w3.org/TR/rdf11-concepts/

### JSON-LD 1.1

**Status:** Fully compliant

Fluree supports JSON-LD 1.1:
- @context for namespace mappings
- @id for resource identification
- @type for type specification
- @graph for multiple entities
- @value and @type for literals
- @language for language tags
- Nested objects
- Arrays

**Specification:** https://www.w3.org/TR/json-ld11/

### SPARQL 1.1 Query

**Status:** In progress toward full compliance

Supported SPARQL features:
- SELECT queries
- CONSTRUCT queries
- ASK queries
- DESCRIBE queries
- FROM and FROM NAMED clauses
- GRAPH patterns
- OPTIONAL patterns
- UNION patterns
- FILTER expressions
- BIND expressions
- Aggregations (COUNT, SUM, AVG, MIN, MAX, SAMPLE, GROUP_CONCAT) with DISTINCT modifier
- GROUP BY (variables and expressions)
- ORDER BY
- LIMIT and OFFSET
- Subqueries
- Property paths (partial: `+`, `*`, `^`, `|`, `/`; see [SPARQL docs](../query/sparql.md#property-paths))

**Aggregate result types:** COUNT and SUM of integers return `xsd:integer` (per W3C spec), not `xsd:long`. SUM of mixed types and AVG return `xsd:double`.

**W3C Compliance Testing:** Fluree runs the official W3C SPARQL test suite via the `testsuite-sparql` crate. The suite automatically discovers and runs 700+ test cases from W3C manifest files. See the [compliance test guide](../contributing/sparql-compliance.md) for details.

**Specification:** https://www.w3.org/TR/sparql11-query/

### SPARQL 1.1 Update

**Status:** Partial support

Supported:
- INSERT DATA (via JSON-LD transactions)
- DELETE/INSERT WHERE (via WHERE/DELETE/INSERT)

Not yet supported:
- DELETE DATA
- LOAD
- CLEAR
- DROP
- COPY, MOVE, ADD

Use JSON-LD transactions for transaction operations.

**Specification:** https://www.w3.org/TR/sparql11-update/

### Turtle

**Status:** Fully supported

Fluree parses Turtle 1.1:
- @prefix declarations
- Base IRIs
- Abbreviated syntax (a, ;, ,)
- Literals with datatypes and language tags
- Collections
- Blank nodes

**Specification:** https://www.w3.org/TR/turtle/

### JSON Web Signature (JWS)

**Status:** Partial (EdDSA only)

Supported algorithms:
- EdDSA (Ed25519) - **Only supported algorithm**

Not yet supported:
- ES256, ES384, ES512 (ECDSA)
- RS256 (RSA)
- HS256, HS384, HS512 (HMAC)

**Specification:** RFC 7515

**Note:** Requires the `credential` feature flag.

### Verifiable Credentials

**Status:** Planned (not yet implemented)

The credential module currently supports JWS verification only. Full VC support
(proof verification, JSON-LD canonicalization) is planned but not yet available.

**Specification:** https://www.w3.org/TR/vc-data-model/

### Decentralized Identifiers (DIDs)

**Status:** Partial support

Supported DID methods:
- did:key (Ed25519 keys only)

Not yet supported:
- did:web
- did:ion
- did:ethr

**Specification:** https://www.w3.org/TR/did-core/

**Note:** Requires the `credential` feature flag.

## Compile-Time Feature Flags (Cargo)

These features are controlled at compile time via Cargo:

### `fluree-db-api` Features

| Feature | Default | Description |
|---------|---------|-------------|
| `native` | Yes | File storage support |
| `aws` | No | AWS-backed storage support (S3, storage-backed nameservice). Enables `FlureeBuilder::s3()` and S3-based JSON-LD configs. |
| `credential` | No | DID/JWS/VerifiableCredential support for signed queries/transactions. Pulls in crypto dependencies (`ed25519-dalek`, `bs58`). |
| `iceberg` | No | Apache Iceberg/R2RML graph source support |
| `shacl` | No | SHACL constraint validation (requires fluree-db-transact + fluree-db-shacl). Default in server/CLI. |
| `vector` | No | Embedded vector similarity search (HNSW indexes via usearch) |
| `ipfs` | No | IPFS-backed storage via Kubo HTTP RPC |
| `search-remote-client` | No | HTTP client for remote BM25 and vector search services |
| `aws-testcontainers` | No | Opt-in LocalStack-backed S3/DynamoDB tests (auto-start via testcontainers) |
| `full` | No | Convenience bundle: `native`, `credential`, `iceberg`, `shacl`, `ipfs` |

Example:
```toml
[dependencies]
fluree-db-api = { path = "../fluree-db-api", features = ["native", "credential"] }
```

### `fluree-db-server` Features

| Feature | Default | Description |
|---------|---------|-------------|
| `native` | Yes | File storage support (forwards to `fluree-db-api/native`) |
| `credential` | Yes | Signed request verification (forwards to `fluree-db-api/credential`) |
| `shacl` | Yes | SHACL constraint validation (forwards to `fluree-db-api/shacl`) |
| `iceberg` | Yes | Apache Iceberg/R2RML graph source support (forwards to `fluree-db-api/iceberg`) |
| `aws` | No | AWS S3 storage + DynamoDB nameservice (forwards to `fluree-db-api/aws`) |
| `oidc` | No | OIDC JWT verification via JWKS (RS256 tokens from external IdPs) |
| `swagger-ui` | No | Swagger UI endpoint |
| `otel` | No | OpenTelemetry tracing |

To build the server without credential support (faster compile):
```bash
cargo build -p fluree-db-server --no-default-features --features native
```

## Runtime Behavior

Reasoning, SPARQL property paths, and GeoSPARQL functions are always
available in any build that links the corresponding crate features (see
the build-time feature tables above). They are not gated behind a runtime
flag.

Reasoning is opted into per query (via the `reasoning` parameter or the
SPARQL `PRAGMA reasoning` directive) or per ledger (via
`f:reasoningDefaults` in the ledger configuration graph). See
[Query-time reasoning](../query/reasoning.md) and
[Setting groups](../ledger-config/setting-groups.md).

## Parsing Modes

### Strict Mode (Default)

Enforces strict compliance with standards:
- Invalid IRIs rejected
- Type mismatches rejected
- Strict JSON-LD parsing

```bash
./fluree-db-server --strict-mode true
```

### Lenient Mode

More permissive parsing:
- Attempts to fix malformed IRIs
- Coerces types when possible
- Accepts non-standard syntax

```bash
./fluree-db-server --strict-mode false
```

Use lenient mode only when you fully control inputs and explicitly want permissive parsing behavior.

## API Versioning

Current API version: v1

**Version Header:**
```http
X-Fluree-API-Version: 1
```

## Supported Data Formats

### JSON-LD

Supported JSON-LD versions:
- JSON-LD 1.0: Yes
- JSON-LD 1.1: Yes

### SPARQL

Supported SPARQL versions:
- SPARQL 1.0: Yes
- SPARQL 1.1: Yes

### RDF Formats

| Format | Read | Write |
|--------|------|-------|
| JSON-LD | Yes | Yes |
| Turtle | Yes | Yes |
| N-Triples | Planned | Planned |
| N-Quads | Planned | Planned |
| RDF/XML | Planned | No |
| TriG | Planned | Planned |

## Protocol Support

### HTTP Versions

- HTTP/1.1: Fully supported
- HTTP/2: Supported
- HTTP/3: Planned

### TLS Versions

- TLS 1.2: Supported
- TLS 1.3: Supported
- SSL 3.0: Not supported (deprecated)
- TLS 1.0/1.1: Not supported (deprecated)

## Client Support

Fluree works with:

**HTTP Clients:**
- curl
- Postman
- Insomnia
- Any HTTP client library

**RDF Libraries:**
- Apache Jena (Java)
- RDF4J (Java)
- rdflib (Python)
- N3.js (JavaScript)

**SPARQL Clients:**
- Apache Jena ARQ
- RDF4J SPARQLRepository
- Any SPARQL 1.1 client

## Platform Support

### Operating Systems

**Server:**
- Linux (x86_64, aarch64)
- macOS (Intel, Apple Silicon)
- Windows (x86_64)

**Clients:**
- Any OS with HTTP support

### Cloud Platforms

- AWS (native support)
- Google Cloud Platform (via file storage)
- Azure (via file storage)
- Self-hosted / on-premises

### Container Support

- Docker: Full support
- Kubernetes: Full support
- Podman: Supported
- Docker Compose: Full support

## Database Support

### Import Sources

Fluree can import from:

**RDF Databases:**
- Apache Jena TDB
- Virtuoso
- Stardog
- GraphDB
- Any RDF export

**Graph Databases:**
- Neo4j (via RDF export)
- Amazon Neptune (via RDF export)

**Relational Databases:**
- Via R2RML mapping
- Direct SQL query

### Export Formats

Export Fluree data to:
- Turtle files
- JSON-LD documents
- SPARQL CONSTRUCT results
- Any RDF format

## Feature Roadmap

### Planned Features

**Query:**
- SPARQL property paths: remaining operators (`?` zero-or-one, `!` negated set)
- GeoSPARQL
- SPARQL 1.1 Federation
- Full SPARQL UPDATE

**Storage:**
- Additional cloud providers (GCP, Azure)
- Hybrid storage modes

**Security:**
- OAuth 2.0 integration
- SAML support
- Additional DID methods

**Graph Sources:**
- BigQuery integration
- Snowflake integration
- Elasticsearch integration

### Feature Discovery

Feature availability is documented in this compatibility matrix and by
crate feature flags; the standalone server does not expose a `/features`
HTTP endpoint.

## Browser Support

For web applications using Fluree API:

**Supported Browsers:**
- Chrome/Edge 90+
- Firefox 88+
- Safari 14+

**Requirements:**
- Fetch API support
- CORS support
- WebSocket support (for future streaming)

## Tool Support

### RDF Tools

Compatible with standard RDF tools:
- Protégé (ontology editor)
- TopBraid Composer
- RDF validators
- SPARQL editors

### Data Tools

Works with data engineering tools:
- Apache Airflow (via HTTP operators)
- dbt (via SQL proxy with R2RML)
- Apache Spark (via Iceberg)
- Pandas (via query API)

## Version Requirements

### Rust Version

Building from source requires:
- Rust 1.75.0 or later
- Cargo 1.75.0 or later

### Dependencies

Runtime dependencies:
- None (statically linked binary)

Optional dependencies:
- AWS SDK (for AWS storage)

## Related Documentation

- [Glossary](glossary.md) - Term definitions
- [Crate Map](crate-map.md) - Code architecture
- [Getting Started](../getting-started/README.md) - Installation
