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

### RDF 1.2

**Status:** Partial support (edge annotations / reification)

Fluree implements the RDF 1.2 reification model used for edge annotations:
- `rdf:reifies` with triple terms (`<<( s p o )>>`) as the reified object
- Reifiers identified by IRI, blank node, or variable

Fluree also exposes a non-standard extension that reads commit metadata off a
quoted triple (`<< s p o >> f:t ?t`, `f:op ?op`) for transaction-time and
assert/retract introspection.

Not yet supported:
- Turtle 1.2 / TriG 1.2 annotation syntax on ingest (`{| ... |}` annotation
  tails, the `~` reifier, and `<<( ... )>>` triple terms) — convert to JSON-LD,
  or add annotations via SPARQL `INSERT DATA`, instead
- Triple terms as arbitrary object values (only as the `rdf:reifies` object)
- Triple terms in subject position and nested triple terms
- Multiple triples reified by a single annotation

See [Edge annotations](../concepts/edge-annotations.md).

**Specification:** https://www.w3.org/TR/rdf12-concepts/

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
- GROUP BY (variables and expressions, including bare built-in calls like `GROUP BY DATATYPE(?v)`)
- ORDER BY
- LIMIT and OFFSET
- Subqueries (evaluated independently — an inner `ORDER BY`/`LIMIT`/`OFFSET` scopes the sub-SELECT before it joins the enclosing pattern, per SPARQL 1.1 §18.2; there are no correlated/LATERAL sub-SELECTs)
- Blank-node property lists (`[ :p ?o ]`) in subject and object position
- Property paths (`+`, `*`, `?`, `^`, `|`, `/`, `!` negated sets, and transitive over a sequence including inverse steps `(^a/b)+`; see [SPARQL docs](../query/sparql.md#property-paths))

**Aggregate result types:** COUNT and SUM of integers return `xsd:integer` (per W3C spec), not `xsd:long`. SUM of mixed types and AVG return `xsd:double`.

**Empty-group aggregates:** Over an implicit single group (no `GROUP BY`) whose pattern matches nothing, `COUNT`, `SUM`, and `AVG` return their identity `"0"^^xsd:integer` (SPARQL 1.1 §18.5.1); `MIN`, `MAX`, and `SAMPLE` have no identity element and return unbound. A query *with* `GROUP BY` over an empty pattern returns zero rows.

**Expression error semantics:** A *dynamic value* error in a `SELECT`/`BIND`/`ORDER BY` expression (e.g. arithmetic on incompatible operand types) leaves that variable unbound for the solution and the query still returns the remaining rows (SPARQL 1.1 §18.5 `Extend`); the same error in a `FILTER` eliminates the solution (§17.2). *Structural* errors — a built-in called with the wrong arity, an unknown datatype IRI — describe a malformed query and are reported as a query error. (Transactions evaluate their `WHERE` clause in strict mode, so a computed value error fails the transaction rather than silently writing an unbound value.)

**`TZ()` / `TIMEZONE()` report UTC (deviation from §17.4.5.8-9):** Fluree normalizes temporal values to UTC and does not persist the source offset — the binary index stores an instant and nothing else — so `TZ` returns `"Z"` and `TIMEZONE` returns `"PT0S"` for every temporal value, rather than the offset the literal was written with. The spec expects the source offset, and the two W3C tests covering it (`functions/tz-01`, `functions/timezone-01`) are registered as not-supported. The reason is determinism across the storage lanes: the offset survives only while a value sits in novelty, so reading it back made these functions answer `"-08:00"` before a background reindex and `"Z"` after — the same query over unchanged data returning a different result, with no write behind it and no way for a caller to predict which they would get. Preserving the offset instead would need either an arena handle (destroying the inline key ordering that dateTime range pushdown depends on) or a sidecar consulted only by these two functions; neither is warranted for a value the caller can supply by rendering in whichever zone they want. **Ordering, comparison and range queries are unaffected** — those use the normalized instant, which is exactly what is stored. Applications that need a wall-clock offset should store it as its own property.

**Date/time arithmetic (extension beyond §17.3):** The operator mapping table maps `-` only over numeric operands, so `?d1 - ?d2` on two temporal values is a type error under the published spec — and, per the expression-error rule above, that surfaces as an unbound variable rather than a failure. Fluree instead evaluates `xsd:dateTime - xsd:dateTime`, `xsd:date - xsd:date` and `xsd:time - xsd:time` to a signed, timezone-normalized `xsd:dayTimeDuration`, following the XPath operators (`op:subtract-dateTimes`, `op:subtract-dates`, `op:subtract-times`) as specified by [SEP-0002](https://github.com/w3c/sparql-dev/blob/main/SEP/SEP-0002/sep-0002.md). This matches the other engines that implement these operators (Stardog, GraphDB, RDFox, Jena, Comunica), but queries using it are **not portable** to a processor implementing only the published spec. Subtraction is the only operator defined over temporal operands; `+`, `*`, `/`, `%` and mixed pairs such as `dateTime - date` remain type errors. The rest of SEP-0002 — `dateTime ± duration`, duration arithmetic, duration ordering, `ADJUST()` — is not implemented. Note this is *not* anticipated 1.2 behavior: the SPARQL 1.2 draft does not extend the operator table either. See [Date/Time Arithmetic](../query/sparql.md#datetime-arithmetic).

**Dataset clauses (§13.2):** A `FROM` / `FROM NAMED` clause defines the query's dataset exhaustively — the default graph is the union of the `FROM` clauses, and `GRAPH ?g` ranges over exactly the `FROM NAMED` graphs. `FROM NAMED` with no `FROM` therefore gives an **empty default graph**. **Changed in 4.1.4:** the HTTP endpoints now implement this; earlier releases substituted a ledger's default graph, and separately enumerated the ledger alias as an extra named graph, which duplicated every `GRAPH ?g` solution. The embedded Rust API was already conformant, so this removes a divergence between surfaces rather than introducing one. The change covers all four HTTP query surfaces — ledger-scoped and connection-scoped `/query`, plus both streaming routes — and the JSON-LD `fromNamed` form follows the identical semantics, so byte-equivalent SPARQL and JSON-LD queries return the same result. A query with no dataset clause keeps its existing behavior. Requests that name only named graphs while also carrying patterns outside `GRAPH { ... }` / `["graph", ...]` receive an `x-fdb-warning` response header. See [Datasets and named graphs](../concepts/datasets-and-named-graphs.md#http-endpoints-and-default-graph-behavior).

**`x-fdb-warning` is a permanent advisory, not a migration aid.** It has no sunset and no opt-out. It describes a query *shape* whose §13.2 semantics are perennially surprising — naming only named graphs and then matching outside them — so it fires on every query surface wherever that shape appears, including the connection-scoped route where §13.2 was already in force before 4.1.4 and no behavior changed. It is named `warning` rather than `deprecation` deliberately: nothing it points at is going away. The response body and status are unaffected — the status is always the one the request earned on its own — so clients should treat the header as informational and must not key error handling on its presence.

**Graph selectors in dataset source objects (4.1.4):** A source object may narrow a source from a whole ledger to one named graph inside it. The selector is now accepted as either `graph` or `@graph` in **both** object forms — `fromNamed` entries and `from` / `to` source objects. Earlier releases read only `@graph` in `fromNamed` and only `graph` in `from`, and silently ignored the other spelling, so a source written with the wrong key resolved to the entire ledger and returned a wider result set with a `200` rather than an error. Queries that were relying on that silent widening will now see only the graph they named.

**W3C Compliance Testing:** Fluree runs the official W3C SPARQL test suite via the `testsuite-sparql` crate. The suite automatically discovers and runs 700+ test cases from W3C manifest files. See the [compliance test guide](../contributing/sparql-compliance.md) for details.

**Specification:** https://www.w3.org/TR/sparql11-query/

### SPARQL 1.1 Update

**Status:** Partial support

Supported:
- INSERT DATA (including `GRAPH <iri> { ... }` named-graph blocks)
- DELETE DATA (including `GRAPH <iri> { ... }` named-graph blocks)
- DELETE WHERE (default graph only — `GRAPH` blocks are rejected)
- DELETE/INSERT WHERE, including the `DELETE { } WHERE { }` and
  `INSERT { } WHERE { }` short forms, with optional `WITH`/`USING` clauses and
  `GRAPH <iri> { ... }` blocks in templates

Not yet supported:
- Variable graph names (`GRAPH ?g { ... }`) in any update — only ground IRIs
- `GRAPH` blocks inside DELETE WHERE
- LOAD
- CLEAR
- DROP
- CREATE
- COPY, MOVE, ADD

JSON-LD transactions remain available as an alternative write surface.

**Specification:** https://www.w3.org/TR/sparql11-update/

### SPARQL 1.2

**Status:** Partial support (annotations)

Supported query and update annotation syntax:
- Anonymous annotation blocks: `?s ?p ?o {| ?ap ?av |}`
- Named reifiers: `?s ?p ?o ~ ?r {| ... |}` (IRI, blank-node, or variable reifier)
- `rdf:reifies` form with `<<( s p o )>>` triple terms
- Annotations in `INSERT DATA` / `DELETE DATA`

Not yet supported:
- Triple-term accessor functions: `TRIPLE()`, `SUBJECT()`, `PREDICATE()`,
  `OBJECT()`, `isTRIPLE()`
- Triple terms as arbitrary values, in `CONSTRUCT` patterns, or in subject
  position; multi-triple and nested annotations
- Named-graph edge annotations in SPARQL UPDATE (default graph only)
- W3C SPARQL 1.2 test-suite execution (manifests present but not yet run)

**Specification:** https://www.w3.org/TR/sparql12-query/

### Turtle

**Status:** Fully supported

Fluree parses Turtle 1.1:
- @prefix declarations
- Base IRIs
- Abbreviated syntax (a, ;, ,)
- Literals with datatypes and language tags
- Collections
- Blank nodes

Resource limits: recursive constructs (property lists, collections, reified
triples) may nest at most 128 levels deep, and a single parse accepts at most
4 GiB of input; either limit produces a clean parse error.

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

### Behavior changes

Response-shape changes within v1 that a client may need to account for.

#### `fluree-track-policy` is now parsed by the server

Previously the server recognised `fluree-track-meta`, `fluree-track-fuel`, and
`fluree-track-time` but not `fluree-track-policy` — a request carrying only
that header was not treated as tracked, and the server returned the plain
untracked body. It is now parsed like its siblings, so **a request sending only
`fluree-track-policy` receives the tracked envelope** (`{"status", "result",
"policy", ...}`) where it previously received a bare result array.

Nobody could have depended on the *tracked* shape for this header, since it was
never honored. The affected case is a client that sends the header and parses a
bare array — including any `fluree` CLI older than this release talking to a
newer server, where `--track-policy` was a silent no-op. Two ways to adapt:

- Read `result` when the body is an object and the body itself when it is an
  array. This is what the CLI does, and it works against either server.
- Or stop sending the header if you do not want the tally.

`fluree-track-meta` behavior is unchanged; it has always implied policy
tracking. See [Tracking and Fuel](../query/tracking-and-fuel.md).

#### Tracked responses carry `policy_enforcement`

Tracked responses gained an optional `policy_enforcement` sibling (and an
`x-fdb-policy-enforcement` response header; on the NDJSON streaming endpoint,
an optional field on the terminal `end` record). It is additive and present
only when a non-root policy context governed the request, so clients that
ignore unknown fields are unaffected. See
[Detecting that policy was applied](../security/policy-in-queries.md#detecting-that-policy-was-applied).

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
| N-Triples | Yes | Yes |
| N-Quads | Yes | Yes |
| TriG | Yes | Yes |
| RDF/XML | No | CONSTRUCT/DESCRIBE results only |

Import accepts `.ttl`, `.nt`, `.nq`, `.trig`, and `.jsonld`/`.jsonl` files, each
with transparent `.gz` / `.zst` decompression.

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
- SPARQL property paths: nested transitive steps inside a composite repeated unit (`(a+/b)+`); `{n,m}` depth ranges
- SPARQL 1.1 Federation (`SERVICE`)
- Full SPARQL UPDATE (LOAD, CLEAR, DROP, CREATE, COPY, MOVE, ADD; variable graph names)
- GeoSPARQL: remaining OGC functions (only `geof:distance` is implemented today)
- RDF 1.2 / SPARQL 1.2: Turtle 1.2 annotation syntax on ingest; triple-term accessor functions; W3C 1.2 test-suite execution

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
