# Headers, Content Types, and Request Sizing

This document covers HTTP headers, content type negotiation, request size limits, and related considerations for the Fluree HTTP API.

## Request Headers

### Content-Type

Specifies the format of the request body.

**Supported Values:**

**JSON-LD Transactions and Queries:**
```http
Content-Type: application/json
```
Default for JSON-LD transactions and JSON-LD queries.

```http
Content-Type: application/ld+json
```
Explicit JSON-LD content type.

**SPARQL Queries:**
```http
Content-Type: application/sparql-query
```
For SPARQL SELECT, ASK, CONSTRUCT queries.

```http
Content-Type: application/sparql-update
```
For SPARQL UPDATE operations. See [SPARQL UPDATE](../query/sparql.md#sparql-update) for supported operations.

**RDF Formats:**
```http
Content-Type: text/turtle
```
For Turtle RDF format transactions. Supported on `/insert` (fast direct path), `/upsert`, `/sync` and the Graph Store `/data` endpoint. `application/x-turtle` is accepted as an alias. On `/insert` and `/upsert`, a Turtle body that contains graph blocks is read as TriG.

```http
Content-Type: application/trig
```
For TriG format transactions with named graphs (GRAPH blocks). Supported on `/insert` and `/upsert`, and on `/sync` and the Graph Store `/data` endpoint (one graph per request). `application/x-trig` is accepted as an alias.

```http
Content-Type: application/n-triples
```
For N-Triples bodies, parsed as Turtle; accepted wherever Turtle is.

```http
Content-Type: application/rdf+xml
```
RDF/XML is an output format only; it is not accepted as a request body.

### Accept

Specifies the desired response format.

**Supported Values:**

```http
Accept: application/json
```
Compact JSON format (default).

```http
Accept: application/ld+json
```
Full JSON-LD with @context.

```http
Accept: application/sparql-results+json
```
SPARQL JSON Results format (for SPARQL queries).

```http
Accept: application/sparql-results+xml
```
SPARQL XML Results format (for SPARQL SELECT/ASK queries).

```http
Accept: text/turtle
```
Turtle (for CONSTRUCT/DESCRIBE queries and Graph Store `GET`).

```http
Accept: application/n-triples
```
N-Triples (for CONSTRUCT/DESCRIBE queries and Graph Store `GET`).

```http
Accept: application/rdf+xml
```
RDF/XML graph format (for CONSTRUCT/DESCRIBE queries and Graph Store `GET`).

```http
Accept: application/trig
Accept: application/n-quads
```
TriG and N-Quads (for CONSTRUCT/DESCRIBE queries). Required, with JSON-LD, for a CONSTRUCT whose template writes into named graphs (`GRAPH` blocks).

```http
Accept: application/vnd.fluree.agent+json
```
Agent JSON format — optimized for LLM/agent consumption. Returns a self-describing envelope with schema, compact rows, and pagination support. See [Output Formats](../query/output-formats.md#agent-json-format) for details.

Use the `Fluree-Max-Bytes` header to set a byte budget for response truncation:
```http
Fluree-Max-Bytes: 32768
```

**Multiple Accept Values:**

You can specify multiple formats with quality values:

```http
Accept: application/ld+json; q=1.0, application/json; q=0.8
```

The server will choose the best match based on quality values and support.

### Authorization

Authentication credentials. Only required when the server has authentication enabled for the relevant endpoint group (see [Configuration](../operations/configuration.md)).

**Bearer Token (Ed25519 JWS or OIDC):**
```http
Authorization: Bearer eyJhbGciOiJFZERTQSIsImp3ayI6eyJrdHkiOiJPS1AiLCJjcnYiOiJFZDI1NTE5IiwieCI6Ii4uLiJ9fQ...
```

The server automatically dispatches to the correct verification path based on the token header:
- Tokens with an embedded `jwk` field use the Ed25519 verification path
- Tokens with a `kid` field use the OIDC/JWKS verification path (requires `oidc` feature)

**Signed Requests:**

For JWS/VC signed request bodies, set Content-Type to `application/jose`:
```http
Content-Type: application/jose
```

See [Signed Requests](signed-requests.md) for details.

### Content-Length

The server requires Content-Length for all POST requests:

```http
Content-Length: 1234
```

Most HTTP clients set this automatically.

### Accept-Encoding

The server does not compress responses and ignores `Accept-Encoding`. See
[Compression](#compression).

### User-Agent

Identify your client application:

```http
User-Agent: MyApp/1.0.0 (https://example.com)
```

Helpful for server logs and troubleshooting.

### X-Request-ID

Client-supplied request ID for tracing:

```http
X-Request-ID: abc-123-def-456
```

The server includes this in its logs for correlation; it does not echo it in the response. When a request queues background indexing work, the copied `X-Request-ID` also appears on the background indexer worker logs so you can connect the foreground request and later indexing activity in plain log search.

### Fluree-Min-T

Request a hard read-after-write guarantee for query and explain endpoints:

```http
Fluree-Min-T: 42
```

Before executing the request, the server refreshes the referenced ledger(s) until each has reached at least the requested transaction time. If the target `t` is not visible before `query_min_t_timeout_ms`, the server returns a read-after-write timeout error. JSON-LD query bodies may also specify the same requirement as `opts.min-t`, `opts.min_t`, or `opts.minT`; body opts take precedence over the header for that query body.

Numeric time-travel snapshots such as `from: "ledger:main@t:42"` also wait until that `t` is visible, then query that pinned snapshot. `Fluree-Min-T` is useful when the query itself reads current HEAD but must not run until a known transaction has arrived.

The header must resolve to at least one target ledger. On a ledger-scoped endpoint the endpoint's ledger is used; on the connection-scoped query endpoint the target comes from the query's `FROM` clause. Sending `Fluree-Min-T` to the connection-scoped endpoint with no `FROM` (and no other resolvable ledger) is rejected with `400 Bad Request` rather than silently ignored.

## Response Headers

### Content-Type

Indicates the format of the response body:

```http
Content-Type: application/json; charset=utf-8
```

### Content-Length

Size of the response body in bytes:

```http
Content-Length: 5678
```

### Tracking Headers

A request that asks for tracking (see [Fluree Request Headers](#fluree-request-headers)) gets
its metrics in the response body and in these headers:

| Header | Content |
|--------|---------|
| `x-fdb-time` | Execution time, e.g. `12.34ms` |
| `x-fdb-fuel` | Fuel consumed |
| `x-fdb-policy` | Per-policy statistics, as base64-encoded JSON |
| `x-fdb-policy-enforcement` | JSON, sent only when policy governed the request |
| `x-fdb-reasoning` | JSON, sent only when a reasoning mode ran; `"capped": true` means results may be incomplete |

Query responses carry no transaction-time, commit, `ETag` or `Cache-Control` headers. Read a
ledger's current `t` from [`GET /info/<ledger>`](endpoints.md).

### Rate Limit Headers

The server does not rate-limit requests and sends no `X-RateLimit-*` headers. If a reverse
proxy or API gateway in front of it enforces rate limits, any such headers come from that
layer.

## Content Type Details

### IRI form: absolute vs. abbreviated

Response formats fall into two families, and they treat IRIs differently.

**W3C result serializations** — `application/sparql-results+json`, `application/sparql-results+xml`,
`text/csv`, `text/tab-separated-values` — always emit **absolute** IRIs. A query's `PREFIX` and `BASE`
declarations do not shorten them. These formats carry no prefix map and no base slot, so an abbreviated
IRI in one of them could not be expanded back by the consumer; the specs define the value as the
absolute IRI.

**JSON-LD-flavored formats** — `application/ld+json`, `application/json`,
`application/vnd.fluree.agent+json`, typed JSON, NDJSON, and the `fluree` CLI's display output —
abbreviate IRIs against the query's `@context` / `PREFIX` prologue. That is intentional: the consumer
either receives the context alongside the data or is a human reading a terminal.

If you need absolute IRIs from a SPARQL query, request `application/sparql-results+json`.

### JSON-LD (application/json, application/ld+json)

**Request Example:**

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  },
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "schema:Person",
      "schema:name": "Alice"
    }
  ]
}
```

**Compact vs Expanded:**

`application/json` returns compact JSON:
```json
[
  { "name": "Alice" }
]
```

`application/ld+json` returns with full context:
```json
{
  "@context": {
    "name": "http://schema.org/name"
  },
  "@graph": [
    { "name": "Alice" }
  ]
}
```

### SPARQL Query (application/sparql-query)

**Request Example:**

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX schema: <http://schema.org/>

SELECT ?name
FROM <mydb:main>
WHERE {
  ?person a schema:Person .
  ?person schema:name ?name .
}
```

Plain text SPARQL query in the request body.

### SPARQL Results JSON (application/sparql-results+json)

**Response Example:**

```json
{
  "head": {
    "vars": ["name"]
  },
  "results": {
    "bindings": [
      {
        "name": {
          "type": "literal",
          "value": "Alice",
          "datatype": "http://www.w3.org/2001/XMLSchema#string"
        }
      }
    ]
  }
}
```

Follows W3C SPARQL 1.1 Query Results JSON Format specification. `uri` values are always absolute
IRIs, and a literal carries its `datatype` unless it is an `xsd:string` — see
[IRI form](#iri-form-absolute-vs-abbreviated).

### Turtle (text/turtle)

**Transaction Request:**

```turtle
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .

ex:alice a schema:Person ;
  schema:name "Alice" ;
  schema:age 30 .
```

**CONSTRUCT Response:**

```turtle
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .

ex:alice a schema:Person .
ex:alice schema:name "Alice" .
```

## Request Size Limits

### Default Limits

The server enforces a single request body size limit to prevent resource exhaustion. It
applies to every request body — transactions, queries, and history requests alike:

- Default limit: 50 MB (52428800 bytes)
- Configurable: `--body-limit` (env `FLUREE_BODY_LIMIT`, config file `body_limit`)

### Exceeding Limits

If a request exceeds size limits:

**Status Code:** `413 Payload Too Large`

**Response:**
```json
{
  "error": "request body exceeds the configured limit",
  "status": 413,
  "@type": "err:db/PayloadTooLarge"
}
```

### Configuration

Set a custom limit when starting the server:

```bash
fluree server run -- --body-limit 20971520   # 20 MB
```

See [Configuration](../operations/configuration.md) for all server options.

### Response Size

The server has no configurable response size limit. To keep large result sets manageable, use
LIMIT and pagination:

```json
{
  "select": ["?name"],
  "where": [...],
  "limit": 1000,
  "offset": 0
}
```

## Compression

The server neither compresses responses nor decodes compressed request bodies
(`Content-Encoding: gzip`). To compress traffic, put a reverse proxy in front of the server
and let it handle compression.

## Character Encoding

All text content uses UTF-8 encoding.

**Request:**
```http
Content-Type: application/json; charset=utf-8
```

**Response:**
```http
Content-Type: application/json; charset=utf-8
```

Unicode characters are supported in:
- IRIs
- Literal values
- Property names
- Comments

## CORS Headers

For web browser access, the server supports Cross-Origin Resource Sharing (CORS).

### CORS Request Headers

**Preflight Request:**
```http
OPTIONS /query HTTP/1.1
Origin: https://example.com
Access-Control-Request-Method: POST
Access-Control-Request-Headers: Content-Type
```

### CORS Response Headers

**Preflight Response:**
```http
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: *
Access-Control-Allow-Headers: Content-Type
```

The requested headers are mirrored back in `Access-Control-Allow-Headers`.

**Actual Response:**
```http
Access-Control-Allow-Origin: *
```

### CORS Configuration

CORS is either on or off. It is on by default; disable it with `--cors-enabled=false`,
`FLUREE_CORS_ENABLED=false`, or `cors_enabled = false` in the `[server]` section of the config
file. When on, the server allows any
origin and any method and does not send `Access-Control-Allow-Credentials`.

The server has no per-origin, per-method, or per-header allow lists. To restrict CORS to
specific origins, disable it on the server and set the CORS headers in a reverse proxy in
front of it.

## Caching Headers

Query and transaction responses carry no `ETag` or `Cache-Control` headers, and the server does
not answer conditional requests (`If-None-Match`) for them. Only the storage proxy's block
endpoints, used for replication, send an `ETag` and an immutable `Cache-Control`.

## Fluree Request Headers

| Header | Value | Effect |
|--------|-------|--------|
| `fluree-ledger` | Ledger ID | Target ledger for connection-scoped endpoints; a ledger in the path takes precedence |
| `fluree-identity` | Identity IRI | Identity whose policies apply |
| `fluree-policy` | JSON | Inline policy for the request |
| `fluree-policy-class` | IRI(s), comma-separated or repeated | Policy classes to apply |
| `fluree-policy-values` | JSON | Values bound into policy queries |
| `fluree-default-allow` | `true` / `false` | Access when no policy matches; overrides the ledger's `f:defaultAllow` |
| `fluree-track-meta` | `true` | Track everything below |
| `fluree-track-fuel` | `true` | Report fuel consumed |
| `fluree-track-time` | `true` | Report execution time |
| `fluree-track-policy` | `true` | Report per-policy statistics |
| `fluree-max-fuel` | Number | Fail the query once it consumes this much fuel |
| `fluree-min-t` | Transaction `t` | See [Fluree-Min-T](#fluree-min-t) |

A SPARQL request can carry these options in its text instead, as
[`# PRAGMA` comments](../query/sparql.md#request-options--pragma) (all but `fluree-ledger` and the
inline `fluree-policy`); a pragma wins over the header that names the same option.

Whether a request may choose its own identity or policy depends on the server's
authorization settings; see [Policy in Queries](../security/policy-in-queries.md). Tracking and
fuel are covered in [Tracking and Fuel Limits](../query/tracking-and-fuel.md).

There is no per-request timeout header. Queries are bounded by the server-wide
`--query-timeout-ms` (`FLUREE_QUERY_TIMEOUT_MS`, default 15 minutes); see
[Configuration](../operations/configuration.md).

## Best Practices

### 1. Always Set Content-Type

Explicitly set Content-Type for all requests:

```http
Content-Type: application/json
```

### 2. Use Appropriate Accept Headers

Request the format you need:

```http
Accept: application/json
```

### 3. Include User-Agent

Identify your application:

```http
User-Agent: MyApp/1.0.0
```

### 4. Monitor Rate Limits

If a proxy or gateway in front of the server enforces rate limits, check its headers and back
off when needed:

```javascript
const remaining = response.headers.get('X-RateLimit-Remaining');
if (remaining < 10) {
  // Slow down requests
}
```

### 5. Use Request IDs

Include request IDs for tracing:

```http
X-Request-ID: uuid-v4-here
```

## Related Documentation

- [Overview](overview.md) - API overview
- [Endpoints](endpoints.md) - Endpoint reference
- [Signed Requests](signed-requests.md) - Authentication
- [Errors](errors.md) - Error handling
