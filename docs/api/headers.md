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
For SPARQL UPDATE operations. See [SPARQL Transactions](../transactions/sparql.md) for supported operations.

**RDF Formats:**
```http
Content-Type: text/turtle
```
For Turtle RDF format transactions. Supported on `/insert` (fast direct path) and `/upsert`.

```http
Content-Type: application/trig
```
For TriG format transactions with named graphs (GRAPH blocks). **Only supported on `/upsert`** - returns 400 error on `/insert` because named graph ingestion requires the upsert path.

```http
Content-Type: application/n-triples
```
For N-Triples format (future support).

```http
Content-Type: application/rdf+xml
```
For RDF/XML format (future support).

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
Turtle RDF format (for CONSTRUCT queries).

```http
Accept: application/rdf+xml
```
RDF/XML graph format (for CONSTRUCT/DESCRIBE queries).

```http
Accept: application/vnd.fluree.agent+json
```
Agent JSON format — optimized for LLM/agent consumption. Returns a self-describing envelope with schema, compact rows, and pagination support. See [Output Formats](../query/output-formats.md#agent-json-format) for details.

Use the `Fluree-Max-Bytes` header to set a byte budget for response truncation:
```http
Fluree-Max-Bytes: 32768
```

```http
Accept: application/n-triples
```
N-Triples format (future support).

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

Request compressed responses:

```http
Accept-Encoding: gzip, deflate
```

The server will compress responses when appropriate, reducing bandwidth usage.

**Response Header:**
```http
Content-Encoding: gzip
```

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

The server will include this in logs and response headers for correlation. When a request queues background indexing work, the copied `X-Request-ID` also appears on the background indexer worker logs so you can connect the foreground request and later indexing activity in plain log search.

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

### X-Fluree-T

The transaction time of the data returned (for queries):

```http
X-Fluree-T: 42
```

Useful for tracking which version of data was queried.

### X-Fluree-Commit

The commit ContentId of the data returned:

```http
X-Fluree-Commit: abc123def456789...
```

### ETag

Entity tag for caching:

```http
ETag: "abc123def456"
```

Can be used with `If-None-Match` for conditional requests.

### Cache-Control

Caching directives:

**For current queries:**
```http
Cache-Control: no-cache
```

**For historical queries:**
```http
Cache-Control: public, max-age=31536000, immutable
```

Historical queries are immutable and cache indefinitely.

### X-RateLimit Headers

Rate limit information (if enabled):

```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1642857600
```

### X-Request-ID

Echo of client-supplied request ID or server-generated ID:

```http
X-Request-ID: abc-123-def-456
```

### X-Response-Time

Server processing time in milliseconds:

```http
X-Response-Time: 45
```

## Content Type Details

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

Follows W3C SPARQL 1.1 Query Results JSON Format specification.

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

The server enforces size limits to prevent resource exhaustion:

**Transaction Requests:**
- Default limit: 10 MB
- Configurable: `--max-transaction-size`

**Query Requests:**
- Default limit: 1 MB
- Configurable: `--max-query-size`

**History Requests:**
- Default limit: 1 MB
- Configurable: `--max-history-size`

### Exceeding Limits

If a request exceeds size limits:

**Status Code:** `413 Payload Too Large`

**Response:**
```json
{
  "error": "Request body exceeds maximum size of 10485760 bytes",
  "status": 413,
  "@type": "err:http/PayloadTooLarge"
}
```

### Configuration

Set custom limits when starting the server:

```bash
./fluree-db-server \
  --max-transaction-size 20971520 \    # 20 MB
  --max-query-size 2097152 \           # 2 MB
  --max-response-size 104857600        # 100 MB
```

### Response Size Limits

The server also limits response sizes:

**Default limit:** 100 MB

If a query result exceeds the limit:

**Status Code:** `413 Payload Too Large`

**Response:**
```json
{
  "error": "Query result exceeds maximum response size",
  "status": 413,
  "@type": "err:http/ResponseTooLarge"
}
```

**Solution:** Use LIMIT and pagination:

```json
{
  "select": ["?name"],
  "where": [...],
  "limit": 1000,
  "offset": 0
}
```

## Compression

### Request Compression

Send compressed requests (for large transactions):

```http
Content-Encoding: gzip
Content-Type: application/json
```

The request body should be gzip-compressed JSON.

### Response Compression

Request compressed responses:

```http
Accept-Encoding: gzip, deflate
```

The server will compress responses when:
- Client accepts compression
- Response is larger than threshold (typically 1 KB)
- Content-Type is compressible

**Response Headers:**
```http
Content-Encoding: gzip
Vary: Accept-Encoding
```

**Compression Benefits:**
- Reduced bandwidth usage (typically 70-90% for JSON)
- Faster response times on slower connections
- Lower costs for cloud deployments

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
Access-Control-Allow-Origin: https://example.com
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization
Access-Control-Max-Age: 86400
```

**Actual Response:**
```http
Access-Control-Allow-Origin: https://example.com
Access-Control-Allow-Credentials: true
```

### CORS Configuration

Configure CORS when starting the server:

```bash
./fluree-db-server \
  --cors-origin "https://example.com" \
  --cors-methods "GET,POST,OPTIONS" \
  --cors-headers "Content-Type,Authorization"
```

**Allow all origins (development only):**
```bash
./fluree-db-server --cors-origin "*"
```

Never use `--cors-origin "*"` in production with credentials.

## Caching Headers

### ETag and Conditional Requests

The server supports ETags for efficient caching.

**Initial Request:**
```http
GET /ledgers/mydb:main HTTP/1.1
```

**Response:**
```http
HTTP/1.1 200 OK
ETag: "abc123def456"
Cache-Control: no-cache
```

**Conditional Request:**
```http
GET /ledgers/mydb:main HTTP/1.1
If-None-Match: "abc123def456"
```

**Not Modified Response:**
```http
HTTP/1.1 304 Not Modified
ETag: "abc123def456"
```

### Immutable Historical Data

Historical queries with time specifiers are immutable:

**Query:**
```http
POST /query HTTP/1.1
{"from": "mydb:main@t:100", ...}
```

**Response:**
```http
HTTP/1.1 200 OK
Cache-Control: public, max-age=31536000, immutable
ETag: "mydb:main@t:100:query-hash"
```

Clients can cache these responses indefinitely.

## Custom Headers

### X-Fluree-Fuel-Limit

Set query fuel limit to prevent runaway queries:

```http
X-Fluree-Fuel-Limit: 1000000
```

See [Tracking and Fuel Limits](../query/tracking-and-fuel.md) for details.

### X-Fluree-Timeout

Set query timeout in milliseconds:

```http
X-Fluree-Timeout: 30000
```

### X-Fluree-Policy

Specify a policy to apply (if authorized):

```http
X-Fluree-Policy: ex:restrictive-policy
```

## Best Practices

### 1. Always Set Content-Type

Explicitly set Content-Type for all requests:

```http
Content-Type: application/json
```

### 2. Accept Compression

Always request compression for better performance:

```http
Accept-Encoding: gzip, deflate
```

### 3. Use Appropriate Accept Headers

Request the format you need:

```http
Accept: application/json
```

### 4. Include User-Agent

Identify your application:

```http
User-Agent: MyApp/1.0.0
```

### 5. Handle ETags

Implement ETag caching for frequently accessed resources:

```javascript
const etag = localStorage.getItem('ledger-etag');
if (etag) {
  headers['If-None-Match'] = etag;
}
```

### 6. Monitor Rate Limits

Check rate limit headers and back off when needed:

```javascript
const remaining = response.headers.get('X-RateLimit-Remaining');
if (remaining < 10) {
  // Slow down requests
}
```

### 7. Use Request IDs

Include request IDs for tracing:

```http
X-Request-ID: uuid-v4-here
```

## Related Documentation

- [Overview](overview.md) - API overview
- [Endpoints](endpoints.md) - Endpoint reference
- [Signed Requests](signed-requests.md) - Authentication
- [Errors](errors.md) - Error handling
