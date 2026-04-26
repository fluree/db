# HTTP API

The Fluree HTTP API provides RESTful endpoints for all database operations. This section documents the complete API surface including request formats, authentication, and error handling.

## Core Endpoints

### [Overview](overview.md)

High-level introduction to the Fluree HTTP API, including:
- API design principles
- Authentication overview
- Rate limiting and quotas
- API versioning

### [Endpoints](endpoints.md)

Complete reference for all HTTP endpoints:
- `POST /update` - Submit update transactions (WHERE/DELETE/INSERT or SPARQL UPDATE)
- `POST /query` - Execute queries
- `GET /v1/fluree/ledgers` - List ledgers
- `GET /health` - Health checks
- `GET /v1/fluree/stats` - Server status
- And more...

### [Headers, Content Types, and Request Sizing](headers.md)

HTTP headers and request format details:
- Content-Type negotiation
- Accept headers for response formats
- Request size limits
- Compression support
- Custom headers

### [Signed Requests (JWS/VC)](signed-requests.md)

Cryptographically signed and verifiable requests:
- JSON Web Signature (JWS) format
- Verifiable Credentials (VC) support
- Public key verification
- DID authentication
- Signature validation

### [Errors and Status Codes](errors.md)

HTTP status codes and error responses:
- Standard HTTP status codes
- Fluree-specific error codes
- Error response format
- Troubleshooting common errors

## API Characteristics

### RESTful Design

The Fluree API follows REST principles:
- Resource-oriented URLs
- Standard HTTP methods (GET, POST)
- Stateless requests
- Standard status codes

### Content Negotiation

Fluree supports multiple content types for requests and responses:

**Request Content-Types:**
- `application/json` - JSON-LD transactions and queries
- `application/sparql-query` - SPARQL queries
- `text/turtle` - Turtle RDF format
- `application/ld+json` - Explicit JSON-LD

**Response Content-Types:**
- `application/json` - Default JSON format
- `application/ld+json` - JSON-LD with context
- `application/sparql-results+json` - SPARQL result format

### Authentication

Fluree supports multiple authentication mechanisms:

1. **No Authentication** (development only)
2. **Signed Requests** (JWS/VC for production)
3. **API Keys** (simple token-based auth)
4. **Bearer Tokens** (JWT authentication)

See [Signed Requests](signed-requests.md) for cryptographic authentication details.

## Quick Examples

### Transaction Request

```bash
curl -X POST http://localhost:8090/v1/fluree/insert?ledger=mydb:main \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/"
    },
    "@graph": [
      { "@id": "ex:alice", "ex:name": "Alice" }
    ]
  }'
```

### Query Request

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "from": "mydb:main",
    "select": ["?name"],
    "where": [
      { "@id": "?person", "ex:name": "?name" }
    ]
  }'
```

### SPARQL Query

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/sparql-query" \
  -d 'SELECT ?name FROM <mydb:main> WHERE { ?person ex:name ?name }'
```

### Health Check

```bash
curl http://localhost:8090/health
```

## API Clients

### Command Line (curl)

All examples in this documentation use `curl` for simplicity. Curl is available on all major platforms.

### Programming Languages

Fluree's HTTP API can be accessed from any language with HTTP client support:

**JavaScript/TypeScript:**
```javascript
const response = await fetch('http://localhost:8090/v1/fluree/query', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    from: 'mydb:main',
    select: ['?name'],
    where: [{ '@id': '?person', 'ex:name': '?name' }]
  })
});
const results = await response.json();
```

**Python:**
```python
import requests

response = requests.post('http://localhost:8090/v1/fluree/query', json={
    'from': 'mydb:main',
    'select': ['?name'],
    'where': [{'@id': '?person', 'ex:name': '?name'}]
})
results = response.json()
```

**Java:**
```java
HttpClient client = HttpClient.newHttpClient();
HttpRequest request = HttpRequest.newBuilder()
    .uri(URI.create("http://localhost:8090/v1/fluree/query"))
    .header("Content-Type", "application/json")
    .POST(HttpRequest.BodyPublishers.ofString(queryJson))
    .build();
HttpResponse<String> response = client.send(request, 
    HttpResponse.BodyHandlers.ofString());
```

## Development vs Production

### Development Setup

For local development, the API typically runs without authentication:

```bash
./fluree-db-server --port 8090 --storage memory
```

Access: `http://localhost:8090`

### Production Setup

For production deployments, enable authentication and use HTTPS:

```bash
./fluree-db-server \
  --port 8090 \
  --storage aws \
  --require-signed-requests \
  --https-cert /path/to/cert.pem \
  --https-key /path/to/key.pem
```

Access: `https://api.yourdomain.com`

Always use:
- HTTPS in production
- Signed requests or API keys
- Rate limiting
- Request size limits

## Performance Considerations

### Request Size Limits

Default limits (configurable):
- Transaction size: 10MB
- Query size: 1MB
- Response size: 100MB

See [Headers and Request Sizing](headers.md) for details.

### Connection Management

- Keep-alive connections supported
- HTTP/2 support available
- WebSocket support for streaming (planned)

### Caching

- Query results can be cached (ETag support)
- Immutable historical queries cache well
- Current queries should not be cached aggressively

## Related Documentation

- [Getting Started](../getting-started/README.md) - Quickstart guides
- [Transactions](../transactions/README.md) - Transaction details
- [Query](../query/README.md) - Query language documentation
- [Security](../security/README.md) - Policy and access control
- [Operations](../operations/README.md) - Configuration and deployment
