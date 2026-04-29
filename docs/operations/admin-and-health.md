# Admin, Health, and Stats

This document covers administrative operations, health monitoring, and server statistics for Fluree deployments.

## Health Endpoints

### GET /health

Basic health check:

```bash
curl http://localhost:8090/health
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

Use this endpoint for:
- Load balancer health checks
- Container orchestration (Kubernetes liveness/readiness probes)
- Monitoring systems

**Kubernetes Example:**
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8090
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /health
    port: 8090
  initialDelaySeconds: 5
  periodSeconds: 5
```

## Statistics Endpoints

### GET /v1/fluree/stats

Server statistics:

```bash
curl http://localhost:8090/v1/fluree/stats
```

**Response:**
```json
{
  "uptime_secs": 3600,
  "storage_type": "file",
  "indexing_enabled": true,
  "cached_ledgers": 3,
  "version": "0.1.0"
}
```

| Field | Description |
|-------|-------------|
| `uptime_secs` | Server uptime in seconds |
| `storage_type` | Storage mode (`memory` or `file`) |
| `indexing_enabled` | Whether background indexing is enabled |
| `cached_ledgers` | Number of ledgers currently cached |
| `version` | Server version |

## Diagnostic endpoints

### GET /v1/fluree/whoami

Diagnostic endpoint for debugging Bearer tokens.

- If no token is present, returns `token_present=false`.
- If a token is present, attempts to **cryptographically verify** it using the same verification logic as authenticated endpoints (embedded-JWK Ed25519 and JWKS/OIDC when enabled/configured).
- On verification failure, returns `verified=false` and includes an `error` string. Some unverified decoded fields may be included for debugging.

```bash
curl http://localhost:8090/v1/fluree/whoami \
  -H "Authorization: Bearer eyJ..."
```

## CLI discovery

### GET /.well-known/fluree.json

Discovery document used by the CLI when adding a remote (`fluree remote add`) or when running `fluree auth login` with no configured auth type.

Standalone `fluree-server` returns:

- `{"version":1,"api_base_url":"/v1/fluree"}` when no auth is enabled
- `{"version":1,"api_base_url":"/v1/fluree","auth":{"type":"token"}}` when any server auth mode is enabled (data/events/admin)

OIDC-capable implementations can return `auth.type="oidc_device"` plus `issuer`, `client_id`, and `exchange_url`.
The CLI treats `oidc_device` as "OIDC interactive login": it uses device-code when the IdP supports it, otherwise authorization-code + PKCE (localhost callback).

Implementations MAY also return `api_base_url` to tell the CLI where the Fluree API is mounted (for example,
when the API is hosted under `/v1/fluree` or on a separate `data` subdomain).

See [Auth contract (CLI ↔ Server)](../design/auth-contract.md) for the full schema and behavior.

### GET /v1/fluree/info/<ledger...>

Get detailed ledger metadata:

```bash
curl "http://localhost:8090/v1/fluree/info/mydb:main"
```

**Minimum fields used by the Fluree CLI:**

- `t` (required)
- `commitId` (required for `fluree push` when `t > 0`)

**Optional query params:**

- By default, `ledger-info` returns the **full novelty-aware** stats view, including real-time datatype details and class ref edges.
- **`realtime_property_details=false`**: switch `ledger-info` to the lighter fast novelty-aware stats layer that keeps counts current but skips lookup-backed class/ref enrichment.
- **`include_property_datatypes=false`**: omit `stats.properties[*].datatypes` when you want a smaller payload.
- **`include_property_estimates=true`**: include index-derived `ndv-values`, `ndv-subjects`, and selectivity fields under `stats.properties[*]`.

Example:

```bash
curl "http://localhost:8090/v1/fluree/info/mydb:main"
```

**Response:**
```json
{
  "ledger": "mydb:main",
  "t": 150,
  "commitId": "bafybeig...commitT150",
  "indexId": "bafybeig...indexRootT145",
  "commit": {
    "commit_id": "bafybeig...commitT150",
    "t": 150
  },
  "index": {
    "id": "bafybeig...indexRootT145",
    "t": 145
  },
  "stats": {
    "flakes": 12345,
    "size": 1048576,
    "indexed": 145,
    "properties": {
      "ex:name": {
        "count": 3,
        "last-modified-t": 150
      }
    },
    "classes": {
      "ex:Person": {
        "count": 2,
        "properties": {
          "ex:worksFor": {
            "count": 2,
            "refs": { "ex:Organization": 2 },
            "ref-classes": { "ex:Organization": 2 }
          },
          "ex:name": {}
        },
        "property-list": ["ex:name", "ex:worksFor"]
      }
    }
  }
}
```

#### Stats freshness (real-time vs indexed)

- **Real-time (includes novelty)**:
  - `commit` and top-level `t` reflect the latest committed head.
  - `stats.flakes` and `stats.size` are derived from the current ledger stats view (indexed + novelty deltas).
  - `stats.classes[*].properties` / `property-list` will include properties introduced in novelty, even when the update does not restate `@type`.
  - `stats.properties[*].datatypes` is real-time by default.
  - `stats.classes[*].properties[*].refs` is real-time by default.

- **As-of last index**:
  - `stats.indexed` is the last index \(t\). If `commit.t > indexed`, the index is behind the head.
  - NDV-related fields in `stats.properties[*]` (`ndv-values`, `ndv-subjects`) and selectivity derived from them are only as current as the last index refresh, so they are omitted by default and only included when `include_property_estimates=true`.
  - `stats.properties[*].datatypes` are omitted only when `include_property_datatypes=false` is requested.
  - Class property ref-edge counts (`stats.classes[*].properties[*].refs`) fall back to the lighter indexed/fast path only when `realtime_property_details=false` is requested.

### GET /v1/fluree/exists/<ledger...>

Check if a ledger exists:

```bash
curl "http://localhost:8090/v1/fluree/exists/mydb:main"
```

**Response:**
```json
{
  "ledger": "mydb:main",
  "exists": true
}
```

This is a lightweight check that only queries the nameservice without loading the ledger.

## Administrative Operations

### POST /v1/fluree/create

Create a new ledger:

```bash
curl -X POST http://localhost:8090/v1/fluree/create \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main"}'
```

**Response (201 Created):**
```json
{
  "ledger": "mydb:main",
  "t": 0,
  "tx-id": "fluree:tx:sha256:abc123...",
  "commit": {
    "commit_id": "bafybeig...commitT0"
  }
}
```

**Authentication:** When `--admin-auth-mode=required`, requires Bearer token from a trusted issuer.

See [Admin Authentication](../api/endpoints.md#admin-authentication) for details.

### POST /v1/fluree/drop

Drop (delete) a ledger:

```bash
# Soft drop (retract from nameservice, preserve files)
curl -X POST http://localhost:8090/v1/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main"}'

# Hard drop (delete all files - IRREVERSIBLE)
curl -X POST http://localhost:8090/v1/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main", "hard": true}'
```

**Response:**
```json
{
  "ledger": "mydb:main",
  "status": "dropped",
  "files_deleted": 23
}
```

| Status | Description |
|--------|-------------|
| `dropped` | Successfully dropped |
| `already_retracted` | Was previously dropped |
| `not_found` | Ledger doesn't exist |

**Authentication:** When `--admin-auth-mode=required`, requires Bearer token from a trusted issuer.

**Drop Modes:**
- **Soft** (default): Retracts from nameservice, files remain (recoverable)
- **Hard**: Deletes all files (irreversible)

See [Dropping Ledgers](../getting-started/rust-api.md#dropping-ledgers) for more details.

## API Specification

### GET /swagger.json

OpenAPI specification:

```bash
curl http://localhost:8090/swagger.json
```

Returns the OpenAPI 3.0 specification for the server API.

## Monitoring Best Practices

### 1. Use Health Checks

Configure your infrastructure to poll `/health`:

```bash
# Simple monitoring script
while true; do
  curl -sf http://localhost:8090/health > /dev/null || echo "ALERT: Server unhealthy"
  sleep 10
done
```

### 2. Track Server Stats

Periodically collect statistics:

```bash
curl http://localhost:8090/v1/fluree/stats | jq .
```

Key metrics to track:
- `uptime_secs`: Detect restarts
- `cached_ledgers`: Cache efficiency

### 3. Monitor Ledger Health

For each critical ledger:

```bash
curl "http://localhost:8090/v1/fluree/info/mydb:main" | jq .
```

Watch for:
- Index lag (`commit.t` vs `index.t`)
- Unexpected state changes

### 4. Set Up Alerts

Alert conditions:
- Health check failures
- Server restarts (low uptime)
- High index lag

### 5. Log Analysis

Enable structured logging:

```bash
fluree-server --log-level info 2>&1 | jq .
```

Search for:
- `level: "error"` - Errors
- `level: "warn"` - Warnings
- Slow query patterns

## Security Considerations

### Protect Admin Endpoints

In production, enable admin authentication:

```bash
fluree-server \
  --admin-auth-mode required \
  --admin-auth-trusted-issuer did:key:z6Mk...
```

This protects `/v1/fluree/create`, `/v1/fluree/drop`, and other admin-protected
API routes from unauthorized access.

### Limit Endpoint Exposure

Consider network-level restrictions:
- Health endpoint: Available to load balancers
- Stats endpoint: Internal monitoring only
- Admin endpoints: Restricted access

### Audit Logging

Admin operations are logged. Monitor for:
- Ledger creation
- Ledger drops
- Authentication failures

## Related Documentation

- [Configuration](configuration.md) - Server configuration options
- [Query Peers](query-peers.md) - Distributed deployment
- [Telemetry](telemetry.md) - Logging configuration
- [API Endpoints](../api/endpoints.md) - Full endpoint reference
