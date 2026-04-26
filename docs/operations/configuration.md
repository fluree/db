# Configuration

Fluree server is configured via a configuration file, command-line flags, and environment variables.

## Configuration Methods

### Configuration File (TOML, JSON, or JSON-LD)

The server reads configuration from `.fluree/config.toml` (or `.fluree/config.jsonld`) — the same file used by the Fluree CLI. Server settings live under the `[server]` section (or `"server"` key in JSON/JSON-LD). The server walks up from the current working directory looking for `.fluree/config.toml` or `.fluree/config.jsonld`, falling back to the global Fluree **config** directory (`$FLUREE_HOME`, or the platform config directory — see table below).

#### Global Directory Layout

When `$FLUREE_HOME` is set, both config and data share that single directory. When it is not set, the platform's config and data directories are used:

| Content                     | Linux                   | macOS                                  | Windows                 |
| --------------------------- | ----------------------- | -------------------------------------- | ----------------------- |
| Config (`config.toml`)      | `~/.config/fluree`      | `~/Library/Application Support/fluree` | `%LOCALAPPDATA%\fluree` |
| Data (`storage/`, `active`) | `~/.local/share/fluree` | `~/Library/Application Support/fluree` | `%LOCALAPPDATA%\fluree` |

On Linux, config and data directories are separated per the XDG Base Directory specification. On macOS and Windows both resolve to the same directory. When directories are split, `fluree init --global` writes an absolute `storage_path` into `config.toml` so the server can locate the data directory regardless of working directory.

```bash
# Use default config file discovery
fluree-server

# Override config file path
fluree-server --config /etc/fluree/config.toml

# Activate a profile
fluree-server --profile prod
```

Example `config.toml`:

```toml
[server]
listen_addr = "0.0.0.0:8090"
storage_path = "/var/lib/fluree"
log_level = "info"
# cache_max_mb = 4096  # global cache budget (MB); default: tiered fraction of RAM (30% <4GB, 40% 4-8GB, 50% ≥8GB)

[server.indexing]
enabled = true
reindex_min_bytes = 100000
reindex_max_bytes = 1000000

[server.auth.data]
mode = "required"
trusted_issuers = ["did:key:z6Mk..."]
```

JSON is also supported (detected by `.json` file extension):

```json
{
  "server": {
    "listen_addr": "0.0.0.0:8090",
    "storage_path": "/var/lib/fluree",
    "indexing": { "enabled": true }
  }
}
```

#### JSON-LD Format

JSON-LD config files (`.jsonld` extension) add a `@context` that maps config keys to the Fluree config vocabulary (`https://ns.flur.ee/config#`), making the file valid JSON-LD. Generate one with:

```bash
fluree init --format jsonld
```

Example `.fluree/config.jsonld`:

```json
{
  "@context": {
    "@vocab": "https://ns.flur.ee/config#"
  },
  "_comment": "Fluree Configuration — JSON-LD format.",
  "server": {
    "listen_addr": "0.0.0.0:8090",
    "storage_path": ".fluree/storage",
    "log_level": "info",
    "indexing": {
      "enabled": false,
      "reindex_min_bytes": 100000,
      "reindex_max_bytes": 1000000
    }
  },
  "profiles": {
    "prod": {
      "server": {
        "log_level": "warn",
        "indexing": { "enabled": true }
      }
    }
  }
}
```

The `@context` is validated at load time (using the JSON-LD parser) but does not affect config value resolution — `serde` ignores unknown keys like `@context` and `_comment`. If both `config.toml` and `config.jsonld` exist in the same directory, TOML takes precedence and a warning is logged.

### Profiles

Profiles allow environment-specific overrides. Define them in `[profiles.<name>.server]` and activate with `--profile <name>`:

```toml
[server]
log_level = "info"

[profiles.dev.server]
log_level = "debug"

[profiles.prod.server]
log_level = "warn"
[profiles.prod.server.indexing]
enabled = true
[profiles.prod.server.auth.data]
mode = "required"
```

Profile values are deep-merged onto `[server]` — only the fields present in the profile are overridden.

### Command-Line Flags

```bash
fluree-server \
  --listen-addr 0.0.0.0:8090 \
  --storage-path /var/lib/fluree \
  --log-level info
```

### Environment Variables

All CLI flags have corresponding environment variables with `FLUREE_` prefix:

```bash
export FLUREE_LISTEN_ADDR=0.0.0.0:8090
export FLUREE_STORAGE_PATH=/var/lib/fluree
export FLUREE_LOG_LEVEL=info

fluree-server
```

### Precedence

Configuration precedence (highest to lowest):

1. Command-line flags
2. Environment variables
3. Profile overrides (`[profiles.<name>.server]`)
4. Config file (`[server]`)
5. Built-in defaults

### Error Handling

If `--config` or `--profile` is specified and the configuration cannot be loaded (file not found, parse error, missing profile), the server **exits with an error**. This prevents silent misconfiguration in production.

If the config file is auto-discovered (no explicit `--config`) and cannot be parsed, the server logs a warning and continues with CLI/env/default values only.

## Server Configuration

### Listen Address

Address and port to bind to:

| Flag            | Env Var              | Default        |
| --------------- | -------------------- | -------------- |
| `--listen-addr` | `FLUREE_LISTEN_ADDR` | `0.0.0.0:8090` |

```bash
fluree-server --listen-addr 0.0.0.0:9090
```

### Storage Path

Path for file-based storage. If not specified, defaults to `.fluree/storage` relative to the working directory (the same location used by `fluree init`):

| Flag             | Env Var               | Default           |
| ---------------- | --------------------- | ----------------- |
| `--storage-path` | `FLUREE_STORAGE_PATH` | `.fluree/storage` |

```bash
# Explicit storage path (e.g. production)
fluree-server --storage-path /var/lib/fluree

# Default: uses .fluree/storage in the working directory
fluree-server
```

### Connection Configuration (S3, DynamoDB, etc.)

For storage backends beyond local files — S3, DynamoDB nameservice, split commit/index storage, encryption — use a JSON-LD connection config file:

| Flag                  | Env Var                    | Default |
| --------------------- | -------------------------- | ------- |
| `--connection-config` | `FLUREE_CONNECTION_CONFIG`  | None    |

When set, the server builds its storage and nameservice from the connection config file instead of using `--storage-path`. The file uses the same JSON-LD format as the [Fluree API connection config](../reference/connection-config-jsonld.md).

```bash
# S3 + DynamoDB via connection config
fluree server run --connection-config /etc/fluree/connection.jsonld

# Or via environment variable
FLUREE_CONNECTION_CONFIG=/etc/fluree/connection.jsonld fluree server run
```

Example connection config (`connection.jsonld`):

```json
{
  "@context": {
    "@base": "https://ns.flur.ee/config/connection/",
    "@vocab": "https://ns.flur.ee/system#"
  },
  "@graph": [
    {
      "@id": "commitStorage",
      "@type": "Storage",
      "s3Bucket": "fluree-commits",
      "s3Prefix": "fluree-data/"
    },
    {
      "@id": "indexStorage",
      "@type": "Storage",
      "s3Bucket": "fluree-indexes--use1-az4--x-s3"
    },
    {
      "@id": "publisher",
      "@type": "Publisher",
      "dynamodbTable": "fluree-nameservice",
      "dynamodbRegion": "us-east-1"
    },
    {
      "@id": "conn",
      "@type": "Connection",
      "commitStorage": { "@id": "commitStorage" },
      "indexStorage": { "@id": "indexStorage" },
      "primaryPublisher": { "@id": "publisher" }
    }
  ]
}
```

**Behavior notes:**

- `--connection-config` and `--storage-path` are mutually exclusive. If both are set, `--connection-config` takes precedence (a warning is logged).
- Server-level settings (`--cache-max-mb`, `--indexing-enabled`, `--reindex-min-bytes`, `--reindex-max-bytes`) override any equivalent values from the connection config.
- If `--indexing-enabled` is not explicitly set (defaults to `false`), indexing settings from the connection config are cleared. Set `--indexing-enabled` explicitly if your connection config should control indexing.
- AWS credentials and region are resolved via the standard AWS SDK chain (env vars, instance profile, `~/.aws/config`, etc.) — they are not part of the connection config.
- The connection config can use `envVar` indirection for sensitive fields like S3 bucket names or encryption keys (see [ConfigurationValue](../reference/connection-config-jsonld.md#configurationvalue-env-var-indirection)).

**Config file equivalent:**

```toml
[server]
connection_config = "/etc/fluree/connection.jsonld"
```

#### Capabilities by Backend

Not all nameservice backends support all features. The server checks capabilities at runtime:

| Feature                 | File (local)   | DynamoDB       | Storage-backed |
| ----------------------- | -------------- | -------------- | -------------- |
| Query / transact        | Yes            | Yes            | Yes            |
| Event subscriptions     | Yes            | No             | No             |
| Default context (read)  | Yes            | Yes            | Yes            |
| Default context (write) | Yes            | Yes            | No             |

If a capability is not available, the server returns an appropriate error (e.g., 501 for event subscriptions with DynamoDB).

### CORS

Enable Cross-Origin Resource Sharing:

| Flag             | Env Var               | Default |
| ---------------- | --------------------- | ------- |
| `--cors-enabled` | `FLUREE_CORS_ENABLED` | `true`  |

When enabled, allows requests from any origin.

### Body Limit

Maximum request body size in bytes:

| Flag           | Env Var             | Default           |
| -------------- | ------------------- | ----------------- |
| `--body-limit` | `FLUREE_BODY_LIMIT` | `52428800` (50MB) |

### Log Level

Logging verbosity:

| Flag          | Env Var            | Default |
| ------------- | ------------------ | ------- |
| `--log-level` | `FLUREE_LOG_LEVEL` | `info`  |

Options: `trace`, `debug`, `info`, `warn`, `error`

### Cache Size

Global cache budget (MB):

| Flag              | Env Var              | Default                |
| ----------------- | -------------------- | ---------------------- |
| `--cache-max-mb`  | `FLUREE_CACHE_MAX_MB`| `30/40/50% of RAM (tiered: <4GB / 4-8GB / ≥8GB)`    |

### Background Indexing

Enable background indexing and configure novelty backpressure thresholds:

| Flag                  | Env Var                    | Default   | Description                                     |
| --------------------- | -------------------------- | --------- | ----------------------------------------------- |
| `--indexing-enabled`  | `FLUREE_INDEXING_ENABLED`  | `false`   | Enable background indexing                      |
| `--reindex-min-bytes` | `FLUREE_REINDEX_MIN_BYTES` | `100000`  | Soft threshold (triggers background indexing)   |
| `--reindex-max-bytes` | `FLUREE_REINDEX_MAX_BYTES` | `1000000` | Hard threshold (blocks commits until reindexed) |

Config file equivalent:

```toml
[server.indexing]
enabled = true
reindex_min_bytes = 100000   # 100 KB
reindex_max_bytes = 1000000  # 1 MB
```

## Server Role Configuration

### Server Role

Operating mode: transaction server or query peer:

| Flag            | Env Var              | Default       |
| --------------- | -------------------- | ------------- |
| `--server-role` | `FLUREE_SERVER_ROLE` | `transaction` |

Options:

- `transaction`: Write-enabled, produces events stream
- `peer`: Read-only, subscribes to transaction server

### Transaction Server URL (Peer Mode)

Base URL of the transaction server (required in peer mode):

| Flag              | Env Var                |
| ----------------- | ---------------------- |
| `--tx-server-url` | `FLUREE_TX_SERVER_URL` |

```bash
fluree-server \
  --server-role peer \
  --tx-server-url http://tx.internal:8090
```

## Authentication Configuration

### Replication vs Query Access

Fluree enforces a hard boundary between **replication-scoped** and **query-scoped** access:

- **Replication** (`fluree.storage.*`): Raw commit and index block transfer for peer sync and CLI `fetch`/`pull`/`push`. These operations bypass dataset policy (data must be bit-identical). Replication tokens are operator/service-account credentials — never issue them to end users.
- **Query** (`fluree.ledger.read/write.*`): Application-level data access through the query engine with full dataset policy enforcement. Query tokens are appropriate for end users and application service accounts.

A user holding only query-scoped tokens **cannot** clone or pull a ledger. They can `fluree track` a remote ledger (forwarding queries/transactions to the server) but cannot replicate its storage locally.

### Events Endpoint Authentication

Protect the `/v1/fluree/events` SSE endpoint:

| Flag                           | Env Var                              | Default |
| ------------------------------ | ------------------------------------ | ------- |
| `--events-auth-mode`           | `FLUREE_EVENTS_AUTH_MODE`            | `none`  |
| `--events-auth-audience`       | `FLUREE_EVENTS_AUTH_AUDIENCE`        | None    |
| `--events-auth-trusted-issuer` | `FLUREE_EVENTS_AUTH_TRUSTED_ISSUERS` | None    |

Modes:

- `none`: No authentication
- `optional`: Accept tokens but don't require them
- `required`: Require valid Bearer token

Supports both Ed25519 (embedded JWK) and OIDC/JWKS (RS256) tokens when the `oidc` feature is enabled and `--jwks-issuer` is configured. For OIDC tokens, issuer trust is implicit — only tokens signed by keys from configured JWKS endpoints will verify. For Ed25519 tokens, the issuer must appear in `--events-auth-trusted-issuer`.

```bash
# Ed25519 tokens only
fluree-server \
  --events-auth-mode required \
  --events-auth-trusted-issuer did:key:z6Mk...

# OIDC + Ed25519 (both work simultaneously)
fluree-server \
  --events-auth-mode required \
  --jwks-issuer "https://auth.example.com=https://auth.example.com/.well-known/jwks.json" \
  --events-auth-trusted-issuer did:key:z6Mk...
```

### Data API Authentication

Protect query/transaction endpoints (including `/v1/fluree/query/{ledger...}`,
`/v1/fluree/insert/{ledger...}`, `/v1/fluree/upsert/{ledger...}`,
`/v1/fluree/update/{ledger...}`, `/v1/fluree/info/{ledger...}`, and
`/v1/fluree/exists/{ledger...}`):

| Flag                               | Env Var                                 | Default |
| ---------------------------------- | --------------------------------------- | ------- |
| `--data-auth-mode`                 | `FLUREE_DATA_AUTH_MODE`                 | `none`  |
| `--data-auth-audience`             | `FLUREE_DATA_AUTH_AUDIENCE`             | None    |
| `--data-auth-trusted-issuer`       | `FLUREE_DATA_AUTH_TRUSTED_ISSUERS`      | None    |
| `--data-auth-default-policy-class` | `FLUREE_DATA_AUTH_DEFAULT_POLICY_CLASS` | None    |

Modes:

- `none`: No authentication (default)
- `optional`: Accept tokens but don't require them (development only)
- `required`: Require either a valid Bearer token **or** a signed request (JWS/VC)

Bearer token scopes:

- **Read**: `fluree.ledger.read.all=true` or `fluree.ledger.read.ledgers=[...]`
- **Write**: `fluree.ledger.write.all=true` or `fluree.ledger.write.ledgers=[...]`

Back-compat: `fluree.storage.*` claims imply **read** scope for data endpoints.

```bash
fluree-server \
  --data-auth-mode required \
  --data-auth-trusted-issuer did:key:z6Mk...
```

### OIDC / JWKS Token Verification

When the `oidc` feature is enabled, the server can verify JWT tokens signed by external identity
providers (e.g., Fluree Cloud Service) using JWKS (JSON Web Key Set) endpoints. This is in addition to the
existing embedded-JWK (Ed25519 `did:key`) verification path.

**Dual-path dispatch**: The server inspects each Bearer token's header:

- **Embedded JWK** (Ed25519): Uses the existing `verify_jws()` path — no JWKS needed.
- **kid header** (RS256): Uses OIDC/JWKS path — fetches the signing key from the issuer's JWKS endpoint.

Both paths coexist; no configuration change is needed for existing Ed25519 tokens.

| Flag               | Env Var                 | Default | Description                       |
| ------------------ | ----------------------- | ------- | --------------------------------- |
| `--jwks-issuer`    | `FLUREE_JWKS_ISSUERS`   | None    | OIDC issuer to trust (repeatable) |
| `--jwks-cache-ttl` | `FLUREE_JWKS_CACHE_TTL` | `300`   | JWKS cache TTL in seconds         |

The `--jwks-issuer` flag takes the format `<issuer_url>=<jwks_url>`:

```bash
fluree-server \
  --data-auth-mode required \
  --jwks-issuer "https://solo.example.com=https://solo.example.com/.well-known/jwks.json"
```

For multiple issuers, repeat the flag or use comma separation in the env var:

```bash
# CLI flags (repeatable)
fluree-server \
  --jwks-issuer "https://issuer1.example.com=https://issuer1.example.com/.well-known/jwks.json" \
  --jwks-issuer "https://issuer2.example.com=https://issuer2.example.com/.well-known/jwks.json"

# Environment variable (comma-separated)
export FLUREE_JWKS_ISSUERS="https://issuer1.example.com=https://issuer1.example.com/.well-known/jwks.json,https://issuer2.example.com=https://issuer2.example.com/.well-known/jwks.json"
```

**Behavior details:**

- JWKS endpoints are fetched at startup (`warm()`) but the server starts even if they're unreachable.
- Keys are cached and refreshed when a `kid` miss occurs (rate-limited to one refresh per issuer every 10 seconds).
- The token's `iss` claim must exactly match a configured issuer URL — unconfigured issuers are rejected immediately with a clear error.
- Data API, events, admin, and storage proxy endpoints all support JWKS verification. A single `--jwks-issuer` flag enables OIDC tokens across all endpoint groups. MCP auth continues to use the existing Ed25519 path only.

#### Connection-Scoped SPARQL Scope Enforcement

When a Bearer token is present for connection-scoped SPARQL queries (`/v1/fluree/query` with
`Content-Type: application/sparql-query`), the server enforces ledger scope:

- FROM / FROM NAMED clauses are parsed to extract ledger IDs (`name:branch`).
- Each ledger ID is checked against the token's read scope (`fluree.ledger.read.all` or `fluree.ledger.read.ledgers`).
- Out-of-scope ledgers return 404 (no existence leak).
- If no FROM clause is present, the query proceeds normally (the engine handles missing dataset errors).

### Admin Endpoint Authentication

Protect `/v1/fluree/create`, `/v1/fluree/drop`, `/v1/fluree/reindex`, branch
administration, and Iceberg mapping endpoints:

| Flag                          | Env Var                             | Default |
| ----------------------------- | ----------------------------------- | ------- |
| `--admin-auth-mode`           | `FLUREE_ADMIN_AUTH_MODE`            | `none`  |
| `--admin-auth-trusted-issuer` | `FLUREE_ADMIN_AUTH_TRUSTED_ISSUERS` | None    |

Modes:

- `none`: No authentication (development)
- `required`: Require valid Bearer token (production)

Supports both Ed25519 (embedded JWK) and OIDC/JWKS (RS256) tokens when the `oidc` feature is enabled and `--jwks-issuer` is configured. For OIDC tokens, issuer trust is implicit — only tokens signed by keys from configured JWKS endpoints will verify. For Ed25519 tokens, the issuer must appear in `--admin-auth-trusted-issuer` or the fallback `--events-auth-trusted-issuer`.

```bash
# Ed25519 tokens only
fluree-server \
  --admin-auth-mode required \
  --admin-auth-trusted-issuer did:key:z6Mk...

# OIDC (trust comes from --jwks-issuer, no did:key issuers needed)
fluree-server \
  --admin-auth-mode required \
  --jwks-issuer "https://auth.example.com=https://auth.example.com/.well-known/jwks.json"
```

If no admin-specific issuers are configured, falls back to `--events-auth-trusted-issuer`.

### MCP Endpoint Authentication

Protect the `/mcp` Model Context Protocol endpoint:

| Flag                        | Env Var                           | Default |
| --------------------------- | --------------------------------- | ------- |
| `--mcp-enabled`             | `FLUREE_MCP_ENABLED`              | `false` |
| `--mcp-auth-trusted-issuer` | `FLUREE_MCP_AUTH_TRUSTED_ISSUERS` | None    |

```bash
fluree-server \
  --mcp-enabled \
  --mcp-auth-trusted-issuer did:key:z6Mk...
```

## Peer Mode Configuration

### Peer Subscription

Configure what the peer subscribes to:

| Flag                              | Description                                     |
| --------------------------------- | ----------------------------------------------- |
| `--peer-subscribe-all`            | Subscribe to all ledgers and graph sources      |
| `--peer-ledger <ledger-id>`       | Subscribe to specific ledger (repeatable)       |
| `--peer-graph-source <ledger-id>` | Subscribe to specific graph source (repeatable) |

```bash
fluree-server \
  --server-role peer \
  --tx-server-url http://tx:8090 \
  --peer-subscribe-all
```

Or subscribe to specific resources:

```bash
fluree-server \
  --server-role peer \
  --tx-server-url http://tx:8090 \
  --peer-ledger books:main \
  --peer-ledger users:main
```

### Peer Events Configuration

| Flag                  | Env Var                    | Description                                                     |
| --------------------- | -------------------------- | --------------------------------------------------------------- |
| `--peer-events-url`   | `FLUREE_PEER_EVENTS_URL`   | Custom events URL (default: `{tx_server_url}/v1/fluree/events`) |
| `--peer-events-token` | `FLUREE_PEER_EVENTS_TOKEN` | Bearer token for events (supports `@filepath`)                  |

### Peer Reconnection

| Flag                          | Default | Description             |
| ----------------------------- | ------- | ----------------------- |
| `--peer-reconnect-initial-ms` | `1000`  | Initial reconnect delay |
| `--peer-reconnect-max-ms`     | `30000` | Maximum reconnect delay |
| `--peer-reconnect-multiplier` | `2.0`   | Backoff multiplier      |

### Peer Storage Access

| Flag                    | Env Var                      | Default  |
| ----------------------- | ---------------------------- | -------- |
| `--storage-access-mode` | `FLUREE_STORAGE_ACCESS_MODE` | `shared` |

Options:

- `shared`: Direct storage access (requires `--storage-path` or `--connection-config`)
- `proxy`: Proxy reads through transaction server

For proxy mode:

| Flag                         | Env Var                           |
| ---------------------------- | --------------------------------- |
| `--storage-proxy-token`      | `FLUREE_STORAGE_PROXY_TOKEN`      |
| `--storage-proxy-token-file` | `FLUREE_STORAGE_PROXY_TOKEN_FILE` |

## Storage Proxy Configuration (Transaction Server)

Storage proxy provides **replication-scoped** access to raw storage for peer servers and CLI replication commands (`fetch`/`pull`/`push`). Tokens must carry `fluree.storage.*` claims — query-scoped tokens (`fluree.ledger.read/write.*`) are not sufficient. See [Replication vs Query Access](#replication-vs-query-access) above.

Enable storage proxy endpoints for peers without direct storage access:

| Flag                                   | Env Var                                     | Default |
| -------------------------------------- | ------------------------------------------- | ------- |
| `--storage-proxy-enabled`              | `FLUREE_STORAGE_PROXY_ENABLED`              | `false` |
| `--storage-proxy-trusted-issuer`       | `FLUREE_STORAGE_PROXY_TRUSTED_ISSUERS`      | None    |
| `--storage-proxy-default-identity`     | `FLUREE_STORAGE_PROXY_DEFAULT_IDENTITY`     | None    |
| `--storage-proxy-default-policy-class` | `FLUREE_STORAGE_PROXY_DEFAULT_POLICY_CLASS` | None    |
| `--storage-proxy-debug-headers`        | `FLUREE_STORAGE_PROXY_DEBUG_HEADERS`        | `false` |

```bash
# Ed25519 trust (did:key):
fluree-server \
  --storage-proxy-enabled \
  --storage-proxy-trusted-issuer did:key:z6Mk...

# OIDC/JWKS trust (same --jwks-issuer flag used by other endpoints):
fluree-server \
  --storage-proxy-enabled \
  --jwks-issuer "https://solo.example.com=https://solo.example.com/.well-known/jwks.json"
```

> **JWKS support**: When `--jwks-issuer` is configured, storage proxy endpoints accept RS256 OIDC tokens in addition to Ed25519 JWS tokens. The `--jwks-issuer` flag is shared with data, admin, and events endpoints — a single flag enables OIDC across all endpoint groups.

## Complete Configuration Examples

### Development (Memory Storage)

```bash
fluree-server \
  --log-level debug
```

### Single Server (File Storage)

```bash
fluree-server \
  --storage-path /var/lib/fluree \
  --indexing-enabled \
  --log-level info
```

### Production with Admin Auth

```bash
fluree-server \
  --storage-path /var/lib/fluree \
  --indexing-enabled \
  --admin-auth-mode required \
  --admin-auth-trusted-issuer did:key:z6Mk... \
  --log-level info
```

### Transaction Server with Events Auth

```bash
fluree-server \
  --storage-path /var/lib/fluree \
  --events-auth-mode required \
  --events-auth-trusted-issuer did:key:z6Mk... \
  --storage-proxy-enabled \
  --admin-auth-mode required
```

### Production with OIDC (All Endpoints)

```bash
fluree-server \
  --storage-path /var/lib/fluree \
  --indexing-enabled \
  --jwks-issuer "https://auth.example.com=https://auth.example.com/.well-known/jwks.json" \
  --data-auth-mode required \
  --events-auth-mode required \
  --admin-auth-mode required \
  --storage-proxy-enabled
```

### Query Peer (Shared Storage)

```bash
fluree-server \
  --server-role peer \
  --tx-server-url http://tx.internal:8090 \
  --storage-path /var/lib/fluree \
  --peer-subscribe-all \
  --peer-events-token @/etc/fluree/peer-token.jwt
```

### Query Peer (Proxy Storage)

```bash
fluree-server \
  --server-role peer \
  --tx-server-url http://tx.internal:8090 \
  --storage-access-mode proxy \
  --storage-proxy-token @/etc/fluree/storage-proxy.jwt \
  --peer-subscribe-all \
  --peer-events-token @/etc/fluree/peer-token.jwt
```

### S3 + DynamoDB (Connection Config)

```bash
fluree server run \
  --connection-config /etc/fluree/connection.jsonld \
  --indexing-enabled \
  --reindex-min-bytes 100000 \
  --reindex-max-bytes 5000000 \
  --cache-max-mb 4096
```

With a config file:

```toml
[server]
connection_config = "/etc/fluree/connection.jsonld"
cache_max_mb = 4096

[server.indexing]
enabled = true
reindex_min_bytes = 100000
reindex_max_bytes = 5000000

[server.auth.data]
mode = "required"
trusted_issuers = ["did:key:z6Mk..."]
```

### S3 Peer (Shared Storage via Connection Config)

```bash
fluree server run \
  --server-role peer \
  --tx-server-url http://tx.internal:8090 \
  --connection-config /etc/fluree/connection.jsonld \
  --peer-subscribe-all \
  --peer-events-token @/etc/fluree/peer-token.jwt
```

## Environment Variables Reference

| Variable                                | Description                                     | Default                                                                 |
| --------------------------------------- | ----------------------------------------------- | ----------------------------------------------------------------------- |
| `FLUREE_HOME`                           | Global Fluree directory (unified config + data) | Platform dirs (see [Global Directory Layout](#global-directory-layout)) |
| `FLUREE_CONFIG`                         | Config file path                                | `.fluree/config.{toml,jsonld}` (auto-discovered)                        |
| `FLUREE_PROFILE`                        | Configuration profile name                      | None                                                                    |
| `FLUREE_LISTEN_ADDR`                    | Server address:port                             | `0.0.0.0:8090`                                                          |
| `FLUREE_STORAGE_PATH`                   | File storage path                               | `.fluree/storage`                                                       |
| `FLUREE_CONNECTION_CONFIG`              | JSON-LD connection config file path             | None                                                                    |
| `FLUREE_CORS_ENABLED`                   | Enable CORS                                     | `true`                                                                  |
| `FLUREE_INDEXING_ENABLED`               | Enable background indexing                      | `false`                                                                 |
| `FLUREE_REINDEX_MIN_BYTES`              | Soft reindex threshold (bytes)                  | `100000`                                                                |
| `FLUREE_REINDEX_MAX_BYTES`              | Hard reindex threshold (bytes)                  | `1000000`                                                               |
| `FLUREE_CACHE_MAX_MB`                   | Global cache budget (MB)                        | `30/40/50% of RAM (tiered: <4GB / 4-8GB / ≥8GB)`                                                     |
| `FLUREE_BODY_LIMIT`                     | Max request body bytes                          | `52428800`                                                              |
| `FLUREE_LOG_LEVEL`                      | Log level                                       | `info`                                                                  |
| `FLUREE_SERVER_ROLE`                    | Server role                                     | `transaction`                                                           |
| `FLUREE_TX_SERVER_URL`                  | Transaction server URL                          | None                                                                    |
| `FLUREE_EVENTS_AUTH_MODE`               | Events auth mode                                | `none`                                                                  |
| `FLUREE_EVENTS_AUTH_TRUSTED_ISSUERS`    | Events trusted issuers                          | None                                                                    |
| `FLUREE_DATA_AUTH_MODE`                 | Data API auth mode                              | `none`                                                                  |
| `FLUREE_DATA_AUTH_AUDIENCE`             | Data API expected audience                      | None                                                                    |
| `FLUREE_DATA_AUTH_TRUSTED_ISSUERS`      | Data API trusted issuers                        | None                                                                    |
| `FLUREE_DATA_AUTH_DEFAULT_POLICY_CLASS` | Data API default policy class                   | None                                                                    |
| `FLUREE_ADMIN_AUTH_MODE`                | Admin auth mode                                 | `none`                                                                  |
| `FLUREE_ADMIN_AUTH_TRUSTED_ISSUERS`     | Admin trusted issuers                           | None                                                                    |
| `FLUREE_MCP_ENABLED`                    | Enable MCP endpoint                             | `false`                                                                 |
| `FLUREE_MCP_AUTH_TRUSTED_ISSUERS`       | MCP trusted issuers                             | None                                                                    |
| `FLUREE_STORAGE_ACCESS_MODE`            | Peer storage mode                               | `shared`                                                                |
| `FLUREE_STORAGE_PROXY_ENABLED`          | Enable storage proxy                            | `false`                                                                 |

## Command-Line Reference

```bash
fluree-server --help
```

## Best Practices

### 1. Keep Secrets Out of Config Files

Tokens and credentials should not be stored as plaintext in config files (which may be committed to version control or readable by other processes). Three options, in order of preference:

**Environment variables** (recommended for production):

```bash
export FLUREE_PEER_EVENTS_TOKEN=$(cat /etc/fluree/token.jwt)
export FLUREE_STORAGE_PROXY_TOKEN=$(cat /etc/fluree/proxy-token.jwt)
```

**`@filepath` references** in config files or CLI flags (reads the file at startup):

```toml
[server.peer]
events_token = "@/etc/fluree/peer-token.jwt"
storage_proxy_token = "@/etc/fluree/proxy-token.jwt"
```

```bash
--peer-events-token @/etc/fluree/token.jwt
```

**Direct values** (development only): If a secret-bearing field contains a literal token in the config file, the server logs a warning at startup recommending `@filepath` or env vars.

The following config file fields support `@filepath` resolution:

| Config file key            | Env var alternative          |
| -------------------------- | ---------------------------- |
| `peer.events_token`        | `FLUREE_PEER_EVENTS_TOKEN`   |
| `peer.storage_proxy_token` | `FLUREE_STORAGE_PROXY_TOKEN` |

### 2. Enable Admin Auth in Production

Always protect admin endpoints in production:

```bash
fluree-server \
  --admin-auth-mode required \
  --admin-auth-trusted-issuer did:key:z6Mk...
```

### 3. Use File Storage for Persistence

Memory storage is lost on restart:

```bash
# Development only
fluree-server

# Production
fluree-server --storage-path /var/lib/fluree
```

### 4. Monitor Logs

Use structured logging for production:

```bash
fluree-server --log-level info 2>&1 | jq .
```

## Remote Connections

Remote connections enable SPARQL `SERVICE` federation against other Fluree instances. A remote connection maps a name to a server URL and bearer token. Once registered, queries can reference any ledger on that server using `SERVICE <fluree:remote:<name>/<ledger>> { ... }`.

### Rust API

Register remote connections on the `FlureeBuilder`:

```rust
let fluree = FlureeBuilder::file("./data")
    .remote_connection("acme", "https://acme-fluree.example.com", Some(token))
    .remote_connection("partner", "https://partner.example.com", None)
    .build()?;
```

Each call registers a named connection. The name is used in SPARQL queries:

```sparql
SERVICE <fluree:remote:acme/customers:main> { ?s ?p ?o }
SERVICE <fluree:remote:partner/inventory:main> { ?item ex:sku ?sku }
```

### Connection Parameters

| Parameter | Description |
|-----------|-------------|
| `name` | Alias used in `fluree:remote:<name>/...` URIs |
| `base_url` | Server URL (e.g., `https://acme-fluree.example.com`). The query path `/v1/fluree/query/{ledger}` is appended automatically. |
| `token` | Optional bearer token for authentication. Sent as `Authorization: Bearer <token>` on every request. |

The default per-request timeout is 30 seconds. Requests that exceed this produce a query error (or empty results with `SERVICE SILENT`).

### Security

Bearer tokens are stored in memory on the `Fluree` instance. They are never serialized to storage, included in nameservice records, or exposed through info/admin endpoints. If the token needs rotation, rebuild the `Fluree` instance with an updated token, or use `set_remote_service()` to inject a custom executor with token refresh logic.

### Feature Flag

The HTTP transport for remote SERVICE requires the `search-remote-client` Cargo feature (which enables `reqwest`). Without this feature, remote connections can be registered but queries against them will fail at runtime. The feature is enabled by default in the server binary.

See [SPARQL: Remote Fluree Federation](../query/sparql.md#remote-fluree-federation) for query syntax and examples.

## Related Documentation

- [Query Peers](query-peers.md) - Peer mode and replication
- [Storage Modes](storage.md) - Storage backend details
- [Telemetry](telemetry.md) - Monitoring configuration
- [Admin and Health](admin-and-health.md) - Health check endpoints
