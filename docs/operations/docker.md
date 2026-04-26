# Running Fluree with Docker

The official image (`fluree/server`) ships the `fluree` binary on a slim Debian base. This guide covers what's inside the image, how to configure it (env vars, mounted config files, CLI flags), and worked recipes for the common production patterns.

## What's in the Image

| Aspect             | Value                                  |
| ------------------ | -------------------------------------- |
| Base               | `debian:trixie-slim`                   |
| Entrypoint         | `/usr/local/bin/fluree-entrypoint.sh`  |
| Default command    | `fluree server run`                    |
| `WORKDIR`          | `/var/lib/fluree`                      |
| `VOLUME`           | `/var/lib/fluree`                      |
| Exposed port       | `8090`                                 |
| Runtime user       | `fluree` (UID `1000`, GID `1000`)      |
| Healthcheck        | `GET /health` every 30s                |
| Default log filter | `RUST_LOG=info`                        |

**Entrypoint behavior:** on first start, if `/var/lib/fluree/.fluree/` does not exist, the entrypoint runs `fluree init` to create a default `.fluree/config.toml` and `.fluree/storage/` directory. Subsequent starts skip init. Any arguments passed to `docker run` after the image name are forwarded to `fluree server run`, so you can append CLI flags (e.g. `--log-level debug`) directly.

## Quick Start

```bash
docker run --rm -p 8090:8090 fluree/server:latest
```

Verify:

```bash
curl http://localhost:8090/health
```

Data lives inside the container's writable layer here — fine for trying things out, lost when the container is removed. For anything beyond a smoke test, mount a volume.

## Persisting Data

The image declares `VOLUME /var/lib/fluree`. Mount a host directory or named volume there:

```bash
# Named volume (recommended)
docker run -d --name fluree \
  -p 8090:8090 \
  -v fluree-data:/var/lib/fluree \
  fluree/server:latest

# Host bind mount — make sure the directory is writable by UID 1000
mkdir -p ./fluree-data && sudo chown 1000:1000 ./fluree-data
docker run -d --name fluree \
  -p 8090:8090 \
  -v "$PWD/fluree-data:/var/lib/fluree" \
  fluree/server:latest
```

The volume holds both `.fluree/config.toml` (config) and `.fluree/storage/` (ledger data) by default.

## Three Ways to Configure

Fluree resolves configuration with this precedence (highest wins):

1. **CLI flags** appended after the image name
2. **Environment variables** (`FLUREE_*`) set with `-e` or `environment:`
3. **Profile overrides** (`[profiles.<name>.server]`) when you pass `--profile`
4. **Config file** at `.fluree/config.toml` or `.fluree/config.jsonld`
5. **Built-in defaults**

You can use any one of these — or, more typically, layer them: bake a base config file into a volume, then tweak per-environment with env vars or compose overrides.

> **Heads up — log level:** The Dockerfile sets `ENV RUST_LOG=info`. The console log filter uses `RUST_LOG` if it is non-empty and only falls back to `FLUREE_LOG_LEVEL` when `RUST_LOG` is unset. Inside this image you must override `RUST_LOG` to change console verbosity:
>
> ```bash
> docker run -e RUST_LOG=debug fluree/server:latest
> ```

### 1. Environment Variables Only

Every CLI flag has a `FLUREE_*` env var equivalent (see [Configuration](configuration.md)). For simple deployments this is the lowest-friction path:

```bash
docker run -d --name fluree \
  -p 8090:8090 \
  -v fluree-data:/var/lib/fluree \
  -e FLUREE_LISTEN_ADDR=0.0.0.0:8090 \
  -e FLUREE_STORAGE_PATH=/var/lib/fluree/.fluree/storage \
  -e FLUREE_INDEXING_ENABLED=true \
  -e FLUREE_REINDEX_MIN_BYTES=1000000 \
  -e FLUREE_REINDEX_MAX_BYTES=10000000 \
  -e FLUREE_CACHE_MAX_MB=2048 \
  -e RUST_LOG=info \
  fluree/server:latest
```

### 2. Mounted Config File (JSON-LD or TOML)

Author a config file on the host, then mount it at `/var/lib/fluree/.fluree/config.jsonld` (or `.toml`). The server walks up from `WORKDIR=/var/lib/fluree` and picks it up automatically.

`./fluree-config/config.jsonld`:

```json
{
  "@context": { "@vocab": "https://ns.flur.ee/config#" },
  "server": {
    "listen_addr": "0.0.0.0:8090",
    "storage_path": "/var/lib/fluree/.fluree/storage",
    "log_level": "info",
    "cache_max_mb": 2048,
    "indexing": {
      "enabled": true,
      "reindex_min_bytes": 1000000,
      "reindex_max_bytes": 10000000
    }
  },
  "profiles": {
    "prod": {
      "server": {
        "log_level": "warn",
        "cache_max_mb": 8192
      }
    }
  }
}
```

```bash
docker run -d --name fluree \
  -p 8090:8090 \
  -v fluree-data:/var/lib/fluree \
  -v "$PWD/fluree-config/config.jsonld:/var/lib/fluree/.fluree/config.jsonld:ro" \
  fluree/server:latest --profile prod
```

If both `config.toml` and `config.jsonld` exist in the same directory, TOML wins and the server logs a warning. Pick one format.

The TOML equivalent (`./fluree-config/config.toml`):

```toml
[server]
listen_addr = "0.0.0.0:8090"
storage_path = "/var/lib/fluree/.fluree/storage"
log_level = "info"
cache_max_mb = 2048

[server.indexing]
enabled = true
reindex_min_bytes = 1000000
reindex_max_bytes = 10000000

[profiles.prod.server]
log_level = "warn"
cache_max_mb = 8192
```

You can also stash the config outside `WORKDIR` and point at it explicitly:

```bash
docker run -d --name fluree \
  -p 8090:8090 \
  -v fluree-data:/var/lib/fluree \
  -v "$PWD/fluree-config:/etc/fluree:ro" \
  fluree/server:latest --config /etc/fluree/config.jsonld
```

### 3. Layered: File + Env Var Overrides

The common production shape: bake the base config into the image or volume, then let the orchestrator override per-environment with `FLUREE_*` env vars. Env vars beat the file — no file edit needed to bump cache size in staging vs. prod.

```bash
docker run -d --name fluree \
  -p 8090:8090 \
  -v fluree-data:/var/lib/fluree \
  -v "$PWD/fluree-config/config.jsonld:/var/lib/fluree/.fluree/config.jsonld:ro" \
  -e FLUREE_CACHE_MAX_MB=4096 \
  -e RUST_LOG=warn \
  fluree/server:latest
```

## Common Configuration Recipes

### Tuning the LRU Cache

`cache_max_mb` is the global budget for the in-memory index/flake cache. The default is a tiered fraction of system RAM (30%/40%/50% for <4GB/4–8GB/≥8GB hosts). On a container with a hard memory limit, **set this explicitly** — the auto-tier reads host RAM, not the cgroup limit, and can over-allocate.

```yaml
# docker-compose.yml fragment
services:
  fluree:
    image: fluree/server:latest
    mem_limit: 6g
    environment:
      FLUREE_CACHE_MAX_MB: 3072    # ~50% of the cgroup limit
```

Or in JSON-LD:

```json
{
  "@context": { "@vocab": "https://ns.flur.ee/config#" },
  "server": { "cache_max_mb": 3072 }
}
```

### Background Indexing

Indexing is **off by default**. Enable it for production write workloads — without it, every commit writes to novelty and queries get slower as novelty grows.

| Setting              | Meaning                                                      |
| -------------------- | ------------------------------------------------------------ |
| `indexing.enabled`   | Turn the background indexer on                               |
| `reindex_min_bytes`  | Soft threshold — novelty above this triggers a background reindex |
| `reindex_max_bytes`  | Hard threshold — commits **block** above this until reindexing catches up |

Tune `min`/`max` based on commit volume. Defaults (100 KB / 1 MB) are conservative; busy ledgers should raise both:

```toml
[server.indexing]
enabled = true
reindex_min_bytes = 5000000     # 5 MB — start indexing in the background
reindex_max_bytes = 50000000    # 50 MB — block commits at this point
```

```bash
docker run -d \
  -e FLUREE_INDEXING_ENABLED=true \
  -e FLUREE_REINDEX_MIN_BYTES=5000000 \
  -e FLUREE_REINDEX_MAX_BYTES=50000000 \
  fluree/server:latest
```

### CORS and Request Body Size

```toml
[server]
cors_enabled = true
body_limit = 104857600    # 100 MB — raise for bulk imports
```

### Authentication (Production)

Require a Bearer token on data and admin endpoints. The trusted issuer is the `did:key` of your token signer.

```toml
[server.auth.data]
mode = "required"
trusted_issuers = ["did:key:z6Mk..."]

[server.auth.admin]
mode = "required"
trusted_issuers = ["did:key:z6Mk..."]
```

For OIDC/JWKS (e.g. an external IdP), set `--jwks-issuer` or `FLUREE_JWKS_ISSUERS`:

```bash
docker run -d \
  -e FLUREE_DATA_AUTH_MODE=required \
  -e FLUREE_JWKS_ISSUERS="https://auth.example.com=https://auth.example.com/.well-known/jwks.json" \
  fluree/server:latest
```

See [Configuration → Authentication](configuration.md#authentication-configuration) for the full matrix.

### S3 + DynamoDB (Distributed Storage)

For multi-node or cloud deployments, point the server at a JSON-LD **connection config** describing your storage and nameservice. AWS credentials come from the standard SDK chain (env vars, IAM role, etc.) — they are **not** part of the connection config.

`./fluree-config/connection.jsonld`:

```json
{
  "@context": {
    "@base": "https://ns.flur.ee/config/connection/",
    "@vocab": "https://ns.flur.ee/system#"
  },
  "@graph": [
    { "@id": "commitStorage", "@type": "Storage",
      "s3Bucket": "fluree-prod-commits", "s3Prefix": "data/" },
    { "@id": "indexStorage", "@type": "Storage",
      "s3Bucket": "fluree-prod-indexes" },
    { "@id": "publisher", "@type": "Publisher",
      "dynamodbTable": "fluree-nameservice", "dynamodbRegion": "us-east-1" },
    { "@id": "conn", "@type": "Connection",
      "commitStorage": { "@id": "commitStorage" },
      "indexStorage":  { "@id": "indexStorage" },
      "primaryPublisher": { "@id": "publisher" } }
  ]
}
```

```bash
docker run -d --name fluree \
  -p 8090:8090 \
  -v "$PWD/fluree-config:/etc/fluree:ro" \
  -e AWS_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  -e FLUREE_CONNECTION_CONFIG=/etc/fluree/connection.jsonld \
  -e FLUREE_INDEXING_ENABLED=true \
  fluree/server:latest
```

`--connection-config` and `--storage-path` are mutually exclusive. See [Configuration → Connection Configuration](configuration.md#connection-configuration-s3-dynamodb-etc) and the [DynamoDB guide](dynamodb-guide.md) for backend-specific setup.

### Query Peer

Run as a read-only peer that subscribes to a transaction server's event stream:

```bash
docker run -d --name fluree-peer \
  -p 8090:8090 \
  -v fluree-peer-data:/var/lib/fluree \
  -e FLUREE_SERVER_ROLE=peer \
  -e FLUREE_TX_SERVER_URL=http://tx.internal:8090 \
  fluree/server:latest --peer-subscribe-all
```

See [Query peers and replication](query-peers.md) for the proxy-mode and auth options.

## Docker Compose: Full Example

A production-leaning single-node setup with a mounted JSON-LD config, env-var overrides, named data volume, and resource limits:

```yaml
services:
  fluree:
    image: fluree/server:latest
    container_name: fluree
    restart: unless-stopped
    ports:
      - "8090:8090"
    volumes:
      - fluree-data:/var/lib/fluree
      - ./fluree-config/config.jsonld:/var/lib/fluree/.fluree/config.jsonld:ro
    environment:
      RUST_LOG: info
      FLUREE_CACHE_MAX_MB: 4096
      FLUREE_INDEXING_ENABLED: "true"
      FLUREE_REINDEX_MIN_BYTES: "5000000"
      FLUREE_REINDEX_MAX_BYTES: "50000000"
      # Auth — point at your trusted did:key signer
      FLUREE_DATA_AUTH_MODE: required
      FLUREE_DATA_AUTH_TRUSTED_ISSUERS: did:key:z6Mk...
      FLUREE_ADMIN_AUTH_MODE: required
      FLUREE_ADMIN_AUTH_TRUSTED_ISSUERS: did:key:z6Mk...
    mem_limit: 8g
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://127.0.0.1:8090/health"]
      interval: 30s
      timeout: 3s
      start_period: 15s
      retries: 3
    command: ["--profile", "prod"]

volumes:
  fluree-data:
```

```bash
docker compose up -d
docker compose logs -f fluree
```

## Troubleshooting

**Container restarts after `fluree init`.** First-run init only runs when `/var/lib/fluree/.fluree/` is missing. If the volume is owned by a non-`1000` UID, init fails. Fix with `sudo chown -R 1000:1000 ./fluree-data` on the host.

**Mounted config file is ignored.** Confirm the mount path and the file extension. The server only auto-discovers `.fluree/config.toml` or `.fluree/config.jsonld` under the working directory. Anything else needs `--config <path>` (or `FLUREE_CONFIG=<path>`). If both formats are present in the same directory, TOML wins — check the startup logs for the warning.

**Setting `FLUREE_LOG_LEVEL` doesn't change console output.** The image's `ENV RUST_LOG=info` shadows it. Override with `-e RUST_LOG=debug` instead.

**`cache_max_mb` auto-default is too large under a memory limit.** The auto-tier reads host RAM, not the cgroup. Set `FLUREE_CACHE_MAX_MB` (or `cache_max_mb` in the file) to a value sized to the container limit.

**Health check failing.** `curl http://localhost:8090/health` from your host. If the server is up but the healthcheck fails, the listen address is probably bound to `127.0.0.1` inside the container — set `FLUREE_LISTEN_ADDR=0.0.0.0:8090`.

## Related Documentation

- [Configuration reference](configuration.md) — full flag/env/file matrix
- [Storage modes](storage.md) — memory / file / AWS / IPFS
- [JSON-LD connection configuration](../reference/connection-config-jsonld.md) — schema for `connection.jsonld`
- [Query peers and replication](query-peers.md) — peer-mode deployments
- [Quickstart: Server](../getting-started/quickstart-server.md) — first-run walkthrough
