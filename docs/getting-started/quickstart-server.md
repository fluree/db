# Quickstart: Run the Server

This guide will get the Fluree server running on your machine in minutes.

## Installation

### Option 1: Shell Installer (macOS / Linux)

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/fluree/db/releases/latest/download/fluree-db-cli-installer.sh | sh
```

### Option 2: Homebrew (macOS / Linux)

```bash
brew install fluree/tap/fluree
```

### Option 3: PowerShell (Windows)

Open PowerShell and run:

```powershell
irm https://github.com/fluree/db/releases/latest/download/fluree-db-cli-installer.ps1 | iex
```

Then open a **new** PowerShell session and verify `fluree --version`. The installer adds `%USERPROFILE%\bin` to your `PATH`. The binary is unsigned, so Windows SmartScreen may prompt on first run — click **More info → Run anyway**.

### Option 4: Download Pre-built Binary

Download the latest release for your platform from [GitHub Releases](https://github.com/fluree/db/releases):

```bash
# Linux (x86_64)
curl -L https://github.com/fluree/db/releases/latest/download/fluree-db-cli-x86_64-unknown-linux-gnu.tar.xz | tar xJ
chmod +x fluree-db-cli-x86_64-unknown-linux-gnu/fluree

# macOS (Apple Silicon)
curl -L https://github.com/fluree/db/releases/latest/download/fluree-db-cli-aarch64-apple-darwin.tar.xz | tar xJ
chmod +x fluree-db-cli-aarch64-apple-darwin/fluree
```

### Option 5: Build from Source

If you have Rust installed:

```bash
# Clone the repository
git clone https://github.com/fluree/db.git
cd db

# Build the CLI (includes embedded server)
cargo build --release -p fluree-db-cli

# Binary will be at target/release/fluree
```

### Option 6: Docker

```bash
# Pull the image
docker pull fluree/server:latest

# Run the container
docker run -p 8090:8090 fluree/server:latest
```

For configuration (mounted JSON-LD/TOML config files, env vars, persistent volumes, S3+DynamoDB, query peers, full Compose example), see [Running with Docker](../operations/docker.md).

## Start the Server

### Default Storage (Development)

`fluree server run` needs a Fluree project directory; create one with `fluree init` if you
haven't. By default the server stores data in the project's `.fluree/storage` directory:

```bash
fluree init
fluree server run
```

You should see output like:

```text
INFO fluree_db_cli::commands::server: Starting Fluree server (foreground) version="…" addr=0.0.0.0:8090 storage="file"
```

### File Storage (Persistent)

For persistent storage, specify a storage path:

```bash
fluree server run --storage-path /var/lib/fluree
```

### Custom Port

```bash
fluree server run --listen-addr 0.0.0.0:9090
```

### Debug Logging

```bash
fluree server run --log-level debug
```

## Verify Installation

### Check Server Health

```bash
curl http://localhost:8090/health
```

Expected response:

```json
{
  "status": "ok",
  "version": "4.0.4"
}
```

### Create a Ledger

```bash
curl -X POST http://localhost:8090/v1/fluree/create \
  -H "Content-Type: application/json" \
  -d '{"ledger": "test:main"}'
```

### Insert Data

```bash
curl -X POST "http://localhost:8090/v1/fluree/insert" \
  -H "Content-Type: application/json" \
  -H "fluree-ledger: test:main" \
  -d '{
    "@context": {"ex": "http://example.org/"},
    "@id": "ex:alice",
    "ex:name": "Alice"
  }'
```

### Query Data

```bash
curl -X POST "http://localhost:8090/v1/fluree/query" \
  -H "Content-Type: application/json" \
  -d '{
    "from": "test:main",
    "select": {"?s": ["*"]},
    "where": [["?s", "ex:name", "?name"]]
  }'
```

## Understanding the Server

### Endpoints

Default server endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/v1/fluree/create` | POST | Create a ledger |
| `/v1/fluree/drop` | POST | Drop a ledger |
| `/v1/fluree/query` | GET/POST | Execute queries |
| `/v1/fluree/insert` | POST | Insert data |
| `/v1/fluree/update` | POST | Update with WHERE/DELETE/INSERT |
| `/v1/fluree/events` | GET | SSE event stream |

See the [API Reference](../api/endpoints.md) for complete endpoint documentation.

### Storage Modes

**File** (default; `.fluree/storage`, or set with `--storage-path`):
- Persistent local file storage
- Data survives restarts
- Best for single-server deployments

**Memory** (via `--connection-config`; see [Storage Modes](../operations/storage.md#memory-storage)):
- Fast, in-process storage
- Data lost on restart
- Best for development and testing

### Configuration

All options can be set via CLI flags or environment variables. `fluree server run` takes the
most common server flags directly; pass any other server flag after `--`
(for example `fluree server run -- --cache-max-mb 4096`):

```bash
# CLI flag
fluree server run --storage-path /data --log-level debug

# Environment variables
export FLUREE_LISTEN_ADDR=0.0.0.0:9090
export FLUREE_LOG_LEVEL=debug
fluree server run
```

The storage path is the exception: set it with `--storage-path` or `storage_path` in the
config file, since `fluree server run` overrides `FLUREE_STORAGE_PATH`.

See [Configuration](../operations/configuration.md) for all options.

## Common Configurations

### Development

```bash
fluree server run --log-level debug
```

### Production (Single Server)

```bash
fluree server run \
  --storage-path /var/lib/fluree \
  -- \
  --indexing-enabled \
  --events-auth-mode required \
  --events-auth-trusted-issuer did:key:z6Mk...
```

### With Background Indexing

```bash
fluree server run \
  --storage-path /var/lib/fluree \
  -- \
  --indexing-enabled
```

## Docker Deployment

For the full Docker guide — image internals, configuration via env vars vs mounted JSON-LD/TOML config files, persistent volumes, LRU cache and indexing tuning, S3+DynamoDB connection configs, query peers, and a production-ready Compose example — see [Running with Docker](../operations/docker.md).

Minimal persistent run:

```bash
docker run -d --name fluree \
  -p 8090:8090 \
  -v fluree-data:/var/lib/fluree \
  fluree/server:latest
```

## Troubleshooting

### Port Already in Use

```bash
# Use a different port
fluree server run --listen-addr 0.0.0.0:9090
```

### Permission Denied (File Storage)

```bash
sudo chown -R $USER:$USER /var/lib/fluree
chmod -R 755 /var/lib/fluree
```

### Server Won't Start

Check logs with debug level:

```bash
fluree server run --log-level debug
```

### Connection Refused

Verify the server is running and check the listen address:

```bash
# Listen on all interfaces (not just localhost)
fluree server run --listen-addr 0.0.0.0:8090
```

## Next Steps

Now that your server is running:

1. [Create a Ledger](quickstart-ledger.md) - Set up your first database
2. [Write Data](quickstart-write.md) - Insert your first records
3. [Query Data](quickstart-query.md) - Retrieve and explore your data

For production deployments:

- [Configuration](../operations/configuration.md) - All server options
- [Query Peers](../operations/query-peers.md) - Horizontal scaling
- [Admin Authentication](../api/endpoints.md#admin-authentication) - Protect admin endpoints
