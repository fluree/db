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

### Option 3: Download Pre-built Binary

Download the latest release for your platform from [GitHub Releases](https://github.com/fluree/db/releases):

```bash
# Linux (x86_64)
curl -L https://github.com/fluree/db/releases/latest/download/fluree-db-cli-x86_64-unknown-linux-gnu.tar.xz | tar xJ
chmod +x fluree-db-cli-x86_64-unknown-linux-gnu/fluree

# macOS (Apple Silicon)
curl -L https://github.com/fluree/db/releases/latest/download/fluree-db-cli-aarch64-apple-darwin.tar.xz | tar xJ
chmod +x fluree-db-cli-aarch64-apple-darwin/fluree
```

### Option 4: Build from Source

If you have Rust installed:

```bash
# Clone the repository
git clone https://github.com/fluree/db.git
cd db

# Build the CLI (includes embedded server)
cargo build --release -p fluree-db-cli

# Binary will be at target/release/fluree
```

### Option 5: Docker

```bash
# Pull the image
docker pull fluree/server:latest

# Run the container
docker run -p 8090:8090 fluree/server:latest
```

## Start the Server

### Memory Storage (Development)

Start the server with in-memory storage (data is lost on restart):

```bash
fluree server run
```

You should see output like:

```text
INFO fluree_db_server: Starting Fluree server
INFO fluree_db_server: Storage mode: memory
INFO fluree_db_server: Server listening on 0.0.0.0:8090
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
  "version": "0.1.0"
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
| `/fluree/create` | POST | Create a ledger |
| `/fluree/drop` | POST | Drop a ledger |
| `/fluree/query` | GET/POST | Execute queries |
| `/fluree/insert` | POST | Insert data |
| `/fluree/update` | POST | Update with WHERE/DELETE/INSERT |
| `/fluree/events` | GET | SSE event stream |

See the [API Reference](../api/endpoints.md) for complete endpoint documentation.

### Storage Modes

**Memory** (default):
- Fast, in-process storage
- Data lost on restart
- Best for development and testing

**File** (with `--storage-path`):
- Persistent local file storage
- Data survives restarts
- Best for single-server deployments

### Configuration

All options can be set via CLI flags or environment variables:

```bash
# CLI flag
fluree server run --storage-path /data --log-level debug

# Environment variables
export FLUREE_STORAGE_PATH=/data
export FLUREE_LOG_LEVEL=debug
fluree server run
```

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
  --indexing-enabled \
  --admin-auth-mode required \
  --admin-auth-trusted-issuer did:key:z6Mk...
```

### With Background Indexing

```bash
fluree server run \
  --storage-path /var/lib/fluree \
  --indexing-enabled
```

## Docker Deployment

### Basic Run

```bash
docker run -d \
  --name fluree \
  -p 8090:8090 \
  fluree/server:latest
```

### With Persistent Storage

```bash
docker run -d \
  --name fluree \
  -p 8090:8090 \
  -v /path/to/data:/data \
  -e FLUREE_STORAGE_PATH=/data \
  fluree/server:latest
```

### Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  fluree:
    image: fluree/server:latest
    ports:
      - "8090:8090"
    environment:
      FLUREE_STORAGE_PATH: /data
      FLUREE_LOG_LEVEL: info
      FLUREE_INDEXING_ENABLED: "true"
    volumes:
      - fluree-data:/data
    restart: unless-stopped

volumes:
  fluree-data:
```

Start with:

```bash
docker-compose up -d
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
