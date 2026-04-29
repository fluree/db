# server

Manage the Fluree HTTP server from the CLI. The server inherits the same `.fluree/` context (config file, storage path) as the CLI — one directory, two modes of interaction.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `run`      | Run the server in the foreground (Ctrl-C to stop) |
| `start`    | Start the server as a background process |
| `stop`     | Stop a backgrounded server |
| `status`   | Show server status (PID, address, health) |
| `restart`  | Stop and restart a backgrounded server |
| `logs`     | View server logs |

## Common Options

These options are available on `run`, `start`, and `restart`:

| Option | Description |
|--------|-------------|
| `--listen-addr <ADDR>` | Listen address (e.g., `0.0.0.0:8090`) |
| `--storage-path <PATH>` | Storage path override (local file storage) |
| `--connection-config <FILE>` | JSON-LD connection config for S3, DynamoDB, etc. |
| `--log-level <LEVEL>` | Log level (`trace`, `debug`, `info`, `warn`, `error`) |
| `--profile <NAME>` | Configuration profile to activate |
| `-- <ARGS>...` | Additional server flags (passed through to server config) |

`--storage-path` and `--connection-config` are mutually exclusive. Use `--storage-path` for local file storage or `--connection-config` for remote backends (S3, DynamoDB, split storage). See [Configuration](../operations/configuration.md#connection-configuration-s3-dynamodb-etc) for details.

When no flags are provided, the server discovers its configuration using the same search as the CLI: it walks up from the current working directory looking for a `.fluree/config.toml` (or `config.jsonld`), then falls back to the global Fluree config directory (`$FLUREE_HOME`, or the platform config directory — see [Configuration](../operations/configuration.md)). Server settings live under the `[server]` section. The CLI's `--config` flag is also honored.

## run

Run the server in the foreground. Logs go to stderr. Press Ctrl-C for graceful shutdown.

```bash
# Start with defaults from config.toml
fluree server run

# Override listen address
fluree server run --listen-addr 127.0.0.1:9090

# S3 + DynamoDB backend
fluree server run --connection-config /etc/fluree/connection.jsonld

# Pass through advanced server flags
fluree server run -- --cors-enabled --indexing-enabled
```

## start

Start the server as a background daemon. Writes PID and metadata to `.fluree/` and redirects output to `.fluree/server.log`.

```bash
# Start in background
fluree server start

# Preview resolved config without starting
fluree server start --dry-run

# Start with overrides
fluree server start --listen-addr 0.0.0.0:8090 --log-level debug
```

The `--dry-run` flag prints the fully resolved configuration (config file + env + flag overrides merged) without actually starting the server. Useful for debugging "why is it using port X?".

## stop

Stop a backgrounded server by sending SIGTERM and waiting for graceful shutdown (up to 10 seconds).

```bash
fluree server stop

# Force kill after timeout
fluree server stop --force
```

## status

Check whether the server is running. Shows PID, listen address, uptime, storage path, and performs an HTTP health check.

```bash
fluree server status
```

Example output:

```
ok: Server is running
  pid:          12345
  listen_addr:  0.0.0.0:8090
  storage_path: /path/to/.fluree/storage
  started_at:   2026-02-16T10:30:00Z
  uptime:       2h 15m 30s
  health:       ok
  log:          /path/to/.fluree/server.log
```

When using `--connection-config`, the status shows the connection config path instead of the storage path:

```
ok: Server is running
  pid:          12345
  listen_addr:  0.0.0.0:8090
  connection:   /etc/fluree/connection.jsonld
  started_at:   2026-02-16T10:30:00Z
  uptime:       2h 15m 30s
  health:       ok
```

## restart

Stop and restart a backgrounded server. Recovers the original arguments from `.fluree/server.meta.json`. New flag overrides can be applied on restart.

```bash
fluree server restart

# Restart with a different log level
fluree server restart --log-level debug
```

## logs

View server log output from `.fluree/server.log`.

```bash
# Last 50 lines (default)
fluree server logs

# Last 100 lines
fluree server logs -n 100

# Follow (like tail -f)
fluree server logs -f
```

## Auto-Routing

When a local server is running (started via `fluree server start`), CLI commands that support remote execution are **automatically routed through the server's HTTP API**. This applies to:

- `fluree query`
- `fluree insert`
- `fluree upsert`
- `fluree list`
- `fluree info`

The CLI detects the running server by checking `.fluree/server.meta.json` and verifying the PID is alive. When auto-routing is active, you'll see a hint on stderr:

```
  server: routing through local server at 0.0.0.0:8090 (use --direct to bypass)
```

### Opting out

Use the `--direct` global flag to bypass auto-routing and execute directly via the CLI's file-based path:

```bash
# Route through server (default when server is running)
fluree query 'SELECT * WHERE { ?s ?p ?o } LIMIT 10'

# Bypass server, execute directly
fluree query --direct 'SELECT * WHERE { ?s ?p ?o } LIMIT 10'
```

### Crash detection

If the server has crashed or been killed, the CLI detects the stale PID and falls back to direct execution with a notice:

```
  notice: local server (pid 12345) is no longer running; executing directly
```

Use `fluree server status` to check server health, or `fluree server logs` to view crash output.

## Runtime Files

When a background server is running, these files are created in the `.fluree/` data directory:

| File | Description |
|------|-------------|
| `server.pid` | PID of the background server process |
| `server.log` | stdout + stderr from the background server |
| `server.meta.json` | Metadata for `restart` and `status` (PID, address, args, start time) |

These files are cleaned up automatically by `fluree server stop`.

## Configuration

The server uses the same config file as the CLI (discovered via walk-up or global fallback — see above). Server-specific settings live under the `[server]` section:

```toml
[server]
listen_addr = "0.0.0.0:8090"
storage_path = "/var/lib/fluree"
log_level = "info"
cors_enabled = true
# cache_max_mb = 4096  # global cache budget (MB); default: tiered fraction of RAM (30% <4GB, 40% 4-8GB, 50% ≥8GB)

[server.indexing]
enabled = true
reindex_min_bytes = 100_000
# reindex_max_bytes defaults to 20% of system RAM; uncomment to override
# reindex_max_bytes = 536_870_912  # 512 MB
```

For S3/DynamoDB backends, use `connection_config` instead of `storage_path`:

```toml
[server]
connection_config = "/etc/fluree/connection.jsonld"
cache_max_mb = 4096

[server.indexing]
enabled = true
```

Indexing settings live under the `[server.indexing]` subsection, not directly on `[server]`. Authentication settings similarly use `[server.auth.events]`, `[server.auth.data]`, etc.

See [Configuration](../operations/configuration.md) for the full list of server options.

## Feature Flags

The `server` subcommand requires the `server` Cargo feature (enabled by default). If compiled without it:

```bash
fluree server run
# error: server support not compiled. Rebuild with `--features server`.
```

For S3/DynamoDB support via `--connection-config`, the `aws` feature must be enabled:

```bash
cargo build -p fluree-db-cli --features aws
```

Without this feature, S3 storage configs in the connection config will produce a clear error at startup.
