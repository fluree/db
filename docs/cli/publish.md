# fluree publish

Publish a local ledger to a remote server. Creates the ledger on the remote if it doesn't exist, pushes all local commits, and configures upstream tracking for subsequent `push`/`pull`.

## Usage

```bash
fluree publish <REMOTE> [LEDGER] [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<REMOTE>` | Remote name (e.g., "origin") |
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Options

| Option | Description |
|--------|-------------|
| `--remote-name <NAME>` | Remote ledger name (defaults to local ledger name) |

## Description

`fluree publish` is the reverse of `fluree clone`. It takes a locally-created ledger and pushes it to a remote server in a single operation:

1. Checks if the ledger exists on the remote (`GET /exists`)
2. Creates it if not (`POST /create`)
3. Pushes all local commits (`POST /push`)
4. Configures upstream tracking so subsequent `fluree push` and `fluree pull` work

This is intended for the "create locally, deploy to server" workflow. If the remote ledger already has data (t > 0), the command will fail — use `fluree push` instead for incremental updates.

## Examples

```bash
# Publish active ledger to origin
fluree publish origin

# Publish a specific ledger
fluree publish origin mydb

# Publish with a different name on the remote
fluree publish origin mydb --remote-name production-db

# Typical workflow: create locally, develop, then publish
fluree create mydb
fluree insert mydb -e '{"@id": "ex:test", "ex:name": "Test"}'
fluree publish origin mydb
```

## Prerequisites

- A remote must be configured: `fluree remote add origin <url>`
- The remote must support the Fluree HTTP API (see [Server implementation guide](../design/server-implementation.md))
- A valid auth token if the remote requires authentication: `fluree auth login --remote origin`

## After Publishing

Once published, the ledger has upstream tracking configured. Use standard sync commands:

```bash
# Push new local commits to remote
fluree push

# Pull remote changes
fluree pull
```

## See Also

- [push](push.md) - Push incremental commits to upstream
- [pull](pull.md) - Pull changes from upstream
- [clone](clone.md) - Clone a remote ledger locally (reverse of publish)
- [remote](remote.md) - Manage remote server configuration
- [upstream](upstream.md) - Manage upstream tracking
- [export](export.md) - Export ledger as `.flpack` for file-based transfer
