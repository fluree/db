# fluree remote

Manage remote servers for syncing ledgers.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `add` | Add a remote server |
| `remove` | Remove a remote |
| `list` | List all configured remotes |
| `show` | Show details for a remote |
| `erasure-authority` | Whether clone and pull accept a remote's permanent erasures |

---

## fluree remote add

Add a remote server configuration.

### Usage

```bash
fluree remote add <NAME> <URL> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Remote name (e.g., `origin`) |
| `<URL>` | Server URL (e.g., `http://localhost:8090`) |

### Options

| Option | Description |
|--------|-------------|
| `--token <TOKEN>` | Authentication token (or `@filepath` to read from file) |
| `--erasure-authority` | Accept the [permanent erasures](../transactions/permanent-erasure.md#clones-and-pulls) this remote's history carries |

### Examples

```bash
# Add a remote without authentication
fluree remote add origin http://localhost:8090

# Add a remote with inline token
fluree remote add prod https://api.example.com --token eyJ...

# Add a remote with token from file
fluree remote add staging https://staging.example.com --token @~/.fluree/staging-token
```

---

## fluree remote remove

Remove a remote configuration.

### Usage

```bash
fluree remote remove <NAME>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Remote name to remove |

### Examples

```bash
fluree remote remove origin
```

---

## fluree remote list

List all configured remotes.

### Usage

```bash
fluree remote list
```

### Output

```
┌─────────┬─────────────────────────────┬───────┐
│ Name    │ URL                         │ Auth  │
├─────────┼─────────────────────────────┼───────┤
│ origin  │ http://localhost:8090       │ none  │
│ prod    │ https://api.example.com     │ token │
└─────────┴─────────────────────────────┴───────┘
```

---

## fluree remote show

Show detailed information about a remote.

### Usage

```bash
fluree remote show <NAME>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Remote name |

### Output

```
Remote:
  Name: origin
  Type: HTTP
  URL:  http://localhost:8090
  Auth: token configured
```

### fluree remote ledgers

List the ledgers your token can access on a remote, with the serving tiers
each offers (`query` = the remote executes queries; `blocks` = raw index
content is served for peer-mode local execution). Requires a bearer token
with storage scope; the listing is the remote's auth-filtered catalog.

```bash
fluree remote ledgers [NAME]   # NAME defaults to the only configured remote
```

```
LEDGER            COMMIT T   INDEX T   SERVING
inventory:main    42         40        query+blocks
orders:main       17         17        query
```

Track one for peer-mode local querying with
`fluree track add <ledger> --remote <name> --mode peer` (see [track](track.md)).

## fluree remote erasure-authority

Whether `clone` and `pull` accept the permanent erasures a remote's history carries.

```bash
fluree remote erasure-authority <NAME> <true|false>
```

An erasure received from a designated remote removes values from history this storage already holds,
which nothing undoes. Designate only a remote trusted to authorize erasures: its proofs show a value
was removed correctly, not who was allowed to remove it. Off by default; a clone or pull that brings
erasures from an undesignated remote is refused.

## See Also

- [upstream](upstream.md) - Configure upstream tracking
- [clone](clone.md) - Clone a ledger from a remote
- [fetch](fetch.md) - Fetch refs from a remote
- [token](token.md) - Create authentication tokens
