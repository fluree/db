# fluree remote

Manage remote servers for syncing ledgers.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `add` | Add a remote server |
| `remove` | Remove a remote |
| `list` | List all configured remotes |
| `show` | Show details for a remote |

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

## See Also

- [upstream](upstream.md) - Configure upstream tracking
- [clone](clone.md) - Clone a ledger from a remote
- [fetch](fetch.md) - Fetch refs from a remote
- [token](token.md) - Create authentication tokens
