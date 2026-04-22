# fluree config

Manage configuration settings.

## Usage

```bash
fluree config <COMMAND>
```

## Subcommands

| Command | Description |
|---------|-------------|
| `get <KEY>` | Get a configuration value |
| `set <KEY> <VALUE>` | Set a configuration value |
| `list` | List all configuration values |
| `set-origins <LEDGER> --file <PATH>` | Set CID fetch origins for a ledger (writes a `LedgerConfig` to CAS and updates `config_id`) |

## Description

Manages configuration stored in `.fluree/config.toml`. Configuration uses dotted keys for nested values (e.g., `storage.path`).

## Examples

### Get a value

```bash
fluree config get storage.path
```

Output:
```
/custom/storage/path
```

### Set a value

```bash
fluree config set storage.path /custom/storage/path
```

Output:
```
Set 'storage.path' = "/custom/storage/path"
```

### List all values

```bash
fluree config list
```

Output:
```
storage.path = "/custom/storage/path"
storage.encryption = "aes256"
```

If no configuration is set:
```
(no configuration set)
```

## Configuration File

Configuration is stored in `.fluree/config.toml`:

```toml
[storage]
path = "/custom/storage/path"
encryption = "aes256"
```

## Errors

Getting a key that doesn't exist:
```
error: configuration key 'nonexistent' is not set
```

## See Also

- [init](init.md) - Initialize project directory
- [prefix](prefix.md) - Manage IRI prefixes

---

## fluree config set-origins

Store a `LedgerConfig` blob in local CAS and update the ledger's nameservice record to point to it via `config_id`.

This enables **origin-based** `fluree pull` (when no upstream remote is configured) and improves `fluree clone --origin` by allowing the remote to advertise multiple fallback origins.

### Usage

```bash
fluree config set-origins <LEDGER> --file <PATH>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<LEDGER>` | Ledger ID (e.g., `mydb` or `mydb:main`) |

### Options

| Option | Description |
|--------|-------------|
| `--file <PATH>` | Path to a JSON file containing a `LedgerConfig` |

### LedgerConfig File Format

The file is canonical JSON using compact `f:` keys (not JSON-LD):

```json
{
  "f:origins": [
    { "f:priority": 10, "f:enabled": true, "f:transport": "http://localhost:8090", "f:auth": { "f:mode": "none" } }
  ],
  "f:replication": { "f:preferPack": true, "f:maxPackMiB": 64 }
}
```

Notes:

- `f:transport` is an origin base URL. The CLI normalizes it the same way as remotes: it will append `/fluree` if missing and will use `GET /.well-known/fluree.json` discovery when available.
- Auth requirements are declarative. Credentials are not stored in the `LedgerConfig`.

### Current Limitations

- `fluree pull` via origins currently does not attach a Bearer token from any credential store, so only origins with `f:auth.f:mode = "none"` are usable for pull today.
- `fluree clone --origin ... --token ...` can use a Bearer token for origin fetch.
