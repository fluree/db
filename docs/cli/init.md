# fluree init

Initialize a new Fluree project directory.

## Usage

```bash
fluree init [OPTIONS]
```

## Options

| Option     | Description                                                                       |
| ---------- | --------------------------------------------------------------------------------- |
| `--global` | Create global config and data directories instead of a local `.fluree/` directory |

## Description

Creates a `.fluree/` directory in the current working directory (or global directories with `--global`). This directory stores:

- `active` - The currently active ledger name
- `config.toml` - Configuration settings
- `prefixes.json` - IRI prefix mappings for compact IRIs
- `storage/` - Ledger data

Running `init` is idempotent - it won't overwrite existing configuration.

## Examples

```bash
# Initialize in current directory
fluree init

# Initialize global config
fluree init --global
```

## Global Directory

With `--global`, the directories are determined by:

1. `$FLUREE_HOME` environment variable (if set) — both config and data go in this single directory.
2. Platform directories (when `$FLUREE_HOME` is not set):

| Content                                      | Linux                                                      | macOS                                  | Windows                 |
| -------------------------------------------- | ---------------------------------------------------------- | -------------------------------------- | ----------------------- |
| Config (`config.toml`)                       | `$XDG_CONFIG_HOME/fluree` (default: `~/.config/fluree`)    | `~/Library/Application Support/fluree` | `%LOCALAPPDATA%\fluree` |
| Data (`storage/`, `active`, `prefixes.json`) | `$XDG_DATA_HOME/fluree` (default: `~/.local/share/fluree`) | `~/Library/Application Support/fluree` | `%LOCALAPPDATA%\fluree` |

On macOS and Windows both resolve to the same directory (unified); on Linux config and data are separated per XDG conventions.

The generated `config.toml` will contain an absolute `storage_path` pointing to the data directory when the directories are split.

## See Also

- [create](create.md) - Create a new ledger after initialization
