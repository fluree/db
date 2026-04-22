# fluree context

Manage the default JSON-LD context for a ledger.

## Usage

```bash
fluree context <COMMAND>
```

## Subcommands

| Command | Description |
|---------|-------------|
| `get [LEDGER]` | Show the default JSON-LD context |
| `set [LEDGER]` | Replace the default JSON-LD context |

## Description

Each ledger can have a **default context** — a JSON object mapping prefixes to IRIs (e.g., `{"ex": "http://example.org/"}`). When a JSON-LD query or transaction is sent via the **Fluree server or CLI** and omits its own `@context`, the ledger's default context is injected automatically. When using `fluree-db-api` directly, this injection does not happen unless explicitly opted into.

Default context is populated automatically during bulk import (from Turtle `@prefix` declarations). This command allows reading or replacing it after the fact.

The context is stored in content-addressed storage (CAS) and referenced from the nameservice config. Updates use compare-and-set semantics, so concurrent writers are safely handled.

## context get

Show the current default context.

```bash
fluree context get [LEDGER]
```

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

### Examples

```bash
# Show context for active ledger
fluree context get

# Show context for a specific ledger
fluree context get mydb
```

Output (pretty-printed JSON):

```json
{
  "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
  "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
  "xsd": "http://www.w3.org/2001/XMLSchema#",
  "owl": "http://www.w3.org/2002/07/owl#",
  "ex": "http://example.org/"
}
```

If no default context has been set, a message is printed to stderr.

## context set

Replace the default context with a new JSON object.

```bash
fluree context set [LEDGER] [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

| Option | Description |
|--------|-------------|
| `-e, --expr <JSON>` | Inline JSON context |
| `-f, --file <PATH>` | Read context from a JSON file |

If neither `-e` nor `-f` is provided, context is read from stdin.

The body can be either a bare JSON object or wrapped in `{"@context": {...}}` — both forms are accepted.

### Examples

```bash
# Set inline
fluree context set mydb -e '{"ex": "http://example.org/", "foaf": "http://xmlns.com/foaf/0.1/"}'

# Set from file
fluree context set mydb -f context.json

# Pipe from stdin
cat context.json | fluree context set mydb

# Wrapped form also accepted
fluree context set mydb -e '{"@context": {"ex": "http://example.org/"}}'
```

## See Also

- [prefix](prefix.md) — Manage CLI-local prefix mappings (stored in project config, not the ledger)
- [export](export.md) — Export ledger data (the default context drives prefix output)
- [IRIs, namespaces, and JSON-LD @context](../concepts/iri-and-context.md) — Conceptual overview
