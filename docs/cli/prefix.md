# fluree prefix

Manage IRI prefix mappings.

## Usage

```bash
fluree prefix <COMMAND>
```

## Subcommands

| Command | Description |
|---------|-------------|
| `add <PREFIX> <IRI>` | Add a prefix mapping |
| `remove <PREFIX>` | Remove a prefix mapping |
| `list` | List all prefix mappings |

## Description

Manages IRI prefix mappings stored in `.fluree/prefixes.json`. These prefixes are used to expand compact IRIs in commands like `history`.

## Examples

### Add a prefix

```bash
fluree prefix add ex http://example.org/
fluree prefix add foaf http://xmlns.com/foaf/0.1/
fluree prefix add schema https://schema.org/
```

Output:
```
Added prefix: ex = <http://example.org/>
```

### List prefixes

```bash
fluree prefix list
```

Output:
```
ex: <http://example.org/>
foaf: <http://xmlns.com/foaf/0.1/>
schema: <https://schema.org/>
```

If no prefixes are defined:
```
(no prefixes defined)

Add prefixes with: fluree prefix add <prefix> <iri>
Example: fluree prefix add ex http://example.org/
```

### Remove a prefix

```bash
fluree prefix remove foaf
```

Output:
```
Removed prefix: foaf
```

## Usage with History

Once prefixes are defined, you can use compact IRIs:

```bash
# Instead of:
fluree history http://example.org/alice

# Use:
fluree history ex:alice
```

## IRI Best Practices

IRI namespaces should end with `/` or `#`:

```bash
# Good
fluree prefix add ex http://example.org/
fluree prefix add foaf http://xmlns.com/foaf/0.1/

# Warning (will still work but may cause issues)
fluree prefix add bad http://example.org
```

## Storage

Prefixes are stored in `.fluree/prefixes.json`:

```json
{
  "ex": "http://example.org/",
  "foaf": "http://xmlns.com/foaf/0.1/"
}
```

## See Also

- [history](history.md) - Uses prefix expansion
- [config](config.md) - Manage other configuration
