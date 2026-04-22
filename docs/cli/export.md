# fluree export

Export ledger data as Turtle, N-Triples, N-Quads, TriG, or JSON-LD.

## Usage

```bash
fluree export [LEDGER] [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Options

| Option | Description |
|--------|-------------|
| `--format <FORMAT>` | Output format: `turtle` (or `ttl`), `ntriples` (or `nt`), `jsonld`, `trig`, or `nquads` (default: `turtle`) |
| `--all-graphs` | Export default + all named graphs including system graphs (dataset export). Requires `--format trig` or `--format nquads`. |
| `--graph <IRI>` | Export a specific named graph by IRI. Mutually exclusive with `--all-graphs`. |
| `--context <JSON>` | JSON-LD context for prefix declarations. Overrides the ledger's default context. |
| `--context-file <FILE>` | Read context from a JSON file. Overrides the ledger's default context. |
| `--at <TIME>` | Export data as of a specific point in time. Accepts a transaction number (`5`), ISO-8601 datetime (`2024-01-15T10:30:00Z`), or commit CID prefix (`abc123def456`). If omitted, exports at the latest committed time (including data committed but not yet persisted to index). |

## Formats

### turtle / jsonld (data snapshot)

Exports a point-in-time snapshot of all triples in the ledger. Output goes to stdout.

### ledger (native pack)

Exports the full native ledger — all commits, transaction blobs, indexes, and dictionaries — as a `.flpack` file. This format preserves the complete history and can be imported into a new Fluree instance via `fluree create <name> --from <file>.flpack`.

The `.flpack` format uses the `fluree-pack-v1` binary wire protocol (the same format used by `fluree clone` and `fluree pull` for network transfers).

All formats (Turtle, N-Triples, N-Quads, TriG, JSON-LD) read directly from the binary SPOT index with a novelty overlay, so export always includes the latest committed transactions — even those not yet persisted to index. Memory usage stays constant regardless of dataset size. JSON-LD streams one subject at a time, so memory is O(largest subject), not O(dataset).

### Prefixes / Context

Turtle, TriG, and JSON-LD output use prefix compaction to produce compact, readable output. The prefix map is resolved in this order:

1. `--context` or `--context-file` (explicit override)
2. The ledger's default context (set via `fluree context set`)
3. No prefixes (falls back to full IRIs)

The context format is a JSON object mapping prefixes to namespace IRIs:

```json
{"ex": "http://example.org/", "schema": "http://schema.org/"}
```

### Prerequisites

All export formats require a binary index. Ledgers that have only been created and inserted into (without an index build) cannot be exported. Run the server to trigger index building first.

## Examples

```bash
# Export as Turtle (default) — uses ledger's default context for prefixes
fluree export > backup.ttl

# Export as Turtle with custom prefixes
fluree export --context '{"ex": "http://example.org/"}' > backup.ttl

# Export as Turtle with prefixes from a file
fluree export --context-file prefixes.json > backup.ttl

# Export as N-Triples (no prefixes, one triple per line)
fluree export --format ntriples > backup.nt

# Export as JSON-LD
fluree export --format jsonld > backup.jsonld

# Export all graphs as TriG
fluree export --all-graphs --format trig > backup.trig

# Export all graphs as N-Quads
fluree export --all-graphs --format nquads > backup.nq

# Export a specific named graph
fluree export --graph "http://example.org/g1" --format turtle > g1.ttl

# Export data as of a specific transaction number
fluree export --at 5 > snapshot-at-t5.ttl

# Export data as of an ISO-8601 datetime
fluree export --at "2024-06-15T12:00:00Z" > snapshot.ttl

# Export data as of a specific commit
fluree export --at abc123def456 > at-commit.ttl

# Export specific ledger
fluree export production > prod-backup.ttl

# Pipe to other tools
fluree export | grep "example.org"
```

## Output

### Turtle (default)

```turtle
@prefix ex: <http://example.org/> .

ex:alice
    a ex:Person ;
    ex:name "Alice" .
ex:bob
    a ex:Person ;
    ex:name "Bob" .
```

### N-Triples

```nt
<http://example.org/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/alice> <http://example.org/name> "Alice" .
```

### TriG (all graphs)

```trig
@prefix ex: <http://example.org/> .

ex:alice
    ex:name "Alice" .

GRAPH ex:g1 {
ex:bob
    ex:name "Bob" .
}
```

### N-Quads (all graphs)

```nq
<http://example.org/alice> <http://example.org/name> "Alice" .
<http://example.org/bob> <http://example.org/name> "Bob" <http://example.org/g1> .
```

### JSON-LD

```json
{
  "@context": {
    "ex": "http://example.org/"
  },
  "@graph": [
    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"},
    {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob", "ex:age": {"@value": 25, "@type": "http://www.w3.org/2001/XMLSchema#long"}}
  ]
}
```

JSON-LD output uses prefix compaction from the context. Value encoding rules:

- Plain strings (`xsd:string`) → JSON string (no `@type`)
- Booleans → native JSON `true`/`false`
- Integers/longs → `{"@value": 42, "@type": "xsd:long"}` (explicit datatype)
- Decimals → `{"@value": "3.14", "@type": "xsd:decimal"}`
- Doubles → `{"@value": 3.14, "@type": "xsd:double"}`
- Language-tagged strings → `{"@value": "Bonjour", "@language": "fr"}`
- References → `{"@id": "ex:other"}`
- Single-cardinality properties are unwrapped (not in `[]`)
- Multi-cardinality properties use arrays

## API Usage

The export feature is available at the API level for upstream applications:

```rust
use fluree_db_api::export::ExportFormat;

// Turtle with default context
let stats = fluree.export("mydb")
    .format(ExportFormat::Turtle)
    .write_to(&mut writer)
    .await?;

// N-Quads with all graphs
let stats = fluree.export("mydb")
    .format(ExportFormat::NQuads)
    .all_graphs()
    .write_to(&mut writer)
    .await?;

// Turtle with custom prefixes
let stats = fluree.export("mydb")
    .format(ExportFormat::Turtle)
    .context(&json!({"ex": "http://example.org/"}))
    .write_to(&mut writer)
    .await?;

// JSON-LD with prefix compaction
let stats = fluree.export("mydb")
    .format(ExportFormat::JsonLd)
    .context(&json!({"ex": "http://example.org/"}))
    .write_to(&mut writer)
    .await?;

// Export a specific named graph
let stats = fluree.export("mydb")
    .format(ExportFormat::Turtle)
    .graph("http://example.org/g1")
    .write_to(&mut writer)
    .await?;

// Time-travel: export as of transaction t=5
let stats = fluree.export("mydb")
    .format(ExportFormat::Turtle)
    .as_of(TimeSpec::at_t(5))
    .write_to(&mut writer)
    .await?;

// Time-travel: export as of an ISO-8601 datetime
let stats = fluree.export("mydb")
    .format(ExportFormat::Turtle)
    .as_of(TimeSpec::at_time("2024-06-15T12:00:00Z"))
    .write_to(&mut writer)
    .await?;

// Convenience: write directly to stdout
let stats = fluree.export("mydb")
    .format(ExportFormat::Turtle)
    .to_stdout()
    .await?;
```

## See Also

- [context](context.md) - Manage default JSON-LD context (prefix map)
- [query](query.md) - Run custom queries
