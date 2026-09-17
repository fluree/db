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
| `--format <FORMAT>` | Output format: `turtle` (or `ttl`), `ntriples` (or `nt`), `jsonld`, `trig`, `nquads`, or `ledger` (`.flpack` archive). Defaults to `turtle`, or to `ledger` when `-o` names a `.flpack` file. |
| `--all-graphs` | Export the default graph plus every named graph (dataset export). Requires `--format trig` or `--format nquads`. The ledger's system graphs are excluded — see `--system-graphs`. |
| `--system-graphs` | Also emit the ledger's system graphs (`#txn-meta`, `#config`) under `--all-graphs`. Diagnostic only. |
| `--graph <IRI>` | Export a specific named graph by IRI. Mutually exclusive with `--all-graphs`. |
| `--raw-reifies` | Emit edge annotations as raw `f:reifies*` system triples instead of RDF 1.2 annotation syntax (pre-4.2 output). |
| `--context <JSON>` | JSON-LD context for prefix declarations. Overrides the ledger's default context. |
| `--context-file <FILE>` | Read context from a JSON file. Overrides the ledger's default context. |
| `--at <TIME>` | Export data as of a specific point in time. `t:<N>` (transaction number), `t:latest` or `latest`, `iso:<ISO-8601>` (commit event time), `recorded:<ISO-8601>` (the wall-clock time the commit was recorded), or `commit:<hex-prefix>` (min 6 chars). A bare transaction number, ISO-8601 timestamp or commit prefix also works; a bare integer is read as a transaction number, so use `commit:<prefix>` to force an all-digit prefix. If omitted, exports at the latest committed time (including data committed but not yet persisted to index). |

## Formats

### turtle / jsonld (data snapshot)

Exports a point-in-time snapshot of all triples in the ledger. Output goes to stdout.

### ledger (native pack)

Exports the full native ledger — all commits, transaction blobs, indexes, and dictionaries — as a `.flpack` file. This format preserves the complete history and can be imported into a new Fluree instance via `fluree create <name> --from <file>.flpack`.

The `.flpack` format uses the `fluree-pack-v1` binary wire protocol (the same format used by `fluree clone` and `fluree pull` for network transfers).

All formats (Turtle, N-Triples, N-Quads, TriG, JSON-LD) read directly from the binary SPOT index with a novelty overlay, so export always includes the latest committed transactions — even those not yet persisted to index. Memory usage stays constant regardless of dataset size. JSON-LD streams one subject at a time, so memory is O(largest subject), not O(dataset).

### Named graphs

`--all-graphs` is opt-in even for `trig` and `nquads`. Without it, a dataset-format export carries only the default graph — so every export prints a summary to **stderr**, and names the flag when it left graphs behind:

```
$ fluree export mydb --format trig > mydb.trig
✓ Exported 'mydb' (1 triples, 1 graphs)
  warning: 1 named graph not exported; pass --all-graphs to include it
```

stdout carries only the RDF, so redirecting it still produces a clean file.

### System graphs

Every ledger has two system graphs, `urn:fluree:<ledger>:main#txn-meta` (commit metadata) and `…#config`. `--all-graphs` does not export them, because a file that contains them is not portable: their IRIs name the ledger that produced them, so re-importing into a ledger of the same name routes those triples onto the target's own reserved graph ids where they are unreachable, and importing into a differently-named ledger lands a foreign ledger's commit history in an ordinary user graph.

`--system-graphs` emits them anyway, for diagnostics. Use `--format ledger` to move a ledger — it carries commits rather than re-serializing triples, and round-trips losslessly.

### Edge annotations (RDF 1.2)

An edge annotation attaches a reifier to one specific triple: `ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.8 |}`. Fluree stores that as seven reserved `f:reifies*` system facts plus the reifier's own properties. Export used to emit those system facts verbatim — output no other RDF 1.2 tool understands, and which Fluree's own insert and update surfaces reject as system-controlled predicates.

Export now emits annotation syntax by default:

| Format | Output |
|---|---|
| `turtle`, `trig` | `ex:alice ex:knows ex:bob ~ ex:claim1 ~ ex:claim2 .` |
| `ntriples`, `nquads` | `<ex:claim1> <rdf:reifies> <<( <ex:alice> <ex:knows> <ex:bob> )>> .` |
| `jsonld` | `{"@id": "ex:bob", "@annotation": {"@id": "ex:claim1"}}` |

The reifier's own properties are emitted where the scan reaches them, as an ordinary subject later in the stream — not inlined in a `{| … |}` body. Both spellings mean the same thing in RDF 1.2 and both re-import identically; keeping the body out of line is what lets export stay one streaming pass over the index.

```turtle
ex:alice
    ex:knows ex:bob ~ ex:claim1 .
ex:claim1
    ex:confidence "0.8"^^xsd:decimal .
```

`--raw-reifies` restores the pre-4.2 output. That output only re-imports through `fluree create --from`; the insert and update surfaces reject hand-written `f:reifies*` triples.

A ledger that has never carried an annotation pays nothing for any of this: export reads one flag on the snapshot and runs the scan it always ran.

**Known limit.** Annotations written *inside a named graph* are resolved correctly when export reads them from the sealed annotation arena or from the novelty overlay. They are dropped when export falls back to the **base-index scan**, which is blind to them — the fallback taken by a ledger whose index reports annotations but for which no arena was sealed. SPARQL reads them in every case. Export says so rather than dropping them quietly:

```
  warning: 1 edge annotations could not be resolved and are NOT in the output;
           re-run with --raw-reifies to emit them as f:reifies* triples
```

`FLUREE_EXPORT_ANNOTATION_SCAN=1` forces the base-index scan in place of the sealed annotation arena — how to compare the two sources without rebuilding an index. They agree except on annotations inside a named graph, which the arena resolves and the scan cannot see; forcing the scan on such a ledger produces the warning above.

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

None. Export reads the binary index when there is one and the novelty overlay for anything committed since — so a ledger that has been created and inserted into but never indexed exports the same triples, with no index build and no growth in on-disk footprint.

Export is a read: it never writes to the ledger. If you want an index, build one explicitly with `fluree index <ledger>`.

### Choosing `--format`

`--format` defaults to `turtle`, with one inference: when `-o` names a file ending in `.flpack`, the format defaults to `ledger`, because that extension is what `fluree create --from` reads. Passing an RDF `--format` *and* a `.flpack` output name is refused rather than guessed — writing Turtle into a file named `.flpack` is what `fluree export mydb -o mydb.flpack` used to do silently.

## Examples

```bash
# Export as Turtle (default) — uses ledger's default context for prefixes
fluree export > backup.ttl

# Export a full ledger archive — `.flpack` implies --format ledger
fluree export mydb -o mydb.flpack

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

The default graph is written as top-level triples; each named graph gets a `GRAPH` block. System graphs are not included.

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
// stats.triples_written, stats.graphs_written, stats.rows_skipped,
// stats.named_graphs_omitted — the last is non-zero when the ledger has
// named graphs this export did not cover.

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
