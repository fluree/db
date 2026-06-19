# fluree multi-query

Execute a multi-query envelope — bundle multiple JSON-LD and/or SPARQL
queries into a single request that runs them in parallel against one
shared snapshot moment.

## Usage

```bash
fluree multi-query [FILE] [OPTIONS]
```

## Arguments

| Arguments | Behavior |
|-----------|----------|
| (none) | Read envelope JSON from `-e`, `-f`, or stdin |
| `<file>` | Path to envelope JSON file |

## Options

| Option | Description |
|--------|-------------|
| `-e, --expr <JSON>` | Inline envelope JSON (alternative to positional file or `-f`) |
| `-f, --file <FILE>` | Read envelope from a file |
| `--format <FORMAT>` | Per-alias result format: `json` (default — per-language defaults) or `typed-json` (always-typed `@value` / `@type` literal shape). Matches the same flag on [`fluree query`](query.md). |
| `--normalize-arrays` | Always wrap multi-value JSON-LD properties in arrays. Matches the same flag on [`fluree query`](query.md); applies to JSON-LD aliases. |
| `--output <VIEW>` | Envelope display: `json` (compact, default), `pretty` (indented), or `aliases` (per-alias sections). Controls how the response envelope is printed to the terminal — does **not** affect alias result formatting (use `--format` for that). |
| `--remote <NAME>` | Execute against a named remote server |
| `--as <IDENTITY>` | Bearer identity to assume (subject to the impersonation gate — see [Policy Contract](server-integration.md#policy-enforcement-contract)) |
| `--policy-class <IRI>` | Policy class IRI(s); repeatable |
| `--policy <JSON>` | Inline policy JSON (`fluree-policy`) |
| `--policy-file <FILE>` | Read inline policy JSON from a file |
| `--policy-values <JSON>` | Variable bindings for parameterized policies |
| `--policy-values-file <FILE>` | Read policy-values JSON from a file |
| `--default-allow` | Permit access when no matching policy rules exist |

Policy flags ride on the underlying HTTP request as `fluree-policy-*`
headers; the server folds them into the envelope's top-level `opts`
before validation, and the standard envelope → sub-query opts merge carries them into every alias. **They take effect on JSON-LD sub-queries** via the same code path single-query `/query` uses. **For SPARQL sub-queries** the headers are accepted and bearer ledger-scope still applies, but identity / policy threading via `QueryConnectionOptions` is not consumed — the same gap that exists for connection-scoped SPARQL on `/query`. See [Limitations](#limitations) for the canonical list.

## Description

Bundles N independent queries against a shared per-ledger snapshot.
Each sub-query carries its own `from` and its own language (JSON-LD or
SPARQL); the server runs them in parallel under bounded concurrency
and returns a per-alias response map plus an aggregate `status`.

See [Multi-query envelope](../api/multi-query.md) for the full envelope wire format, response shape, snapshot semantics, merge rules, bounds, and current limitations.

## Transport

`fluree multi-query` runs the envelope through whichever of three transports applies:

1. **`--remote <name>`** — explicit; routes through the named remote from `remotes.toml`. OIDC token refresh is persisted back to `config.toml` after the round-trip.
2. **Auto-route to a locally running `fluree server`** — used when `--remote` is omitted and `server.meta.json` reports a live pid. Suppressed by `--direct`.
3. **In-process local** — `fluree multi-query` calls `Fluree::multi_query()` directly against the storage tree configured for this `.fluree/` directory. Used when neither `--remote` nor a running local server is available, or when `--direct` is set. This is the natural counterpart to `fluree query` running locally.

In-process mode reads the same storage path, indexing thresholds, and prefix table as every other local CLI command. No HTTP, no server, no auth — the caller already has direct access to the storage tree.

## Examples

### Inline envelope, locally running server

```bash
fluree server start &
fluree multi-query -e '{
  "queries": {
    "people": {
      "language": "jsonld",
      "query": {
        "@context": {"ex": "http://example.org/"},
        "from": "mydb",
        "select": ["?name"],
        "where": {"@id": "?p", "ex:name": "?name"}
      }
    },
    "orders": {
      "language": "sparql",
      "query": "PREFIX ex: <http://example.org/> SELECT ?id FROM <mydb> WHERE { ?o ex:orderId ?id }"
    }
  }
}'
```

### Envelope file, named remote

```bash
fluree multi-query envelope.json --remote origin
```

### Stdin, pretty-printed response

```bash
cat envelope.json | fluree multi-query --remote origin --output pretty
```

### Typed JSON alias results + pretty envelope view

Pick the per-alias result shape with `--format` and the envelope display
with `--output`. They're independent: `--format` controls what the
server formats into each `results` entry, `--output` controls how the
CLI prints the whole response on your terminal.

```bash
fluree multi-query envelope.json --format typed-json --normalize-arrays --output pretty
```

### Per-alias section view

The `--output aliases` view prints a section per alias, with successful
results and per-alias errors clearly separated. Useful for shell-piping
results when you only want to inspect one alias and the response is
large.

```bash
fluree multi-query envelope.json --remote origin --output aliases
```

Output (abbreviated):

```
status: ok
asOf:   2024-01-01T12:00:00.123Z
  mydb @ t:42

# people (ok)
[
  { "name": "Alice" },
  ...
]

# orders (ok)
{ "head": ..., "results": ... }
```

## Per-alias result format (`--format`)

| Value | JSON-LD aliases | SPARQL aliases |
|-------|-----------------|----------------|
| `json` (default) | JSON-LD shape | SPARQL Results JSON |
| `typed-json` | typed `@value`/`@type` shape | typed `@value`/`@type` shape (cross-language) |

`--normalize-arrays` wraps single-valued JSON-LD properties in arrays.
On JSON-LD aliases it composes with whichever `--format` is in effect.
On SPARQL aliases it's a no-op — SPARQL Results JSON already has its own
binding shape and isn't affected. This matches the same flags on
[`fluree query`](query.md).

`--format typed-json` is cross-language by design — it gives every alias
a unified typed shape. `--format json` (with or without
`--normalize-arrays`) keeps SPARQL aliases on their SPARQL Results JSON
default, so you can `--normalize-arrays` a mixed envelope without
silently changing the SPARQL alias's wire shape.

Format selection rides on the wire as `Fluree-Output-Format` /
`Fluree-Normalize-Arrays` headers when using `--remote` or auto-routing
to a local server; in-process mode wires them straight into the api
crate's `MultiQueryBuilder::format(...)`.

Only JSON-producing shapes are valid inside a multi-query envelope —
TSV / CSV / SPARQL XML / RDF XML are rejected upstream because the
envelope's `results` map can't embed byte/string payloads. Use single
queries against `fluree query` when you need those.

## Envelope display (`--output`)

| Value | Shape | Use case |
|-------|-------|----------|
| `json` (default) | Compact JSON, one line | Machine processing; piping into `jq` |
| `pretty` | Indented JSON | Human reading |
| `aliases` | Per-alias section header + indented result block | Quick visual inspection; per-alias debugging |

The response shape itself is documented in
[Multi-query envelope → Response shape](../api/multi-query.md#response-shape).

## Status mapping

`fluree multi-query` exits **0** when the server returned HTTP 200,
regardless of the body's `status` field. Clients that need to branch
on aggregate outcome (`ok` / `partial` / `all_failed`) should inspect
the response JSON. A non-zero exit code indicates an envelope-level
failure (validation 4xx, snapshot resolution 5xx, transport error, or
envelope JSON malformed).

## Limitations

The full list lives in the [envelope reference](../api/multi-query.md#limitations). Highlights:

- History queries (`to` field / `FROM <…> TO <…>`) are rejected with 400.
- Envelope-level `opts.max-fuel` is rejected with 400 (per-sub-query `opts.max-fuel` still works).
- Response size cap is enforced at assembly, not throughout dispatch; peak memory during dispatch can exceed the envelope cap.
- `opts.t` at any level inside the envelope is rejected — use `from` or envelope `asOf`.
- SPARQL sub-queries do not consume merged policy opts — same gap as single-query connection-scoped SPARQL.

## See also

- [Multi-query envelope (HTTP reference)](../api/multi-query.md)
- [fluree query](query.md) — single-query CLI
- [Server integration: `fluree multi-query`](server-integration.md#fluree-multi-query)
