# Streaming query (NDJSON)

> **Endpoint:** `POST /v1/fluree/stream/query/<ledger...>`
> **Content type (response):** `application/x-ndjson`
> **Status:** v1 — SELECT only; some shapes explicitly rejected (see [Unsupported shapes](#unsupported-shapes)).

Stream SELECT results incrementally as newline-delimited JSON ("NDJSON")
instead of buffering the entire result set into a single JSON response body.
Each line is one self-describing record. A wall-clock **heartbeat** keeps the
connection alive past proxy idle timeouts (e.g. CloudFront/ALB ~60s) during
long-running queries.

This is a separate endpoint from [`/query`](endpoints.md#post-query); the
standard buffered query path is unchanged.

## When to use it

- **Large result sets** — start consuming rows before the whole result is
  built, and avoid holding the entire serialized response in memory.
- **Long-running queries behind a proxy** — heartbeats keep the connection from
  being killed by an idle timeout while the query is still executing.
- **Pipelined clients** — process each row as it arrives (ETL, export, UI
  incremental render).

## When *not* to use it

- You need a single JSON document, ASK/CONSTRUCT/DESCRIBE, `selectOne`, JSON-LD
  hydration, or history (`to`) queries — use [`/query`](endpoints.md#post-query).
- Blocking operators dominate the query. `ORDER BY` / `GROUP BY` / aggregates
  buffer internally and emit in a burst at the end, so streaming yields no rows
  until they finish (heartbeats still flow — see [Heartbeats](#heartbeats)).

## Request

The ledger is taken from the greedy path tail (`/stream/query/<ledger...>`),
exactly like [`/query/<ledger...>`](endpoints.md#post-query). The query body is
content-type-negotiated, same as `/query`:

- `Content-Type: application/json` — a JSON-LD query document.
- `Content-Type: application/sparql-query` — a raw SPARQL `SELECT` string.

```bash
curl -N -X POST http://localhost:8090/v1/fluree/stream/query/my/ledger \
  -H 'Content-Type: application/json' \
  -d '{
        "@context": { "ex": "http://example.org/" },
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
      }'
```

> `curl -N` disables output buffering so records print as they arrive.

## Response: the NDJSON record protocol

The response is a stream of newline-terminated JSON objects, one per line. The
`type` field discriminates the record kind.

| `type` | Shape | Meaning |
|--------|-------|---------|
| `head` | `{"type":"head","vars":["s","name"]}` | First record. The ordered output column names. Emitted before the first row is pulled, so the client learns the schema immediately. |
| `row` | `{"type":"row","row":{ ... }}` | One result row. The `row` body is a [SPARQL-Results-JSON binding object](https://www.w3.org/TR/sparql11-results-json/) (`{"name":{"type":"literal","value":"Alice"}}`) — byte-identical to the `bindings` entries `/query` would return. |
| `heartbeat` | `{"type":"heartbeat","t_ms":14982,"fuel":84.213}` | Keep-alive emitted during stalls. `fuel` is present only when fuel tracking is active. |
| `end` | `{"type":"end","rows":2,"t":42,"fuel":1.01,"time":"3.4ms"}` | **Success terminator.** Final row count plus `t`/`fuel`/`time` when tracked. |
| `error` | `{"type":"error","error":{"code":"timeout","message":"..."},"rows":1}` | **Failure terminator.** Carries a machine-readable `code` (see below), a human `message`, and rows emitted before the failure. Emitted *instead of* `end`. |

Example stream:

```
{"type":"head","vars":["name"]}
{"type":"row","row":{"name":{"type":"literal","value":"Alice"}}}
{"type":"row","row":{"name":{"type":"literal","value":"Bob"}}}
{"type":"end","rows":2,"t":42,"fuel":1.02,"time":"2.1ms"}
```

### Terminal record requirement (read this)

Every successful stream ends with exactly one `end` record; every failed stream
ends with exactly one `error` record. **A client MUST treat the absence of a
terminal record as a failure** (a truncated/dropped stream), not as success.

This matters because the HTTP status is committed to `200 OK` as soon as the
first byte is flushed. If the connection drops mid-stream (proxy timeout,
network failure, server crash), the bytes received so far are
indistinguishable from a complete result *unless* you require the explicit
terminator. Do not assume "connection closed cleanly" means "all rows
received" — require `end`.

### Error codes

The `error` record's `error.code` is a stable, machine-readable string so
clients can branch on the failure kind without parsing the message:

| `code` | Meaning |
|--------|---------|
| `timeout` | The server query timeout fired (the query ran too long). |
| `fuel_exhausted` | The query exceeded its `max-fuel` budget. |
| `cancelled` | The query was cancelled (e.g. the client disconnected). |
| `invalid_query` | The query was rejected at plan/validation time. |
| `resource_limit` | A non-fuel resource limit was hit. |
| `internal` | An unexpected server-side error. |

A `code` you don't recognize should be treated as a generic failure. Note that
because the `200 OK` is committed before execution, even a request that fails
the fuel floor immediately is reported as a single `error` terminal on a `200`
stream — not as a `4xx`. (`4xx` is reserved for failures detected *before* the
stream starts: parse errors and [unsupported shapes](#unsupported-shapes).)

### Heartbeats

When no record has flowed for ~15s, the server emits a `heartbeat` record. This
is driven by a wall-clock timer in the transport layer, independent of query
execution, so it fires even while a blocking operator (a large `ORDER BY` /
`GROUP BY` drain) is producing no rows. When the query was submitted with fuel
tracking, the heartbeat carries the live running `fuel` total, which climbs as
scans charge — a useful "still making progress" signal during a long stall.

Clients should ignore unknown record types (forward compatibility) and simply
skip `heartbeat` records.

## Unsupported shapes

The streaming endpoint covers SELECT result rows only. The following are
rejected with a `4xx` **before** the stream starts (so you get a normal JSON
error, not a `200` stream), and should use [`/query`](endpoints.md#post-query):

- **ASK** — boolean result, nothing to stream.
- **CONSTRUCT / DESCRIBE** — produce an RDF graph, not solution rows.
- **`selectOne`** — single-object JSON-LD shape.
- **JSON-LD hydration** — needs async per-row database expansion during
  formatting.
- **History (`to`)** — a top-level JSON-LD `to` uses a distinct history
  execution path.
- **SPARQL `FROM` / `FROM NAMED`** and **SPARQL policy** — see below.

## Auth, policy, and dataset behavior

The streaming endpoint enforces policy identically to `/query`, by routing to
the same execution path.

- **JSON-LD, no policy / single ledger** — runs the lean single-ledger path.
- **JSON-LD with `from`/`fromNamed`, multi-ledger, or any policy input**
  (request `opts.identity` / `opts.policy-class`, a server
  `default_policy_class`, or `Fluree-Policy*` / `Fluree-Identity` headers) —
  **upgrades to the connection/dataset path**, which builds a policy-wrapped
  dataset and enforces per-graph policy exactly as `/query` does. A restricted
  identity streams strictly fewer rows than an unrestricted one.
- **Bearer scope** — a token must be authorized for the path ledger and every
  ledger referenced via `from`/`fromNamed`; out-of-scope requests return `404`
  (no existence leak), same as `/query`.
- **`fluree-min-t`** freshness barriers and the stored default-context
  injection are applied before planning, matching `/query`.

### SPARQL + policy is not yet supported

A **SPARQL** request that carries any policy-scoping signal — a server
`default_policy_class`, an authenticated identity, or any
`Fluree-Identity` / `Fluree-Policy` / `Fluree-Policy-Class` /
`Fluree-Policy-Values` / `Fluree-Default-Allow` header — is **refused** with a
`4xx` rather than run unpoliced. SPARQL `FROM` / `FROM NAMED` is likewise
rejected. Use [`/query`](endpoints.md#post-query) for policy-scoped or
multi-ledger SPARQL. (JSON-LD policy streaming is fully supported, as above.)

## Fuel and tracking

The endpoint tracks fuel and time by default (honoring `max-fuel` from JSON-LD
`opts`). The running fuel total rides on `heartbeat` records and the final
total on the `end` record. A `max-fuel` overrun surfaces as an `error`
terminal.

## Compression

NDJSON streaming relies on records being flushed promptly. Do **not** place a
buffering response-compression layer in front of `/stream/*`; gzip middleware
that coalesces small writes defeats both incremental delivery and the
heartbeat. The server sends `Cache-Control: no-transform` to discourage
intermediaries from re-encoding the body.

## Consuming the stream

### JavaScript / TypeScript

```ts
const res = await fetch(`/v1/fluree/stream/query/${ledger}`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(query),
});

const reader = res.body!.getReader();
const decoder = new TextDecoder();
let buf = "";
let sawTerminal = false;

while (true) {
  const { value, done } = await reader.read();
  if (done) break;
  buf += decoder.decode(value, { stream: true });
  let nl: number;
  while ((nl = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, nl).trim();
    buf = buf.slice(nl + 1);
    if (!line) continue;
    const rec = JSON.parse(line);
    switch (rec.type) {
      case "head": /* rec.vars */ break;
      case "row": handleRow(rec.row); break;
      case "heartbeat": /* progress: rec.fuel */ break;
      case "end": sawTerminal = true; break;
      case "error": sawTerminal = true; throw new Error(rec.error.message);
    }
  }
}
// REQUIRED: a stream that ends without `end`/`error` was truncated.
if (!sawTerminal) throw new Error("stream truncated before terminal record");
```

### Rust (reqwest)

```rust
let resp = client
    .post(format!("{base}/v1/fluree/stream/query/{ledger}"))
    .json(&query)
    .send()
    .await?;

let mut lines = tokio::io::BufReader::new(
    tokio_util::io::StreamReader::new(
        resp.bytes_stream().map_err(std::io::Error::other),
    ),
)
.lines();

let mut saw_terminal = false;
while let Some(line) = lines.next_line().await? {
    if line.is_empty() { continue; }
    let rec: serde_json::Value = serde_json::from_str(&line)?;
    match rec["type"].as_str() {
        Some("row") => handle_row(&rec["row"]),
        Some("end") => { saw_terminal = true; }
        Some("error") => { saw_terminal = true; anyhow::bail!("{}", rec["error"]["message"]); }
        _ => {} // head, heartbeat, unknown
    }
}
anyhow::ensure!(saw_terminal, "stream truncated before terminal record");
```

## Relationship to `/query`

`/stream/query/<ledger>` is purpose-built for incremental delivery; it does not
replace [`/query`](endpoints.md#post-query). The standard endpoint remains the
path for ASK/CONSTRUCT/DESCRIBE, hydration, history, SPARQL policy/`FROM`, and
any client that wants a single buffered JSON (or TSV/CSV/XML) document.
