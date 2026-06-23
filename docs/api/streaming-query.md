# Streaming query (NDJSON)

> **Endpoints:**
> - `POST /v1/fluree/stream/query/<ledger...>` — ledger-scoped (ledger in path)
> - `POST /v1/fluree/stream/query` — connection-scoped (ledger from `from`/`FROM`)
>
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

Two forms, mirroring `/query`:

- **Ledger-scoped** — `POST /stream/query/<ledger...>`. The ledger is the greedy
  path tail, exactly like [`/query/<ledger...>`](endpoints.md#post-query).
- **Connection-scoped** — `POST /stream/query` (no path ledger). The ledger(s)
  come entirely from the request: JSON-LD `from`/`fromNamed` (or the
  `Fluree-Ledger` header), or SPARQL `FROM`/`FROM NAMED`. This is always the
  connection/dataset path; a request with no ledger spec is rejected `4xx`.

Either form is content-type-negotiated:

- `Content-Type: application/json` — a JSON-LD query document.
- `Content-Type: application/sparql-query` — a raw SPARQL `SELECT` string.

```bash
# Ledger-scoped
curl -N -X POST http://localhost:8090/v1/fluree/stream/query/my/ledger \
  -H 'Content-Type: application/json' \
  -d '{"@context":{"ex":"http://example.org/"},
       "select":["?name"],"where":{"@id":"?s","ex:name":"?name"}}'

# Connection-scoped (ledgers from `from`)
curl -N -X POST http://localhost:8090/v1/fluree/stream/query \
  -H 'Content-Type: application/json' \
  -d '{"@context":{"ex":"http://example.org/"},"from":["a:main","b:main"],
       "select":["?name"],"where":{"@id":"?s","ex:name":"?name"}}'
```

> `curl -N` disables output buffering so records print as they arrive.

> **Note:** connection-scoped **SPARQL** does not apply per-request identity
> policy (it has no single ledger to resolve against), so a `/stream/query`
> SPARQL request carrying policy signals is rejected — use the ledger-scoped
> `/stream/query/<ledger>` (which enforces SPARQL policy) or `/query`.
> Connection-scoped **JSON-LD** policy is enforced normally.

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

When no record has flowed for the heartbeat interval (default 15s, configurable
via `FLUREE_STREAM_HEARTBEAT_MS` / `--stream-heartbeat-ms`; `0` disables), the
server emits a `heartbeat` record. This is driven by a wall-clock timer in the
transport layer, independent of query execution, so it fires even while a
blocking operator (a large `ORDER BY` / `GROUP BY` drain) is producing no rows.
Set the interval below your fronting proxy's idle timeout. When the query was submitted with fuel
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
- **History (`to` / `FROM … TO …`)** — top-level JSON-LD `to` and the SPARQL
  history range use a distinct history execution path.

## Auth, policy, and dataset behavior

The streaming endpoint routes through the same execution path as `/query` and
enforces policy the same way `/query` does — which differs by query language and
route, exactly as on `/query`:

- **No policy, single ledger** — runs the lean single-ledger path.
- **`from`/`fromNamed` (JSON-LD), SPARQL `FROM`/`FROM NAMED`, multi-ledger, or a
  policy input** — **routes to the connection/dataset path**, which builds a
  policy-wrapped dataset and enforces per-graph policy. A restricted
  identity/policy-class streams strictly fewer rows than an unrestricted one.

**JSON-LD policy** is enforced on both endpoint forms (ledger-scoped and
connection). Inputs: `opts.identity` / `opts.policy-class`, the server
`default_policy_class`, or `Fluree-Policy*` / `Fluree-Identity` headers.

**SPARQL policy** is enforced **only on the ledger-scoped route**
(`/stream/query/<ledger>`). SPARQL has no body `opts`, so policy arrives via the
resolved identity (bearer / `Fluree-Identity`) and the `Fluree-Policy*` /
`Fluree-Default-Allow` headers; `FROM`/`FROM NAMED` select named graphs *within*
the path ledger. The **connection-scoped** SPARQL form has no single ledger to
resolve an identity against, so it **rejects** explicit policy signals (the
`Fluree-Identity` / `Fluree-Policy*` / `Fluree-Default-Allow` headers) rather
than run them unenforced — use the ledger-scoped route or `/query`. This matches
`/query`, where connection SPARQL is likewise not identity-policy-scoped.

> **`default_policy_class`** is a JSON-LD-path setting: it is applied to JSON-LD
> queries (on both `/query` and streaming) but **not** to SPARQL on either
> endpoint. (Making it global across query languages would be a separate change
> affecting `/query` too.)

- **Bearer scope** — a token must be authorized for the path ledger and every
  ledger referenced via `from`/`fromNamed` / SPARQL `FROM`; out-of-scope
  requests return `404` (no existence leak), same as `/query`.
- **`fluree-min-t`** freshness barriers and the stored default-context
  injection are applied before planning, matching `/query`.

The only SPARQL dataset feature still rejected outright is the **history range**
(`FROM <…> TO <…>`) — use [`/query`](endpoints.md#post-query) for that.

## Fuel and tracking

The endpoint tracks fuel and time by default. `max-fuel` is honored from JSON-LD
`opts.max-fuel` and, for SPARQL (which has no body `opts`), from the
`Fluree-Max-Fuel` header. The running fuel total rides on `heartbeat` records
and the final total on the `end` record; a `max-fuel` overrun surfaces as a
`{"type":"error","error":{"code":"fuel_exhausted"}}` terminal.

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

This is an HTTP *client* consuming the endpoint over the wire. To produce the
same NDJSON stream **in-process** from the `fluree-db-api` library (without the
HTTP server), use `Fluree::plan_stream_query` + `run_stream_query` — see
[Streaming query results (NDJSON)](../getting-started/rust-api.md#streaming-query-results-ndjson)
in the Rust library guide.

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

### CLI (`fluree query --format ndjson`)

The [`fluree query`](../cli/query.md) command consumes this stream for you.
`--format ndjson` prints one **bare** binding object per line (the inner `row`
body, with `head`/`heartbeat`/terminal consumed internally); add `--envelope`
to print the full record protocol verbatim. The CLI exits non-zero on an `error`
terminal or a truncated stream, and exits cleanly on a closed downstream pipe.

```bash
# bare rows, jq-friendly
fluree query --format ndjson 'SELECT ?name WHERE { ?s <http://example.org/name> ?name }'

# verbatim protocol (head/row/heartbeat/end/error)
fluree query --remote origin --format ndjson --envelope -f big-select.rq
```

For a **local** ledger the CLI drives the in-process producer
(`run_stream_query`) directly; with `--remote` it POSTs to this endpoint and
streams the response. Time travel (`--at`) and per-request policy on the
streaming path are supported only via `--remote` (they route through the
server's dataset path). See [`fluree query`](../cli/query.md#ndjson-streaming)
for the full scope.

## Relationship to `/query`

`/stream/query/<ledger>` is purpose-built for incremental delivery; it does not
replace [`/query`](endpoints.md#post-query). The standard endpoint remains the
path for ASK/CONSTRUCT/DESCRIBE, hydration, history (JSON-LD `to` or SPARQL
`FROM … TO …`), and any client that wants a single buffered JSON (or
TSV/CSV/XML) document.
