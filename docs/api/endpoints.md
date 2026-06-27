# API Endpoints

Complete reference for all Fluree HTTP API endpoints.

## Base URL / versioning

All endpoints listed below are under the server’s **API base URL** (`api_base_url` from `GET /.well-known/fluree.json`).

- Standalone `fluree-server` default: `api_base_url = "/v1/fluree"`
- All curl examples in this document use the full URL including the base path (e.g., `http://localhost:8090/v1/fluree/query/<ledger...>`)

## Discovery and diagnostics

### GET /.well-known/fluree.json

CLI auth discovery endpoint. Used by `fluree remote add` and `fluree auth login` to auto-configure authentication for a remote.

See [Auth contract (CLI ↔ Server)](../design/auth-contract.md) for the full schema.

Standalone `fluree-server` returns:

- `{"version":1,"api_base_url":"/v1/fluree"}` when no server auth is enabled
- `{"version":1,"api_base_url":"/v1/fluree","auth":{"type":"token"}}` when any server auth mode is enabled (data/events/admin)

OIDC-capable implementations should return `auth.type="oidc_device"` plus `issuer`, `client_id`, and `exchange_url`.
The CLI treats `oidc_device` as "OIDC interactive login": it uses device-code when the IdP supports it, otherwise authorization-code + PKCE.

Implementations MAY also return `api_base_url` to tell the CLI where the Fluree API is mounted (for example,
when the API is hosted under `/v1/fluree` or on a separate `data` subdomain).

Implementations MAY also return an `import` block advertising `.flpack` import capabilities (`modes` incl. `multipart-put`, `direct_max_bytes`, multipart hints) so the CLI can negotiate the upload path — see [Negotiated upload import](#negotiated-upload-import-import-upload).

### GET {api_base_url}/whoami

Diagnostic endpoint for Bearer tokens. Returns a summary of the principal:

- `token_present`: whether a Bearer token was present
- `verified`: whether cryptographic verification succeeded
- `auth_method`: `"embedded_jwk"` (Ed25519) or `"oidc"` (JWKS/RS256)
- identity + scope summary (when verified)

This endpoint is intended for debugging and operator support. See also [Admin, health, and stats](../operations/admin-and-health.md).

## Transaction Endpoints

### POST /update

Submit an **update** transaction (WHERE/DELETE/INSERT JSON-LD or SPARQL UPDATE) to write data to a ledger.

**URL:**
```
POST /update?ledger={ledger-id}
POST /update/{ledger-id}
```

**Query Parameters:**
- `ledger` (required for /update): Target ledger (format: `name:branch`)
- `context` (optional): URL to default JSON-LD context

**Request Headers:**

For JSON-LD transactions:
```http
Content-Type: application/json
Accept: application/json
```

For SPARQL UPDATE:
```http
Content-Type: application/sparql-update
Accept: application/json
```

Note: Turtle/TriG are not accepted on `/update`. Use `/insert` (Turtle) or `/upsert` (Turtle/TriG).

**Request Body (JSON-LD):**

JSON-LD transaction document:
```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "@graph": [
    { "@id": "ex:alice", "ex:name": "Alice" }
  ]
}
```

Or WHERE/DELETE/INSERT update:
```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "where": [
    { "@id": "ex:alice", "ex:age": "?oldAge" }
  ],
  "delete": [
    { "@id": "ex:alice", "ex:age": "?oldAge" }
  ],
  "insert": [
    { "@id": "ex:alice", "ex:age": 31 }
  ]
}
```

**Request Body (SPARQL UPDATE):**

```sparql
PREFIX ex: <http://example.org/ns/>

INSERT DATA {
  ex:alice ex:name "Alice" .
  ex:alice ex:age 30 .
}
```

Or with DELETE/INSERT:

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE {
  ?person ex:age ?oldAge .
}
INSERT {
  ?person ex:age 31 .
}
WHERE {
  ?person ex:name "Alice" .
  ?person ex:age ?oldAge .
}
```

`INSERT DATA` and `DELETE DATA` accept named-graph blocks per SPARQL 1.1 Update
§3.1.1 (`QuadData`). A `GRAPH <iri> { ... }` block routes its (ground) triples
into the named graph; this is what RDF4J's `SPARQLConnection.add(stmts, context)`
emits:

```sparql
INSERT DATA {
  GRAPH <https://example.org/g/1> {
    <https://example.org/s/1> <https://example.org/p> "v" .
  }
}
```

The graph name must be a fixed IRI — a variable graph name (`GRAPH ?g { ... }`)
is rejected, since DATA must be ground. Use DELETE/INSERT … WHERE for variable
graph targets.

**Response:**

```json
{
  "t": 5,
  "timestamp": "2024-01-22T10:30:00.000Z",
  "commit_id": "bafybeig...commitT5",
  "flakes_added": 3,
  "flakes_retracted": 1,
  "previous_commit_id": "bafybeig...commitT4"
}
```

**Status Codes:**
- `200 OK` - Transaction successful
- `400 Bad Request` - Invalid transaction syntax
- `401 Unauthorized` - Authentication required
- `403 Forbidden` - Not authorized for this ledger
- `404 Not Found` - Ledger not found
- `409 Conflict` - Optimistic-concurrency conflict that survived the server's
  bounded reconcile-and-retry (rare; safe to retry the request)
- `413 Payload Too Large` - Transaction exceeds size limit
- `500 Internal Server Error` - Server error

**Concurrency:** Writes to a single ledger are serialized by a per-ledger write
lock (concurrent writes to *different* ledgers proceed in parallel). When a
transaction is lowered/sequenced against a snapshot that is no longer the head
by commit time — two writers racing on a first-time namespace code, or a cached
writer state that fell behind the durable head — the server reconciles the
cached state to the current head and re-tries (bounded, up to 16 attempts). A
`409 Conflict` is returned only if the retry budget is exhausted; clients should
treat it as retryable (distinct from a `400` bad request).

**Examples:**

JSON-LD transaction:
```bash
curl -X POST "http://localhost:8090/v1/fluree/update?ledger=mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "@context": { "ex": "http://example.org/ns/" },
    "@graph": [{ "@id": "ex:alice", "ex:name": "Alice" }]
  }'
```

SPARQL UPDATE (ledger-scoped endpoint):
```bash
curl -X POST http://localhost:8090/v1/fluree/update/mydb:main \
  -H "Content-Type: application/sparql-update" \
  -d 'PREFIX ex: <http://example.org/ns/>
      INSERT DATA { ex:alice ex:name "Alice" }'
```

SPARQL UPDATE (connection-scoped with header):
```bash
curl -X POST http://localhost:8090/v1/fluree/update \
  -H "Content-Type: application/sparql-update" \
  -H "Fluree-Ledger: mydb:main" \
  -d 'PREFIX ex: <http://example.org/ns/>
      DELETE { ?s ex:age ?old } INSERT { ?s ex:age 31 }
      WHERE { ?s ex:name "Alice" . ?s ex:age ?old }'
```

Note: Turtle and TriG are not accepted on `/update`. Use `/insert` (Turtle) or `/upsert` (Turtle/TriG).

### POST /insert

Insert new data into a ledger. Data must not conflict with existing data.

**URL:**
```
POST /insert?ledger={ledger-id}
POST /insert/{ledger-id}
```

**Supported Content Types:**
- `application/json` - JSON-LD
- `text/turtle` - Turtle (fast direct flake path)
- `application/n-triples` - N-Triples (parsed as Turtle)

**Note:** TriG (`application/trig`) is **not supported** on the insert endpoint. Named graph ingestion via GRAPH blocks requires the upsert path. Use `/upsert` for TriG data.

**Example (JSON-LD):**
```bash
curl -X POST "http://localhost:8090/v1/fluree/insert?ledger=mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "@context": { "ex": "http://example.org/ns/" },
    "@graph": [{ "@id": "ex:alice", "ex:name": "Alice" }]
  }'
```

**Example (Turtle):**
```bash
curl -X POST "http://localhost:8090/v1/fluree/insert?ledger=mydb:main" \
  -H "Content-Type: text/turtle" \
  -d '@prefix ex: <http://example.org/ns/> .
      ex:alice ex:name "Alice" ; ex:age 30 .'
```

### POST /upsert

Upsert data into a ledger. For each (subject, predicate) pair, existing values are retracted before new values are asserted.

**URL:**
```
POST /upsert?ledger={ledger-id}
POST /upsert/{ledger-id}
```

**Supported Content Types:**
- `application/json` - JSON-LD
- `text/turtle` - Turtle
- `application/n-triples` - N-Triples (parsed as Turtle)
- `application/trig` - TriG with named graphs

**Example (JSON-LD):**
```bash
curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "@context": { "ex": "http://example.org/ns/" },
    "@id": "ex:alice",
    "ex:age": 31
  }'
```

**Example (TriG with named graphs):**
```bash
curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
  -H "Content-Type: application/trig" \
  -d '@prefix ex: <http://example.org/ns/> .

      # Default graph
      ex:company ex:name "Acme Corp" .

      # Named graph for products
      GRAPH <http://example.org/graphs/products> {
          ex:widget ex:name "Widget" ;
                    ex:price "29.99"^^xsd:decimal .
      }'
```

Both W3C TriG graph-block forms are accepted: the SPARQL-style keyword form
`GRAPH <iri> { ... }` shown above and the compact form `<iri> { ... }` (the
`GRAPH` keyword is optional per the [TriG grammar](https://www.w3.org/TR/trig/#sec-graph)).
The compact form is what stock RDF tooling — rdflib, Apache Jena, RDF4J — emits
by default, so payloads generated by those libraries are ingested as-is.

### POST /push/*ledger

Push precomputed commit v2 blobs to the server.

This endpoint is intended for Git-like workflows (`fluree push`) where a client has written commits locally and wants the server to validate and commit them.

**URL:**

```
POST /push/<ledger...>
```

**Request Headers:**

```http
Content-Type: application/json
Accept: application/json
Authorization: Bearer <token>
Idempotency-Key: <string>   (optional; recommended)
```

If `Idempotency-Key` is provided, servers MAY treat `POST /push/*ledger` as idempotent for that key (same request body + key should yield the same response), returning the prior success response instead of `409` on client retry after timeouts.

**Request Body:**

JSON object:

- `commits`: array of base64-encoded commit v2 blobs (oldest → newest)
- `blobs` (optional): map of `{ cid: base64Bytes }` for referenced blobs (currently: `commit.txn` when present)

**Response Body (200 OK):**

```json
{
  "ledger": "mydb:main",
  "accepted": 3,
  "head": {
    "t": 42,
    "commit_id": "bafy...headCommit"
  },
  "indexing": {
    "enabled": false,
    "needed": true,
    "novelty_size": 524288,
    "index_t": 30,
    "commit_t": 42
  }
}
```

| Field | Description |
|-------|-------------|
| `indexing.enabled` | Whether background indexing is active on this server. |
| `indexing.needed` | Whether novelty has exceeded `reindex_min_bytes` and indexing should be triggered. |
| `indexing.novelty_size` | Current novelty size in bytes after the push. |
| `indexing.index_t` | Transaction time of the last indexed state. |
| `indexing.commit_t` | Transaction time of the latest committed data (after push). |

When `enabled` is `false` (external indexer mode), the caller should use `needed` and related fields to decide whether to trigger indexing through its own mechanism.

**Error Responses:**

- `409 Conflict`: head changed / diverged / first commit `t` did not match next-t
- `422 Unprocessable Entity`: invalid commit bytes, missing referenced blob, or retraction invariant violation

### GET /show/*ledger

Fetch and decode a single commit's contents with resolved IRIs. This is the server-side equivalent of `fluree show` — it returns assertions, retractions, and flake tuples with IRIs compacted using the ledger's namespace prefix table.

**URL:**

```
GET /show/<ledger...>?commit=<ref>
```

**Query Parameters:**

- `commit` (required): Commit identifier — `t:<N>` for transaction number, hex-digest prefix (min 6 chars), or full CID

**Request Headers:**

```http
Authorization: Bearer <token>   (when data auth is enabled)
```

**Response Body (200 OK):**

```json
{
  "id": "bagaybqabciq...",
  "t": 5,
  "time": "2026-03-12T16:58:18.395474217+00:00",
  "size": 327,
  "previous": "bagaybqabciq...",
  "asserts": 1,
  "retracts": 1,
  "@context": {
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "schema": "http://schema.org/"
  },
  "flakes": [
    ["urn:fsys:dataset:zoho3", "schema:dateModified", "2026-03-12T14:15:30Z", "xsd:string", false],
    ["urn:fsys:dataset:zoho3", "schema:dateModified", "2026-03-12T16:58:16Z", "xsd:string", true]
  ]
}
```

Each flake is a tuple: `[subject, predicate, object, datatype, operation]`. Operation `true` = assert (added), `false` = retract (removed). When metadata is present (language tag, list index, or named graph), a 6th element is appended.

**Policy filtering:** Flakes are filtered by the caller's data-auth identity (extracted from the Bearer token) and the server's configured `default_policy_class`. When neither is present, all flakes are returned (root/admin access). Flakes the caller cannot read are silently omitted — the `asserts` and `retracts` counts reflect only the visible flakes. Unlike the query endpoints, show does not accept per-request policy overrides via headers or request body.

**Responses:**

- `200 OK`: Decoded commit returned
- `400 Bad Request`: Missing or invalid `commit` parameter
- `401 Unauthorized`: Bearer token required but missing
- `404 Not Found`: Ledger or commit not found
- `501 Not Implemented`: Proxy storage mode (no local index available)

**Peer mode:** Forwards to the transactor.

### GET /log/*ledger

Return a paginated list of lightweight commit summaries (newest-first by `t`). Server-side equivalent of `fluree log`. Read-auth — does **not** require storage-replication permissions, unlike `/commits`.

**URL:**

```
GET /log/<ledger...>?limit=<N>
```

**Query Parameters:**

- `limit` (optional, default `100`): Number of summaries to return. Server clamps to a hard maximum (reference: `5000`).

**Request Headers:**

```http
Authorization: Bearer <token>   (when data auth is enabled)
```

**Response Body (200 OK):**

```json
{
  "ledger_id": "mydb:main",
  "commits": [
    {
      "t": 12,
      "commit_id": "bafy...",
      "time": "2026-04-25T12:00:00Z",
      "asserts": 3,
      "retracts": 0,
      "flake_count": 3,
      "message": null
    }
  ],
  "count": 12,
  "truncated": false
}
```

`commits` is strictly newest-first by `t` and capped by `limit`. `count` is the full chain length; `truncated == count > commits.len()`. `message` is extracted from `txn_meta` when an `f:message` entry with a string value is present, otherwise `null`. Each summary mirrors `fluree_db_core::CommitSummary`.

**Branch-aware walk:** The walk loads commit envelopes via a branch-aware content store so it can cross fork points — pre-fork commits live under the source branch's namespace.

**Responses:**

- `200 OK`: Summaries returned (possibly empty array when the ledger has no commits)
- `401 Unauthorized`: Bearer token required but missing
- `404 Not Found`: Ledger does not exist; or the bearer cannot `can_read`
- `5xx`: Storage / nameservice errors during walk

**Peer mode:** Forwards to the transactor.

### GET /commits/*ledger

Export commit blobs from a ledger using stable cursors. Pages walk backward via each commit's `parents` — O(limit) per page regardless of ledger size. Used by `fluree pull` and `fluree clone`.

**Requires replication-grade permissions** (`fluree.storage.*`). The storage proxy must be enabled on the server.

**URL:**

```
GET /commits/<ledger...>?limit=100&cursor_id=<cid>
```

**Query Parameters:**

- `limit` (optional): Max commits per page (default 100, server clamps to max 500)
- `cursor_id` (optional): Commit CID cursor for pagination. Omit for first page (starts from head). Use `next_cursor_id` from the previous response for subsequent pages.

**Request Headers:**

```http
Authorization: Bearer <token>   (requires fluree.storage.* claims)
```

**Response Body (200 OK):**

```json
{
  "ledger": "mydb:main",
  "head_commit_id": "bafy...headCommit",
  "head_t": 42,
  "commits": ["<base64>", "<base64>"],
  "blobs": { "bafy...txnBlob": "<base64>" },
  "newest_t": 42,
  "oldest_t": 41,
  "next_cursor_id": "bafy...prevCommit",
  "count": 2,
  "effective_limit": 100
}
```

- `commits`: Raw commit v2 blobs, newest → oldest within each page.
- `blobs`: Referenced txn blobs keyed by CID string.
- `next_cursor_id`: CID cursor for the next page; `null` when genesis is reached.
- `effective_limit`: Actual limit used (after server clamping).

**Responses:**

- `200 OK`: Page of commits returned
- `401 Unauthorized`: Missing or invalid storage token
- `404 Not Found`: Storage proxy not enabled, ledger not found, or not authorized for this ledger

**Pagination:**

Commit CIDs in the immutable chain are stable cursors. New commits appended to the head do not affect backward pointers, so cursors remain valid across pages even when new commits arrive between requests.

### POST /pack/*ledger

Stream all missing CAS objects for a ledger in a single binary response. This is the primary transport for `fluree clone` and `fluree pull`, replacing multiple paginated `GET /commits` requests or per-object `GET /storage/objects` fetches with a single streaming request.

**Requires replication-grade permissions** (`fluree.storage.*`). The storage proxy must be enabled on the server.

**URL:**

```
POST /pack/<ledger...>
```

**Request Headers:**

```http
Content-Type: application/json
Accept: application/x-fluree-pack
Authorization: Bearer <token>   (requires fluree.storage.* claims)
```

**Request Body:**

```json
{
  "protocol": "fluree-pack-v1",
  "want": ["bafy...remoteHead"],
  "have": ["bafy...localHead"],
  "include_indexes": true,
  "include_txns": true,
  "want_index_root_id": "bafy...indexRoot",
  "have_index_root_id": "bafy...localIndexRoot"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `protocol` | string | Yes | Must be `"fluree-pack-v1"` |
| `want` | string[] | Yes | ContentId CIDs the client wants (typically the remote commit head) |
| `have` | string[] | No | ContentId CIDs the client already has (typically the local commit head). Server stops walking the commit chain when it reaches a `have` CID. Empty for full clone. |
| `want_index_root_id` | string | No | Index root CID the client wants (typically remote nameservice `index_head_id`). Required when `include_indexes=true`. |
| `have_index_root_id` | string | No | Index root CID the client already has (typically local nameservice `index_head_id`). Used for index artifact diff. |
| `include_indexes` | bool | Yes | Include index artifacts in the stream. When true, the stream contains commit + txn objects plus index root/branch/leaf/dict artifacts. |
| `include_txns` | bool | Yes | Include original transaction blobs referenced by each commit. When false, only commits (and optionally index artifacts) are streamed — commit envelopes still reference their `txn` CIDs, but the client will not have the transaction payloads locally. The ledger state is fully reconstructable from commits + indexes; transactions are the original request payloads (e.g., JSON-LD insert/update requests). |

**Response:**

Binary stream using the `fluree-pack-v1` wire format (`Content-Type: application/x-fluree-pack`):

```
[Preamble: FPK1 + version(1)] [Header frame] [Data frames...] [End frame]
```

| Frame | Type byte | Content |
|-------|-----------|---------|
| Header | `0x00` | JSON metadata: protocol version, capabilities, `commit_count`, `index_artifact_count`, `estimated_total_bytes` |
| Data | `0x01` | CID binary + raw object bytes (commit, txn blob, or index artifact) |
| Error | `0x02` | UTF-8 error message (terminates stream) |
| Manifest | `0x03` | JSON metadata for phase transitions (e.g. start of index phase) |
| End | `0xFF` | End of stream (no payload) |

Data frames are streamed in **oldest-first topological order** (parents before children), so the client can write objects to CAS as they arrive without buffering the entire stream.

The Header frame includes an `estimated_total_bytes` field that the CLI uses to warn users before large transfers (~1 GiB or more). The estimate is ratio-based (derived from commit count) and may differ from actual transfer size. Set to `0` for commits-only requests.

**Status Codes:**

- `200 OK`: Binary pack stream
- `401 Unauthorized`: Missing or invalid storage token
- `404 Not Found`: Storage proxy not enabled, ledger not found, or not authorized for this ledger

**Example:**

```bash
# Download all commits for a ledger (full clone)
curl -X POST "http://localhost:8090/v1/fluree/pack/mydb:main" \
  -H "Content-Type: application/json" \
  -H "Accept: application/x-fluree-pack" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"protocol":"fluree-pack-v1","want":["bafy...head"],"have":[],"include_indexes":false,"include_txns":true}' \
  --output pack.bin

# Download commits without transaction payloads (smaller clone, read-only use)
curl -X POST "http://localhost:8090/v1/fluree/pack/mydb:main" \
  -H "Content-Type: application/json" \
  -H "Accept: application/x-fluree-pack" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"protocol":"fluree-pack-v1","want":["bafy...head"],"have":[],"include_indexes":true,"include_txns":false,"want_index_root_id":"bafy...indexRoot"}' \
  --output pack.bin

# Download only missing commits (incremental pull)
curl -X POST "http://localhost:8090/v1/fluree/pack/mydb:main" \
  -H "Content-Type: application/json" \
  -H "Accept: application/x-fluree-pack" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"protocol":"fluree-pack-v1","want":["bafy...remoteHead"],"have":["bafy...localHead"],"include_indexes":false,"include_txns":true}' \
  --output pack.bin

# Download commits + index artifacts (default for CLI pull/clone)
curl -X POST "http://localhost:8090/v1/fluree/pack/mydb:main" \
  -H "Content-Type: application/json" \
  -H "Accept: application/x-fluree-pack" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"protocol":"fluree-pack-v1","want":["bafy...head"],"have":[],"include_indexes":true,"include_txns":true,"want_index_root_id":"bafy...indexRoot"}' \
  --output pack.bin
```

### POST /import/*ledger

Create a **new** ledger by restoring a `.flpack` archive — the inbound
counterpart of `POST /pack/*ledger`. The request body is the raw `.flpack`
stream (as produced by `fluree export <ledger> --format ledger` or
`Fluree::archive_ledger`). The server streams the archive into storage — commits,
transaction blobs, and any prebuilt index artifacts — then finalizes the commit
and index heads from the archive's embedded nameservice manifest.

The new ledger is named by the URL path and is **independent of the source
ledger's name**, so the same archive can be restored under any name. Unlike
`/push`, the archive is trusted byte-for-byte (every frame is SHA-256 verified)
and not replayed, and the prebuilt index rides along — so the restored ledger is
immediately queryable with no reindex.

**Admin-protected** (same bracket as `/create` and `/drop`): the body carries
prebuilt index artifacts the server did not produce, so this is an admin-grade
operation. The archive is decoded frame-by-frame and never buffered whole, so
multi-gigabyte archives restore without exhausting server memory.

**URL:**

```
POST /import/<ledger...>
```

**Request Headers:**

```http
Content-Type: application/x-fluree-pack
Authorization: Bearer <token>   (admin token when configured)
```

**Request Body:** the raw `.flpack` byte stream.

**Response:** `201 Created` with a JSON summary:

```json
{
  "ledger_id": "restored-db:main",
  "commits": 12,
  "txn_blobs": 12,
  "index_artifacts": 34,
  "commit_t": 12,
  "index_t": 12
}
```

`index_artifacts` is `0` and `index_t` is omitted for a commits-only archive
(exported with `--no-indexes`); such a ledger replays from commits on first load.

**Status codes:**

- `201 Created`: ledger restored
- `400 Bad Request`: malformed archive (bad preamble/frame, missing manifest, or a manifest head CID not present in the archive)
- `409 Conflict`: a ledger with that name already exists
- `401 Unauthorized`: missing or invalid admin token

On any mid-stream failure the partially-created ledger is rolled back, so a
failed import never leaves a live, half-ingested ledger behind.

**Example:**

```bash
# Restore an archive into a brand-new ledger named "restored-db:main"
curl -X POST "http://localhost:8090/v1/fluree/import/restored-db:main" \
  -H "Content-Type: application/x-fluree-pack" \
  -H "Authorization: Bearer $TOKEN" \
  --data-binary @mydb.flpack
```

This is the transport behind `fluree create restored-db --remote origin --from mydb.flpack`.

### Negotiated upload import (`/import-upload`)

For clients that cannot send a large body to `POST /import` (e.g. behind a payload-capped gateway), an optional out-of-band upload handshake lets the client upload the `.flpack` directly to object storage, then have the server restore from it asynchronously. Servers advertise support in discovery (`GET /.well-known/fluree.json`):

```jsonc
"import": {
  "modes": ["direct", "presigned-put", "multipart-put"],  // negotiated modes
  "direct_max_bytes": 6291456,             // archives larger than this negotiate
  "multipart_threshold_bytes": 5368709120, // ≥ this size → multipart (5 GiB PUT cap)
  "multipart_part_size_bytes": 268435456   // target part size hint (≥ 5 MiB for S3)
}
```

| Step | Endpoint | Result |
|------|----------|--------|
| Mint | `POST /import-upload` `{ledger, size?}` | single: `{ import_id, upload: { method, url, headers, expires_at_unix } }` — or multipart (when `size ≥ threshold`): `{ import_id, multipart: { upload_id, part_size_bytes, parts:[{part_number,url,headers}], expires_at_unix } }` |
| Upload | `PUT <upload.url>` (single) **or** `PUT <part.url>` per part (multipart) | bytes staged (direct to object storage; **no bearer auth** — the URL is the capability). Each part PUT returns its `ETag`. |
| Complete | `POST /import-upload/{import_id}/complete` | body: empty (single) or `{ parts:[{part_number, etag}] }` (multipart) → `202` `{ import_id, status:"running" }` — restore begins asynchronously |
| Status | `GET /import-upload/{import_id}` | `{ status, result?, error? }`, `status ∈ {awaiting-upload, running, succeeded, failed}` |

Mint / complete / status are **admin-protected**; the blob/part `PUT`s are token-authorized via the minted URL. The server picks single-PUT vs multipart by the declared `size` (a single S3 PUT caps at 5 GiB). On `succeeded`, `result` is the same summary as `POST /import`. The Fluree server ships a reference backend behind `FLUREE_IMPORT_PRESIGN_ENABLED=true` (stages parts to local disk and concatenates them); production servers mint presigned object-store URLs and drive `CreateMultipartUpload`/`CompleteMultipartUpload`. See the [Negotiated Upload Import Contract](../cli/server-integration.md#negotiated-upload-import-contract) for the full implementer spec.

This is the transport behind `fluree create … --remote … --from big.flpack` when the server is size-capped — the CLI negotiates automatically.

## Storage Proxy Endpoints

These endpoints are intended for peer mode and `fluree clone`/`pull` workflows. They require the storage proxy to be enabled on the server and use replication-grade Bearer tokens (`fluree.storage.*` claims).

### GET /storage/ns/:ledger-id

Fetch the nameservice record for a ledger.

**URL:**

```
GET /storage/ns/{ledger-id}
```

**Request Headers:**

```http
Authorization: Bearer <token>   (requires fluree.storage.* claims)
```

**Response (200 OK):**

```json
{
  "ledger_id": "mydb:main",
  "name": "mydb",
  "branch": "main",
  "commit_head_id": "bafy...commitCid",
  "commit_t": 42,
  "index_head_id": "bafy...indexCid",
  "index_t": 40,
  "default_context": null,
  "retracted": false,
  "config_id": "bafy...configCid"
}
```

| Field | Description |
|-------|-------------|
| `ledger_id` | Canonical ledger ID (e.g., "mydb:main") |
| `name` | Ledger name without branch (e.g., "mydb") |
| `config_id` | CID of the `LedgerConfig` object (origin discovery), if set |

**Status Codes:**

- `200 OK`: Record found
- `404 Not Found`: Storage proxy disabled, ledger not found, or not authorized

### POST /storage/block

Fetch a storage block (index branch or leaf) by CID. The server derives the storage address internally. Leaf blocks are always policy-filtered before return.

Only replication-relevant content kinds are allowed (commits, txns, config, index roots/branches/leaves, dict blobs). Internal metadata kinds (GC records, stats sketches, graph source snapshots) are rejected with 404.

**URL:**

```
POST /storage/block
```

**Request Headers:**

```http
Content-Type: application/json
Authorization: Bearer <token>
Accept: application/octet-stream | application/x-fluree-flakes | application/x-fluree-flakes+json
```

**Request Body:**

Both fields are required:

```json
{
  "cid": "bafy...branchOrLeafCid",
  "ledger": "mydb:main"
}
```

**Responses:**

- `200 OK`: Block bytes (branches) or encoded flakes (leaves)
- `400 Bad Request`: Invalid CID string
- `404 Not Found`: Block not found, disallowed kind, or not authorized

### GET /storage/objects/:cid

Fetch a CAS (content-addressed storage) object by its content identifier. Returns the raw bytes of the stored object after verifying integrity.

This is a **replication-grade** endpoint for `fluree clone`/`pull` workflows. The client knows the CID (from the nameservice record or the commit chain) and wants the raw bytes.

**URL:**

```
GET /storage/objects/{cid}?ledger={ledger-id}
```

**Path Parameters:**

- `cid`: CIDv1 string (base32-lower multibase, e.g., `"bafybeig..."`)

**Query Parameters:**

- `ledger` (required): Ledger ID (e.g., `"mydb:main"`). Required because storage paths are ledger-scoped.

**Request Headers:**

```http
Authorization: Bearer <token>   (requires fluree.storage.* claims)
```

**Kind Allowlist:**

All replication-relevant content kinds are served:

| Kind | Description |
|------|-------------|
| `commit` | Commit chain blobs |
| `txn` | Transaction data blobs |
| `config` | LedgerConfig origin discovery objects |
| `index-root` | Binary index root (FIR6) |
| `index-branch` | Index branch manifests |
| `index-leaf` | Index leaf files |
| `dict` | Dictionary artifacts (predicates, subjects, strings, etc.) |

Only `GarbageRecord` (internal GC metadata) returns 404.

**Response Headers:**

- `Content-Type: application/octet-stream`
- `X-Fluree-Content-Kind`: Content kind label (`commit`, `txn`, `config`, `index-root`, `index-branch`, `index-leaf`, `dict`)

**Response Body:**

Raw bytes of the stored object.

**Integrity Verification:**

The server verifies the hash of the stored bytes against the CID before returning. Commit blobs are format-sniffed:

- **Commit-v2 blobs** (`FCV2` magic): Uses the canonical sub-range hash (SHA-256 over the payload excluding the trailing hash + signature block).
- **All other blobs** (txn, config, future commit formats): Full-bytes SHA-256.

If verification fails, the server returns `500 Internal Server Error` — this indicates storage corruption.

**Status Codes:**

- `200 OK`: Object found and integrity verified
- `400 Bad Request`: Invalid CID string
- `404 Not Found`: Object not found, disallowed kind, not authorized, or storage proxy disabled
- `500 Internal Server Error`: Hash verification failed (storage corruption)

**Example:**

```bash
# Fetch a commit blob by CID
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/v1/fluree/storage/objects/bafybeig...commitCid?ledger=mydb:main"

# Fetch a config blob by CID
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/v1/fluree/storage/objects/bafybeig...configCid?ledger=mydb:main"

# Fetch an index leaf by CID
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/v1/fluree/storage/objects/bafybeig...leafCid?ledger=mydb:main"
```

## Nameservice Sync Endpoints

Used by replication clients and peer instances to push ref updates, initialize
ledgers, and fetch snapshots of all nameservice records. These are the
server-side counterpart to the `fluree-db-nameservice-sync` crate.

**Authorization:** All endpoints require a Bearer token with storage-proxy
permissions. Per-alias endpoints verify the principal is authorized for that
ledger. `/snapshot` filters results to the principal's authorized scope
(`storage_all` returns everything; otherwise results are filtered to
`storage_ledgers` and graph sources are excluded).

**Availability:** These endpoints are only available on transaction servers
(direct storage mode). Proxy-mode instances return `404 Not Found`.

### POST /nameservice/refs/{alias}/commit

Compare-and-set push for a ledger's commit-head ref.

**Request Body:**

```json
{
  "expected": { /* RefValue or null for initial creation */ },
  "new":      { /* RefValue */ }
}
```

**Response (200 OK — updated):**

```json
{ "status": "updated", "ref": { /* new RefValue */ } }
```

**Response (409 Conflict — CAS failed):**

```json
{ "status": "conflict", "actual": { /* current server-side RefValue */ } }
```

### POST /nameservice/refs/{alias}/index

Compare-and-set push for a ledger's index-head ref. Same request/response shape
as `/commit` above.

### POST /nameservice/refs/{alias}/init

Create a ledger entry in the nameservice if it does not already exist.
Idempotent.

**Response:**

```json
{ "created": true }   // new ledger entry was registered
{ "created": false }  // already existed; no change
```

### GET /nameservice/snapshot

Return a full snapshot of all ledger (`NsRecord`) and graph-source
(`GraphSourceRecord`) records visible to the caller.

**Response:**

```json
{
  "ledgers":       [ /* NsRecord, … */ ],
  "graph_sources": [ /* GraphSourceRecord, … */ ]
}
```

**Status Codes:**
- `200 OK` — snapshot returned
- `401 Unauthorized` — missing/invalid storage-proxy token
- `404 Not Found` — endpoint disabled (proxy mode)

## Query Endpoints

### POST /query

Execute a query against one or more ledgers.

**URL:**
```
POST /query
GET  /query?query={urlencoded-sparql}   # SPARQL Protocol GET form
```

The `GET` form is provided for W3C SPARQL Protocol compliance. It accepts SPARQL queries via the `query` query parameter; the body forms below are preferred for larger queries and for JSON-LD. The same form is available on the ledger-scoped `/query/{ledger}` route.

**Optional Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `default-context` | boolean | `false` | When `true`, use the ledger's stored default JSON-LD context if the request omits its own `@context` (JSON-LD) or `PREFIX` declarations (ledger-scoped SPARQL). |

**Request Headers:**
```http
Content-Type: application/json
Accept: application/json
Fluree-Min-T: 42  # optional read-after-write guarantee
```

Or for SPARQL:
```http
Content-Type: application/sparql-query
Accept: application/sparql-results+json
Fluree-Min-T: 42  # optional read-after-write guarantee
```

`Fluree-Min-T` makes the server refresh the referenced ledger(s) until they have reached at least that transaction time before executing the query. JSON-LD bodies can also send `opts.min-t`, `opts.min_t`, or `opts.minT`; body opts take precedence over the header.

**Request Body (JSON-LD Query):**

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "from": "mydb:main",
  "select": ["?name", "?age"],
  "where": [
    { "@id": "?person", "ex:name": "?name" },
    { "@id": "?person", "ex:age": "?age" }
  ],
  "orderBy": ["?name"],
  "limit": 100
}
```

**Multi-ledger / dataset queries:** The connection-scoped `/query` route (no
`{ledger}` in the path) accepts the full dataset `from` surface, not just a
single ledger string:

- `"from": ["a:main", "b:main"]` — union the listed ledgers as the default graph.
- `"from": { "@id": "a:main@t:5" }` — a single source with time-travel / graph selectors.
- `"fromNamed": { "alias": { "@id": "b:main" } }` — named graphs addressed by a
  GRAPH pattern in the `where` (a query may use `fromNamed` with no `from`).

The query is authorized against the first resolvable source for the bearer
scope; per-ledger policy is then applied to each source during execution.

**Request Body (SPARQL):**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?age
FROM <mydb:main>
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
}
ORDER BY ?name
LIMIT 100
```

**Response (JSON-LD Query):**

```json
[
  { "name": "Alice", "age": 30 },
  { "name": "Bob", "age": 25 }
]
```

**Response (SPARQL):**

```json
{
  "head": {
    "vars": ["name", "age"]
  },
  "results": {
    "bindings": [
      {
        "name": { "type": "literal", "value": "Alice" },
        "age": { "type": "literal", "value": "30", "datatype": "http://www.w3.org/2001/XMLSchema#integer" }
      }
    ]
  }
}
```

**SPARQL output negotiation (`Accept` header):**

The full byte-format negotiation below is available on the **ledger-scoped**
[`POST /query/{ledger}`](#post-queryledger) route. The **connection-scoped**
`POST /query` route (SPARQL with `FROM <ledger>`) returns pre-formatted JSON only
— it supports the JSON family (JSON-LD, SPARQL-results JSON, AgentJson) but not the
byte formats; RDF/XML, SPARQL-results XML, and CSV/TSV require the ledger-scoped
route (see the [connection-scoped note](#connection-scoped-sparql-output) below).

| Query form | Default (no/`*/*`/`application/json`) | `application/ld+json` | `application/rdf+xml` | `application/sparql-results+json` | `text/csv` / `text/tab-separated-values` | `application/sparql-results+xml` | `application/vnd.fluree.agent+json` |
|---|---|---|---|---|---|---|---|
| `SELECT` / `ASK` | SPARQL-results JSON | JSON-LD | **406** | SPARQL-results JSON | CSV / TSV | SPARQL-results XML | AgentJson |
| `CONSTRUCT` / `DESCRIBE` | **JSON-LD** | JSON-LD | RDF/XML | JSON-LD | **406** | **406** | **406** |

A `CONSTRUCT` / `DESCRIBE` produces an RDF graph, which has no solution/binding-table
form, so it is always returned as **JSON-LD** (`Content-Type: application/ld+json`)
unless `application/rdf+xml` is explicitly requested; the solution-table formats
(SPARQL-results XML, CSV/TSV, AgentJson) are rejected with `406`. A `SELECT` / `ASK`
defaults to SPARQL-results JSON and only switches to JSON-LD when `application/ld+json`
is requested explicitly — a bare `application/json` keeps the SPARQL-results-JSON
shape — and RDF/XML (a graph format) is rejected with `406`.

<a name="connection-scoped-sparql-output"></a>
> **Connection-scoped `POST /query` (SPARQL):** this route returns pre-formatted
> JSON only. The JSON-family columns above apply (CONSTRUCT/DESCRIBE → JSON-LD;
> SELECT/ASK → SPARQL-results JSON, or JSON-LD with `Accept: application/ld+json`;
> AgentJson via `application/vnd.fluree.agent+json`, rejected `406` for graph
> queries). CSV/TSV are rejected with `406`, and `application/rdf+xml` /
> `application/sparql-results+xml` are **not** negotiated here — use
> `POST /query/{ledger}` for those byte formats.

**Status Codes:**
- `200 OK` - Query successful
- `400 Bad Request` - Invalid query syntax
- `401 Unauthorized` - Authentication required
- `404 Not Found` - Ledger not found
- `406 Not Acceptable` - Requested output format is not available for this query form (e.g. RDF/XML for a `SELECT`)
- `413 Payload Too Large` - Query exceeds size limit
- `500 Internal Server Error` - Server error
- `503 Service Unavailable` - Query timeout or resource limit

**Example:**

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "from": "mydb:main",
    "select": ["?name"],
    "where": [{ "@id": "?person", "ex:name": "?name" }]
  }'
```

### POST /query/{ledger}

Execute a query against a specific ledger (ledger-scoped).

This endpoint is designed for **single-ledger** queries, but supports selecting **named graphs inside the ledger**.

**URL:**
```
POST /query/{ledger}
```

**Default graph semantics:**
- If the request does not specify a graph selector, the query runs against the ledger's **default graph**.
- The built-in **txn-meta** graph can be selected as either:
  - JSON-LD: `"from": "txn-meta"`, or
  - SPARQL: `FROM <txn-meta>`

**Named graph selection (within the same ledger):**

- **JSON-LD**: you can use `"from"` to pick a graph in this ledger:
  - `"from": "default"` → default graph
  - `"from": "txn-meta"` → txn-meta graph
  - `"from": "<graph IRI>"` → a user-defined named graph IRI within this ledger
  - Structured form: `"from": { "@id": "<ledger>", "graph": "<graph selector>" }`

- **SPARQL**: if the query includes `FROM` / `FROM NAMED`, the server interprets those IRIs as **graphs within this ledger** (not other ledgers):
  - `FROM <default>` / `FROM <txn-meta>` / `FROM <graph IRI>` selects the default graph for triple patterns outside `GRAPH {}`.
  - `FROM NAMED <graph IRI>` makes that named graph available via `GRAPH <graph IRI> { ... }`.

**Ledger mismatch protection:**

If the body includes a ledger reference that targets a different ledger than `{ledger}`, the server returns `400 Bad Request` with a "Ledger mismatch" error.

**Examples:**

JSON-LD (query txn-meta):

```bash
curl -X POST "http://localhost:8090/v1/fluree/query/mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "from": "txn-meta",
    "select": ["?commit", "?t"],
    "where": [{ "@id": "?commit", "https://ns.flur.ee/db#t": "?t" }]
  }'
```

JSON-LD (query a user-defined named graph by IRI):

```bash
curl -X POST "http://localhost:8090/v1/fluree/query/mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "from": "http://example.org/graphs/products",
    "select": ["?name"],
    "where": [{ "@id": "?p", "http://example.org/ns/name": "?name" }]
  }'
```

SPARQL (select txn-meta as default graph):

```bash
curl -X POST "http://localhost:8090/v1/fluree/query/mydb:main" \
  -H "Content-Type: application/sparql-query" \
  -d 'PREFIX f: <https://ns.flur.ee/db#>
SELECT ?commit ?t
FROM <txn-meta>
WHERE { ?commit f:t ?t }'
```

### History Queries via POST /query

Query the history of entities using the standard `/query` endpoint with `from` and `to` keys specifying the time range.

**Request Body:**

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "from": "mydb:main@t:1",
  "to": "mydb:main@t:latest",
  "select": ["?name", "?age", "?t", "?op"],
  "where": [
    { "@id": "ex:alice", "ex:name": { "@value": "?name", "@t": "?t", "@op": "?op" } },
    { "@id": "ex:alice", "ex:age": "?age" }
  ],
  "orderBy": "?t"
}
```

The `@t` and `@op` annotations capture transaction metadata:
- **@t** - Transaction time (integer) when the fact was asserted or retracted.
- **@op** - Operation type as a boolean: `true` for assertions, `false` for retractions. (Mirrors `Flake.op` on disk; constants `"assert"` / `"retract"` are not accepted.)

Both annotations work uniformly for literal-valued and IRI-valued objects.

**Response:**

```json
[
  ["Alice", 30, 1, true],
  ["Alice", 30, 5, false],
  ["Alicia", 31, 5, true]
]
```

**Example:**

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": { "ex": "http://example.org/ns/" },
    "from": "mydb:main@t:1",
    "to": "mydb:main@t:latest",
    "select": ["?name", "?t", "?op"],
    "where": [
      { "@id": "ex:alice", "ex:name": { "@value": "?name", "@t": "?t", "@op": "?op" } }
    ],
    "orderBy": "?t"
  }'
```

**SPARQL History Query:**

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/sparql-query" \
  -d 'PREFIX ex: <http://example.org/ns/>
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?name ?t ?op
FROM <mydb:main@t:1>
TO <mydb:main@t:latest>
WHERE {
  << ex:alice ex:name ?name >> f:t ?t .
  << ex:alice ex:name ?name >> f:op ?op .
}
ORDER BY ?t'
```

### POST /stream/query and /stream/query/{ledger}

Stream SELECT results incrementally as newline-delimited JSON
(`application/x-ndjson`) instead of buffering the whole result into one
response body, with a heartbeat that keeps long-running queries alive past
proxy idle timeouts. Same content-type negotiation as `/query` (JSON-LD or
`application/sparql-query`). Two forms: ledger-scoped (ledger in the greedy
path tail) and connection-scoped (`POST /stream/query`, no path ledger — the
ledger(s) come from JSON-LD `from`/`fromNamed` or SPARQL `FROM`).

```bash
curl -N -X POST http://localhost:8090/v1/fluree/stream/query/my/ledger \
  -H 'Content-Type: application/json' \
  -d '{"@context":{"ex":"http://example.org/"},"select":["?name"],"where":{"@id":"?s","ex:name":"?name"}}'
```

The response is one self-describing JSON record per line (`head` → `row`* with
interleaved `heartbeat`s → a terminal `end` or `error`). SELECT only; ASK,
CONSTRUCT/DESCRIBE, `selectOne`, hydration, and history (JSON-LD `to` / SPARQL
`FROM … TO …`) are rejected with `4xx`. Policy, `from`/`fromNamed`, SPARQL
`FROM`, and multi-ledger queries (JSON-LD and SPARQL) are enforced identically
to `/query`. See **[Streaming query (NDJSON)](streaming-query.md)** for the full
record protocol, the terminal-record (truncation) contract, policy behavior,
and client examples.

### POST /multi-query

Execute a bundle of independent JSON-LD and/or SPARQL queries in parallel against a single shared snapshot moment, with envelope-level `@context` / `opts` defaults that lift into each sub-query.

**URL:**
```
POST /multi-query
```

**Request Headers:**
```http
Content-Type: application/json
```

**Request Body (envelope):**

```json
{
  "@context": { "ex": "http://example.org/" },
  "asOf":     "2024-01-01T12:00:00Z",
  "opts":     { "meta": true, "timeoutMs": 30000, "maxConcurrency": 8 },
  "queries": {
    "alice": {
      "language": "jsonld",
      "query": {
        "from":   "myledger:main",
        "select": ["?name"],
        "where":  { "@id": "?p", "ex:name": "?name" }
      }
    },
    "bob": {
      "language": "sparql",
      "query":    "SELECT ?name FROM <other:main> WHERE { ?p ex:name ?name }"
    }
  }
}
```

**Response:**

```json
{
  "status":  "ok",
  "snapshot": {
    "asOf":    "2024-01-01T12:00:00Z",
    "ledgers": { "myledger:main": 1042, "other:main": 87 }
  },
  "results": { "alice": [...], "bob": {...} }
}
```

(`errors` is omitted when no sub-query failed; `meta` is omitted when `opts.meta` is unset.) Clients branch on `body.status` (`"ok"` | `"partial"` | `"all_failed"`) rather than HTTP code for the aggregate outcome; per-alias errors and timeouts live inside `errors` when present.

**Status Codes:**
- `200 OK` — envelope parsed and executed; body's `status` reports the per-alias aggregate (including `all_failed`).
- `400 Bad Request` — envelope validation failed (bounds, `asOf` collision, missing `from`, history query, envelope `max-fuel`, `maxConcurrency: 0`, malformed body).
- `401 Unauthorized` — authentication required and missing.
- `500 Internal Server Error` — envelope infra failed (snapshot resolution couldn't load a ledger; response exceeds the size cap during assembly).

**Full reference:** see [Multi-query envelope](multi-query.md) for the complete envelope contract, merge rules, snapshot semantics, bounds table, examples, and the explicit list of current limitations (history queries, envelope-level fuel budget, response cap enforcement, SPARQL policy gap).

### GET/POST /explain

Return a query plan without executing the query. Accepts the same body formats and authentication as `/query` (JSON-LD, SPARQL via `application/sparql-query` or `?query=`, and JWS/VC signed requests).

**URL:**
```
GET  /explain[/{ledger...}]
POST /explain[/{ledger...}]
```

**Behavior:**
- JSON-LD body: returns the logical plan for the parsed query.
- SPARQL body: returns the plan for the parsed SPARQL query. The ledger-scoped endpoint (`/explain/{ledger}`) rejects queries containing `FROM` / `FROM NAMED` — strip dataset clauses to explain the core plan.
- SPARQL UPDATE is rejected (HTTP 400) — use `/update` for updates.
- Same ledger-scope enforcement for Bearer tokens as `/query`.

**Response:**

A JSON object describing the logical / physical plan. Shape mirrors the query engine's internal plan representation; treat it as informational and non-stable across releases.

**Status Codes:**
- `200 OK` — plan returned
- `400 Bad Request` — SPARQL UPDATE sent, or `FROM` clauses on the ledger-scoped explain
- `401 Unauthorized` — authentication required and missing
- `404 Not Found` — ledger not found or not authorized

**Examples:**

```bash
# Explain a SPARQL query
curl -X POST http://localhost:8090/v1/fluree/explain/mydb \
  -H "Content-Type: application/sparql-query" \
  --data 'SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10'

# Explain a JSON-LD query
curl -X POST http://localhost:8090/v1/fluree/explain/mydb \
  -H "Content-Type: application/json" \
  -d '{"select":["?s"],"where":{"@id":"?s"}}'
```

## Nameservice Metadata

The standalone server does not expose a general-purpose `POST /nameservice/query`
endpoint. Use `GET /ledgers` to list ledgers and graph sources,
`GET /info/{ledger-id}` for metadata about a single ledger or graph source, and
`GET /nameservice/snapshot` for authenticated remote-sync snapshots.

## Ledger Management Endpoints

### GET /ledgers

List all ledgers and graph sources.

**URL:**
```
GET /ledgers
```

**Response:**

```json
{
  "ledgers": [
    {
      "ledger_id": "mydb:main",
      "branch": "main",
      "commit_t": 5,
      "index_t": 5,
      "created": "2024-01-22T10:00:00.000Z",
      "last_updated": "2024-01-22T10:30:00.000Z"
    },
    {
      "ledger_id": "mydb:dev",
      "branch": "dev",
      "commit_t": 3,
      "index_t": 2,
      "created": "2024-01-22T11:00:00.000Z",
      "last_updated": "2024-01-22T11:15:00.000Z"
    }
  ]
}
```

**Example:**

```bash
curl http://localhost:8090/v1/fluree/ledgers
```

For metadata about a specific ledger or graph source, use `GET /info/{ledger-id}`.
To create a ledger, use `POST /create`.

### POST /create

Create a new ledger.

**URL:**
```
POST /create
```

**Authentication:** When admin auth is enabled (`--admin-auth-mode=required`), requires Bearer token from a trusted issuer. See [Admin Authentication](#admin-authentication).

**Request Body:**

```json
{
  "ledger": "mydb:main"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger` | string | Yes | Ledger ID (e.g., "mydb" or "mydb:main") |

**Response:**

```json
{
  "ledger": "mydb:main",
  "t": 0,
  "commit_id": "bafybeig...commitT0"
}
```

| Field | Description |
|-------|-------------|
| `ledger` | Normalized ledger ID |
| `t` | Transaction time (0 for new ledger) |
| `commit_id` | ContentId of the initial commit |

**Status Codes:**
- `201 Created` - Ledger created successfully
- `400 Bad Request` - Invalid request body
- `401 Unauthorized` - Bearer token required (when admin auth enabled)
- `409 Conflict` - Ledger already exists
- `500 Internal Server Error` - Server error

**Examples:**

```bash
# Create ledger (no auth required in default mode)
curl -X POST http://localhost:8090/v1/fluree/create \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main"}'

# Create ledger with auth token (when admin auth enabled)
curl -X POST http://localhost:8090/v1/fluree/create \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJ..." \
  -d '{"ledger": "mydb:main"}'

# Create with short ledger ID (auto-resolves to :main)
curl -X POST http://localhost:8090/v1/fluree/create \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb"}'
```

### POST /drop

Drop a whole ledger (every branch under the supplied name) or, as a
fallback, a graph source with the same name.

**URL:**
```
POST /drop
```

**Authentication:** When admin auth is enabled (`--admin-auth-mode=required`), requires Bearer token from a trusted issuer. See [Admin Authentication](#admin-authentication).

**Request Body:**

```json
{
  "ledger": "mydb",
  "hard": false
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger` | string | Yes | Ledger name (e.g., `"mydb"`). Any branch-qualified form (including `"mydb:main"`) is **rejected** with a `400` — use the [`POST /drop-branch`](#post-drop-branch) endpoint (or call `drop_branch` in the Rust API) to drop a single branch. |
| `hard` | boolean | No | If `true`, delete managed storage artifacts and purge the nameservice records. Default: `false` (soft drop). |

**Scope:**

`/drop` operates on the **whole ledger** — every branch under the ledger name, including any retracted-but-not-purged branches. Branches are dropped leaf-first so that if the operation aborts mid-way the surviving state stays consistent (orphan parents, never dangling children). The cross-branch `@shared/dicts/` namespace is cleaned up at the very end.

**Drop Modes:**

- **Soft drop** (`hard: false`, default): Marks every branch as retracted in the nameservice and preserves storage artifacts. Aliases remain reserved; normal create/load paths treat the ledger as unavailable.
- **Hard drop** (`hard: true`): Deletes managed storage artifacts for every branch and purges the nameservice records so the name can be reused. **This is irreversible for deleted artifacts.**

If no ledger is found by name, the server tries the same name as a graph source on branch `main`. Graph source hard-drop cleanup is best effort; graph-source fallback responses omit `branches_dropped` and `files_deleted`.

**Response:**

```json
{
  "ledger_id": "mydb",
  "status": "dropped",
  "files_deleted": 73,
  "branches_dropped": ["mydb:feature-x", "mydb:dev", "mydb:main"]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ledger_id` | string | Ledger name (or graph source ID if the graph-source fallback handled the request) |
| `status` | string | Aggregate status across branches. One of: `"dropped"`, `"already_retracted"`, `"not_found"` |
| `files_deleted` | integer | Number of managed storage artifacts deleted (sum across branches + `@shared/dicts/` cleanup); omitted when zero |
| `branches_dropped` | string[] | Per-branch `ledger_id`s that were dropped, in leaf-first order; omitted when empty |
| `warnings` | string[] | Non-fatal cleanup warnings; omitted when empty |

**Status Codes:**
- `200 OK` - Drop successful (or already dropped/not found)
- `400 Bad Request` - Invalid request body, or any branch-qualified ledger id was supplied
- `401 Unauthorized` - Bearer token required (when admin auth enabled)
- `500 Internal Server Error` - Branch enumeration failed, or another unrecoverable error

**Drop Sequence:**

1. Parses input. `"mydb"` is the canonical form; any branch-qualified id (`"mydb:main"`, `"mydb:dev"`, …) returns a `400`.
2. Enumerates every NsRecord under the ledger name (including retracted ones).
3. Sorts branches leaf-first via the `source_branch` parent pointers.
4. Cancels and waits for pending background indexing on each branch.
5. For each branch (leaf-first): deletes managed storage artifacts (hard mode) and retracts (soft) or removes the NS record (hard). Hard mode uses the parent-aware drop path so child counts on surviving parents stay accurate even under partial failure.
6. Hard mode only: wipes the cross-branch `{ledger_name}/@shared/dicts/` namespace.
7. Disconnects every branch from the ledger cache.

**Idempotency:**

Safe to call multiple times:
- Returns `"already_retracted"` when every branch was already retracted (hard mode still proceeds with cleanup for these).
- Returns `"not_found"` without touching storage when no nameservice record exists for the ledger name. Truly orphaned artifacts with no nameservice pointer are **not** swept by `/drop`; that's a separate admin concern.

**Examples:**

```bash
# Soft drop the whole "mydb" ledger
curl -X POST http://localhost:8090/v1/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb"}'

# Hard drop (delete every branch's artifacts + @shared/dicts - IRREVERSIBLE)
curl -X POST http://localhost:8090/v1/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "hard": true}'

# Drop with auth token (when admin auth enabled)
curl -X POST http://localhost:8090/v1/fluree/drop \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJ..." \
  -d '{"ledger": "mydb", "hard": true}'

# Backwards-compatible form (accepted with a warning; prefer the bare name)
curl -X POST http://localhost:8090/v1/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main"}'
```

### GET /context/{ledger...}

Get the default JSON-LD context for a ledger.

**URL:**
```
GET /context/{ledger-id}
```

**Path Parameters:**
- `ledger-id`: Ledger identifier (e.g., `mydb` or `mydb:main`)

**Response:**

```json
{
  "@context": {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "ex": "http://example.org/"
  }
}
```

If no default context has been set, `"@context"` is `null`.

**Status Codes:**
- `200 OK` - Context returned (may be `null`)
- `404 Not Found` - Ledger does not exist

**Example:**

```bash
curl http://localhost:8090/v1/fluree/context/mydb:main
```

### PUT /context/{ledger...}

Replace the default JSON-LD context for a ledger.

**URL:**
```
PUT /context/{ledger-id}
```

**Path Parameters:**
- `ledger-id`: Ledger identifier (e.g., `mydb` or `mydb:main`)

**Request Body:**

A JSON object mapping prefixes to IRIs. Either a bare object or wrapped in `{"@context": {...}}`:

```json
{
  "ex": "http://example.org/",
  "foaf": "http://xmlns.com/foaf/0.1/",
  "schema": "http://schema.org/"
}
```

**Response (success):**

```json
{
  "status": "updated"
}
```

**Status Codes:**
- `200 OK` - Context replaced successfully
- `400 Bad Request` - Body is not a valid JSON object; or peer mode (writes not available)
- `404 Not Found` - Ledger does not exist
- `409 Conflict` - Concurrent update conflict (retry the request)

**Concurrency:** The update uses compare-and-set semantics internally (up to 3 retries). A 409 means all retries were exhausted — this is rare and indicates heavy concurrent updates.

**Cache invalidation:** After a successful update, the server invalidates the cached ledger state. Subsequent queries will use the new context.

**Examples:**

```bash
# Set context
curl -X PUT http://localhost:8090/v1/fluree/context/mydb:main \
  -H "Content-Type: application/json" \
  -d '{"ex": "http://example.org/", "foaf": "http://xmlns.com/foaf/0.1/"}'

# Wrapped form also accepted
curl -X PUT http://localhost:8090/v1/fluree/context/mydb:main \
  -H "Content-Type: application/json" \
  -d '{"@context": {"ex": "http://example.org/"}}'
```

### POST /branch

Create a new branch for a ledger.

**URL:**
```
POST /branch
```

**Authentication:** When admin auth is enabled (`--admin-auth-mode=required`), requires Bearer token from a trusted issuer. See [Admin Authentication](#admin-authentication).

**Request Body:**

```json
{
  "ledger": "mydb",
  "branch": "feature-x",
  "source": "main",
  "at": "t:5"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger` | string | Yes | Ledger name without branch suffix (e.g., "mydb") |
| `branch` | string | Yes | New branch name to create (e.g., "feature-x") |
| `source` | string | No | Source branch to create from. Default: `"main"` |
| `at` | string | No | Commit on the source branch to start from. `"t:N"` for a transaction number, or a hex digest / full CID for prefix resolution. When omitted, the branch starts at the source's current HEAD. `t:` / prefix resolution requires the source to be indexed. |

**Response:**

```json
{
  "ledger_id": "mydb:feature-x",
  "branch": "feature-x",
  "source": "main",
  "t": 5
}
```

| Field | Description |
|-------|-------------|
| `ledger_id` | Full ledger:branch identifier for the new branch |
| `branch` | Branch name |
| `source` | Source branch this was created from |
| `t` | Transaction time of the commit at the branch point |

**Status Codes:**
- `201 Created` - Branch created successfully
- `400 Bad Request` - Invalid request body (including malformed `at` value)
- `401 Unauthorized` - Bearer token required (when admin auth enabled)
- `404 Not Found` - Source branch does not exist, or `at` commit is not reachable from source HEAD
- `409 Conflict` - Branch already exists
- `500 Internal Server Error` - Server error

**Examples:**

```bash
# Create branch from main (default source)
curl -X POST http://localhost:8090/v1/fluree/branch \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "branch": "feature-x"}'

# Create branch from a specific source branch
curl -X POST http://localhost:8090/v1/fluree/branch \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "branch": "staging", "source": "dev"}'

# Branch at a historical commit on main
curl -X POST http://localhost:8090/v1/fluree/branch \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "branch": "rewind", "at": "t:5"}'
```

### GET /branch/{ledger-name}

List all non-retracted branches for a ledger.

**URL:**
```
GET /branch/{ledger-name}
```

**Response:**

```json
[
  {
    "branch": "main",
    "ledger_id": "mydb:main",
    "t": 5
  },
  {
    "branch": "feature-x",
    "ledger_id": "mydb:feature-x",
    "t": 5,
    "source": "main"
  }
]
```

| Field | Description |
|-------|-------------|
| `branch` | Branch name |
| `ledger_id` | Full ledger:branch identifier |
| `t` | Current transaction time on this branch |
| `source` | Source branch (only present for branches created via `/branch`) |

**Examples:**

```bash
curl http://localhost:8090/v1/fluree/branch/mydb
```

### POST /drop-branch

Drop a branch from a ledger. Admin-protected.

**URL:**
```
POST /drop-branch
```

**Request body:**

```json
{
  "ledger": "mydb",
  "branch": "feature-x"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger` | string | Yes | Ledger name without branch suffix (e.g., "mydb") |
| `branch` | string | Yes | Branch name to drop (e.g., "feature-x") |

**Response body (200 OK):**

```json
{
  "ledger_id": "mydb:feature-x",
  "status": "dropped",
  "deferred": false,
  "files_deleted": 5,
  "cascaded": [],
  "warnings": []
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ledger_id` | string | Full ledger:branch identifier of the dropped branch |
| `status` | string | Drop status (`"dropped"`, `"already_retracted"`, `"not_found"`) |
| `deferred` | boolean | `true` if the branch has children — retracted but storage preserved |
| `files_deleted` | integer | Number of storage artifacts removed; omitted when zero |
| `cascaded` | string[] | List of ancestor branch ledger_ids that were cascade-dropped; omitted when empty |
| `warnings` | string[] | Any non-fatal warnings during the drop; omitted when empty |

**Behavior:**

- **Cannot drop `main`**: Returns 400 Bad Request.
- **Leaf branch** (no children): Fully drops — deletes storage artifacts, purges NsRecord, decrements parent's child count. If the parent was previously retracted and its child count reaches 0, the parent is cascade-dropped too.
- **Branch with children** (`branches > 0`): Retracted (hidden from listings, rejects new transactions) but storage is preserved for children. When the last child is eventually dropped, the retracted parent is cascade-purged automatically.

**Status codes:**

- `200 OK` - Branch dropped (or deferred) successfully
- `400 Bad Request` - Cannot drop the main branch
- `404 Not Found` - Ledger or branch does not exist
- `500 Internal Server Error` - Server error

**Examples:**

```bash
# Drop a leaf branch
curl -X POST http://localhost:8090/v1/fluree/drop-branch \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "branch": "feature-x"}'

# Drop a branch with children (will be deferred)
curl -X POST http://localhost:8090/v1/fluree/drop-branch \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "branch": "dev"}'
```

### POST /drop-graph

Drop a single named graph from one branch of a ledger by transactionally
retracting every triple currently asserted under that graph IRI. The drop
runs as a normal commit at `t = current_t + 1` — history at earlier `t`
values is preserved. Admin-protected.

**URL:**
```
POST /drop-graph
```

**Request body:**

```json
{
  "ledger": "mydb:main",
  "graph": "urn:example:org/payroll"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger` | string | Yes | Full ledger identifier. A bare ledger name (`"mydb"`) is normalized to `"mydb:main"`. |
| `graph` | string | Yes | Full **absolute** IRI of the named graph to drop. Must have a `<scheme>:<rest>` head (relative references like `payroll` are rejected) and contain no whitespace or RFC 3987-excluded characters. Leading/trailing whitespace is rejected, not trimmed. |

**Response body (200 OK):**

```json
{
  "ledger_id": "mydb:main",
  "graph_iri": "urn:example:org/payroll",
  "retracted": 42,
  "committed": true,
  "t": 18
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ledger_id` | string | Normalized `ledger:branch` identifier the drop targeted |
| `graph_iri` | string | Graph IRI that was dropped (echoed) |
| `retracted` | integer | Number of flakes retracted by the drop commit; `0` for a no-op |
| `committed` | boolean | `true` when a new commit was produced; `false` for a no-op drop on an empty graph |
| `t` | integer | Current commit `t` for the branch after the drop |

**Behavior:**

- **Transactional and history-preserving.** A query `as-of` an earlier `t` still sees the graph populated.
- **Per-branch.** Only affects the targeted branch — sibling branches that share the same graph IRI are not touched.
- **Refuses system graphs.** The default graph, `urn:fluree:{ledger_id}#txn-meta`, and `urn:fluree:{ledger_id}#config` cannot be dropped (400 Bad Request).
- **Refuses unknown graphs.** Returns 404 when `graph` is not registered in the ledger's graph registry — the call never auto-registers a new graph slot.
- **Idempotent.** A second call on an already-empty graph returns `committed: false`, `retracted: 0` without producing a commit.

**Status codes:**

- `200 OK` - Drop succeeded (commit produced or no-op)
- `400 Bad Request` - Malformed IRI, empty IRI, or system-graph IRI
- `401`/`403` - Admin token required and absent/invalid
- `404 Not Found` - Ledger or graph IRI not found
- `500 Internal Server Error` - Server error

**Examples:**

```bash
# Drop a named graph on the default branch
curl -X POST http://localhost:8090/v1/fluree/drop-graph \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "graph": "urn:example:org/payroll"}'

# Drop on a non-default branch
curl -X POST http://localhost:8090/v1/fluree/drop-graph \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:feature-x", "graph": "http://example.org/graphs/scratch"}'
```

### POST /rebase

Rebase a branch onto its source branch's current HEAD. Admin-protected.

**URL:**
```
POST /rebase
```

**Request body:**

```json
{
  "ledger": "mydb",
  "branch": "feature-x",
  "strategy": "take-both"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger` | string | Yes | Ledger name without branch suffix (e.g., "mydb") |
| `branch` | string | Yes | Branch name to rebase (e.g., "feature-x") |
| `strategy` | string | No | Conflict resolution strategy (default: "take-both"). Options: `take-both`, `abort`, `take-source`, `take-branch`, `skip` |

**Response body (200 OK):**

```json
{
  "ledger_id": "mydb:feature-x",
  "branch": "feature-x",
  "fast_forward": false,
  "replayed": 3,
  "skipped": 0,
  "conflicts": 1,
  "failures": 0,
  "total_commits": 3,
  "source_head_t": 8
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ledger_id` | string | Full ledger:branch identifier |
| `branch` | string | Branch name |
| `fast_forward` | bool | `true` if the branch had no unique commits |
| `replayed` | number | Number of commits successfully replayed |
| `skipped` | number | Number of commits skipped (Skip strategy) |
| `conflicts` | number | Number of conflicts detected |
| `failures` | number | Number of commits that failed validation |
| `total_commits` | number | Total branch commits considered |
| `source_head_t` | number | Transaction time of the source branch HEAD |

**Conflict strategies:**

| Strategy | Behavior |
|----------|----------|
| `take-both` | Replay as-is, both values coexist (multi-cardinality) |
| `abort` | Fail on first conflict, no changes applied |
| `take-source` | Drop branch's conflicting flakes (source wins) |
| `take-branch` | Keep branch's flakes, retract source's conflicting values |
| `skip` | Skip entire commit if any flakes conflict |

**Status codes:**

- `200 OK` - Rebase completed successfully
- `400 Bad Request` - Cannot rebase main, invalid strategy, or missing branch point
- `404 Not Found` - Ledger or branch does not exist
- `409 Conflict` - Rebase aborted due to conflict (abort strategy)
- `500 Internal Server Error` - Server error

**Examples:**

```bash
# Rebase with default strategy (take-both)
curl -X POST http://localhost:8090/v1/fluree/rebase \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "branch": "feature-x"}'

# Rebase with abort strategy (fail on conflicts)
curl -X POST http://localhost:8090/v1/fluree/rebase \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "branch": "feature-x", "strategy": "abort"}'
```

### POST /merge

Merge a source branch into a target branch. Admin-protected.

Fast-forward merges copy the source commit chain into the target namespace and advance the target HEAD. When the target has diverged, Fluree performs a general merge: it computes the source and target deltas since their common ancestor, resolves overlapping `(s, p, g)` conflicts according to the requested strategy, and creates a merge commit on the target branch.

**URL:**
```
POST /merge
```

**Request body:**

```json
{
  "ledger": "mydb",
  "source": "feature-x",
  "target": "dev",
  "strategy": "take-both"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger` | string | Yes | Ledger name without branch suffix (e.g., "mydb") |
| `source` | string | Yes | Source branch to merge from (e.g., "feature-x") |
| `target` | string | No | Target branch to merge into (defaults to source's parent branch) |
| `strategy` | string | No | Conflict resolution strategy for non-fast-forward merges. Defaults to `take-both`. Options: `take-both`, `abort`, `take-source`, `take-branch` |

**Conflict strategies:**

| Strategy | Behavior |
|----------|----------|
| `take-both` | Keep source flakes as-is, so both source and target values can coexist |
| `abort` | Fail if conflicts are detected; no merge commit is created |
| `take-source` | Source wins: keep source flakes and retract target's conflicting values |
| `take-branch` | Target wins: drop source flakes for conflicting keys |

`skip` is a rebase-only strategy and is not supported for non-fast-forward merges.

**Response body (200 OK):**

```json
{
  "ledger_id": "mydb:dev",
  "target": "dev",
  "source": "feature-x",
  "fast_forward": false,
  "new_head_t": 8,
  "commits_copied": 3,
  "conflict_count": 1,
  "strategy": "take-both"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ledger_id` | string | Full ledger:branch identifier of the target |
| `target` | string | Target branch name |
| `source` | string | Source branch name |
| `fast_forward` | bool | Whether this merge advanced the target directly to the source HEAD |
| `new_head_t` | number | New commit HEAD transaction time of the target |
| `commits_copied` | number | Number of commit blobs copied to the target namespace |
| `conflict_count` | number | Number of overlapping `(s, p, g)` keys detected during a non-fast-forward merge |
| `strategy` | string | Conflict strategy used for a non-fast-forward merge. Omitted for fast-forward merges |

**Status codes:**

- `200 OK` - Merge completed successfully
- `400 Bad Request` - Source has no branch point (e.g., main), self-merge, unknown strategy, or unsupported merge strategy
- `404 Not Found` - Ledger or branch does not exist
- `409 Conflict` - Merge aborted due to conflicts when using the `abort` strategy, or the target HEAD changed during commit publishing
- `500 Internal Server Error` - Server error

**Examples:**

```bash
# Merge feature-x into its parent (inferred from branch point)
curl -X POST http://localhost:8090/v1/fluree/merge \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "source": "feature-x"}'

# Merge dev into main (explicit target)
curl -X POST http://localhost:8090/v1/fluree/merge \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "source": "dev", "target": "main"}'

# Non-fast-forward merge with source-winning conflict resolution
curl -X POST http://localhost:8090/v1/fluree/merge \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "source": "dev", "target": "main", "strategy": "take-source"}'
```

### GET /merge-preview/{ledger-name}

Read-only preview of merging a source branch into a target branch. Returns the rich diff — ahead/behind commit summaries, conflict keys, and fast-forward eligibility — without mutating any nameservice or content store state.

Bearer token required when `data_auth.mode = required`; reads are gated on `bearer.can_read(ledger)`.

**URL:**
```
GET /merge-preview/{ledger-name}?source={source}&target={target}&max_commits={n}&max_conflict_keys={n}&include_conflicts={bool}&include_conflict_details={bool}&strategy={strategy}
```

**Path / Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `ledger` (path) | string | Yes | Ledger name (e.g., "mydb") |
| `source` | string | Yes | Source branch to merge from (e.g., "feature-x") |
| `target` | string | No | Target branch (defaults to the source's parent branch) |
| `max_commits` | number | No | Cap on per-side commit summaries returned (default 500). Server clamps to a hard maximum of 5,000 — values above are silently lowered. Bounds response size, **not** divergence-walk cost (the unbounded `count` is still computed). |
| `max_conflict_keys` | number | No | Cap on conflict keys returned (default 200). Server clamps to a hard maximum of 5,000. Bounds response size, **not** the conflict-delta walks. |
| `include_conflicts` | bool | No | When false, skips the conflict computation (default true). Use this to make the preview cheap on diverged branches. |
| `include_conflict_details` | bool | No | When true, includes source/target flake values for the returned conflict keys. Defaults to false. Details are computed after `max_conflict_keys` is applied. |
| `strategy` | string | No | Strategy used to annotate conflict details. Defaults to `take-both`. Options: `take-both`, `abort`, `take-source`, `take-branch`. |

**Response body (200 OK):**

```json
{
  "source": "feature-x",
  "target": "main",
  "ancestor": { "commit_id": "bafy...", "t": 5 },
  "ahead": {
    "count": 3,
    "commits": [
      { "t": 8, "commit_id": "bafy...", "time": "2026-04-25T12:00:00Z",
        "asserts": 2, "retracts": 0, "flake_count": 2, "message": null }
    ],
    "truncated": false
  },
  "behind": { "count": 1, "commits": [...], "truncated": false },
  "fast_forward": false,
  "mergeable": true,
  "conflicts": {
    "count": 1,
    "keys": [{ "s": [100, "alice"], "p": [100, "status"], "g": null }],
    "truncated": false,
    "strategy": "take-source",
    "details": [
      {
        "key": { "s": [100, "alice"], "p": [100, "status"], "g": null },
        "source_values": [["ex:alice", "ex:status", "active", "xsd:string", true]],
        "target_values": [["ex:alice", "ex:status", "archived", "xsd:string", true]],
        "resolution": {
          "source_action": "kept",
          "target_action": "retracted",
          "outcome": "source-wins"
        }
      }
    ]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `source` | string | Source branch name |
| `target` | string | Target branch name (resolved from default when not supplied) |
| `ancestor` | object \| null | Common ancestor `{commit_id, t}`. `null` when both heads are absent |
| `ahead` | object | Commits on source not on target (`count`, `commits`, `truncated`) |
| `behind` | object | Commits on target not on source |
| `fast_forward` | bool | True when target HEAD == ancestor (or both heads absent) |
| `mergeable` | bool | False only when the selected preview strategy would abort, e.g. `strategy=abort` with conflicts. This is a strategy/conflict signal, not full transaction validation. `mergeable=true` does not guarantee a subsequent `POST /merge` will succeed; it only reflects the conflict/strategy interaction at preview time. |
| `conflicts` | object | Overlapping `(s, p, g)` keys touched on both sides since the ancestor. Empty when `fast_forward` or `include_conflicts=false` |

Per-commit summaries (`ahead.commits[]` / `behind.commits[]`) are newest-first and include assert/retract counts plus an optional `message` extracted from `txn_meta` when an `f:message` string entry is present.

When `include_conflict_details=true`, `conflicts.details[]` contains one entry for each returned conflict key. `source_values` and `target_values` are the current asserted values for that key at each branch HEAD, using the same resolved flake tuple format as `/show`: `[subject, predicate, object, datatype, operation]`, with an optional metadata object as the 6th tuple item. The `resolution` object is an annotation only; preview does not apply the strategy or mutate state.

**Status codes:**

- `200 OK` — Preview computed successfully
- `400 Bad Request` — Source has no branch point (e.g., main), `source == target`, unknown strategy, unsupported preview strategy, `include_conflict_details=true` with `include_conflicts=false`, or `strategy=abort` with `include_conflicts=false`
- `401 Unauthorized` — Bearer token required
- `404 Not Found` — Ledger or branch does not exist (or bearer cannot read it)

**Examples:**

```bash
# Default target (source's parent), defaults for caps and conflict computation
curl "http://localhost:8090/v1/fluree/merge-preview/mydb?source=feature-x"

# Counts only — skip the conflict walks for a faster response
curl "http://localhost:8090/v1/fluree/merge-preview/mydb?source=dev&target=main&include_conflicts=false"

# Cap commit lists at 50 per side
curl "http://localhost:8090/v1/fluree/merge-preview/mydb?source=dev&max_commits=50"

# Include value details and labels for a source-winning merge
curl "http://localhost:8090/v1/fluree/merge-preview/mydb?source=dev&target=main&include_conflict_details=true&strategy=take-source"
```

### GET /info/{ledger-id}

Get ledger metadata. Used by the CLI for `info`, `push`, `pull`, and `clone`.

**URL:**
```
GET /info/{ledger-id}
```

**Path Parameters:**
- `ledger-id`: Ledger ID (e.g., "mydb" or "mydb:main")

**Response (non-proxy mode):**

Returns comprehensive ledger metadata including namespace codes, property stats, and class counts. Always includes:

```json
{
  "ledger_id": "mydb:main",
  "t": 42,
  "commitId": "bafybeig...headCommitCid",
  "indexId": "bafybeig...indexRootCid",
  "namespaces": { ... },
  "properties": { ... },
  "classes": [ ... ]
}
```

**Response (proxy storage mode):**

Returns simplified nameservice-only metadata:

```json
{
  "ledger_id": "mydb:main",
  "t": 42,
  "commit_head_id": "bafybeig...commitCid",
  "index_head_id": "bafybeig...indexCid"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger_id` | string | Yes | Canonical ledger ID |
| `t` | integer | **Yes** | Current transaction time. Used by push/pull for head comparison. |
| `commitId` | string | No | Head commit CID (non-proxy mode) |
| `commit_head_id` | string | No | Head commit CID (proxy mode) |

> **Important:** The `t` field is required by the CLI for push/pull/clone operations. See [CLI-Server API Contract](../design/cli-server-contract.md) for details.

**Optional query parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `realtime_property_details` | boolean | true | When `false`, use the lighter fast novelty-aware stats path instead of the default full lookup-backed path |
| `include_property_datatypes` | boolean | true | Include datatype info for properties |
| `include_property_estimates` | boolean | false | Include index-derived NDV/selectivity estimates for properties |

**Status Codes:**
- `200 OK` - Ledger found
- `401 Unauthorized` - Authentication required
- `404 Not Found` - Ledger not found

**Examples:**

```bash
# Get ledger info
curl "http://localhost:8090/v1/fluree/info/mydb:main"

# With auth token
curl "http://localhost:8090/v1/fluree/info/mydb:main" \
  -H "Authorization: Bearer eyJ..."
```

### GET /exists/{ledger-id}

Check if a ledger exists in the nameservice.

**URL:**
```
GET /exists/{ledger-id}
```

**Path Parameters:**
- `ledger-id`: Ledger ID (e.g., "mydb" or "mydb:main")

**Response:**

```json
{
  "ledger": "mydb:main",
  "exists": true
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ledger` | string | Ledger ID (echoed back) |
| `exists` | boolean | Whether the ledger is registered in the nameservice |

**Status Codes:**
- `200 OK` - Check completed successfully (regardless of whether ledger exists)
- `500 Internal Server Error` - Server error

**Usage Notes:**

This is a lightweight check that only queries the nameservice without loading the ledger data. Use this to:

- Check if a ledger exists before attempting to load it
- Implement conditional create-or-load logic
- Validate ledger IDs in application code

**Examples:**

```bash
# Check a ledger ID
curl "http://localhost:8090/v1/fluree/exists/mydb:main"

# Conditional create-or-load in shell
if curl -s "http://localhost:8090/v1/fluree/exists/mydb" | jq -e '.exists == false' > /dev/null; then
  curl -X POST http://localhost:8090/v1/fluree/create \
    -H "Content-Type: application/json" \
    -d '{"ledger": "mydb"}'
fi
```

## System Endpoints

### GET /health

Health check endpoint for monitoring.

**URL:**
```
GET /health
```

**Response:**

```json
{
  "status": "healthy",
  "version": "0.1.0",
  "storage": "memory",
  "uptime_ms": 123456
}
```

**Status Codes:**
- `200 OK` - System healthy
- `503 Service Unavailable` - System unhealthy

**Example:**

```bash
curl http://localhost:8090/health
```

### GET /stats

Detailed server statistics.

**URL:**
```
GET /stats
```

**Response:**

```json
{
  "version": "0.1.0",
  "uptime_ms": 123456789,
  "storage": {
    "mode": "memory",
    "total_bytes": 12345678,
    "ledgers": 5
  },
  "queries": {
    "total": 1234,
    "active": 3,
    "average_duration_ms": 45
  },
  "transactions": {
    "total": 567,
    "average_duration_ms": 89
  },
  "indexing": {
    "active": true,
    "pending_ledgers": 2
  }
}
```

**Example:**

```bash
curl http://localhost:8090/v1/fluree/stats
```

## Events Endpoint

### GET /events

Server-Sent Events (SSE) stream of nameservice changes for ledgers and graph sources. Available on transaction servers only (not peers).

**Query parameters:**

| Parameter | Description |
|-----------|-------------|
| `all=true` | Subscribe to all ledgers and graph sources |
| `ledger=<id>` | Subscribe to a specific ledger (repeatable) |
| `graph-source=<id>` | Subscribe to a specific graph source (repeatable) |

**Event types:**

| Event | Description |
|-------|-------------|
| `ns-record` | A ledger or graph source was published/updated |
| `ns-retracted` | A ledger or graph source was deleted |

**Authentication:** Configurable via `--events-auth-mode none|optional|required`. See [Query peers and replication](../operations/query-peers.md) for full details including auth configuration, event payloads, and peer subscription setup.

## Graph Source Endpoints

> **Note:** HTTP endpoints for BM25 and vector index lifecycle management (create, sync, drop) are not yet implemented in the server. BM25 and vector indexes are currently managed via the Rust API (`Bm25CreateConfig`, `create_full_text_index`, `sync_bm25_index`, `drop_full_text_index`). See [BM25 Full-Text Search](../indexing-and-search/bm25.md) and [Vector Search](../indexing-and-search/vector-search.md) for API usage.
>
> BM25 search **is** available in queries via the `f:graphSource` / `f:searchText` pattern in where clauses — see the query documentation for details.

Graph source metadata can be discovered via `GET /ledgers` or `GET /info/{graph-source-id}`.

### POST {api_base_url}/iceberg/map

Map an Iceberg table (or R2RML-mapped relational source backed by Iceberg) as a graph source. Admin-protected — requires the admin Bearer token when an admin token is configured. Available only when the server is built with the `iceberg` feature.

**URL:**
```
POST {api_base_url}/iceberg/map
```

For the standalone server and Docker image defaults, this is:

```bash
POST http://localhost:8090/v1/fluree/iceberg/map
```

**Request Body:**

```json
{
  "name": "warehouse-orders",
  "mode": "rest",
  "catalog_uri": "https://polaris.example.com/api/catalog",
  "table": "sales.orders",
  "branch": "main",
  "r2rml": "@prefix rr: <http://www.w3.org/ns/r2rml#> . ...",
  "r2rml_type": "text/turtle",
  "warehouse": "prod",
  "auth_bearer": "…",
  "oauth2_token_url": "https://idp.example.com/token",
  "oauth2_client_id": "…",
  "oauth2_client_secret": "…",
  "no_vended_credentials": false,
  "s3_region": "us-east-1",
  "s3_endpoint": "https://s3.example.com",
  "s3_path_style": false,
  "table_location": "s3://bucket/warehouse/sales/orders"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Graph source name (required) |
| `mode` | string | `rest` (default) or `direct` |
| `catalog_uri` | string | REST catalog URI (required in `rest` mode) |
| `table` | string | Table identifier `namespace.table` (required in `rest` mode) |
| `table_location` | string | S3 table location (required in `direct` mode) |
| `r2rml` | string | Inline R2RML mapping (Turtle/JSON-LD). Omit to auto-generate a direct mapping. |
| `r2rml_type` | string | Media type of `r2rml` (`text/turtle`, `application/ld+json`) |
| `branch` | string | Branch name (default: `main`) |
| `auth_bearer` | string | Bearer token for catalog auth |
| `oauth2_*` | string | OAuth2 client-credentials flow for the catalog |
| `warehouse` | string | Warehouse identifier |
| `no_vended_credentials` | bool | Disable vended credentials |
| `s3_region`, `s3_endpoint`, `s3_path_style` | | S3 overrides for `direct` mode |

**Response:**

```json
{
  "graph_source_id": "warehouse-orders:main",
  "table_identifier": "sales.orders",
  "catalog_uri": "https://polaris.example.com/api/catalog",
  "connection_tested": true,
  "mapping_source": "r2rml-inline",
  "triples_map_count": 3,
  "mapping_validated": true
}
```

**Status Codes:**
- `201 Created` — graph source created
- `400 Bad Request` — missing required fields or invalid R2RML
- `401/403` — admin auth required
- `500 Internal Server Error` — catalog connection or mapping failure

See also the CLI wrapper: [fluree iceberg map](../cli/iceberg.md).

## Admin Endpoints

### POST /reindex

Trigger a full manual reindex for a ledger. Walks the entire commit chain and rebuilds the binary index from scratch using the server's configured indexer settings. Admin-protected — requires the admin Bearer token when admin auth is enabled.

This endpoint runs the reindex synchronously and returns when the new root is committed. For large ledgers it may run for many minutes; configure your HTTP client timeout accordingly. In peer mode, the request is forwarded to the transaction server.

**URL:**
```
POST /reindex
```

**Request Body:**

```json
{
  "ledger": "mydb:main"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ledger` | string | Ledger alias (`name` or `name:branch`). Required. |
| `opts`   | object | Reserved for future per-request indexer overrides. Currently accepted but ignored. |

**Example:**

```bash
curl -X POST http://localhost:8090/v1/fluree/reindex \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <admin-token>' \
  -d '{"ledger": "mydb:main"}'
```

**Response:**

```json
{
  "ledger_id": "mydb:main",
  "index_t": 42,
  "root_id": "fluree:cid:bafy…",
  "stats": {
    "flake_count": 184273,
    "leaf_count": 614,
    "branch_count": 23,
    "total_bytes": 47185920
  },
  "fuel": 1734.0
}
```

| Field | Description |
|-------|-------------|
| `ledger_id` | Ledger alias the reindex was run against |
| `index_t` | Transaction time the new index was built at (matches the head commit) |
| `root_id` | ContentId of the newly written index root |
| `stats.flake_count` | Total flakes in the rebuilt index |
| `stats.leaf_count` | Number of leaf nodes written |
| `stats.branch_count` | Number of branch nodes written |
| `stats.total_bytes` | Bytes written to storage during the reindex |
| `fuel` | Total decimal fuel charged for the reindex's CAS writes (1.000 per write + 1.000 per re-encoded leaflet in FLI3 leaves). `0.0` if the index was already current. See [Tracking and Fuel](../query/tracking-and-fuel.md#indexing-fuel) for the full schedule. |

**Status Codes:**
- `200 OK` — reindex complete
- `400 Bad Request` — missing/invalid `ledger`
- `401/403` — admin auth required
- `404 Not Found` — ledger does not exist
- `500 Internal Server Error` — reindex failed

When triggering indexing through the Rust API instead, see `Fluree::reindex` and `ReindexOptions`. For background incremental indexing (which runs automatically as commits are made), see [Background indexing](../indexing-and-search/background-indexing.md).

### POST /export/*ledger

Return ledger data as RDF in the requested format (Turtle, N-Triples, N-Quads, TriG, or JSON-LD). Server-side equivalent of `fluree export`.

**Auth bracket: admin-protected** — same middleware as `/create`, `/drop`, `/reindex`, and the branch admin endpoints. Today's implementation reads from the binary index without per-flake policy filtering, so it does not live in the data-read bracket alongside `/query` and `/show`. Adding policy-filtered streaming export would let it move to read-auth in the future.

**URL:**

```
POST /export/<ledger...>
```

**Request Body:**

```json
{
  "format": "turtle",
  "all_graphs": false,
  "graph": "http://example.org/people",
  "context": { "ex": "http://example.org/" },
  "at": "t:42"
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `format` | string | No | `"turtle"` | One of `turtle`/`ttl`, `ntriples`/`nt`, `nquads`/`n-quads`, `trig`, `jsonld`/`json-ld`/`json`. Case-insensitive. |
| `all_graphs` | bool | No | `false` | Export every named graph as a dataset. Requires `format` ∈ `trig` / `nquads`. Mutually exclusive with `graph`. |
| `graph` | string | No | — | IRI of a single named graph to export. Mutually exclusive with `all_graphs`. |
| `context` | object | No | ledger default | Prefix map for Turtle/TriG/JSON-LD output. Either a bare object or `{"@context": {…}}`. |
| `at` | string | No | latest | Time spec — integer (`"42"`), ISO-8601 datetime, or commit CID prefix. |

An empty body is treated as all-default (Turtle export at HEAD).

**Response Headers:**

| Format | Content-Type |
|--------|--------------|
| Turtle | `text/turtle; charset=utf-8` |
| N-Triples | `application/n-triples; charset=utf-8` |
| N-Quads | `application/n-quads; charset=utf-8` |
| TriG | `application/trig; charset=utf-8` |
| JSON-LD | `application/ld+json; charset=utf-8` |

**Response Body (200 OK):**

The raw RDF for the requested format. The reference server today buffers the full export in memory before responding; implementations are free to stream chunked bodies, and clients MUST be prepared to read until EOF.

**Status Codes:**

- `200 OK` — export complete
- `400 Bad Request` — unknown format; conflicting `all_graphs` + `graph`; `all_graphs` with non-dataset format; unknown graph IRI; malformed JSON; ledger not indexed (`ApiError::Config`)
- `401` / `403` — admin token required and absent/invalid
- `404 Not Found` — ledger does not exist
- `5xx` — storage / nameservice / encoding errors

**Peer mode:** Forwards to the transactor.

## Admin Authentication

Administrative endpoints (`/create`, `/drop`, `/reindex`, branch operations, and Iceberg mapping when enabled) can be protected with Bearer token authentication.

### Configuration

Enable admin authentication with CLI flags:

```bash
# Production: require trusted tokens
fluree-server \
  --admin-auth-mode=required \
  --admin-auth-trusted-issuer=did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK

# Development: no authentication (default)
fluree-server --admin-auth-mode=none
```

**Environment Variables:**
- `FLUREE_ADMIN_AUTH_MODE`: `none` (default) or `required`
- `FLUREE_ADMIN_AUTH_TRUSTED_ISSUERS`: Comma-separated list of trusted did:key identifiers

### Token Format

Admin tokens use the same JWS format as other Fluree tokens. Required claims:

```json
{
  "iss": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
  "exp": 1705932000,
  "sub": "admin@example.com"
}
```

| Claim | Required | Description |
|-------|----------|-------------|
| `iss` | Yes | Issuer did:key (must be in trusted issuers list) |
| `exp` | Yes | Expiration timestamp (Unix seconds) |
| `sub` | No | Subject identifier |
| `fluree.identity` | No | Identity for audit logging |

### Making Authenticated Requests

Include the token in the Authorization header:

```bash
curl -X POST http://localhost:8090/v1/fluree/create \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJhbGciOiJFZERTQSIsImp3ayI6ey..." \
  -d '{"ledger": "mydb:main"}'
```

### Issuer Trust

Tokens must be signed by a trusted issuer. Configure trusted issuers:

```bash
# Single issuer
--admin-auth-trusted-issuer=did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK

# Multiple issuers
--admin-auth-trusted-issuer=did:key:z6Mk... \
--admin-auth-trusted-issuer=did:key:z6Mn...

# Fallback to events auth issuers
--events-auth-trusted-issuer=did:key:z6Mk...
```

If no admin-specific issuers are configured, admin auth falls back to `--events-auth-trusted-issuer`.

### Response Codes

- `401 Unauthorized`: Missing or invalid Bearer token
- `401 Unauthorized`: Token expired
- `401 Unauthorized`: Untrusted issuer

## Error Responses

All endpoints may return error responses in this format (and should return `Content-Type: application/json`):

```json
{
  "error": "Human-readable error message",
  "status": 409,
  "@type": "err:db/Conflict",
  "cause": {
    "error": "Optional nested error detail",
    "status": 409,
    "@type": "err:db/SomeInnerError"
  }
}
```

See [Errors and Status Codes](errors.md) for complete error reference.

## CLI Compatibility Requirements

This section summarizes the contract that third-party server implementations (e.g., Solo) must follow to be compatible with the Fluree CLI (`fluree-db-cli`). The CLI discovers the API base URL via `fluree remote add` and constructs endpoint URLs as `{base_url}/{operation}/{ledger}`.

### Required endpoints

| Endpoint | CLI commands |
|----------|-------------|
| `GET /info/{ledger}` | `info`, `push`, `pull`, `clone` |
| `GET /show/{ledger}?commit=<ref>` | `show --remote` |
| `POST /query/{ledger}` | `query` (JSON-LD and SPARQL) |
| `POST /insert/{ledger}` | `insert` |
| `POST /upsert/{ledger}` | `upsert` |
| `GET /exists/{ledger}` | `clone` (pre-create check) |
| `GET /context/{ledger}` | `context get` |
| `PUT /context/{ledger}` | `context set` |
| `GET /ledgers` | `list --remote` |

For sync workflows (`clone`/`push`/`pull`), these additional endpoints are needed:

| Endpoint | CLI commands | Notes |
|----------|-------------|-------|
| `POST /push/{ledger}` | `push` | Required for push |
| `GET /commits/{ledger}` | `clone`, `pull` | Paginated export fallback |
| `POST /pack/{ledger}` | `clone`, `pull` | Preferred bulk transport; CLI falls back to `/commits` on 404/405/501 |
| `GET /storage/ns/{ledger}` | `clone`, `pull` | Pack preflight (head CID discovery) |

### Critical response field: `t`

The `GET /info/{ledger}` response **must** include a `t` field (integer) representing the current transaction time. This field is used by the CLI for:

- **push**: Comparing `local_t` vs `remote_t` to determine what commits to send and detect divergence
- **pull**: Comparing `remote_t` vs `local_t` to determine if new commits are available
- **clone**: Guarding against cloning empty ledgers (`t == 0`) and displaying progress

Omitting `t` from the info response will cause `push` and `pull` to fail with `"remote ledger-info response missing 't'"`.

### Transaction response format

The `/insert` and `/upsert` endpoints should return a JSON object. The CLI displays the full response as pretty-printed JSON. Common fields include `t`, `tx-id`, and `commit.hash`, but the exact shape is not prescribed — the CLI does not parse individual fields from transaction responses.

### Authentication

All endpoints accept `Authorization: Bearer <token>`. On `401`, the CLI attempts a single token refresh (if OIDC is configured) and retries. See [Auth contract](../design/auth-contract.md) for the full authentication lifecycle.

### Error responses

Error bodies should be JSON with an `error` or `message` field. The CLI extracts the first available string from `message` or `error` for display. Plain-text error bodies are also accepted.

## Related Documentation

- [Overview](overview.md) - API overview and principles
- [Headers](headers.md) - HTTP headers and content types
- [Signed Requests](signed-requests.md) - Authentication
- [Errors](errors.md) - Error codes and troubleshooting
