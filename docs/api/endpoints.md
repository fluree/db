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
- `413 Payload Too Large` - Transaction exceeds size limit
- `500 Internal Server Error` - Server error

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

### GET /commits/*ledger

Export commit blobs from a ledger using stable cursors. Pages walk backward via `previous_ref` — O(limit) per page regardless of ledger size. Used by `fluree pull` and `fluree clone`.

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

**Request Headers:**
```http
Content-Type: application/json
Accept: application/json
```

Or for SPARQL:
```http
Content-Type: application/sparql-query
Accept: application/sparql-results+json
```

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

**Status Codes:**
- `200 OK` - Query successful
- `400 Bad Request` - Invalid query syntax
- `401 Unauthorized` - Authentication required
- `404 Not Found` - Ledger not found
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
- **@t** - Transaction time when the value was asserted or retracted
- **@op** - Operation type: `"assert"` or `"retract"`

**Response:**

```json
[
  ["Alice", 30, 1, "assert"],
  ["Alice", 30, 5, "retract"],
  ["Alicia", 31, 5, "assert"]
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

Drop (delete) a ledger.

**URL:**
```
POST /drop
```

**Authentication:** When admin auth is enabled (`--admin-auth-mode=required`), requires Bearer token from a trusted issuer. See [Admin Authentication](#admin-authentication).

**Request Body:**

```json
{
  "ledger": "mydb:main",
  "hard": false
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger` | string | Yes | Ledger ID (e.g., "mydb" or "mydb:main") |
| `hard` | boolean | No | If `true`, permanently delete all storage files. Default: `false` (soft drop) |

**Drop Modes:**

- **Soft drop** (`hard: false`, default): Retracts the ledger from the nameservice but preserves all data files. The ledger can potentially be recovered.
- **Hard drop** (`hard: true`): Permanently deletes all commit and index files. **This is irreversible.**

**Response:**

```json
{
  "ledger": "mydb:main",
  "status": "dropped",
  "files_deleted": {
    "commit": 15,
    "index": 8
  }
}
```

| Field | Description |
|-------|-------------|
| `ledger` | Normalized ledger ID |
| `status` | One of: `"dropped"`, `"already_retracted"`, `"not_found"` |
| `files_deleted` | File counts (only populated for hard drop) |

**Status Codes:**
- `200 OK` - Drop successful (or already dropped/not found)
- `400 Bad Request` - Invalid request body
- `401 Unauthorized` - Bearer token required (when admin auth enabled)
- `500 Internal Server Error` - Server error

**Drop Sequence:**

1. Normalizes the ledger ID (ensures branch suffix like `:main`)
2. Cancels any pending background indexing
3. Waits for in-progress indexing to complete
4. In hard mode: deletes all storage artifacts (commits + indexes)
5. Retracts from nameservice
6. Disconnects from ledger cache

**Idempotency:**

Safe to call multiple times:
- Returns `"already_retracted"` if the ledger was previously dropped
- Hard mode still attempts file deletion even for already-retracted ledgers (useful for cleanup)

**Examples:**

```bash
# Soft drop (retract only, preserve files)
curl -X POST http://localhost:8090/v1/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main"}'

# Hard drop (delete all files - IRREVERSIBLE)
curl -X POST http://localhost:8090/v1/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main", "hard": true}'

# Drop with auth token (when admin auth enabled)
curl -X POST http://localhost:8090/v1/fluree/drop \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJ..." \
  -d '{"ledger": "mydb:main", "hard": true}'

# Drop with short ledger ID (auto-resolves to :main)
curl -X POST http://localhost:8090/v1/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb"}'
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
  "source": "main"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger` | string | Yes | Ledger name without branch suffix (e.g., "mydb") |
| `branch` | string | Yes | New branch name to create (e.g., "feature-x") |
| `source` | string | No | Source branch to create from. Default: `"main"` |

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
| `t` | Transaction time of the source commit at branch point |

**Status Codes:**
- `201 Created` - Branch created successfully
- `400 Bad Request` - Invalid request body
- `401 Unauthorized` - Bearer token required (when admin auth enabled)
- `404 Not Found` - Source branch does not exist
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
  "status": "Dropped",
  "deferred": false,
  "artifacts_deleted": 5,
  "cascaded": [],
  "warnings": []
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ledger_id` | Full ledger:branch identifier of the dropped branch |
| `status` | Drop status (`"Dropped"`, `"AlreadyRetracted"`, `"NotFound"`) |
| `deferred` | `true` if the branch has children — retracted but storage preserved |
| `artifacts_deleted` | Number of storage artifacts removed |
| `cascaded` | List of ancestor branch ledger_ids that were cascade-dropped |
| `warnings` | Any non-fatal warnings during the drop |

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

Merge a source branch into a target branch (fast-forward only). Admin-protected.

Currently only fast-forward merges are supported: the target branch must not have any new commits since the source branch was created from it. If the target has diverged, rebase the source branch first, then merge.

**URL:**
```
POST /merge
```

**Request body:**

```json
{
  "ledger": "mydb",
  "source": "feature-x",
  "target": "dev"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger` | string | Yes | Ledger name without branch suffix (e.g., "mydb") |
| `source` | string | Yes | Source branch to merge from (e.g., "feature-x") |
| `target` | string | No | Target branch to merge into (defaults to source's parent branch) |

**Response body (200 OK):**

```json
{
  "ledger_id": "mydb:dev",
  "target": "dev",
  "source": "feature-x",
  "fast_forward": true,
  "new_head_t": 8,
  "commits_copied": 3
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ledger_id` | string | Full ledger:branch identifier of the target |
| `target` | string | Target branch name |
| `source` | string | Source branch name |
| `fast_forward` | bool | Always `true` (only fast-forward is supported) |
| `new_head_t` | number | New commit HEAD transaction time of the target |
| `commits_copied` | number | Number of commit blobs copied to the target namespace |

**Status codes:**

- `200 OK` - Merge completed successfully
- `400 Bad Request` - Source has no branch point (e.g., main), self-merge, or target mismatch
- `404 Not Found` - Ledger or branch does not exist
- `409 Conflict` - Target has diverged; fast-forward not possible
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

Trigger a full manual reindex for a ledger.

This endpoint triggers background indexing and returns immediately. If you call
indexing through the Rust API via `trigger_index()`, the optional
`TriggerIndexOptions.timeout_ms` is caller-owned: omit it to wait indefinitely,
or set it explicitly when the calling environment has a hard runtime limit such
as AWS Lambda's 15-minute maximum.

**URL:**
```
POST /reindex
```

**Response:**

```json
{
  "ledger": "mydb:main",
  "status": "indexing",
  "target_t": 10
}
```

The request body is `{"ledger": "mydb:main"}`.

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
