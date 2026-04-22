# Implementing Server Support For Fluree CLI

This document is for implementers building a custom server (for example in `../solo3/`) that wants to support the Fluree CLI end-to-end.

The CLI supports two broad categories of remote operations:

- **Data API**: query/update/insert/upsert/info/exists/show (normal ledger operations).
- **Replication / sync**: clone/pull/fetch (content-addressed replication by CID, via pack + storage proxy).

## Base URL And Discovery

The CLI prefers to be configured with a server origin URL (scheme/host/port) and then uses discovery:

- `GET /.well-known/fluree.json` returns `api_base_url` (usually `/v1/fluree`)

The CLI stores the discovered base as the remote's `base_url` and constructs all other endpoints relative to it.

If you do not implement discovery, users must configure the CLI remote URL to already include the API base (for example `http://localhost:8090/v1`), and the CLI will append `/fluree` as needed.

## Minimum Endpoints By CLI Feature

### `fluree remote add`, `fluree auth login`

- `GET /.well-known/fluree.json`

### `fluree fetch` (nameservice refs only)

- `GET {api_base_url}/nameservice/snapshot`
- `POST {api_base_url}/nameservice/refs/:ledger-id/commit`
- `POST {api_base_url}/nameservice/refs/:ledger-id/index`
- `POST {api_base_url}/nameservice/refs/:ledger-id/init`

### `fluree clone`, `fluree pull` (pack-first replication)

Required:

- `GET {api_base_url}/info/*ledger` (existence + remote `t` preflight; see `/info` minimum fields below)
- `GET {api_base_url}/storage/ns/:ledger-id` (remote NsRecord, includes `commit_head_id`, optional `index_head_id`, and optional `config_id`)
- `POST {api_base_url}/pack/*ledger` (binary `fluree-pack-v1` stream)

The CLI sends pack requests with **index artifacts** by default (`include_indexes: true`, `want_index_root_id` from the NsRecord) when the remote advertises an `index_head_id`. Use `--no-indexes` on clone/pull to request commits and txns only. Use `--no-txns` on clone to request commits without original transaction payloads (the commit chain still transfers and remains verifiable). Servers that support pack MUST honor the following request fields:

- `include_indexes: bool` — when `false`, skip index artifact frames.
- `include_txns: bool` — when `false`, skip transaction blob frames. Commits are still streamed; the server must decode each commit's envelope and simply omit the referenced `txn` blob from the stream. The emitted `PackHeader.capabilities` should reflect this (drop `"txns"` from the list).

Servers that support pack should support all combinations of these flags.

Fallbacks (strongly recommended):

- `GET {api_base_url}/commits/*ledger` (paginated export of commit + txn blobs)
- `GET {api_base_url}/storage/objects/:cid?ledger=:ledger-id` (per-object fetch by CID)

### `fluree push` (commit ingestion)

- `POST {api_base_url}/push/*ledger`

This is not storage-proxy replication; it is a transaction operation and should be authorized like normal transactions.

The CLI sends an `Idempotency-Key` header derived from the pushed commit bytes so servers can safely replay a successful push result if the client retries after a timeout.

### `fluree show --remote`

- `GET {api_base_url}/show/*ledger?commit=<ref>`

The `commit` query parameter accepts the same identifiers as the local `fluree show` command: `t:<N>` for transaction number, hex-digest prefix (min 6 chars), or full CID.

**Policy filtering:** The returned flakes are filtered by the caller's data-auth identity (extracted from the Bearer token) and the server's configured `default_policy_class`. When neither is present, all flakes are returned (root/admin access). Flakes the caller cannot read are silently omitted — the `asserts` and `retracts` counts reflect only the visible flakes. Unlike the query endpoints, show does not accept per-request policy overrides via headers or request body.

**Response:** A JSON object with fields: `id`, `t`, `time`, `size`, `previous`, `signer`, `asserts`, `retracts`, `@context`, `flakes`. Each flake is a tuple: `[subject, predicate, object, datatype, operation]`.

**Error responses:**
- `400 Bad Request` — missing or invalid `commit` parameter
- `404 Not Found` — ledger or commit not found
- `501 Not Implemented` — proxy storage mode (no local index available for decoding)

### `fluree publish <remote> [ledger]` (create + push)

Creates a ledger on the remote and pushes all local commits in a single operation.

Required endpoints:

- `GET {api_base_url}/exists/*ledger` (check if ledger already exists)
- `POST {api_base_url}/create` (create empty ledger if not exists)
- `GET {api_base_url}/info/*ledger` (check remote head when ledger exists)
- `POST {api_base_url}/push/*ledger` (push all commits)

**Workflow:**

1. CLI calls `GET /exists?ledger=mydb:main`
2. If `exists: false`, CLI calls `POST /create` with `{"ledger": "mydb:main"}`
3. If `exists: true`, CLI calls `GET /info/mydb:main` and rejects if `t > 0` (remote already has data)
4. CLI walks the full local commit chain (oldest → newest) and sends all commits via `POST /push/mydb:main`
5. CLI configures upstream tracking locally

The `--remote-name` flag allows publishing under a different name on the remote (e.g., `fluree publish origin mydb --remote-name production-db`).

### `fluree create <name> --from <file>.flpack` (native ledger import)

- No server endpoint required (local-only operation)

Imports a `.flpack` file (native ledger pack) into a new local ledger. The `.flpack` format uses the same `fluree-pack-v1` wire format as `POST /pack`. See [Ledger portability](#ledger-portability-flpack-files) below.

### `fluree export --format ledger` (native ledger export)

- No server endpoint required (local-only operation)

Exports a full local ledger (all commits, indexes, dictionaries) as a `.flpack` file. See [Ledger portability](#ledger-portability-flpack-files) below.

### `fluree query`, `fluree insert`, `fluree upsert`, `fluree update`, `fluree track`, `fluree info`, `fluree exists`

- `POST {api_base_url}/query/*ledger`
- `POST {api_base_url}/insert/*ledger`
- `POST {api_base_url}/upsert/*ledger`
- `POST {api_base_url}/update/*ledger`
- `GET {api_base_url}/info/*ledger`
- `GET {api_base_url}/exists/*ledger`

When the CLI is invoked with policy flags (`--as`, `--policy-class`,
`--policy`, `--policy-file`, `--policy-values`, `--policy-values-file`,
`--default-allow`), it carries them on every data API request via the headers
listed below and, for JSON-LD bodies, also injects them into `opts`. To be
CLI-compatible, your server must implement the contract in
[Policy Enforcement Contract](#policy-enforcement-contract).

## Policy Enforcement Contract

CLI policy flags ride on every data API request as both HTTP headers and (for
JSON-LD bodies) body-level `opts` fields. Servers wanting full CLI parity must
honor both transports and apply the **root-impersonation gate** described
below.

### Headers the CLI may send

| Header | CLI flag | Type | Notes |
|---|---|---|---|
| `fluree-identity` | `--as <iri>` | string | Identity IRI to execute as. |
| `fluree-policy-class` | `--policy-class <iri>` | string, repeatable | Send one header per class, OR a single header with comma-separated IRIs. Both forms must accumulate into a single list. |
| `fluree-policy` | `--policy <json>` / `--policy-file` | JSON string | Inline JSON-LD policy document(s). Reject with `400` on parse failure. |
| `fluree-policy-values` | `--policy-values <json>` / `--policy-values-file` | JSON object string | Variable bindings for parameterized policies (keys begin with `?$`). Reject with `400` on parse failure or non-object value. |
| `fluree-default-allow` | `--default-allow` | `"true"` (presence-truthy) | Permit access when no matching policy rules exist. |

For JSON-LD requests (`POST /query/*`, `POST /insert/*`, `POST /upsert/*`,
`POST /update/*` with `Content-Type: application/json`), the CLI **also**
injects each field into the request body's `opts` object using the same names
(`opts.identity`, `opts.policy-class` as a JSON array, `opts.policy`,
`opts.policy-values` as an object, `opts.default-allow` as a bool). Servers
should treat header values as defaults that body values override.

For SPARQL requests (`Content-Type: application/sparql-query`,
`application/sparql-update`), headers are the only transport — the SPARQL body
has no opts block.

### Required server behavior

1. **Build a `PolicyContext`** from the merged opts (header defaults + body
   overrides) and apply it to every query and transaction execution path.
   Without policy fields the request runs under root (no enforcement). With
   any policy field, the policies must be enforced — including for unsigned
   bearer-only transactions, which historically bypassed enforcement.

2. **Force the bearer's identity into `opts.identity`** by default (the
   bearer is the authenticated principal; clients cannot spoof identity by
   setting `opts.identity`). The exception is the impersonation gate below.

3. **Implement the impersonation gate** for JSON-LD `opts.identity`,
   `opts.policy-class`, `opts.policy`, and `opts.policy-values`, plus the
   `fluree-identity` header on SPARQL requests:

   - Resolve the bearer's identity in the target ledger's policy graph.
   - If the lookup returns "subject exists with no `f:policyClass`"
     (the `FoundNoPolicies` outcome — the bearer is unrestricted on this
     ledger), respect the client-supplied identity / policy fields.
   - If the lookup returns "subject has `f:policyClass` assignments"
     (`FoundWithPolicies`) **or** "subject not found" (`NotFound`), force the
     bearer identity into `opts.identity` and ignore the client-supplied
     policy fields — the request runs under the bearer's own policies.
   - `opts.default-allow` is **not** an impersonation field — it only governs
     the absence of matching rules and should not trigger the gate's lookup.

4. **Audit-log impersonations**. When the gate honors a client-supplied
   identity, log at `info` level with the bearer, target, and ledger:

   ```
   policy impersonation: bearer=<bearer-id> target=<as-iri> ledger=<name>
   ```

5. **Set commit `author` to the impersonated identity** for write operations.
   The original bearer is captured in the audit log; the commit's author
   field tracks who the operation was executed *as*.

6. **In proxy/forwarding mode**, defer the gate to the upstream server:
   forward the request as-is and let the upstream resolve the gate against
   its own ledger state.

### Reference behavior

The Fluree reference server implements the gate via
`fluree_db_api::identity_has_no_policies(snapshot, overlay, t, identity_iri)`,
which wraps the three-state `IdentityLookupResult` enum and returns `true`
only for `FoundNoPolicies`. Source: `fluree-db-api/src/policy_builder.rs`.
The route-level wiring (header merge, gate, force-override, audit log,
PolicyContext construction) lives in
`fluree-db-server/src/routes/policy_auth.rs` — useful as a concrete
implementation reference if you're porting the contract to another server.

## Replication Auth Contract

Replication endpoints are intentionally protected more strictly than data reads:

- Pack + commit export + storage proxy endpoints require a Bearer token with `fluree.storage.*` permissions.
- Unauthorized requests should return `404 Not Found` (no existence leak) for these endpoints.

Data API endpoints use normal read/transaction auth (`fluree.ledger.read.*`, `fluree.ledger.write.*`) and should return `401/403/404` as appropriate for your product.

## Pack Protocol Contract

- Endpoint: `POST {api_base_url}/pack/*ledger`
- Request: JSON `PackRequest` with `"protocol":"fluree-pack-v1"`. Includes `include_indexes: bool` (default `true` for clone/pull; `false` with `--no-indexes`), `include_txns: bool` (default `true`; `false` with `--no-txns` on clone), and optional `want_index_root_id` / `have_index_root_id` when the CLI requests index data.
- Response: `Content-Type: application/x-fluree-pack`, streaming frames:
  - Preamble `FPK1` + version byte
  - Header frame (mandatory, first)
  - Data frames: CID binary + raw object bytes
  - Optional Manifest frames (phase transitions)
  - End frame (mandatory termination)

Clients verify integrity:

- Commit-v2 blobs (`FCV2` magic): sub-range hash verification.
- All other objects: full-bytes hash verification by CID.

**Graceful fallback:** If you do not implement pack yet, return `404 Not Found`, `405 Method Not Allowed`, `406 Not Acceptable`, or `501 Not Implemented`. The CLI treats those as "pack not supported" and falls back to `GET /commits` plus `GET /storage/objects/:cid`.

## Storage Proxy Contract

These endpoints exist so a client can fetch bytes by CID without knowing storage layout:

- `GET {api_base_url}/storage/ns/:ledger-id` returns `NsRecord` JSON with CID identity fields:
  - `commit_head_id`, `commit_t`, `index_head_id`, `index_t`, optional `config_id`
- `GET {api_base_url}/storage/objects/:cid?ledger=:ledger-id` returns raw bytes for the CID after verifying integrity.

`/storage/block` is only required for query peers that need server-mediated index-leaf access.

## `/create` Contract

- Endpoint: `POST {api_base_url}/create`
- Request body: `{"ledger": "mydb:main"}`
- Response (201 Created): `{"ledger": "mydb:main", "t": 0}`
- Response (409 Conflict): ledger already exists

If no branch suffix is provided (e.g., `"mydb"`), the server MUST normalize to `"mydb:main"`.

Used by `fluree publish` (and potentially future `fluree create --remote`) to create a ledger on a remote server before pushing commits.

## `/exists` Response Contract

- Endpoint: `GET {api_base_url}/exists?ledger=mydb:main` (or via `fluree-ledger` header)
- Response (200 OK, always): `{"ledger": "mydb:main", "exists": true|false}`

MUST return 200 regardless of whether the ledger exists (the `exists` field carries the result). Should query the nameservice only — no ledger data loading.

## `/info` Response Contract (CLI Minimum)

The CLI currently treats `GET {api_base_url}/info/*ledger` as an opaque JSON object, but it requires these fields:

- `t` (integer): required for `fluree clone` and `fluree pull` preflight and for `fluree push` conflict checks.
- `commitId` (string CID): required for `fluree push` when `t > 0` so it can detect divergence.

Other fields are optional and may be used only for display.

## Origin-Based Replication (LedgerConfig)

The CLI can do origin-based `clone --origin` and `pull` fallback without a named remote by fetching objects via:

- `GET {api_base_url}/storage/objects/:cid?ledger=:ledger-id`

If your nameservice advertises `config_id` on the NsRecord, the CLI will attempt to fetch that `LedgerConfig` blob (by CID) and then use it to try additional origins.

## Graph Source Endpoints (Iceberg, R2RML, BM25, etc.)

The CLI routes graph source operations through the server when one is running. This uses the same auto-routing mechanism as query/insert/etc.: the CLI checks for `server.meta.json` (written by `fluree server start`), verifies the PID is alive, and routes through `http://{listen_addr}/v1/fluree`. Users can bypass with `--direct`.

### `fluree list` (includes graph sources)

- `GET {api_base_url}/ledgers`

Returns a JSON array of **both** ledger records and graph source records. Retracted records are excluded.

**Response fields (required for each entry):**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Ledger or graph source name |
| `branch` | string | Branch name (e.g., `"main"`) |
| `type` | string | One of: `"Ledger"`, `"Iceberg"`, `"R2RML"`, `"BM25"`, `"Vector"`, `"Geo"` |
| `t` | integer | `commit_t` for ledgers, `index_t` for graph sources (0 if not indexed) |

**Example response:**

```json
[
  { "name": "mydb", "branch": "main", "type": "Ledger", "t": 5 },
  { "name": "warehouse-orders", "branch": "main", "type": "Iceberg", "t": 0 },
  { "name": "my-search", "branch": "main", "type": "BM25", "t": 5 }
]
```

The CLI shows a TYPE column only when the response contains non-Ledger entries.

**Error responses:** `500` on internal failure. Empty array `[]` when no records exist.

### `fluree info <name>` (graph source fallback)

- `GET {api_base_url}/info/*name`

Existing endpoint, extended with graph source fallback. Resolution order:

1. Look up `name` as a **ledger** — if found, return the standard ledger info response (unchanged)
2. Look up `name` as a **graph source** (append `:main` if no branch suffix) — if found, return the graph source response below
3. Return `404 Not Found`

**Graph source response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Graph source name |
| `branch` | string | Branch name |
| `type` | string | Source type (e.g., `"Iceberg"`) |
| `graph_source_id` | string | Canonical ID (e.g., `"warehouse-orders:main"`) |
| `retracted` | boolean | Whether retracted |
| `index_t` | integer | Index watermark |
| `index_id` | string? | Index ContentId (omitted if none) |
| `dependencies` | string[]? | Source ledger IDs (omitted if empty) |
| `config` | object? | Parsed configuration JSON (omitted if empty/`{}`) |

**Example:**

```json
{
  "name": "warehouse-orders",
  "branch": "main",
  "type": "Iceberg",
  "graph_source_id": "warehouse-orders:main",
  "retracted": false,
  "index_t": 0,
  "config": {
    "catalog": {
      "type": "rest",
      "uri": "https://polaris.example.com/api/catalog",
      "warehouse": "my-warehouse"
    },
    "table": "sales.orders",
    "io": {
      "vended_credentials": true,
      "s3_region": "us-east-1"
    }
  }
}
```

**CLI detection:** The CLI distinguishes graph source responses from ledger responses by checking for the `graph_source_id` field in the JSON.

### `fluree drop <name>` (graph source fallback)

- `POST {api_base_url}/drop`

Existing endpoint, extended with graph source fallback. Request body is unchanged: `{ "ledger": "<name>", "hard": true }`.

**Resolution order:**

1. Try dropping `name` as a **ledger** — if the drop report has `status: "dropped"` or `status: "already_retracted"`, return that
2. If the ledger drop report has `status: "not_found"`, try dropping as a **graph source** (default branch `"main"`)
3. If both return not found, return the not-found response

**Response:** Same schema as ledger drop: `{ "ledger_id": "name:branch", "status": "dropped"|"already_retracted"|"not_found", "warnings": [...] }`. For graph sources, `ledger_id` contains the graph source ID (e.g., `"warehouse-orders:main"`).

### `fluree iceberg map` (Iceberg graph source creation)

- `POST {api_base_url}/iceberg/map` (admin-protected)

Creates an Iceberg graph source with an R2RML mapping that defines how table rows become RDF triples. This is a write operation and should be admin-protected (same middleware as `/create` and `/drop`).

**Request body fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Graph source name (no colons) |
| `mode` | string | No | `"rest"` (default) or `"direct"` |
| `catalog_uri` | string | REST mode | REST catalog URI |
| `table` | string | No | Table identifier (`namespace.table`); required for REST mode if not specified in R2RML mapping |
| `table_location` | string | Direct mode | S3 URI (`s3://bucket/path/to/table`) |
| `r2rml` | string | Yes | R2RML mapping source (storage address or path) |
| `r2rml_type` | string | No | Mapping media type (e.g., `"text/turtle"`); inferred from extension |
| `branch` | string | No | Branch name (default: `"main"`) |
| `auth_bearer` | string | No | Bearer token for REST catalog auth |
| `oauth2_token_url` | string | No | OAuth2 token endpoint |
| `oauth2_client_id` | string | No | OAuth2 client ID |
| `oauth2_client_secret` | string | No | OAuth2 client secret |
| `warehouse` | string | No | Warehouse identifier (REST mode) |
| `no_vended_credentials` | boolean | No | Disable vended credentials (default: `false`) |
| `s3_region` | string | No | S3 region override |
| `s3_endpoint` | string | No | S3 endpoint override (MinIO, LocalStack) |
| `s3_path_style` | boolean | No | Use path-style S3 URLs (default: `false`) |

**Validation rules:**
- `name` must not be empty or contain `:`
- `r2rml` is required (defines how table rows become RDF triples)
- REST mode requires `catalog_uri`; requires `table` unless specified in R2RML mapping's `rr:tableName`
- Direct mode requires `table_location` (must start with `s3://` or `s3a://`)
- OAuth2 fields must all be provided together (url + id + secret)

**Example — REST catalog with R2RML:**

```json
{
  "name": "warehouse-orders",
  "mode": "rest",
  "catalog_uri": "https://polaris.example.com/api/catalog",
  "table": "sales.orders",
  "r2rml": "mappings/orders.ttl",
  "auth_bearer": "my-token",
  "warehouse": "my-warehouse"
}
```

**Example — REST catalog (table inferred from R2RML `rr:tableName`):**

```json
{
  "name": "airlines",
  "mode": "rest",
  "catalog_uri": "https://polaris.example.com/api/catalog",
  "r2rml": "mappings/airlines.ttl",
  "auth_bearer": "my-token"
}
```

**Example — Direct S3 (no catalog):**

```json
{
  "name": "execution-log",
  "mode": "direct",
  "table_location": "s3://bucket/warehouse/logs/execution_log",
  "r2rml": "mappings/execution_log.ttl",
  "s3_region": "us-east-1"
}
```

**Response (`201 Created`):**

| Field | Type | Present | Description |
|-------|------|---------|-------------|
| `graph_source_id` | string | Always | Created ID (e.g., `"warehouse-orders:main"`) |
| `table_identifier` | string | Always | Table identifier or derived from location |
| `catalog_uri` | string | Always | Catalog URI or S3 location |
| `connection_tested` | boolean | Always | Whether catalog connection was verified (always `false` for direct mode) |
| `mapping_source` | string | Always | R2RML mapping source |
| `triples_map_count` | integer | Always | Number of TriplesMap definitions found |
| `mapping_validated` | boolean | Always | Whether mapping was parsed and compiled successfully |

**Error responses:**
- `400 Bad Request` — validation failures (missing fields, invalid mode, bad table identifier)
- `409 Conflict` — graph source with this name already exists (if your nameservice enforces uniqueness)
- `500 Internal Server Error` — catalog connection failure, mapping load failure, nameservice write failure

### Querying graph sources

Graph source queries work through normal query endpoints. No separate endpoint is needed, but the Rust API has an important distinction:

- Use `query_from()` when the query body carries the dataset (`"from"` in JSON-LD, `FROM` / `FROM NAMED` in SPARQL), or when you are composing multiple sources.
- Use `graph(alias).query()` for a single lazy query target that may be either a native ledger or a mapped graph source.
- Do not use the raw materialized-snapshot path (`fluree.db(&alias)` → `fluree.query(&view, ...)`) for graph source aliases.

> **Important:** The unsupported path is specifically the raw `GraphDb` snapshot flow (`fluree.db(&alias)` → `fluree.query(&view, ...)`). That API assumes you already loaded a native ledger snapshot. Graph source resolution happens in the lazy builder paths (`graph().query()` and `query_from()`), which wire in the R2RML provider and can fall back from "ledger not found" to "mapped graph source".

**Supported query paths:**

```rust
// Connection-level — graph sources resolve transparently
// When compiled with the `iceberg` feature, query_from() automatically
// enables R2RML provider support via .with_r2rml().
f.query_from().sparql(sparql).execute_formatted().await
f.query_from().jsonld(&query_json).execute_formatted().await

// Single-target lazy query — works for ledgers and mapped graph sources
f.graph(alias).query().sparql(sparql).execute_formatted().await

// Ledger-scoped query that may reference graph sources in GRAPH patterns
f.graph(ledger_id).query().sparql(sparql).execute_formatted().await
```

**Do NOT use:**

```rust
// Raw materialized snapshot path — native ledgers only
let view = f.db(&alias).await?;
f.query(&view, query_input).await?  // ❌ No R2RML, no graph source resolution
```

**Query patterns that reference graph sources:**

Graph sources can be queried directly, just like ledgers:

- `POST {api_base_url}/query/execution-log:main` with a SPARQL or JSON-LD query body

Via `FROM` / `FROM NAMED` clauses:

```sparql
SELECT * FROM <execution-log:main> WHERE { ?s ?p ?o } LIMIT 10
```

Via `GRAPH` patterns (joining with ledger data):

```sparql
SELECT ?name ?orderId ?total
FROM <mydb:main>
WHERE {
  ?customer schema:name ?name .
  ?customer ex:customerId ?custId .
  GRAPH <warehouse-orders:main> {
    ?order ex:customerId ?custId .
    ?order ex:orderId ?orderId .
    ?order ex:total ?total .
  }
}
```

**How it works:** When the `iceberg` feature is compiled, `query_from()` and `graph().query()` automatically call `.with_r2rml()`, which constructs a `FlureeR2rmlProvider` that can resolve graph source names to R2RML mappings and route triple patterns through the Iceberg scan engine. The `NameService` trait requires `GraphSourceLookup` (read-only graph source discovery), so graph source resolution is always available at the nameservice layer.

**Known limitation:** `FROM <ledger>, <graph-source>` with bare WHERE patterns (no GRAPH wrapper) — the graph source participates in the dataset but bare triple patterns only scan native indexes. Use explicit `GRAPH <gs:main> { ... }` for the graph source part in mixed-source queries.

### Authentication

- **`POST /iceberg/map`** and **`POST /drop`** are admin-protected (same middleware as `/create`)
- **`GET /ledgers`** and **`GET /info/*name`** are read-only (same auth as other read endpoints)
- **`POST /query/*ledger`** with graph source GRAPH patterns uses normal query auth

## Ledger Portability (.flpack Files)

The CLI supports exporting and importing full native ledgers as `.flpack` files using the `fluree-pack-v1` wire format. This enables ledger portability without a running server.

```bash
# Export a ledger (all commits + indexes + dictionaries)
fluree export mydb --format ledger -o mydb.flpack

# Import into a new instance (can use a different ledger name)
fluree create imported-db --from mydb.flpack
```

The `.flpack` format is identical to the binary stream served by `POST /pack/{ledger}`, with the addition of a **nameservice manifest frame** that carries the metadata needed to reconstruct the nameservice record on import:

```json
{
  "phase": "nameservice",
  "ledger_id": "original-name:main",
  "name": "original-name",
  "branch": "main",
  "commit_head_id": "bafybeig...commitHead",
  "commit_t": 42,
  "index_head_id": "bafybeig...indexRoot",
  "index_t": 40
}
```

**Aliasing on import:** The ledger name provided to `fluree create` determines the local storage path. The data itself is content-addressed (CIDs), so a ledger can be imported under any name. The `ledger_id` inside the index root binary is informational and does not affect CAS resolution.

**Combined with publish:** A typical workflow for moving a ledger from one environment to another:

```bash
# On source machine: export
fluree export mydb --format ledger -o mydb.flpack

# On target machine: import and publish to server
fluree create mydb --from mydb.flpack
fluree remote add prod https://prod.example.com
fluree auth login --remote prod
fluree publish prod mydb
```

## Quick Validation Script

From a clean project directory:

```bash
fluree init
fluree remote add origin http://localhost:8090
fluree auth login --remote origin --token @token.txt

# Ledger operations
fluree fetch origin
fluree clone origin mydb:main
fluree pull mydb:main
fluree push mydb:main

# Publish a local ledger to remote
fluree create local-db
fluree insert local-db -e '{"@id": "ex:test", "ex:val": 1}'
fluree publish origin local-db

# Export / import round-trip
fluree export mydb --format ledger -o mydb.flpack
fluree create imported --from mydb.flpack

# Iceberg operations (requires iceberg feature on server)
fluree iceberg map my-gs \
  --catalog-uri https://polaris.example.com/api/catalog \
  --r2rml mappings/orders.ttl \
  --auth-bearer $POLARIS_TOKEN

fluree list                    # should show mydb (Ledger) + my-gs (Iceberg)
fluree info my-gs              # should show Iceberg config + R2RML mapping
fluree show t:1 --remote origin  # should show decoded commit with resolved IRIs
fluree drop my-gs --force      # should drop the graph source
```
