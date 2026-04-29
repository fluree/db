# Query peers and replication

This document describes how to run `fluree-server` in **transaction** mode (event source + transactions) and **peer** mode (read replica). It also documents the **events stream** (`/v1/fluree/events`) and **storage proxy** endpoints (`/v1/fluree/storage/*`) used to keep peers up to date and/or to proxy storage reads.

This guide is written from an **operator / end-user** standpoint: what to deploy, how to configure it, and what to expect from each mode.

## Server roles

`fluree-server` supports two roles:

- **Transaction server** (`--server-role transaction`)
  - Write-enabled.
  - Produces the nameservice events stream at `GET /v1/fluree/events`.
  - Optionally exposes storage proxy endpoints at `/v1/fluree/storage/*`.
- **Query peer** (`--server-role peer`)
  - Read-only API surface for clients (queries, history, etc.).
  - Subscribes to `GET /v1/fluree/events` from a transaction server to learn about nameservice updates.
  - Reads ledger data from storage (shared-storage deployments), and refreshes on staleness based on the events stream.
  - Forwards write/admin operations to the configured transaction server.

## Events stream (SSE): `GET /v1/fluree/events`

The transaction server exposes a Server-Sent Events (SSE) stream that emits **nameservice changes** for ledgers and graph sources. Query peers use this stream to stay up to date.

### Query parameters

- **`all=true`**: subscribe to all ledgers and graph sources
- **`ledger=<ledger_id>`**: subscribe to a ledger ID (`name:branch`, repeatable)
- **`graph-source=<graph_source_id>`**: subscribe to a graph source ID (`name:branch`, repeatable)

### Authentication and authorization

The `/v1/fluree/events` endpoint can be configured to require Bearer tokens:

- **`--events-auth-mode none|optional|required`**
- **`--events-auth-audience <aud>`** (optional)
- **`--events-auth-trusted-issuer <did:key:...>`** (repeatable)

When authentication is enabled, the token can restrict what the client may subscribe to. Requests that ask for resources not covered by the token are **silently filtered** to the allowed scope.

The repo includes a token generator binary for operator workflows:

- **`fluree-events-token`**: generates Bearer tokens suitable for `GET /v1/fluree/events`

## Peer mode behavior

In peer mode:

- **Write forwarding**: write and admin endpoints are forwarded to the transaction server configured by `--tx-server-url`.
- **Read serving**: query endpoints are served locally, using ledger/index data obtained either from shared storage or via storage proxy reads (see below). History queries are executed via the standard `/query` endpoint with time range specifiers.

### Peer configuration (SSE subscription)

- **`--server-role peer`**
- **`--tx-server-url <base-url>`** (required)
- **`--peer-events-url <url>`** (optional; default is `{tx_server_url}/v1/fluree/events`)
- **`--peer-events-token <token-or-@file>`** (optional; Bearer token for `/v1/fluree/events`)
- Subscribe scope:
  - **`--peer-subscribe-all`** or
  - **`--peer-ledger <ledger_id>`** (repeatable) and/or **`--peer-graph-source <graph_source_id>`** (repeatable)

### Peer storage access modes

Peer servers support two storage access modes:

- **Shared storage** (`--storage-access-mode shared`, default)
  - The peer reads the same storage backend as the transaction server (shared filesystem, shared bucket credentials, etc.).
  - Requires `--storage-path`.
- **Proxy storage** (`--storage-access-mode proxy`)
  - The peer does **not** need direct storage credentials.
  - The peer proxies all storage reads through the transaction server’s `/v1/fluree/storage/*` endpoints.
  - Requires `--tx-server-url` and a **storage proxy token** via `--storage-proxy-token` or `--storage-proxy-token-file`.
  - `--storage-path` is ignored in this mode.

## Storage proxy endpoints (transaction server): `/v1/fluree/storage/*`

Storage proxy endpoints allow a peer to read storage **through** the transaction server, rather than holding storage credentials directly. This is intended for environments where storage is private and peers cannot access it.

Storage proxy supports two kinds of reads:

- **Raw bytes** reads (`Accept: application/octet-stream`) for any block type (commit blobs, branch nodes, leaf nodes).
- **Policy-filtered leaf flakes** reads (`Accept: application/x-fluree-flakes`) for ledger **leaf** nodes only.

### Enablement

Storage proxy endpoints are disabled by default. Enable them on the transaction server:

- **`--storage-proxy-enabled`**
- **`--storage-proxy-trusted-issuer <did:key:...>`** (repeatable; optional if you reuse `--events-auth-trusted-issuer`)
- **`--storage-proxy-default-identity <iri>`** (optional; used when token has no `fluree.identity`)
- **`--storage-proxy-default-policy-class <class-iri>`** (optional; applies policy in addition to identity-based policy)
- **`--storage-proxy-debug-headers`** (optional; debug only—can leak information)

### AuthZ claims (Bearer token)

Storage proxy endpoints require a Bearer token that grants storage proxy permissions:

- **`fluree.storage.all: true`**: access all ledgers (graph source artifacts are denied in v1)
- **`fluree.storage.ledgers: ["books:main", ...]`**: access specific ledgers
- **`fluree.identity: "ex:PeerServiceAccount"`** (optional): identity used for policy evaluation in policy-filtered read mode

Unauthorized requests return **404** (no existence leak).

### Endpoints

#### `GET /v1/fluree/storage/ns/{ledger-id}`

Fetch a nameservice record for a ledger ID. Requires storage proxy authorization for that ledger.

#### `POST /v1/fluree/storage/block`

Fetch a block/blob by **CID**. The request includes the **ledger ID** so the server can authorize the request and derive the physical storage address internally. Currently supports:

- `Accept: application/octet-stream` (raw bytes; always available)
- `Accept: application/x-fluree-flakes` (binary “FLKB” transport of policy-filtered **leaf** flakes only)
- `Accept: application/x-fluree-flakes+json` (debug-only JSON flake transport; leaf flakes only)

If the client requests a flakes format for a **non-leaf** block, the server returns **406 Not Acceptable**. Clients (and peers in proxy mode) should retry with `Accept: application/octet-stream` in that case.

Example request body:

```json
{
  "cid": "bafy...leafOrBranchCid",
  "ledger": "mydb:main"
}
```

##### Policy filtering semantics (leaf flakes)

When a flakes format is requested and the block is a ledger leaf:

- The transaction server loads policy restrictions using the **effective identity** and **effective policy class**:
  - **effective identity**: token `fluree.identity` if present, otherwise `--storage-proxy-default-identity` (if configured)
  - **effective policy class**: `--storage-proxy-default-policy-class` (if configured; token-driven policy class selection may be added later)
- If the resolved policy is **root/unrestricted**, the server returns all leaf flakes (still encoded as FLKB in `application/x-fluree-flakes` mode).
- If the resolved policy is **non-root**, the server filters leaf flakes before encoding them for transport.

> Note: the peer can still apply additional client-facing policy enforcement on top of this. Client-side policy can only further restrict results; it cannot “recover” facts filtered out upstream.

### Security notes and limitations

- **Branch/commit leakage (v1 limitation)**: filtering leaves without rewriting branches/commits can leak structure/existence information to the peer identity. This is currently an accepted v1 limitation.
- **Graph source artifacts (v1)**: storage proxy denies graph-source artifacts by returning 404 even when `fluree.storage.all` is present.

## Deployment examples

### Transaction server (events + storage proxy)

```bash
fluree-server \
  --listen-addr 0.0.0.0:8090 \
  --server-role transaction \
  --storage-path /var/lib/fluree \
  --events-auth-mode required \
  --events-auth-trusted-issuer did:key:z6Mk... \
  --storage-proxy-enabled
```

### Query peer (shared storage)

```bash
fluree-server \
  --listen-addr 0.0.0.0:8091 \
  --server-role peer \
  --tx-server-url http://tx.internal:8090 \
  --storage-path /var/lib/fluree \
  --peer-subscribe-all \
  --peer-events-token @/etc/fluree/peer-events.jwt
```

### Query peer (proxy storage mode)

In proxy storage mode, the peer does not need `--storage-path` and instead needs a storage proxy token:

```bash
fluree-server \
  --listen-addr 0.0.0.0:8091 \
  --server-role peer \
  --tx-server-url http://tx.internal:8090 \
  --storage-access-mode proxy \
  --storage-proxy-token @/etc/fluree/storage-proxy.jwt \
  --peer-subscribe-all \
  --peer-events-token @/etc/fluree/peer-events.jwt
```
