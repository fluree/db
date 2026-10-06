# Remote mounts and serving tiers

This document describes how one Fluree server exposes ledgers to other Fluree
instances (servers or CLI clients), and how a consuming instance mounts a
remote's ledgers as read-only, locally-queryable data sources. It covers the
serving-tier model, per-ledger serving posture, the mount architecture
(composite nameservice + routed storage), and the integrity/caching
semantics that make remotely-fetched content safe to cache indefinitely.

Related documents: [Query peers and replication](../operations/query-peers.md)
(operator guide), [Auth contract](auth-contract.md) (token/claim wire
contract), [Setting groups](../ledger-config/setting-groups.md)
(`f:servingDefaults`), [Storage traits](storage-traits.md).

## The model in one paragraph

A serving Fluree offers each ledger through up to two tiers: **query**
(the server executes queries, with row-level policy applied — its compute)
and **blocks** (the server hands out canonical content-addressed bytes —
index leaves, dictionaries, commits — and the consumer executes queries
locally over them: the consumer's compute, like an Iceberg catalog serving
table files). Access to the blocks tier is **all-or-nothing per (token,
ledger)**: a principal either may read the ledger's full contents or gets
nothing. Fine-grained (row-level) access is served exclusively through the
query tier. A consumer *mounts* a remote under an alias prefix; the remote's
ledgers then behave like local read-only ledgers — full triple semantics,
time-travel, dataset mixing — with all bytes fetched over HTTP on demand and
cached by CID.

## Serving tiers

| Tier | Endpoint(s) | Whose compute | Access granularity | Payload integrity |
|---|---|---|---|---|
| `query` | `/query`, `/query/*ledger`, SPARQL | Server's | Row-level (identity policy) | N/A (results, not content) |
| `blocks` | `GET /storage/objects/{cid}`, `POST /pack`, `GET /commits` | Consumer's | Full ledger or nothing | CID-verified, canonical |
| filtered blocks (reserved) | `POST /storage/block` (FLKB leaf payloads) | Mixed | Row-level | Not CID-verifiable |

The third row exists in the wire protocol (the storage-block endpoint always
policy-filters leaf payloads and never returns raw FLI3) but has no
production consumer: nothing on the read path decodes FLKB leaves. It is the
reserved transport for future fine-grained peer access; see
[Fine-grained future](#fine-grained-future).

Writes are tier-independent: they always ship to the origin's transaction
data plane (`/transact`, `/insert`, …) because commits are ordered by the
ledger's write authority. A consumer with block access and write scope reads
locally and writes remotely.

### Per-(caller, ledger) resolution

The effective tiers for a request are the intersection of three layers:

1. **Server capability** — queries are always served; the blocks tier
   requires the storage proxy to be enabled (`StorageProxyConfig`).
2. **Ledger posture** — the `f:servingDefaults` setting group in the
   ledger's config graph: `f:serveQuery`, `f:serveBlocks` (absent = allowed),
   `f:publicVisibility` (absent = token required). See
   [setting groups](../ledger-config/setting-groups.md).
3. **Token claims** — `fluree.ledger.read.*` for the query tier;
   `fluree.storage.all` / `fluree.storage.ledgers` for the blocks tier
   (full-access replication scope; see [auth contract](auth-contract.md)).

Enforcement points: the query gate runs in the query-route ledger load
(403 with a stable message); the blocks gate runs on `/storage/block`,
`/storage/objects`, `/commits`, and `/pack` (404, no existence leak).

**Serving posture binds only the origin.** `f:servingDefaults` lives in the
ledger's config graph, which replicates with the ledger — but the gates are
enforced only on transaction-role servers. A read-only peer or a consumer
that mounted the blocks always queries its own copy freely: restricting what
a holder of the full bytes does locally is not enforceable, and blocking it
would defeat the purpose of block serving. "Query serving off" therefore
means "the *origin* won't spend query compute", not "this data may not be
queried."

### Advertisement

- `/.well-known/fluree.json` carries a coarse, unauthenticated
  `"serving": {"query": bool, "blocks": bool}` server capability block.
- `GET /storage/ns/{alias}` (authenticated) annotates each nameservice
  record with the computed per-ledger tiers: `"serving": ["query","blocks"]`.
  Because consumers fetch this record for head freshness anyway,
  mode negotiation costs no extra round-trips.
- `GET /nameservice/snapshot` lists the records the token may see (scope
  filtering, 404 anti-leak) — the auth-filtered catalog.

## Mount architecture

A mount makes remote ledgers appear under a local alias prefix: mount
`acme` exposes the remote's `inventory:main` as `acme/inventory:main`.
Two seams carry the whole composition; nothing else in the engine knows
mounts exist.

### Nameservice: `CompositeNameService`

`fluree-db-nameservice/src/mount.rs`. A composite over the local
read-write nameservice and N read-only mounts (`RemoteMount` = prefix +
`Arc<dyn NameServiceLookup>`):

- **Reads** route by prefix: `acme/inventory:main` → strip → remote lookup
  `inventory:main` → the returned record is *localized* (its `ledger_id` and
  `name` re-prefixed) so every downstream consumer — ledger cache keys,
  content-store namespacing, branched-store ancestry walks — operates on the
  local alias.
- **Writes** to mounted aliases fail with a "read-only remote mount" error;
  writes to local aliases delegate to the local publisher. This includes
  `init`, so a local ledger cannot be created that shadows a mount.
- The composite implements the full `NameServicePublisher` surface, giving
  per-alias write capability on top of `NameServiceMode`'s instance-level
  read-write/read-only split.

### Storage: `StorageBackend::Routed`

`fluree-db-core/src/storage.rs`. `StorageBackend::content_store(namespace)`
is the single point where a ledger's namespace binds to a store. The
`Routed` variant holds a default backend plus `(prefix, backend)` mounts and
selects by namespace prefix at exactly that point — so `LedgerState::load`,
`BranchedContentStore` ancestry, and default-context reads all route with no
changes. Admin operations (delete, list) apply only to the default backend.

### Direct S3 access: vended credentials

When the origin's storage is S3, it can hand full-access consumers
short-lived credentials instead of proxying bytes: `GET
/storage/credentials?ledger=…` (behind the same token-scope and
`f:serveBlocks` guards as raw object serving) mints an STS `AssumeRole`
session narrowed by a session policy to the ledger's **name-level** S3
prefix — one grant covers every branch plus the name-scoped `@shared`
dictionary namespace, matching the all-or-nothing raw tier. The response
carries the credentials plus everything a reader needs (`bucket`, `region`,
`endpoint`, `key_prefix`), and the consumer builds a normal S3 reader whose
credentials auto-refresh from the endpoint as grants approach expiry
(`fluree-db-nameservice-sync::vended_s3`). The CLI's peer mode probes the
endpoint and prefers direct S3 (native ranged reads, no origin bandwidth);
a 404 means "not vended here" and reads fall back to the HTTP proxy tier.

Because everything fetched is CID-addressed, direct-from-S3 reads keep the
same integrity and cache-forever semantics as proxied ones. Two deliberate
limits: grants are only minted for **single-bucket** S3 layouts (a split
commit/index configuration would need a two-tier grant), and revocation is
expiry-bound — a minted grant outlives token revocation and `f:serveBlocks`
changes until its TTL lapses, so TTLs default to the STS minimum (15
minutes).

### Transport: `ProxyStorage` / `ProxyNameService`

`fluree-db-nameservice-sync` (the HTTP client crate; the server re-exports
them under `fluree_db_server::peer` for the whole-server peer mode).

`ProxyStorage` implements the `Storage` traits over HTTP with an explicit
read mode:

- **`ProxyReadMode::Raw`** — fetches canonical bytes via
  `GET /storage/objects/{cid}` and verifies every full payload against its
  CID client-side. Supports true HTTP Range reads (`Range: bytes=…` → 206),
  where the server verifies the full object before slicing. This is the mode
  peers and mounts use; it is what makes binary-indexed ledgers readable
  remotely (FLI3 leaves arrive canonical).
- **`ProxyReadMode::Filtered`** — the FLKB negotiation against
  `POST /storage/block`; reserved for fine-grained access.

For mounts, `ProxyStorage::with_local_prefix` strips the mount prefix from
locally-derived aliases before requests go to the remote (the inverse of the
composite's record localization). Dict blobs need one special case: their
`@shared` addresses carry only the ledger *name*, so the client derives a
default-branch alias and the server branch-resolves dict-blob requests to
any live branch of the name.

### Assembly

`FlureeBuilder::with_remote_mount(RemoteMountSpec)` (fluree-db-api). At
build time the backend is wrapped in `Routed` and the nameservice in a
composite. The spec takes transport-agnostic parts
(`Arc<dyn NameServiceLookup>` + `impl Storage`), keeping HTTP out of the API
crate:

```rust
let storage = ProxyStorage::new(url, token, ProxyReadMode::Raw)
    .with_local_prefix("acme");
let ns = Arc::new(ProxyNameService::new(url, token));
let fluree = FlureeBuilder::memory()
    .with_remote_mount(RemoteMountSpec::new("acme", ns, storage))
    .build_memory();
// fluree.graph("acme/inventory:main").query()… — local compute, remote bytes
```

Mounted and local ledgers mix freely in one dataset query
(`"from": ["books:main", "acme/inventory:main"]`).

The whole-server **peer mode** (`--storage-access-mode proxy`) is the
degenerate case of the same components: one upstream, no prefix, read-only
nameservice mode.

## Integrity and caching semantics

Everything the blocks tier serves is canonical content-addressed data, which
gives the consumer-side cache its key property: **a cached CID is valid
forever; eviction is always safe; no invalidation exists**. Only the
nameservice head lookup is per-query state.

- Raw payloads are verified against their CID on the client (and on the
  server before serving). Ranged responses are verified server-side against
  the full object.
- Filtered (FLKB) payloads are *not* canonical: they are a function of
  (CID, identity, policy-at-fetch-time) and must never be written into a
  CAS store or a shared cache under the object's CID — a policy revocation
  must not be servable from a stale cached view. Any future cache for them
  needs identity- and policy-epoch-scoped keys.
- Origin-side garbage collection composes naturally: a consumer's cache miss
  on a collected CID is a 404, bounding time-travel by the origin's
  retention.

## Relationship to adjacent mechanisms

- **SPARQL `SERVICE fluree:remote:…`** — query shipping (origin's compute);
  complementary to mounts (consumer's compute). A client negotiates between
  them from the advertised serving tiers.
- **`fetch`/`pull`/`clone` (nameservice-sync)** — eager full replication
  into local storage. A mount is the lazy, demand-driven counterpart over
  the same raw content; a warm mount cache is effectively a partial clone.
- **Graph sources (Iceberg/R2RML)** — for non-RDF backends with no Fluree
  index. Remote Fluree ledgers deliberately do *not* use the graph-source
  machinery: riding the native ledger path preserves time-travel, policy,
  and dataset semantics.

## Fine-grained future

The reserved extension points for row-level peer access, kept so it can be
added without reworking v1:

- The `POST /storage/block` FLKB tier already produces policy-filtered leaf
  payloads per identity; a read-path FLKB leaf decoder is the missing
  consumer.
- Serving advertisement uses distinct values — a future filtered tier
  advertises as `"blocks:filtered"`, never as `"blocks"`, so existing
  clients cannot misread filtered payloads as canonical.
- Cache writes are discriminated by CID-verifiability; filtered payloads
  arrive unverifiable and take identity/epoch-scoped cache keys.
- A partial-access block claim would be a new claim name, not a
  reinterpretation of `fluree.storage.*` (which remains full-access).

Similarly reserved: `f:publicVisibility` for an anonymous (public dataset)
tier, and generalizing the vended-credential handoff into
`LedgerConfig.origins` (which already models prioritized external origins —
S3/IPFS/CDN — so consumers could discover origins from the record rather
than probing the credentials endpoint).
