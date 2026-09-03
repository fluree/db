# Sharing data with downstream consumers

This guide is for a team running Fluree that wants other teams, partners, or
customers to consume its ledgers — and for those consumers. It covers the
sharing patterns, how a provider enables and scopes each one, how per-ledger
participation is declared, and what the consumer runs on their side.

The one-paragraph model: a serving Fluree offers each ledger through up to
two tiers. With **query serving**, your server executes the consumer's
queries — your compute, with row-level policy applied per identity, so it's
the only tier that supports fine-grained permissioning. With **block
serving**, your server (or S3 directly) hands out the ledger's canonical
index content and the consumer queries it *locally* — their compute, like an
Iceberg catalog serving table files, but strictly all-or-nothing per ledger:
a consumer either may read the whole ledger or gets nothing. Deep dive:
[Remote mounts and serving tiers](../design/remote-mounts.md).

## Choosing a pattern

| | Query serving | Peer / block serving | Full replication |
|---|---|---|---|
| Whose compute | Provider's | Consumer's | Consumer's |
| Access granularity | Row-level (identity policy) | Whole ledger or nothing | Whole ledger or nothing |
| Provider cost per query | Query execution | Bandwidth on first touch, then ~nothing (CID cache) | None (one-time transfer) |
| Consumer freshness | Always current | Current (head check per query) | As of last `pull` |
| Consumer setup | Token only | Token + disk cache | Local storage for the full copy |
| CLI | `fluree track add` | `fluree track add --mode peer` | `fluree clone` / `fluree pull` |

Rules of thumb:

- **Fine-grained permissions → query serving.** It is the only pattern where
  different identities see different rows of the same ledger.
- **Heavy or frequent analytical consumers you trust with the full ledger →
  peer mode.** Your server stops paying query compute; with S3-backed
  storage and [vended credentials](#5-optional-vended-s3-credentials) it stops
  paying bandwidth too.
- **Offline or air-gapped consumers → replication** ([clone](../cli/clone.md),
  [pull](../cli/pull.md)).
- A fourth mechanism, SPARQL `SERVICE <fluree:remote:...>` federation,
  composes remote query results *inside* your own queries — useful for
  cross-organization joins, but it is query shipping under the hood and
  follows the query-serving rules.

The patterns compose per ledger and per consumer: the same server can serve
filtered queries to analysts and raw blocks to a partner's compute cluster.

## Provider setup

### 1. Keys and trusted issuers

All sharing is authorized by Bearer tokens (JWS with an embedded Ed25519
key, or OIDC — see [Authentication](../security/authentication.md)). Trust
is anchored in the **issuer**: your server only accepts tokens signed by
keys you configure.

```bash
# One-time: generate a signing keypair. The did:key is the issuer.
fluree token keygen
# → private key + did:key:z6Mk...
```

Start the server trusting that issuer on the surfaces you intend to serve:

```bash
fluree-server \
  --storage-path /var/lib/fluree \
  # query serving with enforced auth:
  --data-auth-mode required \
  --data-auth-trusted-issuer did:key:z6Mk... \
  # block serving (peer/replication tier):
  --storage-proxy-enabled \
  --storage-proxy-trusted-issuer did:key:z6Mk...
```

Operator details for each flag: [Query peers and
replication](../operations/query-peers.md).

### 2. Mint consumer tokens

The token's claims are the consumer's grant — which ledgers, which tier,
and (for query serving) which identity policy applies. See
[token](../cli/token.md) for the full surface.

```bash
# Query-serving consumer: read two ledgers, policy-bound identity
fluree token create --private-key @signing.key \
  --identity "https://example.org/consumers/acme-analyst" \
  --read-ledger sales:main --read-ledger inventory:main \
  --expires-in 30d

# Peer/block-serving consumer: full-access raw content for one ledger
fluree token create --private-key @signing.key \
  --storage-ledger analytics:main \
  --expires-in 30d
```

The claim vocabulary maps directly to tiers: `fluree.ledger.read.*` grants
query serving; `fluree.storage.*` grants block serving (and replication) —
**issue storage claims only to consumers entitled to the ledger's full
contents**, since raw index blocks bypass row-level policy by design.
`fluree.ledger.write.*` grants writes, which always execute on your server
regardless of how the consumer reads.

Consumers only ever see what their token covers: the catalog endpoint
(`GET /nameservice/snapshot`, surfaced as `fluree remote ledgers`) filters
to the token's scope, and out-of-scope ledgers answer 404.

### 3. Declare per-ledger participation

By default every ledger is served on both tiers. To restrict a ledger,
transact the `f:servingDefaults` setting group into its config graph — it
travels with the ledger and needs no server restart (details:
[setting groups](../ledger-config/setting-groups.md)):

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:analytics:main#config> {
    <urn:cfg:main>    a f:LedgerConfig ;
                      f:servingDefaults <urn:cfg:serving> .
    # "bring your own compute": blocks are served, queries are refused
    <urn:cfg:serving> f:serveQuery  false ;
                      f:serveBlocks true .
}
```

- `f:serveQuery false` → query endpoints answer 403 for this ledger.
- `f:serveBlocks false` → the block/replication endpoints (`/storage/block`,
  `/storage/objects`, `/commits`, `/pack`) answer 404; only query serving
  remains. **This is the posture for any ledger with row-level policies** —
  it forces all access through the policy-enforcing tier.
- The effective tiers are advertised per ledger, so consumers see what's on
  offer (the `SERVING` column of `fluree remote ledgers`).

These gates bind your server's serving surface. A consumer who already
holds the blocks (peer cache, clone) can always query their own copy —
that's inherent to handing over the data, not a policy gap.

### 4. Row-level permissioning (query serving)

Fine-grained sharing = query serving + identity-bound policy. Three pieces
must line up; the full policy language is in the
[policies cookbook](cookbook-policies.md).

**a. Store the policies and tag the identity** (in the shared ledger). The
pattern is the cookbook's pair — a required restriction on the sensitive
property plus a default allow for everything else:

```jsonld
{
  "ledger": "sales:main",
  "insert": [
    { "@id": "ex:margin-restriction", "@type": ["f:AccessPolicy", "ex:PartnerClass"],
      "f:required": true,
      "f:onProperty": [{"@id": "ex:internalMargin"}],
      "f:action": [{"@id": "f:view"}] },
    { "@id": "ex:partner-default-view", "@type": ["f:AccessPolicy", "ex:PartnerClass"],
      "f:action": [{"@id": "f:view"}],
      "f:allow": true },
    { "@id": "https://example.org/consumers/acme-analyst",
      "f:policyClass": [{"@id": "ex:PartnerClass"}] }
  ]
}
```

(The required policy has no `f:query`, so it never grants —
`ex:internalMargin` is simply invisible to this class. Conditional grants
and the full pattern library are in the cookbook.)

**b. Mint the token with that identity** (`--identity` sets
`fluree.identity`, which the server binds non-spoofably to every request):

```bash
fluree token create --private-key @signing.key \
  --identity "https://example.org/consumers/acme-analyst" \
  --read-ledger sales:main --expires-in 30d
```

**c. Keep the ledger query-only** (`f:serveBlocks false`, section 3), so
the policy tier is the only road in.

The consumer does nothing special: they authenticate with the token, and
every query they send is evaluated as that identity — the policy classes
tagged on the identity load automatically.

### 5. Optional: vended S3 credentials

If your storage is S3, block-serving consumers can read **directly from
S3** instead of proxying bytes through your server — same access model,
near-zero serving cost:

```bash
fluree-server ... \
  --storage-vend-enabled \
  --storage-vend-role-arn arn:aws:iam::123456789012:role/fluree-vend
```

Each grant is an STS session narrowed to the requested ledger's S3 prefix,
with a short TTL (default 15 minutes — grants outlive token revocation
until they expire, so keep TTLs short). Requirements and IAM notes:
[query-peers](../operations/query-peers.md). Consumers pick this up
automatically — no configuration on their side.

## Consumer setup

Everything below is driven from the provider's URL plus the token they
issued you.

### Connect and discover

```bash
fluree remote add acme https://data.acme.example
fluree auth login --remote acme --token @acme-token.jwt   # or paste interactively

fluree remote ledgers acme
# LEDGER            COMMIT T   INDEX T   SERVING
# sales:main        1042       1040      query
# analytics:main    77         77        query+blocks
```

The `SERVING` column is your menu: `query` means the provider executes your
queries; `blocks` means you may run them locally in peer mode.

### Pattern: provider executes your queries

```bash
fluree track add sales --remote acme --remote-alias sales:main
fluree query sales 'SELECT ?product ?total WHERE { ... }'
```

Every query is an HTTP round-trip; results reflect the row-level policy
bound to your token's identity. Writes (if your token has write scope) work
the same way: `fluree insert sales ...`.

### Pattern: your compute over their blocks (peer mode)

```bash
fluree track add analytics --remote acme --remote-alias analytics:main --mode peer
fluree query analytics 'SELECT (COUNT(?s) AS ?n) WHERE { ?s a ex:Event }'
```

The first query streams the index blocks it touches (CID-verified) and
caches them on disk; subsequent queries mostly hit the cache, with one
cheap head check per query for freshness. If the provider vends S3
credentials you'll see `notice: reading 'analytics:main' directly from S3`.
Writes still forward to the provider over HTTP — peer mode only changes
where *reads* execute. Cache maintenance: `fluree cache status` /
`fluree cache clear` (always safe — everything is content-addressed and
re-fetched on demand). See [track](../cli/track.md) and
[cache](../cli/cache.md).

### Pattern: full replication

```bash
fluree clone acme analytics    # full copy into local storage
fluree pull analytics          # catch up later
```

After a clone the ledger is fully local — queryable offline, no further
contact with the provider until you pull. Requires the same storage-scope
token as peer mode.

### Programmatic (Rust) consumers

An application embedding Fluree mounts a remote next to its own ledgers —
mounted aliases are read-only and mix freely with local ledgers in one
query:

```rust
let storage = ProxyStorage::from_api_base(url.clone(), token.clone(), ProxyReadMode::Raw)
    .with_local_prefix("acme");
let ns = Arc::new(ProxyNameService::from_api_base(url, token));
let fluree = FlureeBuilder::memory()
    .with_remote_mount(RemoteMountSpec::new("acme", ns, storage))
    .build_memory();
// fluree.graph("acme/analytics:main").query()… — local compute, remote bytes
// FROM ["books:main", "acme/analytics:main"] federates local + mounted
```

## Semantics worth knowing

- **Revocation.** Query serving revokes at token expiry or issuer removal,
  per request. Block serving is revoked for *new* fetches the same way, but
  blocks a consumer already fetched (and any vended S3 grant) remain usable
  until cache-cleared / expired — all-or-nothing sharing means the handed-over
  data is theirs. Posture changes (`f:servingDefaults`) apply on the next
  request.
- **Integrity.** Everything on the block tier is content-addressed and
  verified against its CID on both ends; a consumer's cache never goes
  stale (only nameservice heads move) and is always safe to delete.
- **Freshness.** Peer-mode queries check the provider's head each query, so
  reads are as current as the provider's index. Time travel works against
  whatever history the provider retains.
- **Don't mix tiers for permissioned data.** A ledger with row-level
  policies must not grant `fluree.storage.*` tokens or `f:serveBlocks` —
  raw blocks carry every row.

## Related documentation

- [Remote mounts and serving tiers](../design/remote-mounts.md) — the design
- [Query peers and replication](../operations/query-peers.md) — operator reference
- [Policies cookbook](cookbook-policies.md) — the policy language
- [Authentication](../security/authentication.md) · [Auth contract](../design/auth-contract.md)
- CLI: [remote](../cli/remote.md) · [track](../cli/track.md) · [cache](../cli/cache.md) · [token](../cli/token.md) · [clone](../cli/clone.md) · [pull](../cli/pull.md)
