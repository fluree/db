# Authentication

Fluree supports multiple authentication mechanisms to cover different deployment scenarios — from standalone servers with no external identity provider to managed platforms using OIDC.

This document describes the authentication model, the supported modes, the bearer token claim set, and the access boundary between replication and query operations.

## Identity vs transport

### Identity (who)

Fluree policy enforcement is based on an **identity**, ideally a DID:

- **Preferred**: `did:key:...` — portable across environments, no central identity server required
- **Also possible**: other DIDs or IRIs mapped into Fluree policy (e.g. `ex:alice`)

Policies are stored as RDF triples in the ledger and evaluated at query/transaction time against the requesting identity. See [Policy model](policy-model.md) for details.

### Transport (how requests authenticate)

Two "on-the-wire" mechanisms carry the identity:

| Mechanism | Format | When to use |
|-----------|--------|-------------|
| **Signed requests** | JWS/VC envelope containing the DID | Proof-of-possession; trustless environments |
| **Bearer tokens** | `Authorization: Bearer <JWT>` | Session-based; OIDC/OAuth2 flows |

Bearer tokens are a UX and deployment convenience — they do not replace the identity model. The server extracts the identity from the token claims and enforces the same dataset policies as signed requests.

## Three supported auth modes

### Mode 1 — Decentralized: `did:key` signed requests (no IdP)

- The client holds an Ed25519 keypair and derives a `did:key:...`
- Requests are signed using JWS or Verifiable Credential format
- The server verifies the signature and uses the DID as the principal
- Dataset policies decide allow/deny

This preserves Fluree's core value: no central identity server required.

See [Signed requests (JWS/VC)](../api/signed-requests.md) for the wire format.

### Mode 2 — Standalone server with offline-minted tokens

Designed for: "stand up a server somewhere" (local dev, single-node EC2, etc.).

- An admin generates an Ed25519 keypair with `fluree token keygen`
- The admin mints a scoped Bearer token with `fluree token create`
- The admin provides the token to CLI users or stores it in a secret manager
- The server validates the token's embedded JWK signature and enforces scopes + policy

The **policy identity** remains DID-based (`fluree.identity` claim), so authorization stays dataset/policy driven even though the transport is a Bearer token.

See [CLI token command](../cli/token.md) for minting instructions.

### Mode 3 — OIDC/OAuth2 with an external identity provider

Designed for: managed platforms (e.g., any application using an OIDC provider).

- The IdP authenticates the user (device flow, PKCE, etc.)
- The application knows the user's Fluree dataset entitlements
- The application issues (or exchanges for) a **Fluree-scoped token** carrying:
  - identity (`fluree.identity` — ideally a DID)
  - ledger read/write scopes
  - optional policy class
- The server verifies the token against the provider's JWKS endpoint

This preserves separation of concerns:

- **IdP**: authentication (who logged in)
- **Application**: authorization (what they can access in Fluree)

The server must be configured with `--jwks-issuer` to trust OIDC tokens. See [Configuration — OIDC](../operations/configuration.md#oidc--jwks-token-verification).

## Bearer token claim set

All Fluree Bearer tokens (Mode 2 and Mode 3) share the same claim set. The server extracts identity and scopes from these claims regardless of how the token was signed.

### Standard JWT claims

| Claim | Required | Description |
|-------|----------|-------------|
| `iss` | Yes | Issuer — `did:key:...` for Ed25519 tokens, URL for OIDC tokens |
| `sub` | No | Subject — human-readable identity of the token holder |
| `aud` | No | Audience — target service (e.g. server URL) |
| `exp` | Yes | Expiration time (Unix timestamp) |
| `iat` | Yes | Issued-at time (Unix timestamp) |

### Fluree-specific claims

| Claim | Type | Description |
|-------|------|-------------|
| `fluree.identity` | String (IRI/DID) | Identity for policy enforcement — takes precedence over `sub` |
| `fluree.policy.class` | String (IRI) | Optional policy class for identity-based policy lookup |

### Scope claims

Scopes control which endpoints and ledgers a token can access.

#### Query scopes (`fluree.ledger.*`)

| Claim | Type | Description |
|-------|------|-------------|
| `fluree.ledger.read.all` | Boolean | Read access to all ledgers via data API |
| `fluree.ledger.read.ledgers` | Array of strings | Read access to specific ledgers |
| `fluree.ledger.write.all` | Boolean | Write access to all ledgers via data API |
| `fluree.ledger.write.ledgers` | Array of strings | Write access to specific ledgers |

#### Replication scopes (`fluree.storage.*`)

| Claim | Type | Description |
|-------|------|-------------|
| `fluree.storage.all` | Boolean | Storage/replication access to all ledgers |
| `fluree.storage.ledgers` | Array of strings | Storage/replication access to specific ledgers |

**Back-compat:** `fluree.storage.*` claims also imply data API read access for the same ledgers.

#### Populating `fluree.storage.ledgers` (multi-tenant hint)

If you run an IdP or a request-router that exchanges IdP tokens for Fluree-scoped tokens, prefer populating `fluree.storage.ledgers` rather than granting `fluree.storage.all`.

Recommended conventions for mapping IdP group/role claims to ledger scopes:

- Treat group values like `fluree:storage:<ledger-id>` (example: `fluree:storage:books:main`) as permission to replicate that ledger.
- Optionally support wildcards at the router boundary (example: `fluree:storage:books:*` expands to the set of ledgers your router knows about under `books:`).
- Reserve `fluree.storage.all=true` for admin/service accounts.

#### Event scopes (`fluree.events.*`)

| Claim | Type | Description |
|-------|------|-------------|
| `fluree.events.all` | Boolean | SSE event stream for all ledgers |
| `fluree.events.ledgers` | Array of strings | SSE event stream for specific ledgers |

### Example token payload

```json
{
  "iss": "https://solo.example.com",
  "sub": "alice@example.com",
  "aud": "https://fluree.example.com",
  "exp": 1700000000,
  "iat": 1699996400,
  "fluree.identity": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
  "fluree.ledger.read.all": true,
  "fluree.ledger.write.ledgers": ["mydb:main", "mydb:staging"]
}
```

## Token verification paths

The server supports two verification paths, selected automatically based on the JWT header:

| JWT header | Path | Algorithm | Trust model |
|------------|------|-----------|-------------|
| Contains `jwk` (embedded key) | Ed25519 / did:key | EdDSA | Issuer trust checked against `--events-auth-trusted-issuer` (or admin/storage equivalents) |
| Contains `kid` (key ID) | OIDC / JWKS | RS256 | Issuer must match a `--jwks-issuer`; key fetched from JWKS endpoint |

This dual-path dispatch is transparent to callers — the same `Authorization: Bearer <token>` header works for both paths. The server applies the same scope and identity enforcement regardless of which path verified the signature.

## Replication vs query access boundary

Fluree draws a hard boundary between **replication-scoped** and **query-scoped** access.

### Replication access (`fluree.storage.*`)

Replication operations — nameservice sync, storage proxy reads, and CLI `fetch`/`pull`/`push` — require **root-level** `fluree.storage.*` claims. These operations transfer raw commit data and index blocks; they bypass dataset policy because the data must be bit-identical to what the transaction server wrote.

Replication tokens are intended for **operator and service-account use** (e.g. a peer server's storage-proxy token, or an admin's CLI pull/push workflow). They should never be issued to end users.

### Query access (`fluree.ledger.read/write.*`)

Query operations — `/:ledger/query`, `/:ledger/insert`, connection-scoped SPARQL, etc. — use `fluree.ledger.read/write.*` claims. These go through the full query engine and dataset policy enforcement. The server never exposes raw storage bytes through query endpoints.

Query tokens are appropriate for **end users and application service accounts**. Combined with a `fluree.identity` claim and dataset policies, the server enforces fine-grained row- and property-level access control.

### CLI consequence: `track` vs `pull`

| Command | Access type | Required scope | What happens |
|---------|-------------|----------------|--------------|
| `fluree pull` | Replication | `fluree.storage.*` | Downloads raw commits and indexes into local storage |
| `fluree track` | Query | `fluree.ledger.read/write.*` | Registers a remote ledger; queries forwarded to server |

If a user holds only query-scoped tokens, they **cannot** clone or pull a ledger. They can only `track` it and issue queries/transactions against the remote.

## Identity precedence

When multiple identity signals are present, the server uses this precedence (highest first):

1. **Signed request DID** — proof-of-possession from JWS/VC signature
2. **Bearer token `fluree.identity`** — identity claim in the token
3. **Client-provided headers/body** — only honored when the server is in unauthenticated mode

When auth is present, the server forces `opts.identity` (and optional policy class) from the token, ignoring any client-provided identity in headers or request bodies. This prevents identity spoofing.

## Endpoint coverage

All Bearer-token-authenticated endpoints support both Ed25519 and OIDC verification paths:

| Endpoint group | Extractor | Scopes checked |
|----------------|-----------|----------------|
| Data API (query/update/info/exists) | `MaybeDataBearer` | `fluree.ledger.read/write.*` |
| Admin (create/drop) | `require_admin_token` | Issuer trust |
| Events (SSE) | `MaybeBearer` | `fluree.events.*` |
| Storage proxy | `StorageProxyBearer` | `fluree.storage.*` |
| Nameservice refs | `StorageProxyBearer` | `fluree.storage.*` |

MCP endpoints currently use the Ed25519 path only.

## Security notes

- Tokens are validated server-side on every request; client-side validation is never trusted
- Out-of-scope ledgers return `404` (not `403`) to avoid existence leaks
- `fluree.storage.*` tokens grant raw data access — issue only to trusted operators
- Connection-scoped SPARQL (`FROM`/`FROM NAMED`) requires all referenced ledgers to be within the token's read scope

## See also

- [Signed requests (JWS/VC)](../api/signed-requests.md) — Wire format for signed requests
- [Configuration — OIDC](../operations/configuration.md#oidc--jwks-token-verification) — Server OIDC/JWKS setup
- [CLI auth command](../cli/auth.md) — Managing tokens on remotes
- [CLI token command](../cli/token.md) — Minting Ed25519 tokens
- [Auth contract (CLI ↔ Server)](../design/auth-contract.md) — Discovery, exchange, and refresh protocol
- [Policy model](policy-model.md) — Dataset-level access control
