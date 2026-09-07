# Trusted policy authorization

Authenticated data requests use policy inputs chosen by verified authority. A request body, query source, or `fluree-policy-*` header cannot select a different identity, class set, inline policy, variable binding, or permissive default. Having a ledger identity record without `f:policyClass` assignments does not grant impersonation privileges.

Ordinary identity-bearing tokens resolve their identity's policies, or the server's configured default policy class. Signed request bodies use the signing identity and do not inherit a bearer token's delegation. Explicit caller `default-allow: false` remains available to narrow the default; other caller policy selections are replaced, rather than merged into the authoritative policy set. Ledger configuration's override controls still apply.

An authenticated scope-only service token (neither `fluree.identity` nor `sub`, and no server default class) retains coarse read/write access within its signed scopes. Issuing such a token authorizes unrestricted data access within those scopes, subject to mandatory ledger configuration. For end-user filtering, issue an identity-bearing token or a delegated policy selection. Anonymous requests allowed by `data_auth.mode=none` or `optional` retain direct policy options; those modes are appropriate only when the host intentionally allows anonymous access.

## Embedded applications

After authenticating the user and resolving their grants, construct a `PolicyAuthorization` from application-owned values:

```rust,ignore
use fluree_db_api::{GovernanceOptions, PolicyAuthorization};

let authorization = PolicyAuthorization::from_trusted_options(GovernanceOptions {
    identity: Some("https://app.example/users/123".into()),
    policy_class: Some(vec!["https://app.example/policies/Employee".into()]),
    ..Default::default()
});
let result = fluree.query_from()
    .jsonld(&caller_query)
    .authorization(&authorization)
    .execute_formatted()
    .await?;
```

The builder enforces this selection across its raw, formatted, string, and tracked terminals, including SPARQL. Source-specific policy overrides are removed. A prebuilt `.policy(...)` cannot be combined with `.authorization(...)`.

The type intentionally does not implement `Deserialize`. Constructing it is a trust decision by the embedding application. An empty context engages default-deny enforcement; an application that authorizes an unrestricted default must explicitly set `default_allow: Some(true)`.

The application must separately authorize every ledger/action requested. One context applies uniformly to the query's sources. If sources need different grants, split the requests or select a policy set valid for all of them. For transactions and already-resolved views, use `authorization.options()` with the governance/policy builder. For GraphQL mutations, `graphql_transact_with_authorization` retains the context through schema derivation, each write, and the returned data, rebuilding policy against each new state. `apply_to_jsonld` can bind a JSON request after the application's final option merge. Do not subsequently replace it with caller policy options.

## Application gateways over HTTP

A gateway can place its selected policies in the existing signed data bearer token. The receiver must both trust the token issuer for ordinary data authentication and explicitly permit that issuer to select policies:

```toml
[server.auth.data]
mode = "required"
audience = "fluree-production"
trusted_issuers = ["did:key:<gateway-signing-key>"]
policy_authorities = ["did:key:<gateway-signing-key>"]
```

The CLI equivalent is `--data-auth-policy-authority`, repeatable; the environment variable is `FLUREE_DATA_AUTH_POLICY_AUTHORITIES`. Configuring authorities requires `--data-auth-audience` / `FLUREE_DATA_AUTH_AUDIENCE`. For OIDC, configure the issuer/JWKS pair through the existing OIDC settings and list that issuer URL as a policy authority. The development setting that accepts any issuer does **not** grant policy-authority status.

Example decoded JWT payload (timestamps must be current when issuing):

```json
{
  "iss": "did:key:<gateway-signing-key>",
  "aud": "fluree-production",
  "sub": "https://app.example/users/123",
  "exp": 1788739500,
  "fluree.ledger.read.ledgers": ["customer/data:main"],
  "fluree.ledger.write.ledgers": ["customer/data:main"],
  "fluree.policy": {
    "policy-class": ["https://app.example/policies/Employee"],
    "default-allow": false
  }
}
```

`fluree.identity`, if present, overrides `sub` for the policy subject. The `fluree.policy` object accepts only `policy-class` (array of strings), `policy` (inline policy JSON), `policy-values` (variable-binding object), and `default-allow` (boolean). Unknown fields and a present null context are rejected; omit `fluree.policy` entirely for ordinary authentication. Prefer fully qualified class and identity IRIs. Explicit classes select policies and identity binds `?$identity`; local identity-to-class assignments are not required for this application contract.

The signature covers the selection and its ledger/action scopes, audience, and expiry. The existing token verifier checks those claims; the policy-authority allowlist adds a separate receiver-side capability check. Invalid delegation is rejected rather than silently downgraded to an ordinary token. The receiver trusts an authorized issuer to issue grants within the server's existing issuer/scoping model; it does not independently re-evaluate the gateway's application grants. Do not designate a general login issuer as a policy authority unless it controls these claims appropriately.

This same selection is used by JSON-LD, SPARQL, Cypher, GraphQL, streaming queries, multi-query aliases, transaction routes, push ingestion, and Bolt sessions. Query source and envelope options cannot replace it. The token authorizes a uniform selection over its signed scopes, not a map of different policy sets per ledger.

Storage-proxy endpoints reject tokens containing `fluree.policy`, including tokens with storage scopes, because that surface does not implement delegated policy selection. Use data-query endpoints for delegated access and separate replication credentials for storage access. GraphQL mutations return HTTP 501 on Raft-enabled servers; submit writes through transaction endpoints. GraphQL reads remain available.

## Authorization logging

Enable `RUST_LOG=fluree_db_server::authorization=debug` to record bearer-token scope checks. Each event includes `issuer`, `effective_identity`, `authorization_mode` (`delegated`, `server-default`, `identity`, or `scope-only`), `ledger`, `action`, and `scope_allowed`. Add this target to your existing log filter to retain other logging, for example `RUST_LOG=info,fluree_db_server::authorization=debug`.

These events describe the credential's ledger/action scope decision. Subsequent policy evaluation can still filter results or reject a write. A request may check multiple sources or perform several scope checks; the events are not a count of completed requests. Signed request-body authentication uses its signing identity separately. The events omit tokens, inline policies, and policy-variable values. They are disabled at the default info level; enabling them adds log formatting and output work per scope check.

## Cost and migration

The HTTP token is verified once through the existing verification path. Policy authorization adds claim parsing, an issuer allowlist check, and request-level option normalization. It adds no ledger lookup, second token verification, per-fact work, network request, or Raft proposal. A follower forwards the bearer credential and the leader verifies it before submitting the selected governance through the replicated command queue. Policy construction and enforcement remain separate costs. The embedded query builder clones caller JSON once when binding a context; large inline policy documents also increase token and normalization cost.

To measure request binding locally, run `cargo bench -p fluree-db-api --features credential --bench policy_authorization`. The benchmark compares ordinary and delegated claims selecting the same policy class, with simple and multiple-source requests. It reports latency and allocation bytes, including tracking-allocator overhead. It excludes signature verification, issuer lookup, networking, and database policy evaluation, so its results describe normalization cost rather than end-to-end query latency. Set `CRITERION_HOME` to choose the results directory.

Applications that relied on policy-free identities to override policy headers must migrate to explicit contexts. Embedded applications construct typed contexts after resolving user grants. HTTP gateways issue scoped tokens and configure the receiving server's issuer, policy-authority, and audience settings. Preserve the selected governance through queued writes and credential forwarding. Cluster transport authentication is configured independently of policy delegation.
