# Trusted policy authorization

An application can select users and policies dynamically, or give a downstream client a fixed signed selection. Fluree enforces those policies at the data layer. Ledger scopes and configured policy-override controls still apply.

| Request | Who selects the policy? | Request policy options | No selection supplied |
| --- | --- | --- | --- |
| No credential, auth disabled or optional | Caller | Accepted directly | Ledger defaults; unrestricted if unconfigured |
| Application credential: `"fluree.policy": "request"` | Trusted application, per request | Accepted | Deny |
| Fixed credential: `"fluree.policy": {...}` | Credential issuer | Matching values and `default-allow: false` narrowing accepted; conflicts return 403 | Signed selection applies; an empty context denies |
| Ordinary identity credential or signed request body | Verified identity plus server defaults | Matching values and narrowing accepted; conflicts return 403 | Identity/server-selected policies apply |
| Scope-only credential, without identity or server policy class | Server/ledger configuration | Policy selections rejected; `default-allow: false` accepted | Ledger defaults within signed scopes |

## Fluree behind your application

For local development or a private deployment where the application/network is the trust boundary, run with data authentication disabled. Ordinary request options work directly; no application credential or issuer configuration is required.

For an authenticated deployment, authorize the backend's issuer once:

```toml
[server.auth.data]
mode = "required"
audience = "fluree-production"
policy_authorities = ["did:key:<app-signing-key>"]
```

A policy authority is also a trusted issuer and need not be listed again in `trusted_issuers`. Additional login-only issuers belong in `trusted_issuers` and cannot issue policy capabilities. OIDC authorities still need configured JWKS verification. Only designate an issuer that controls the policy claim, rather than allowing users to set it through profile data.

Create a reusable application credential:

```bash
fluree token create --private-key @app.key --subject backend \
  --audience fluree-production --expires-in 1h \
  --read-ledger customer/data:main --write-ledger customer/data:main \
  --policy-select
```

The CLI signs `"fluree.policy": "request"` together with the audience, expiry, and ledger scopes. Keep this credential in the backend and renew it before expiry. The application authenticates its users, resolves their grants, and supplies the resulting context:

```bash
curl -X POST https://db.example/v1/fluree/query/customer/data:main \
  -H "Authorization: Bearer $APP_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "opts": {
      "identity": "https://app.example/users/123",
      "policy-class": ["https://app.example/policies/Employee"]
    },
    "select": ["?name"],
    "where": {"@id": "?s", "https://schema.org/name": "?name"}
  }'
```

Change `identity`, `policy-class`, inline `policy`, or `policy-values` on the next request without issuing another token. Explicit classes select rules; identity binds `?$identity`. Identity alone selects that identity's classes. Use fully qualified identity and class IRIs.

An omitted or empty application selection denies access, including when locked ledger defaults select allowing rules. An empty class list selects no stored rules and defaults to deny. An explicit empty class list with a deny default and no inline rules is an absolute narrowing: configured grants cannot widen it. For intentionally permissive access, explicitly select `default-allow: true`; shared policy sources can still supply rules. Where overrides are permitted, `policy-class: []` plus `default-allow: true` explicitly selects no stored class rules. Configured override restrictions still govern these choices.

The `fluree-identity`, `fluree-policy-class`, `fluree-policy`, `fluree-policy-values`, and `fluree-default-allow` headers carry policy options for SPARQL, Cypher, GraphQL, push, and commit show. JSON body options override header defaults. Multi-query aliases retain envelope/sub-query precedence. One selection applies across a query's sources; conflicting per-source selections are rejected. A signed request body uses its signing identity and does not inherit an accompanying application's credential authority.

## Fixed signed delegation

Use an object-valued `fluree.policy` when a browser, partner, or other downstream client should carry a fixed selection:

```json
{
  "iss": "did:key:<app-signing-key>",
  "aud": "fluree-production",
  "sub": "https://app.example/users/123",
  "exp": 1789142400,
  "fluree.ledger.read.ledgers": ["customer/data:main"],
  "fluree.policy": {
    "policy-class": ["https://app.example/policies/Employee"],
    "default-allow": false
  }
}
```

`fluree.identity` overrides `sub` for the selected identity. The policy object accepts `policy-class` (string array), `policy` (inline JSON), `policy-values` (binding object), and `default-allow` (boolean). Unknown fields, unknown string modes, and a null claim are rejected. Omit the claim for ordinary authentication.

Both claim forms require a verified signature, a configured policy authority, a matching audience, and valid expiry/scopes. Being an ordinary trusted issuer does not confer policy authority. A policy-free identity record in ledger data confers no delegation authority.

## Embedded applications

The embedded host already owns the authentication boundary. It can keep using `GovernanceOptions`, or bind a resolved application selection explicitly:

```rust,ignore
let authorization = PolicyAuthorization::from_trusted_options(GovernanceOptions {
    identity: Some(user_iri),
    policy_class: Some(granted_classes),
    default_allow: Some(false),
    ..Default::default()
});
let result = fluree.query_from()
    .jsonld(&query)
    .authorization(&authorization)
    .execute_formatted().await?;
```

`PolicyAuthorization` always holds a fixed context. Empty trusted inputs deny; the host authorizes each ledger separately. No token or issuer configuration is required inside the process. The embedded helper silently replaces query-supplied selections with the host's fixed selection, preserving `default-allow: false` narrowing; it does not report selection conflicts. HTTP credential binding instead rejects conflicting selections with 403.

## Transport support and migration

HTTP queries, streaming, transactions, GraphQL, push, and commit show use the same credential selection rules. Bolt supports fixed selections; request-selected credentials without a selection deny, and explicit `imp_user` requests are rejected until impersonation is supported. MCP and storage proxy reject either policy claim because they cannot enforce the complete selection contract. Use separate replication credentials for storage access. `/log` returns read-scoped commit metadata, not policy-filtered flakes. GraphQL mutations on Raft servers return 501; use transaction endpoints.

Clients that previously used policy-free identities to impersonate must use an application credential or fixed delegation. Conflicting `--as` and policy options now return 403 rather than silently displaying another view. Plain and streaming connection reads honor configured defaults, including an explicit deny default with no class selection. Applications that used omitted options for privileged reads must select their intended privileged context explicitly. No-auth, unconfigured deployments remain unrestricted.

Other user-visible changes:

- An explicit `default-allow: false` is now an enforcement input, closing the unrestricted shortcut when it is the only policy option. Configured rules still apply; unmatched access is denied.
- `policy-class: []` explicitly selects no stored class rules instead of being treated as absent. It does not fall back to the identity's classes; configured override restrictions still apply.
- A non-string `identity` or non-boolean `default-allow` in JSON policy options now returns HTTP 400 instead of being silently ignored. Omission and JSON `null` still leave either field unset.
- Policy headers (`fluree-identity`, `fluree-policy-class`, `fluree-policy`, `fluree-policy-values`, and `fluree-default-allow`) now supply defaults for JSON-LD transaction body options even when tracking is disabled. This intentional bug fix also applies to anonymous/no-auth requests. Body options retain precedence, and credential checks still apply.

Enable `RUST_LOG=info,fluree_db_server::authorization=debug` for credential scope events: issuer, effective identity for fixed contexts, mode (`fixed`, `request-selected`, `scope-only`), ledger, action, and scope decision. These precede policy evaluation; request-selected scope events do not identify the dynamically selected user. Tokens, inline policies, and policy values are omitted. See [Benchmarks](../contributing/benches.md) for measuring authorization overhead.

### Ledger configuration reads fail closed

Query, streaming, transaction, and explain paths return an error when the ledger's config graph cannot be read. An unreadable config graph is never treated as "no configuration": doing so would run the request unrestricted on a ledger whose operator may have configured a deny default or a mandatory policy class. Previously such reads were best-effort and fell back to system defaults.

In practice this only surfaces on snapshots that cannot serve any range read, such as a metadata-only historical view loaded without its binary index, where data queries already fail with the same error. A ledger with no config graph is unaffected: a successful read that finds nothing is cached on the view as absent and is not repeated for that snapshot.

### Policy-scoped explain withholds statistics

Explain on a policy-scoped view (any view that is not root, including a plain `default-allow: false` narrowing) returns a plan built without ledger or annotation statistics, and `plan.reason` states that statistics were withheld by policy. A caller who can only see a filtered subset must not learn unfiltered cardinalities from the planner. Unrestricted explains retain their statistics.

The trade-off is that the explained plan may differ from the executed plan: execution still uses the full statistics, so a scoped caller may see a heuristic join order that the engine does not actually choose. When diagnosing performance under policy, run explain with an unrestricted credential against the same query.
