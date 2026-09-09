# Override Control

Fluree's config resolution follows a three-tier precedence model. Each setting group is resolved independently, and an **override control** mechanism governs whether higher-priority sources can change values set at lower tiers.

## Resolution precedence

Settings are resolved from lowest to highest priority:

| Priority | Source | When it applies |
|----------|--------|-----------------|
| 4 (lowest) | System defaults | No config present (allow-all, no SHACL, no reasoning) |
| 3 | Ledger-wide config (`f:LedgerConfig`) | Fallback for any setting not overridden at higher tiers |
| 2 | Per-graph config (`f:GraphConfig`) | Only if ledger-wide override control permits |
| 1 (highest) | Query/transaction-time opts | Only if effective override control permits + identity check passes |

## Override control modes

Each setting group may include an `f:overrideControl` field controlling whether higher-priority sources can override the value.

| Mode | Value | Behavior |
|------|-------|----------|
| No overrides | `f:OverrideNone` | Config values are final. No per-graph or query-time overrides permitted. |
| All overrides | `f:OverrideAll` | Any request can override. Default when `f:overrideControl` is absent. |
| Identity-gated | Object with `f:controlMode: f:IdentityRestricted` | Only requests with a server-verified identity matching `f:allowedIdentities` can override. |

### Identity-gated example

```json
{
  "f:overrideControl": {
    "f:controlMode": { "@id": "f:IdentityRestricted" },
    "f:allowedIdentities": [
      { "@id": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK" }
    ]
  }
}
```

### Identity verification

Override identity is the **auth-layer-verified request identity**, never a value the caller writes into a request. In the code it is the `VerifiedIdentity` type, which has a single constructor; the only places that call it are the server's auth boundaries. The server establishes it from:

- With the `credential` feature: the DID of a verified JWS credential
- With bearer tokens (including OAuth/OIDC tokens the server accepts): the `fluree.identity` claim, falling back to `sub`, of a verified token
- Bolt sessions: the identity claim inside the session's verified token
- MCP tools: the authenticated MCP principal

What does **not** count, in any server mode:

- `"opts": {"identity": "..."}` in query or transaction JSON, and the `fluree-identity` header. Both are policy evaluation context. In unauthenticated server modes they become the policy identity, but override control never consults the policy identity, so no request can satisfy an allow-list there.
- Under root-bearer impersonation, policy evaluation uses the impersonated target while override control uses the bearer's own verified DID.
- Anonymous requests (no verified identity) are always denied by `f:IdentityRestricted`.
- CLI requests are always anonymous for override purposes: the CLI has no auth layer, so its `--identity` and policy flags only set the policy identity.

An application embedding the Rust API is the auth layer for its deployment. It supplies the identity it has verified through `GovernanceOptions::server_identity`, `QueryExecutionOptions::with_server_identity`, and the transact builders' `server_identity` setter; left unset, identity-restricted overrides are denied.

### Query-time vs transact-time overrides

- **Query-time overrides** (reasoning modes, datalog settings, policy opts): the verified identity of the query request. It travels as `QueryExecutionOptions::server_identity` from the request boundary through query preparation, and the API stamps it onto the governance it parses from the request body, including per-source policy overrides.
- **Transact-time overrides** (SHACL `validationMode`, policy defaults on writes): the verified identity of the transaction request, recorded on the stage builder and carried through consensus. It is never derived from the transaction's policy context.

## Monotonicity: per-graph can only tighten

Ledger-wide `f:overrideControl` sets the **maximum permissiveness**. Per-graph configs may only restrict further, never loosen.

Permissiveness ordering: `f:OverrideNone` < `f:IdentityRestricted` < `f:OverrideAll`

The effective per-graph override control is **min(ledger-wide, per-graph)**:

| Ledger-wide | Per-graph | Effective | Why |
|-------------|-----------|-----------|-----|
| `OverrideNone` | `OverrideAll` | **OverrideNone** | Per-graph cannot loosen (warning logged) |
| `IdentityRestricted({alice})` | `OverrideAll` | **IdentityRestricted({alice})** | Per-graph cannot loosen |
| `IdentityRestricted({alice, bob})` | `IdentityRestricted({alice})` | **IdentityRestricted({alice})** | Intersection: per-graph tightens |
| `OverrideAll` | `OverrideNone` | **OverrideNone** | Per-graph tightens (valid) |
| `OverrideAll` | `IdentityRestricted({alice})` | **IdentityRestricted({alice})** | Per-graph tightens (valid) |
| `OverrideAll` | (absent) | **OverrideAll** | Inherits ledger-wide |

When both are `IdentityRestricted`, the effective `allowedIdentities` is the **intersection** of the two lists.

## Resolution algorithm

For each setting group independently:

```
1. Start with system defaults
2. Apply ledger-wide config for this group (if present)
3. Get ledger-wide overrideControl (default: OverrideAll)
4. If ledger-wide overrideControl is OverrideNone:
     → this group is final. Skip to step 8.
5. Apply per-graph config for this group (if present)
6. Compute effective overrideControl:
     = min(ledgerWide, perGraph)
     If both IdentityRestricted: allowedIdentities = intersection
7. Check effective overrideControl against query/txn-time opts:
     OverrideNone         → config values are final
     OverrideAll          → apply query-time opts
     IdentityRestricted   → apply only if request identity matches
8. Result is the effective setting for this group.
```

## Per-group truth tables

### Policy (`f:policyDefaults`)

| Ledger-wide | Per-graph | Query (identity) | Effective | Why |
|-------------|-----------|-------------------|-----------|-----|
| `defaultAllow: false`, OverrideNone | (none) | `defaultAllow: true` (any) | **deny** | No overrides allowed |
| `defaultAllow: false`, OverrideAll | (none) | `defaultAllow: true` (any) | **allow** | All overrides allowed |
| `defaultAllow: false`, IdentityRestricted({alice}) | (none) | `defaultAllow: true` (alice) | **allow** | Alice is authorized |
| `defaultAllow: false`, IdentityRestricted({alice}) | (none) | `defaultAllow: true` (bob) | **deny** | Bob not authorized |
| `defaultAllow: false`, IdentityRestricted({alice}) | (none) | `defaultAllow: true` (anon) | **deny** | No identity = no override |
| `defaultAllow: false`, OverrideNone | `defaultAllow: true` | (none) | **deny** | OverrideNone blocks per-graph |
| `defaultAllow: false`, OverrideAll | `defaultAllow: true` | (none) | **allow** | Per-graph overrides ledger-wide |
| `defaultAllow: true`, OverrideAll | `defaultAllow: false`, OverrideNone | `defaultAllow: true` (any) | **deny** | Per-graph OverrideNone blocks query |
| (none) | (none) | (none) | **allow** | System default (allow-all) |

### Reasoning (`f:reasoningDefaults`)

| Ledger-wide | Per-graph | Query (identity) | Effective | Why |
|-------------|-----------|-------------------|-----------|-----|
| `modes: [rdfs]`, OverrideNone | (none) | `reasoning: [owl2rl]` (any) | **rdfs** | No overrides |
| `modes: [rdfs]`, OverrideAll | (none) | `reasoning: [owl2rl]` (any) | **owl2rl** | Override allowed |
| `modes: [rdfs]`, IdentityRestricted({alice}) | (none) | `reasoning: [owl2rl]` (alice) | **owl2rl** | Alice authorized |
| `modes: [rdfs]`, IdentityRestricted({alice}) | (none) | `reasoning: [owl2rl]` (bob) | **rdfs** | Bob not authorized |
| `modes: [rdfs]`, OverrideAll | `modes: [owl2rl]` | (none) | **owl2rl** | Per-graph overrides |
| `modes: [rdfs]`, OverrideNone | `modes: [owl2rl]` | (none) | **rdfs** | OverrideNone blocks per-graph |

### SHACL (`f:shaclDefaults`)

| Ledger-wide | Per-graph | Effective | Why |
|-------------|-----------|-----------|-----|
| `enabled: false`, OverrideNone | `enabled: true` | **disabled** | OverrideNone blocks per-graph |
| `enabled: true`, OverrideAll | `enabled: false` | **disabled** | Per-graph disables for its graph |
| `mode: warn`, OverrideAll | `mode: reject` | **reject** | Per-graph overrides |

Transactions can also request a validation mode for themselves via
`opts.validationMode` (`"warn"` / `"reject"`). Gating is **asymmetric**:
strengthening (`warn` → `reject`) is always honored, while softening
(`reject` → `warn`) is granted only when the SHACL group's
`f:overrideControl` permits it for the request's verified identity. A denied
softening request keeps the configured posture (with a server-side warning
log); it does not fail the transaction. The request can never toggle
`f:shaclEnabled`.

| Config mode | Override control | Request (identity) | Effective | Why |
|-------------|------------------|--------------------|-----------|-----|
| `reject` | OverrideAll | `warn` (any) | **warn** | Softening allowed |
| `reject` | OverrideNone | `warn` (any) | **reject** | Softening denied |
| `reject` | IdentityRestricted({remediator}) | `warn` (remediator) | **warn** | Identity authorized |
| `reject` | IdentityRestricted({remediator}) | `warn` (other/none) | **reject** | Not authorized |
| `warn` | OverrideNone | `reject` (any) | **reject** | Strengthening is always free |

The identity checked is the auth-layer-verified one (bearer / credential
DID), not the user-settable `opts.identity` or the `fluree-identity` header,
in every server mode: with no auth layer in play no request can satisfy an
allow-list, and an embedded caller must set the builder's `server_identity`
rather than rely on a policy context. Typical use: a remediation agent whose
corrective writes transiently violate shapes gets per-write softening — under
`IdentityRestricted`, only that agent — without flipping the graph's standing
posture for every other writer.

### Transact (`f:transactDefaults`)

Transact defaults use **additive** merge semantics, unlike other groups. However, the general override control rule still applies: if the ledger-wide `f:overrideControl` is `f:OverrideNone`, per-graph transact defaults are blocked entirely.

| Ledger-wide | Per-graph | Effective | Why |
|-------------|-----------|-----------|-----|
| `uniqueEnabled: true` | `uniqueEnabled: false` | **enabled** | Monotonic OR — cannot disable |
| `uniqueEnabled: true`, sources: `[default]` | sources: `[schemaGraph]` | sources: **[default, schemaGraph]** | Additive — sources accumulate |
| `uniqueEnabled: false` | `uniqueEnabled: true` | **enabled** | Per-graph can enable |
| `uniqueEnabled: true`, OverrideNone | sources: `[schemaGraph]` | sources: **[default]** only | OverrideNone blocks per-graph additions |

## Overridable vs non-overridable fields

Not all fields in a setting group are overridable. Source pointers (where rules/shapes/schema come from) are always config-only:

| Subsystem | Overridable fields | Non-overridable (config-only) |
|-----------|-------------------|-------------------------------|
| `f:policyDefaults` | `f:defaultAllow`, `f:policyClass` | `f:policySource` |
| `f:shaclDefaults` | `f:validationMode`, `f:shaclEnabled` | `f:shapesSource` |
| `f:reasoningDefaults` | `f:reasoningModes` | `f:schemaSource` |
| `f:datalogDefaults` | `f:datalogEnabled`, `f:allowQueryTimeRules` | `f:rulesSource` |

Non-overridable fields can only be changed by writing to the config graph. This prevents a query from redirecting the engine to read rules or schema from an arbitrary graph.

## Per-graph overrides

Per-graph overrides target specific named graphs by IRI:

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:policyDefaults [
      f:defaultAllow true ;
      f:overrideControl f:OverrideAll
    ] ;
    f:graphOverrides (
      [ a f:GraphConfig ;
        f:targetGraph <http://example.org/sensitive> ;
        f:policyDefaults [
          f:defaultAllow false ;
          f:overrideControl f:OverrideNone
        ]
      ]
    ) .
}
```

In this example:
- **All graphs** default to `defaultAllow: true` with `OverrideAll`
- **`http://example.org/sensitive`** overrides to `defaultAllow: false` with `OverrideNone` — no query can override policy for this graph
- `f:targetGraph` uses `f:defaultGraph` for the default graph
