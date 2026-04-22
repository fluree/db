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

Override identity is the **server-verified request identity** (canonical DID string), not a user-supplied query parameter. Specifically:

- With the `credential` feature: the DID from the verified JWS `kid` header
- With server auth middleware: the DID mapped from an OAuth token
- A caller **cannot** become an allowed identity by setting `"opts": {"identity": "..."}` in query JSON — that field is for policy evaluation context, not override authorization
- Anonymous requests (no verified identity) are always denied by `f:IdentityRestricted`

### Query-time vs transact-time overrides

- **Query-time overrides** (reasoning modes, policy opts): identity is the **query caller**
- **Transact-time overrides** (SHACL mode, validation settings): identity is the **transaction signer**

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
| `modes: [rdfs]`, OverrideNone | (none) | `reasoning: [owl2-rl]` (any) | **rdfs** | No overrides |
| `modes: [rdfs]`, OverrideAll | (none) | `reasoning: [owl2-rl]` (any) | **owl2-rl** | Override allowed |
| `modes: [rdfs]`, IdentityRestricted({alice}) | (none) | `reasoning: [owl2-rl]` (alice) | **owl2-rl** | Alice authorized |
| `modes: [rdfs]`, IdentityRestricted({alice}) | (none) | `reasoning: [owl2-rl]` (bob) | **rdfs** | Bob not authorized |
| `modes: [rdfs]`, OverrideAll | `modes: [owl2-rl]` | (none) | **owl2-rl** | Per-graph overrides |
| `modes: [rdfs]`, OverrideNone | `modes: [owl2-rl]` | (none) | **rdfs** | OverrideNone blocks per-graph |

### SHACL (`f:shaclDefaults`)

| Ledger-wide | Per-graph | Effective | Why |
|-------------|-----------|-----------|-----|
| `enabled: false`, OverrideNone | `enabled: true` | **disabled** | OverrideNone blocks per-graph |
| `enabled: true`, OverrideAll | `enabled: false` | **disabled** | Per-graph disables for its graph |
| `mode: warn`, OverrideAll | `mode: reject` | **reject** | Per-graph overrides |

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
