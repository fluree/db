# Programmatic Policy API (Rust)

This guide covers how to use Fluree's policy system programmatically in Rust applications.

## Overview

There are two main approaches to applying policies programmatically:

1. **Identity-based policies** (`wrap_identity_policy_view`): Policies stored in the database and loaded via `f:policyClass` on an identity subject
2. **Inline policies** (`wrap_policy_view` with `opts.policy`): Policies provided directly in the query/transaction options

## Identity-Based Policy Lookup

The recommended approach for production systems. Policies are stored in the ledger and loaded dynamically based on the identity's `f:policyClass` property.

### Storing Policies in the Database

First, insert policies with types that will be referenced by identities:

```rust
let policies = json!({
    "@context": {
        "f": "https://ns.flur.ee/db#",
        "ex": "http://example.org/ns/",
        "schema": "http://schema.org/"
    },
    "@graph": [
        // Identity with policy class assignment
        {
            "@id": "http://example.org/identity/alice",
            "f:policyClass": [{"@id": "ex:EmployeePolicy"}],
            "ex:user": {"@id": "ex:alice"}
        },

        // SSN restriction policy - only see your own SSN
        {
            "@id": "ex:ssnRestriction",
            "@type": ["f:AccessPolicy", "ex:EmployeePolicy"],
            "f:required": true,
            "f:onProperty": [{"@id": "schema:ssn"}],
            "f:action": {"@id": "f:view"},
            "f:query": serde_json::to_string(&json!({
                "where": {
                    "@id": "?$identity",
                    "http://example.org/ns/user": {"@id": "?$this"}
                }
            })).unwrap()
        },

        // Default allow policy for other properties
        {
            "@id": "ex:defaultAllowView",
            "@type": ["f:AccessPolicy", "ex:EmployeePolicy"],
            "f:action": {"@id": "f:view"},
            "f:allow": true
        }
    ]
});

// Prefer the lazy Graph API for transactions
fluree.graph("mydb:main")
    .transact()
    .insert(&policies)
    .commit()
    .await?;
```

### Using wrap_identity_policy_view

Create a policy-wrapped view using an identity IRI:

```rust
use fluree_db_api::{wrap_identity_policy_view, FlureeBuilder, GraphDb};

let fluree = FlureeBuilder::memory().build_memory();
let ledger = fluree.ledger("mydb:main").await?;

// Wrap the ledger with identity-based policy
let wrapped = wrap_identity_policy_view(
    &ledger,
    "http://example.org/identity/alice",  // identity IRI
    true  // default_allow: allow access when no policy matches
).await?;

// Check policy properties
assert!(!wrapped.is_root(), "Should not be root/unrestricted");

// Create a view with the policy applied, then query using the builder
let view = GraphDb::from_ledger_state(&ledger)
    .with_policy(std::sync::Arc::new(wrapped.policy().clone()));

let query = json!({
    "select": ["?s", "?ssn"],
    "where": {
        "@id": "?s",
        "@type": "ex:User",
        "schema:ssn": "?ssn"
    }
});

let result = view.query(&fluree)
    .jsonld(&query)
    .execute()
    .await?;
```

### How Identity Lookup Works

When you call `wrap_identity_policy_view`:

1. Fluree queries for policies via the identity's `f:policyClass`:
   ```sparql
   SELECT ?policy WHERE {
       <identity-iri> f:policyClass ?class .
       ?policy a ?class .
       ?policy a f:AccessPolicy .
   }
   ```

2. Each matching policy's properties are loaded (`f:action`, `f:allow`, `f:query`, `f:onProperty`, etc.)

3. The `?$identity` variable is automatically bound to the identity IRI for use in `f:query` policies

## Inline Policies with policy-values

For cases where policies should not be stored in the database, use inline policies with explicit `?$identity` binding.

### QueryConnectionOptions Pattern

```rust
use fluree_db_api::{QueryConnectionOptions, wrap_policy_view};
use std::collections::HashMap;

let policy = json!([{
    "@id": "ex:inlineSsnPolicy",
    "f:required": true,
    "f:onProperty": [{"@id": "http://schema.org/ssn"}],
    "f:action": "f:view",
    "f:query": serde_json::to_string(&json!({
        "where": {
            "@id": "?$identity",
            "http://example.org/ns/user": {"@id": "?$this"}
        }
    })).unwrap()
}]);

let opts = QueryConnectionOptions {
    policy: Some(policy),
    policy_values: Some(HashMap::from([(
        "?$identity".to_string(),
        json!({"@id": "http://example.org/identity/alice"}),
    )])),
    default_allow: true,
    ..Default::default()
};

let wrapped = wrap_policy_view(&ledger, &opts).await?;
```

### Using query_from with Inline Policy

For FROM-driven queries where policy options are embedded in the query body, use `query_from()`:

```rust
let query = json!({
    "@context": {
        "ex": "http://example.org/ns/",
        "schema": "http://schema.org/"
    },
    "from": "mydb:main",
    "opts": {
        "default-allow": true,
        "policy": [{
            "@id": "inline-ssn-policy",
            "f:required": true,
            "f:onProperty": [{"@id": "http://schema.org/ssn"}],
            "f:action": "f:view",
            "f:query": serde_json::to_string(&json!({
                "where": {
                    "@id": "?$identity",
                    "http://example.org/ns/user": {"@id": "?$this"}
                }
            })).unwrap()
        }],
        "policy-values": {
            "?$identity": {"@id": "http://example.org/identity/alice"}
        }
    },
    "select": ["?s", "?ssn"],
    "where": {
        "@id": "?s",
        "@type": "ex:User",
        "schema:ssn": "?ssn"
    }
});

let result = fluree.query_from()
    .jsonld(&query)
    .execute()
    .await?;
```

## Policy Options Precedence

When multiple policy options are provided, they follow this precedence:

| Priority | Option | Behavior |
|----------|--------|----------|
| 1 (highest) | `opts.identity` | Query `f:policyClass` policies, auto-bind `?$identity` |
| 2 | `opts.policy_class` | Query policies of specified types |
| 3 (lowest) | `opts.policy` | Use inline policy JSON directly |

**Important:** If `opts.identity` is set, inline `opts.policy` is ignored.

## Policy Structure Reference

### f:allow (Static Allow/Deny)

```json
{
    "@id": "ex:allowAll",
    "@type": ["f:AccessPolicy", "ex:MyPolicyClass"],
    "f:action": {"@id": "f:view"},
    "f:allow": true
}
```

### f:query (Dynamic Evaluation)

```json
{
    "@id": "ex:ownerOnly",
    "@type": ["f:AccessPolicy", "ex:MyPolicyClass"],
    "f:action": {"@id": "f:view"},
    "f:onProperty": [{"@id": "schema:ssn"}],
    "f:required": true,
    "f:query": "{\"where\": {\"@id\": \"?$identity\", \"ex:user\": {\"@id\": \"?$this\"}}}"
}
```

### Policy Properties

| Property | Type | Description |
|----------|------|-------------|
| `f:action` | `f:view` / `f:modify` | What action this policy applies to |
| `f:allow` | boolean | Static allow (true) or deny (false) |
| `f:query` | string (JSON) | Query that must return results for access to be granted |
| `f:onProperty` | IRI(s) | Restrict policy to specific properties |
| `f:onSubject` | IRI(s) | Restrict policy to specific subjects |
| `f:onClass` | IRI(s) | Restrict policy to instances of specific classes |
| `f:required` | boolean | If true, this policy MUST allow for access to be granted |
| `f:exMessage` | string | Custom error message when policy denies access |

### Special Variables

| Variable | Binding |
|----------|---------|
| `?$identity` | The identity IRI (from `opts.identity` or `policy_values["?$identity"]`) |
| `?$this` | The subject being accessed (for property-level policies) |

## Policy Combining Algorithm

When multiple policies match a flake, they are combined using **Deny Overrides**:

1. If **any** matching policy explicitly denies (`f:allow: false`), access is **denied**
2. If a targeted policy's `f:query` returns false, access is **denied** (doesn't fall through to Default policies)
3. If any policy allows (`f:allow: true` or `f:query` returns true), access is **granted**
4. If no policies match and `default_allow` is `true` → access is **granted**
5. Otherwise, access is **denied**

> **Identity resolution is three-state:** `FoundWithPolicies` (restrictions apply) → `FoundNoPolicies` (subject exists, no restrictions) → `NotFound` (subject absent, no restrictions). The three-state split determines whether a concrete identity SID is available to bind `?$identity` in policy queries; it does not gate `default_allow`. An unknown identity with `default_allow: true` is granted access — this is the intended behavior for deployments where an application layer handles authorization and Fluree records signed transactions for provenance. Set `default_allow: false` for fail-closed behavior.

**Important:** Inline policies must use full IRIs (e.g., `"http://schema.org/ssn"`), not compact IRIs (e.g., `"schema:ssn"`). Compact IRIs in inline policies are not expanded.

## Transactions with Policy

Policies can also be applied to transactions using the builder API:

```rust
use fluree_db_api::policy_builder;

let policy_ctx = policy_builder::build_policy_context_from_opts(
    &ledger.snapshot,
    ledger.novelty.as_ref(),
    Some(ledger.novelty.as_ref()),
    ledger.t(),
    &qc_opts,
    &[0], // default graph; use resolve_policy_source_g_ids() for config-driven graphs
).await?;

let txn = json!({
    "@context": {"ex": "http://example.org/ns/"},
    "insert": [
        {"@id": "ex:alice", "ex:data": "secret"}
    ]
});

// Use the transaction builder with policy
let result = fluree.graph("mydb:main")
    .transact()
    .update(&txn)
    .policy(policy_ctx)
    .commit()
    .await;

match result {
    Ok(txn_result) => println!("Transaction succeeded at t={}", txn_result.ledger.t()),
    Err(e) => println!("Policy denied: {}", e),
}
```

## Historical Views with Policy

For time-travel queries with policy, load a historical graph and apply policy as a view overlay:

```rust
use fluree_db_api::{GraphDb, QueryConnectionOptions};

// Load a historical view
let graph = fluree.view_at_t("mydb:main", 100).await?;

// Apply policy to create a view
let policy_ctx = policy_builder::build_policy_context_from_opts(
    &ledger.snapshot,
    ledger.novelty.as_ref(),
    Some(ledger.novelty.as_ref()),
    ledger.t(),
    &opts,
    &[0],
).await?;

let view = graph.with_policy(std::sync::Arc::new(policy_ctx));

// Query the historical view with policy applied
let result = view.query(&fluree)
    .jsonld(&query)
    .execute()
    .await?;
```

## API Reference

### wrap_identity_policy_view

```rust
pub async fn wrap_identity_policy_view<'a>(
    ledger: &'a LedgerState,
    identity_iri: &str,
    default_allow: bool,
) -> Result<PolicyWrappedView<'a>>
```

Creates a policy-wrapped view using identity-based `f:policyClass` lookup.

**Parameters:**
- `ledger`: The ledger state to wrap
- `identity_iri`: IRI of the identity subject (will query `f:policyClass`)
- `default_allow`: Whether to allow access when no policies match. Ignored (forced `false`) if the identity IRI has no subject node in the ledger — see combining algorithm step 5

### wrap_policy_view

```rust
pub async fn wrap_policy_view<'a>(
    ledger: &'a LedgerState,
    opts: &QueryConnectionOptions,
) -> Result<PolicyWrappedView<'a>>
```

Creates a policy-wrapped view from query connection options.

**QueryConnectionOptions fields:**
- `identity`: Identity IRI for `f:policyClass` lookup
- `policy`: Inline policy JSON
- `policy_class`: Policy class IRIs to query
- `policy_values`: Variable bindings for policy queries
- `default_allow`: Default access when no policies match

### PolicyWrappedView

```rust
impl PolicyWrappedView {
    /// Check if this is a root/unrestricted policy
    pub fn is_root(&self) -> bool;

    /// Get the underlying policy context
    pub fn policy(&self) -> &PolicyContext;

    /// Get the policy enforcer for query execution
    pub fn enforcer(&self) -> &Arc<QueryPolicyEnforcer>;
}
```

## Best Practices

### 1. Prefer Identity-Based Policies

Store policies in the database for:
- Version control with data
- Audit trail of policy changes
- Dynamic policy updates without code changes
- Time-travel to historical policy states

### 2. Use HTTP IRIs for Identities

HTTP IRIs are more portable than DIDs for identity subjects:

```rust
// Recommended
let identity = "http://example.org/identity/alice";

// Also works but may have encoding issues
let identity = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
```

### 3. Always Set default_allow Explicitly

```rust
// Be explicit about default behavior
let wrapped = wrap_identity_policy_view(&ledger, identity, false).await?;
//                                                          ^^^^^ explicit deny
```

### 4. Handle Policy Errors

```rust
let graph = GraphDb::from_ledger_state(&ledger)
    .with_policy(std::sync::Arc::new(policy_ctx));

match graph.query(&fluree).jsonld(&query).execute().await {
    Ok(result) => process_results(result),
    Err(ApiError::PolicyDenied { message, policy_id }) => {
        log::warn!("Access denied by {}: {}", policy_id, message);
        // Return empty or error to user
    }
    Err(e) => return Err(e),
}
```

## Related Documentation

- [Policy Model](policy-model.md) - Policy structure and evaluation
- [Policy in Queries](policy-in-queries.md) - Query-time enforcement
- [Policy in Transactions](policy-in-transactions.md) - Transaction-time enforcement
- [Cross-ledger policy](cross-ledger-policy.md) - Govern many data ledgers from one model ledger via `f:policySource` with `f:ledger`; `db_with_policy` dispatches automatically when the data ledger's `#config` is cross-ledger.
- [Rust API](../getting-started/rust-api.md) - General Rust API usage
