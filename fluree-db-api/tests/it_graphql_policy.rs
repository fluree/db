//! Policy is a boundary on the *schema*, not only on the rows.
//!
//! A class or property an identity cannot read is absent from introspection
//! rather than present-but-empty, and a derivation made under policy is never
//! served from the cache to another identity. Both are load-bearing: the first
//! is what stops the SDL naming types the caller could not query, the second is
//! what stops one identity being handed another's schema.

use crate::support::{genesis_ledger, MemoryFluree};
use fluree_db_api::graphql::{schema_sdl, GraphQlRequest};
use fluree_db_api::{FlureeBuilder, GovernanceOptions, GraphDb, LedgerState};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/";

fn context() -> JsonValue {
    json!({ "ex": EX, "f": "https://ns.flur.ee/db#" })
}

/// Two classes and a sensitive property, so a policy has something to remove in
/// each dimension.
async fn seed(fluree: &MemoryFluree, ledger_id: &str) -> LedgerState {
    let ledger = genesis_ledger(fluree, ledger_id);
    fluree
        .insert(
            ledger,
            &json!({
                "@context": context(),
                "@graph": [
                    {
                        "@id": "ex:alice",
                        "@type": "ex:Person",
                        "ex:name": "Alice",
                        "ex:ssn": "111-11-1111"
                    },
                    { "@id": "ex:acme", "@type": "ex:Secret", "ex:name": "Acme" }
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger
}

fn root_view(ledger: &LedgerState) -> GraphDb {
    GraphDb::from_ledger_state(ledger).with_default_context(Some(context()))
}

/// A view restricted by the given policy, as the route's `wrap_policy` builds it.
async fn restricted_view(
    fluree: &MemoryFluree,
    ledger: &LedgerState,
    identity: &str,
    policy: JsonValue,
) -> GraphDb {
    let opts = GovernanceOptions {
        identity: Some(identity.to_string()),
        policy: Some(policy),
        default_allow: Some(true),
        ..GovernanceOptions::default()
    };
    fluree
        .wrap_policy(root_view(ledger), &opts, None)
        .await
        .expect("policy view")
}

/// Deny every `ex:Secret` subject.
fn deny_secret_class() -> JsonValue {
    json!([
        {
            "@id": "ex:denySecret",
            "@type": "f:AccessPolicy",
            "f:action": {"@id": "f:view"},
            "f:onClass": [{"@id": "http://example.org/Secret"}],
            "f:allow": false
        }
    ])
}

/// Deny the `ex:ssn` property everywhere.
fn deny_ssn_property() -> JsonValue {
    json!([
        {
            "@id": "ex:denySsn",
            "@type": "f:AccessPolicy",
            "f:action": {"@id": "f:view"},
            "f:onProperty": [{"@id": "http://example.org/ssn"}],
            "f:allow": false
        }
    ])
}

#[tokio::test]
async fn a_denied_class_is_absent_from_the_derived_schema() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-policy-class:main").await;

    let root_sdl = schema_sdl(&root_view(&ledger)).await.expect("root sdl");
    assert!(root_sdl.contains("type Secret"), "{root_sdl}");
    assert!(root_sdl.contains("type Person"), "{root_sdl}");

    let view = restricted_view(&fluree, &ledger, "did:key:zAlice", deny_secret_class()).await;
    let sdl = schema_sdl(&view).await.expect("restricted sdl");

    // Absent, not present-and-empty: a type the caller cannot query must not be
    // named in introspection at all.
    assert!(
        !sdl.contains("type Secret"),
        "denied class leaked into the schema:\n{sdl}"
    );
    assert!(sdl.contains("type Person"), "{sdl}");
    assert!(
        !sdl.contains("secrets("),
        "denied class kept its root field:\n{sdl}"
    );
}

#[tokio::test]
async fn a_denied_property_is_absent_from_its_type() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-policy-property:main").await;

    let root_sdl = schema_sdl(&root_view(&ledger)).await.expect("root sdl");
    assert!(root_sdl.contains("ssn"), "{root_sdl}");

    let view = restricted_view(&fluree, &ledger, "did:key:zAlice", deny_ssn_property()).await;
    let sdl = schema_sdl(&view).await.expect("restricted sdl");

    assert!(
        !sdl.contains("ssn"),
        "denied property leaked into the schema:\n{sdl}"
    );
    assert!(sdl.contains("name"), "{sdl}");
}

/// The cache is keyed on ledger version and context, neither of which changes
/// with identity — so a policy view declines the key outright. Without that,
/// the first (root) request would populate an entry every later identity reads.
#[tokio::test]
async fn a_policy_view_is_not_served_the_cached_root_schema() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-policy-cache:main").await;

    // Populate the cache as root first: the ordering is the whole point.
    let root_sdl = schema_sdl(&root_view(&ledger)).await.expect("root sdl");
    assert!(root_sdl.contains("type Secret"), "{root_sdl}");

    let view = restricted_view(&fluree, &ledger, "did:key:zAlice", deny_secret_class()).await;
    let sdl = schema_sdl(&view).await.expect("restricted sdl");
    assert!(
        !sdl.contains("type Secret"),
        "the restricted view was served the cached root schema:\n{sdl}"
    );

    // And back the other way: the restricted derivation must not have replaced
    // the entry root reads.
    let root_again = schema_sdl(&root_view(&ledger))
        .await
        .expect("root sdl again");
    assert!(
        root_again.contains("type Secret"),
        "root lost a class to a policy view's derivation:\n{root_again}"
    );
}

/// Introspection and execution have to agree: a denied class is not merely
/// hidden from the SDL, it is unqueryable.
#[tokio::test]
async fn a_denied_class_cannot_be_queried() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-policy-query:main").await;

    let view = restricted_view(&fluree, &ledger, "did:key:zAlice", deny_secret_class()).await;
    let response = fluree
        .graphql(&view, &GraphQlRequest::new("{ secrets { id } }"))
        .await
        .expect("graphql request");

    assert!(
        response.get("errors").is_some(),
        "querying a denied class must not succeed: {response}"
    );
}
