// Cypher query strings are written as raw strings (`r#"..."#`) for consistency.
#![allow(clippy::needless_raw_string_hashes)]

//! View-policy enforcement on Cypher metadata reads.
//!
//! The metadata functions (`properties`/`keys`/`labels`/`type`/…) and
//! loop-local member access (`[x IN list | x.prop]`) read graph flakes lazily
//! during scalar expression evaluation. Under a non-root view policy those
//! reads must surface only policy-visible flakes — the same guarantee the scan
//! path already gives. These tests pin both halves: a restrictive policy hides
//! the protected property, and the identical query under no policy returns it
//! (so the feature works generally, not merely fail-closed).

mod support;

use fluree_db_api::{FlureeBuilder, GovernanceOptions};
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, graphdb_from_ledger};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/"
    })
}

/// Two Person nodes, each with a visible `name` and a sensitive `secret`.
/// All properties live under the `ex:` (`http://example.org/`) vocab so they
/// match Cypher's default-`@vocab` resolution of `n.name` / `n.secret`.
async fn seed(fluree: &support::MemoryFluree, ledger_id: &str) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:Person",
                     "ex:name": "Alice", "ex:secret": "ALICE-SECRET"},
                    {"@id": "ex:bob", "@type": "ex:Person",
                     "ex:name": "Bob", "ex:secret": "BOB-SECRET"},
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger
}

/// Policy: deny the `ex:secret` property; allow everything else.
fn deny_secret_policy() -> JsonValue {
    json!([
        {
            "@id": "ex:denySecret",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:onProperty": [{"@id": "http://example.org/secret"}],
            "f:allow": false
        },
        {
            "@id": "ex:allowAll",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:allow": true
        }
    ])
}

fn flatten_strings(v: &JsonValue, out: &mut Vec<String>) {
    match v {
        JsonValue::String(s) => out.push(s.clone()),
        JsonValue::Array(a) => a.iter().for_each(|e| flatten_strings(e, out)),
        JsonValue::Object(o) => o.values().for_each(|e| flatten_strings(e, out)),
        _ => {}
    }
}

/// Every string anywhere in the JSON result, for leak assertions.
fn all_strings(v: &JsonValue) -> Vec<String> {
    let mut out = Vec::new();
    flatten_strings(v, &mut out);
    out
}

#[tokio::test]
async fn cypher_properties_under_policy_hides_protected_property() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/policy-cypher:properties";
    let l = seed(&fluree, ledger_id).await;

    let qc_opts = GovernanceOptions {
        policy: Some(deny_secret_policy()),
        default_allow: true,
        ..Default::default()
    };
    let db_policy = fluree
        .db_with_policy(ledger_id, &qc_opts)
        .await
        .expect("db_with_policy");

    // properties(n) under policy: name present, secret absent.
    let policed = fluree
        .query_cypher(
            &db_policy,
            "MATCH (n:Person) RETURN n.name AS name, properties(n) AS props ORDER BY name",
        )
        .await
        .expect("cypher properties under policy")
        .to_jsonld_async(db_policy.as_graph_db_ref())
        .await
        .expect("jsonld");

    // Inspect the props column (index 1) specifically — not the whole row,
    // so we don't credit the `name` column for `properties(n)`'s output.
    let props_strings: Vec<String> = policed
        .as_array()
        .expect("rows")
        .iter()
        .flat_map(|row| all_strings(&row[1]))
        .collect();
    assert!(
        !props_strings.iter().any(|s| s.contains("SECRET")),
        "properties(n) must not surface the policy-hidden secret: {policed}"
    );
    assert!(
        props_strings.iter().any(|s| s == "Alice"),
        "properties(n) must still include the visible name value: {policed}"
    );

    // Positive control: the same query under no policy DOES return the secret,
    // proving the read works generally and is only filtered, not disabled.
    let db_root = graphdb_from_ledger(&l);
    let rooted = fluree
        .query_cypher(
            &db_root,
            "MATCH (n:Person) RETURN n.name AS name, properties(n) AS props ORDER BY name",
        )
        .await
        .expect("cypher properties root")
        .to_jsonld_async(db_root.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert!(
        all_strings(&rooted).iter().any(|s| s.contains("SECRET")),
        "without policy, properties(n) must include the secret: {rooted}"
    );
}

#[tokio::test]
async fn cypher_keys_under_policy_omits_protected_key() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/policy-cypher:keys";
    seed(&fluree, ledger_id).await;

    let qc_opts = GovernanceOptions {
        policy: Some(deny_secret_policy()),
        default_allow: true,
        ..Default::default()
    };
    let db_policy = fluree
        .db_with_policy(ledger_id, &qc_opts)
        .await
        .expect("db_with_policy");

    let policed = fluree
        .query_cypher(
            &db_policy,
            "MATCH (n:Person) RETURN n.name AS name, keys(n) AS ks ORDER BY name",
        )
        .await
        .expect("cypher keys under policy")
        .to_jsonld_async(db_policy.as_graph_db_ref())
        .await
        .expect("jsonld");

    let leaked = all_strings(&policed);
    assert!(
        !leaked.iter().any(|s| s == "secret"),
        "keys(n) must not list the policy-hidden property key: {policed}"
    );
    assert!(
        leaked.iter().any(|s| s == "name"),
        "keys(n) must still list the visible property key: {policed}"
    );
}

#[tokio::test]
async fn cypher_where_metadata_filter_under_policy_sees_filtered_flakes() {
    // A WHERE referencing a metadata read must also resolve through the policy
    // filter (FilterOperator's async path), not the synchronous inline path.
    // `size(keys(n)) > 1` is true only when a node has >1 visible data property;
    // under the deny-secret policy each node has just `name`, so none match.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/policy-cypher:where";
    let l = seed(&fluree, ledger_id).await;

    let qc_opts = GovernanceOptions {
        policy: Some(deny_secret_policy()),
        default_allow: true,
        ..Default::default()
    };
    let db_policy = fluree
        .db_with_policy(ledger_id, &qc_opts)
        .await
        .expect("db_with_policy");

    let q = "MATCH (n:Person) WHERE size(keys(n)) > 1 RETURN n.name AS name ORDER BY name";

    let policed = fluree
        .query_cypher(&db_policy, q)
        .await
        .expect("cypher where-metadata under policy")
        .to_jsonld_async(db_policy.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        policed.as_array().map_or(0, Vec::len),
        0,
        "with secret hidden, each node has a single visible key → none pass size>1: {policed}"
    );

    // Positive control: without policy both keys are visible, so both pass.
    let db_root = graphdb_from_ledger(&l);
    let rooted = fluree
        .query_cypher(&db_root, q)
        .await
        .expect("cypher where-metadata root")
        .to_jsonld_async(db_root.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        rooted.as_array().map_or(0, Vec::len),
        2,
        "without policy both keys visible → both nodes pass size>1: {rooted}"
    );
}

#[tokio::test]
async fn cypher_list_comprehension_member_under_policy_is_filtered() {
    // The loop-local `x.secret` is an `Expression::Member` over a list element,
    // the one metadata path that bypasses the scan join — it must resolve
    // through the policy-filtered async path.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/policy-cypher:comprehension";
    let l = seed(&fluree, ledger_id).await;

    let qc_opts = GovernanceOptions {
        policy: Some(deny_secret_policy()),
        default_allow: true,
        ..Default::default()
    };
    let db_policy = fluree
        .db_with_policy(ledger_id, &qc_opts)
        .await
        .expect("db_with_policy");

    let q = "MATCH (n:Person) WITH collect(n) AS ns \
             RETURN [x IN ns | x.secret] AS secrets";

    let policed = fluree
        .query_cypher(&db_policy, q)
        .await
        .expect("cypher comprehension under policy")
        .to_jsonld_async(db_policy.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert!(
        !all_strings(&policed).iter().any(|s| s.contains("SECRET")),
        "[x IN ns | x.secret] must not leak hidden values under policy: {policed}"
    );

    // Positive control under no policy: the secrets ARE projected.
    let db_root = graphdb_from_ledger(&l);
    let rooted = fluree
        .query_cypher(&db_root, q)
        .await
        .expect("cypher comprehension root")
        .to_jsonld_async(db_root.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert!(
        all_strings(&rooted).iter().any(|s| s.contains("SECRET")),
        "without policy, [x IN ns | x.secret] must project the secrets: {rooted}"
    );
}
