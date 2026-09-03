//! Ledger-config reasoning must not reach Cypher write-decision probes.
//!
//! A conditional Cypher write chooses its branch by probing the pre-write
//! state: `MERGE` asks whether the pattern already exists, and a bare `DELETE`
//! asks whether the target still has relationships, which Cypher requires it
//! to refuse (`DETACH DELETE` skips that check). Those probes decide what gets
//! written, so they must read asserted data only. An entailed triple is not
//! something the ledger holds; treating one as a match makes a write depend
//! on the reasoner.
//!
//! The failure is not a mis-branch that merely picks `ON MATCH` over `CREATE`.
//! The probe reasons but staging does not, so an entailment-only match sends
//! the write down the `ON MATCH` branch and then finds nothing to update:
//! nothing is created, nothing is set, and the statement is silently a no-op.
//!
//! Seen while reviewing the fluree/db#1577 fix, which moved config-default
//! application to a choke point shared by reads and write probes.

// Cypher query strings are written as raw strings for consistency with the
// other Cypher test files, even when a given query has no inner quotes.
#![allow(clippy::needless_raw_string_hashes)]

mod support;

use fluree_db_api::FlureeBuilder;
use support::{genesis_ledger, graphdb_from_ledger};

/// Seed a ledger whose data entails `name` from `childName` under RDFS, with
/// `f:reasoningDefaults f:rdfs` configured.
///
/// Cypher emits bare predicate IRIs (`childName`, not `ex:childName`), so the
/// `rdfs:subPropertyOf` axiom is written against those same bare IRIs. The
/// returned ledger holds exactly one `:Person`, carrying `childName "Alice"`
/// and no asserted `name`.
async fn seeded_ledger(ledger_id: &str) -> (fluree_db_api::Fluree, fluree_db_api::LedgerState) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, ledger_id);

    let r = fluree
        .transact_cypher(ledger, r#"CREATE (a:Person {childName: "Alice"})"#)
        .await
        .expect("seed node");

    let r = fluree
        .insert(
            r.ledger,
            &serde_json::json!({
                "@id": "childName",
                "http://www.w3.org/2000/01/rdf-schema#subPropertyOf": {"@id": "name"}
            }),
        )
        .await
        .expect("subPropertyOf axiom");

    let config = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <urn:fluree:{ledger_id}#config> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:reasoningModes f:rdfs .
        }}
        "
    );
    let r = fluree
        .stage_owned(r.ledger)
        .upsert_turtle(&config)
        .execute()
        .await
        .expect("reasoning config");

    (fluree, r.ledger)
}

async fn person_count(fluree: &fluree_db_api::Fluree, ledger: &fluree_db_api::LedgerState) -> i64 {
    let db = graphdb_from_ledger(ledger);
    let cj = fluree
        .query_cypher(&db, "MATCH (n:Person) RETURN count(n)")
        .await
        .expect("count query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    cj["results"][0]["data"][0]["row"][0]
        .as_i64()
        .unwrap_or_else(|| panic!("count query returned no integer: {cj}"))
}

/// Does the ledger hold this asserted triple, reasoning off?
///
/// Reasoning is disabled per-query rather than with a view wrapper, so this
/// helper stays independent of whether config defaults respect a wrapper.
async fn has_asserted(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    predicate: &str,
) -> bool {
    let db = graphdb_from_ledger(ledger);
    let out = fluree
        .query(
            &db,
            &serde_json::json!({
                "select": "?s",
                "reasoning": "none",
                "where": {"@id": "?s", predicate: "?v"}
            }),
        )
        .await
        .expect("asserted query")
        .to_jsonld(&ledger.snapshot)
        .expect("jsonld");
    !out.as_array().expect("array").is_empty()
}

/// Control: reasoning really is configured and active for reads on this
/// ledger. Without this the write assertions below could pass for the wrong
/// reason, namely a fixture where nothing entails anything.
#[tokio::test]
async fn reads_see_the_entailed_property() {
    let (fluree, ledger) = seeded_ledger("it/cyprobe-control:main").await;

    let db = graphdb_from_ledger(&ledger);
    let out = fluree
        .query(
            &db,
            &serde_json::json!({
                "select": "?v",
                "where": {"@id": "?s", "name": "?v"}
            }),
        )
        .await
        .expect("read")
        .to_jsonld(&ledger.snapshot)
        .expect("jsonld");

    assert_eq!(
        out,
        serde_json::json!(["Alice"]),
        "fixture is wrong: config RDFS reasoning is not reaching reads"
    );
    assert!(
        !has_asserted(&fluree, &ledger, "name").await,
        "fixture is wrong: `name` must be entailed only, never asserted"
    );
}

/// A bare `MERGE` on an entailment-only match must create. The ledger asserts
/// `childName "Alice"`; it does not assert `name "Alice"`, so the pattern
/// `{name: "Alice"}` has no match to merge onto.
#[tokio::test]
async fn merge_probe_ignores_entailed_match() {
    let (fluree, ledger) = seeded_ledger("it/cyprobe-merge:main").await;
    assert_eq!(person_count(&fluree, &ledger).await, 1, "seed");

    let r = fluree
        .transact_cypher(ledger, r#"MERGE (a:Person {name: "Alice"})"#)
        .await
        .expect("merge");

    assert_eq!(
        person_count(&fluree, &r.ledger).await,
        2,
        "MERGE matched an entailed triple instead of creating"
    );
}

/// The sharp case. With `ON MATCH SET`, a probe that matches by entailment
/// picks the `ON MATCH` branch, whose own pattern then matches nothing during
/// staging. The statement writes nothing at all: no node, no property.
#[tokio::test]
async fn merge_on_match_set_does_not_silently_vanish() {
    let (fluree, ledger) = seeded_ledger("it/cyprobe-mergeset:main").await;

    let r = fluree
        .transact_cypher(
            ledger,
            r#"MERGE (a:Person {name: "Alice"}) ON MATCH SET a.seen = true"#,
        )
        .await
        .expect("merge with ON MATCH SET");

    let created = person_count(&fluree, &r.ledger).await == 2;
    let updated = has_asserted(&fluree, &r.ledger, "seen").await;

    assert!(
        created || updated,
        "MERGE ... ON MATCH SET wrote nothing: the probe matched on an \
         entailed triple, then the ON MATCH branch found no asserted match"
    );
    assert!(
        created,
        "the pattern is not asserted, so MERGE should have taken CREATE"
    );
}

/// The same guarantee on the multi-clause (sequential) driver, which runs its
/// own MERGE probe rather than going through `resolve_conditional_cypher`.
#[tokio::test]
async fn sequential_merge_probe_ignores_entailed_match() {
    let (fluree, ledger) = seeded_ledger("it/cyprobe-seq:main").await;

    let r = fluree
        .transact_cypher(
            ledger,
            r#"MERGE (a:Person {name: "Alice"}) MERGE (b:City {cityName: "Paris"})"#,
        )
        .await
        .expect("sequential merge");

    assert_eq!(
        person_count(&fluree, &r.ledger).await,
        2,
        "sequential MERGE matched an entailed triple instead of creating"
    );
}

// =============================================================================
// Bare DELETE and an entailment-only relationship
// =============================================================================
//
// RDFS cannot build this fixture: every asserted edge touches both of its
// endpoints and the guard probes both directions, so `subPropertyOf` leaves
// the target with an asserted relationship either way. `owl:hasValue` can:
// every instance of the class gets `likes Pizza` from its type alone, so a
// `Person` carrying only a literal property has a relationship by entailment
// and none by assertion.

/// Seed a `Person` with no asserted relationships, a restriction entailing
/// `likes Pizza` for every `Person`, and `f:reasoningModes f:owl2rl`.
async fn seeded_restriction_ledger(
    ledger_id: &str,
) -> (fluree_db_api::Fluree, fluree_db_api::LedgerState) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, ledger_id);

    let r = fluree
        .transact_cypher(ledger, r#"CREATE (a:Person {childName: "Alice"})"#)
        .await
        .expect("seed node");

    let r = fluree
        .insert(
            r.ledger,
            &serde_json::json!({
                "@context": {
                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                    "owl": "http://www.w3.org/2002/07/owl#"
                },
                "@graph": [
                    {"@id": "Person", "rdfs:subClassOf": {"@id": "urn:restr:likes-pizza"}},
                    {"@id": "urn:restr:likes-pizza", "@type": "owl:Restriction",
                     "owl:onProperty": {"@id": "likes"},
                     "owl:hasValue": {"@id": "Pizza"}}
                ]
            }),
        )
        .await
        .expect("hasValue restriction");

    let config = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <urn:fluree:{ledger_id}#config> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:reasoningModes f:owl2rl .
        }}
        "
    );
    let r = fluree
        .stage_owned(r.ledger)
        .upsert_turtle(&config)
        .execute()
        .await
        .expect("reasoning config");

    (fluree, r.ledger)
}

/// Control: the relationship really is entailed for reads and really is not
/// asserted. Both halves matter, since a fixture that asserted `likes` would
/// make the delete below fail for a legitimate reason.
#[tokio::test]
async fn reads_see_the_entailed_relationship() {
    let (fluree, ledger) = seeded_restriction_ledger("it/cyprobe-del-control:main").await;

    let db = graphdb_from_ledger(&ledger);
    let out = fluree
        .query(
            &db,
            &serde_json::json!({
                "select": "?o",
                "where": {"@id": "?s", "likes": {"@id": "?o"}}
            }),
        )
        .await
        .expect("read")
        .to_jsonld(&ledger.snapshot)
        .expect("jsonld");

    assert_eq!(
        out,
        serde_json::json!(["Pizza"]),
        "fixture is wrong: config OWL2-RL reasoning is not entailing the relationship"
    );
    assert!(
        !has_asserted(&fluree, &ledger, "likes").await,
        "fixture is wrong: `likes` must be entailed only, never asserted"
    );
}

/// A bare `DELETE` must succeed when the node's only relationship is
/// entailed. The guard exists to stop a delete from stranding asserted edges;
/// an entailed edge is not stranded by anything, it simply stops being
/// derivable once its subject is gone.
#[tokio::test]
async fn bare_delete_ignores_entailed_relationship() {
    let (fluree, ledger) = seeded_restriction_ledger("it/cyprobe-delete:main").await;
    assert_eq!(person_count(&fluree, &ledger).await, 1, "seed");

    let r = fluree
        .transact_cypher(ledger, r#"MATCH (a:Person {childName: "Alice"}) DELETE a"#)
        .await
        .expect("bare DELETE refused a node whose only relationship is entailed");

    assert_eq!(
        person_count(&fluree, &r.ledger).await,
        0,
        "DELETE succeeded but the node is still there"
    );
}
