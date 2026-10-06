//! Regression tests: R2RML materialization into NAMED GRAPHS must round-trip
//! through the real transaction parser.
//!
//! `nodes_by_graph_to_doc` tags each named-graph node with a per-node `@graph`
//! STRING selector (`{"@id": s, "@graph": "<g>", ...}`) — the only named-graph
//! form `parse_insert`/`parse_upsert` accept. The materializer originally emitted
//! the standard JSON-LD *envelope* `{"@id": g, "@graph": [nodes]}` (an `@graph`
//! ARRAY), which those parsers SKIP: the wrapper collapses to an `@id`-only node
//! and yields zero triples ("an object with only @id is not a valid insert").
//! The materializer's unit tests asserted the emitted JSON *shape* but never
//! round-tripped through the parser, so the mismatch shipped and every
//! data-bearing table failed on the first real `track` with named-graph mappings.
//!
//! These tests exercise the exact emitted shape end-to-end through `Fluree::insert`
//! (the `@type`-union path) and `Fluree::upsert` (the per-predicate path) against a
//! memory ledger, asserting the triples land in the named graph (not the default
//! graph) and stay isolated per graph. `envelope_form_is_rejected_by_insert`
//! documents WHY the envelope form was abandoned.

use crate::support;
use fluree_db_api::FlureeBuilder;
use serde_json::json;

const G1: &str = "https://entities.tdwx.dev/graph/tenant/T1/user/U1";
const G2: &str = "https://entities.tdwx.dev/graph/tenant/T2/user/U2";
const S: &str = "https://entities.tdwx.dev/Article%2F1";
const ARTICLE: &str = "https://www.w3.org/ns/activitystreams#Article";
const ANNOUNCE: &str = "https://www.w3.org/ns/activitystreams#Announce";
const NAME: &str = "https://www.w3.org/ns/activitystreams#name";

#[tokio::test]
async fn per_node_graph_insert_upsert_lands_in_named_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "mat/named-graph:main");

    // The exact shape `nodes_by_graph_to_doc` emits for a named graph: each node
    // carries a per-node `@graph` STRING. `@type` goes via insert (idempotent
    // union), the remaining predicates via upsert (last-writer-wins per predicate).
    let type_doc = json!([{ "@id": S, "@graph": G1, "@type": [ARTICLE] }]);
    let ledger = fluree.insert(ledger, &type_doc).await.unwrap().ledger;
    let pred_doc = json!([{ "@id": S, "@graph": G1, NAME: "Alice" }]);
    let ledger = fluree.upsert(ledger, &pred_doc).await.unwrap().ledger;

    // Both triples (rdf:type + name) are in graph G1.
    let in_g1 = support::query_sparql_formatted(
        &fluree,
        &ledger,
        &format!("SELECT ?p ?o WHERE {{ GRAPH <{G1}> {{ <{S}> ?p ?o }} }}"),
    )
    .await
    .unwrap()
    .to_string();
    assert!(
        in_g1.contains("Article"),
        "rdf:type missing from G1: {in_g1}"
    );
    assert!(in_g1.contains("Alice"), "name missing from G1: {in_g1}");

    // ...and NOT in the default graph. (The shipped bug wrote nothing anywhere;
    // this also guards against a regression that leaks into the default graph.)
    let in_default = support::query_jsonld_formatted(
        &fluree,
        &ledger,
        &json!({ "select": ["?p", "?o"], "where": { "@id": S, "?p": "?o" } }),
    )
    .await
    .unwrap()
    .to_string();
    assert!(
        !in_default.contains("Article") && !in_default.contains("Alice"),
        "subject must not appear in the default graph: {in_default}"
    );
}

#[tokio::test]
async fn same_iri_in_two_graphs_stays_isolated_and_unions_types() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "mat/named-graph-iso:main");

    // Same IRI, two graphs — the per-(tenant,user) override boundary. In G1 it is
    // additionally an Announce (a retweet); the additive @type union must hold
    // WITHIN a graph without leaking across graphs.
    let ledger = fluree
        .insert(
            ledger,
            &json!([{ "@id": S, "@graph": G1, "@type": [ARTICLE, ANNOUNCE] }]),
        )
        .await
        .unwrap()
        .ledger;
    let ledger = fluree
        .insert(
            ledger,
            &json!([{ "@id": S, "@graph": G2, "@type": [ARTICLE] }]),
        )
        .await
        .unwrap()
        .ledger;

    let g1 = support::query_sparql_formatted(
        &fluree,
        &ledger,
        &format!("SELECT ?t WHERE {{ GRAPH <{G1}> {{ <{S}> a ?t }} }}"),
    )
    .await
    .unwrap()
    .to_string();
    assert!(
        g1.contains("Article") && g1.contains("Announce"),
        "G1 should union both classes: {g1}"
    );

    let g2 = support::query_sparql_formatted(
        &fluree,
        &ledger,
        &format!("SELECT ?t WHERE {{ GRAPH <{G2}> {{ <{S}> a ?t }} }}"),
    )
    .await
    .unwrap()
    .to_string();
    assert!(g2.contains("Article"), "G2 should have Article: {g2}");
    assert!(
        !g2.contains("Announce"),
        "G2 must NOT be clobbered by G1's Announce: {g2}"
    );
}

#[tokio::test]
async fn envelope_form_is_rejected_by_insert() {
    // Control: the OLD emission form — a named-graph ENVELOPE `{"@id": g,
    // "@graph": [nodes]}` — is not understood by `parse_insert`. Its `@graph`
    // ARRAY is skipped, the wrapper collapses to `@id`-only, and the insert is
    // empty. This is exactly the failure every data-bearing table hit before the
    // fix, and the reason `nodes_by_graph_to_doc` uses the per-node `@graph` string.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "mat/named-graph-envelope:main");
    let envelope = json!([{ "@id": G1, "@graph": [{ "@id": S, "@type": [ARTICLE] }] }]);
    let res = fluree.insert(ledger, &envelope).await;
    assert!(
        res.is_err(),
        "envelope form must be rejected (documents why the materializer uses per-node @graph); got Ok"
    );
}
