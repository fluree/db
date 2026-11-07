//! Drop named graph integration tests.
//!
//! `drop_named_graph` is SPARQL `DROP GRAPH`: one commit at `t = current + 1`
//! that retracts nothing, after which the graph and its history are no longer
//! queryable. The SPARQL forms are covered in `it_graph_drop.rs`.

#![cfg(feature = "native")]

use crate::support::genesis_ledger;
use fluree_db_api::{FlureeBuilder, GraphSel};
use serde_json::json;

const ALPHA_IRI: &str = "http://example.org/graphs/alpha";
const BETA_IRI: &str = "http://example.org/graphs/beta";

/// Insert a default-graph triple plus one triple each into graphs alpha and beta.
/// Returns the post-insert commit `t`.
async fn seed_two_graphs(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> i64 {
    let ledger = genesis_ledger(fluree, ledger_id).await;
    let trig = format!(
        r#"
        @prefix ex: <http://example.org/> .

        ex:default-subject ex:p "default-graph-value" .

        GRAPH <{ALPHA_IRI}> {{
            ex:alice ex:name "Alice" .
            ex:alice ex:role "engineer" .
        }}

        GRAPH <{BETA_IRI}> {{
            ex:bob ex:name "Bob" .
        }}
        "#,
    );

    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed insert");
    result.receipt.t
}

async fn count_in_graph(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    graph_iri: Option<&str>,
) -> i64 {
    let from = match graph_iri {
        Some(iri) => format!("{ledger_id}#{iri}"),
        None => ledger_id.to_string(),
    };
    // Return the (s, p, o) rows, then count them on the client side. Avoids
    // depending on the exact shape of aggregate `select` projections in
    // JSON-LD queries.
    let q = json!({
        "from": from,
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"}
    });
    let result = fluree.query_connection(&q).await.expect("query connection");
    let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
    let rows = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    rows.as_array().map(|arr| arr.len() as i64).unwrap_or(0)
}

async fn graph_exists(fluree: &fluree_db_api::Fluree, ledger_id: &str, graph_iri: &str) -> bool {
    fluree
        .graph_exists(ledger_id, &GraphSel::Graph(graph_iri.to_string()))
        .await
        .expect("graph_exists")
}

#[tokio::test]
async fn drop_named_graph_drops_only_the_target_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/drop-named-graph/basic:main";

    let pre_drop_t = seed_two_graphs(&fluree, ledger_id).await;
    assert_eq!(pre_drop_t, 1);

    // Sanity: each graph has expected counts before the drop.
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(ALPHA_IRI)).await, 2);
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(BETA_IRI)).await, 1);
    assert_eq!(count_in_graph(&fluree, ledger_id, None).await, 1);

    let report = fluree
        .drop_named_graph(ledger_id, ALPHA_IRI)
        .await
        .expect("drop alpha");

    assert_eq!(report.ledger_id, ledger_id);
    assert_eq!(report.graph_iri, ALPHA_IRI);
    assert_eq!(report.retracted, 0, "a drop retracts nothing");
    assert!(report.committed);
    assert_eq!(report.t, pre_drop_t + 1);

    // Alpha no longer exists; beta and default are untouched.
    assert!(!graph_exists(&fluree, ledger_id, ALPHA_IRI).await);
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(BETA_IRI)).await, 1);
    assert_eq!(count_in_graph(&fluree, ledger_id, None).await, 1);
}

#[tokio::test]
async fn a_dropped_graph_cannot_be_dropped_again() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/drop-named-graph/twice:main";

    seed_two_graphs(&fluree, ledger_id).await;

    fluree
        .drop_named_graph(ledger_id, ALPHA_IRI)
        .await
        .expect("first drop");
    let err = fluree
        .drop_named_graph(ledger_id, ALPHA_IRI)
        .await
        .expect_err("the graph no longer exists");
    assert!(err.to_string().contains("does not exist"), "{err}");
}

#[tokio::test]
async fn an_emptied_graph_still_drops() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/drop-named-graph/emptied:main";
    seed_two_graphs(&fluree, ledger_id).await;

    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let cleared = fluree
        .stage_owned(ledger)
        .txn(fluree_db_transact::Txn::clear_graph(ALPHA_IRI))
        .execute()
        .await
        .expect("clear")
        .receipt
        .t;
    let report = fluree
        .drop_named_graph(ledger_id, ALPHA_IRI)
        .await
        .expect("drop the emptied graph");
    assert!(report.committed);
    assert_eq!(report.t, cleared + 1);
}

#[tokio::test]
async fn drop_named_graph_rejects_default_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/drop-named-graph/default:main";
    seed_two_graphs(&fluree, ledger_id).await;

    let err = fluree
        .drop_named_graph(ledger_id, "")
        .await
        .expect_err("empty IRI must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("default") || msg.contains("required"),
        "error should mention default graph: {msg}",
    );
}

#[tokio::test]
async fn drop_named_graph_rejects_txn_meta_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/drop-named-graph/txn-meta:main";
    seed_two_graphs(&fluree, ledger_id).await;

    let txn_meta_iri = fluree_db_core::graph_registry::txn_meta_graph_iri(ledger_id);
    let err = fluree
        .drop_named_graph(ledger_id, &txn_meta_iri)
        .await
        .expect_err("txn-meta drop must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("txn-meta"),
        "error should mention txn-meta: {msg}",
    );
}

#[tokio::test]
async fn drop_named_graph_rejects_config_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/drop-named-graph/cfg:main";
    seed_two_graphs(&fluree, ledger_id).await;

    let config_iri = fluree_db_core::graph_registry::config_graph_iri(ledger_id);
    let err = fluree
        .drop_named_graph(ledger_id, &config_iri)
        .await
        .expect_err("config drop must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("config"), "error should mention config: {msg}",);
}

#[tokio::test]
async fn drop_named_graph_returns_not_found_for_unknown_iri() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/drop-named-graph/not-found:main";
    seed_two_graphs(&fluree, ledger_id).await;

    let unknown = "http://example.org/graphs/does-not-exist";
    let err = fluree
        .drop_named_graph(ledger_id, unknown)
        .await
        .expect_err("unknown graph IRI must return NotFound");
    let msg = format!("{err}");
    assert!(
        msg.contains("not registered") || msg.contains("not found") || msg.contains("Not"),
        "error should be a NotFound-shaped message: {msg}",
    );
}

#[tokio::test]
async fn drop_named_graph_rejects_malformed_iri() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/drop-named-graph/malformed:main";
    seed_two_graphs(&fluree, ledger_id).await;

    let err = fluree
        .drop_named_graph(ledger_id, "http://example.org/with space")
        .await
        .expect_err("space in IRI must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("Invalid graph IRI") || msg.contains("not allowed"),
        "error should call out the malformed IRI: {msg}",
    );
}

/// Whitespace at the edges is not silently trimmed — the value must be
/// supplied exactly, so a leading-space variant of a registered IRI is
/// rejected up front rather than re-resolved to the trimmed form.
#[tokio::test]
async fn drop_named_graph_rejects_whitespace_padded_iri() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/drop-named-graph/whitespace:main";
    seed_two_graphs(&fluree, ledger_id).await;

    let padded = format!(" {ALPHA_IRI}");
    let err = fluree
        .drop_named_graph(ledger_id, &padded)
        .await
        .expect_err("padded IRI must be rejected, not silently trimmed");
    let msg = format!("{err}");
    assert!(
        msg.contains("Invalid graph IRI"),
        "error should call out the malformed IRI: {msg}",
    );
}

/// The contract is "full IRI"; a relative reference (no scheme) is a 400.
#[tokio::test]
async fn drop_named_graph_rejects_relative_iri() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/drop-named-graph/relative:main";
    seed_two_graphs(&fluree, ledger_id).await;

    let err = fluree
        .drop_named_graph(ledger_id, "alpha")
        .await
        .expect_err("relative IRI must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("scheme") || msg.contains("Invalid graph IRI"),
        "error should mention missing scheme: {msg}",
    );
}

/// Drop takes the graph's history: querying it as of the pre-drop `t` is
/// refused rather than answered from what the drop took.
#[tokio::test]
async fn drop_named_graph_takes_its_history() {
    use fluree_db_api::{DatasetSpec, GraphSource, TimeSpec};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/drop-named-graph/history:main";

    let pre_drop_t = seed_two_graphs(&fluree, ledger_id).await;
    assert_eq!(
        count_in_graph(&fluree, ledger_id, Some(ALPHA_IRI)).await,
        2,
        "sanity: alpha has 2 flakes at HEAD pre-drop",
    );

    let report = fluree
        .drop_named_graph(ledger_id, ALPHA_IRI)
        .await
        .expect("drop alpha");
    assert!(report.committed);
    assert_eq!(report.t, pre_drop_t + 1);

    // HEAD: alpha does not exist.
    assert!(!graph_exists(&fluree, ledger_id, ALPHA_IRI).await);

    // Pre-drop t via DatasetSpec: refused, since the drop took that state.
    let alpha_alias = format!("{ledger_id}#{ALPHA_IRI}");
    let spec = DatasetSpec::new().with_default(
        GraphSource::parse(&alpha_alias)
            .unwrap()
            .with_time(TimeSpec::AtT(pre_drop_t)),
    );
    let Err(err) = fluree.build_dataset_view(&spec).await else {
        panic!("the dropped graph as of before the drop");
    };
    assert!(
        err.to_string()
            .contains(&format!("dropped at t={}", pre_drop_t + 1)),
        "{err}"
    );
}

#[tokio::test]
async fn drop_named_graph_other_branches_unaffected() {
    // Per-branch scope: dropping graph G on `main` must not affect the
    // same IRI's data on a sibling branch.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/drop-named-graph/branch-scope:main";

    seed_two_graphs(&fluree, ledger_id).await;

    // Fork a sibling branch and seed its alpha graph independently.
    let branch_record = fluree
        .create_branch(
            "it/drop-named-graph/branch-scope",
            "feature",
            Some("main"),
            None,
        )
        .await
        .expect("create branch");
    let sibling_id = branch_record.ledger_id.clone();

    // Add additional alpha data on the sibling branch.
    let sibling_ledger = fluree.ledger(&sibling_id).await.expect("load sibling");
    let extra = format!(
        r#"
        @prefix ex: <http://example.org/> .
        GRAPH <{ALPHA_IRI}> {{
            ex:carol ex:name "Carol" .
        }}
        "#,
    );
    fluree
        .stage_owned(sibling_ledger)
        .upsert_turtle(&extra)
        .execute()
        .await
        .expect("sibling insert");

    // Sanity: sibling now has 3 alpha triples (the 2 inherited + 1 new),
    // main still has 2.
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(ALPHA_IRI)).await, 2);
    assert_eq!(
        count_in_graph(&fluree, &sibling_id, Some(ALPHA_IRI)).await,
        3
    );

    // Drop alpha on main only.
    let report = fluree
        .drop_named_graph(ledger_id, ALPHA_IRI)
        .await
        .expect("drop alpha on main");
    assert!(report.committed);

    // Sibling branch's alpha graph is untouched.
    assert!(!graph_exists(&fluree, ledger_id, ALPHA_IRI).await);
    assert_eq!(
        count_in_graph(&fluree, &sibling_id, Some(ALPHA_IRI)).await,
        3,
        "drop on `main` must not touch sibling branches",
    );
}
