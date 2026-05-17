//! Regression: `ledger.named-graphs` must reflect the **live** graph
//! registry, not just whatever the binary index store happens to know
//! about.
//!
//! Before the fix, `build_ledger_block` iterated
//! `BinaryIndexStore::graph_entries()`, so:
//!   - a ledger with no index at all reported only `urn:default`, and
//!   - a ledger with an index but a newer commit that registered a
//!     fresh named graph would omit the new graph until the next
//!     index build.
//!
//! The fix iterates `LedgerSnapshot.graph_registry.iter_entries()`,
//! which is updated at commit-apply time and is therefore registry-
//! accurate at every `t`. These tests pin both scenarios.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::Value as JsonValue;
use support::genesis_ledger;

const ALPHA_IRI: &str = "http://example.org/graphs/alpha";
const BETA_IRI: &str = "http://example.org/graphs/beta";

fn graph_iris(info: &JsonValue) -> Vec<String> {
    info["ledger"]["named-graphs"]
        .as_array()
        .expect("named-graphs is array")
        .iter()
        .filter_map(|e| e["iri"].as_str().map(String::from))
        .collect()
}

/// A ledger with no index built yet still surfaces every user-registered
/// graph in `info.ledger.named-graphs`. Pre-fix the binary index store
/// was absent and the list contained only `urn:default`.
#[tokio::test]
async fn named_graphs_visible_without_an_index() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/ledger-info-named-graphs/no-index:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let trig = format!(
        r#"
        @prefix ex: <http://example.org/> .

        GRAPH <{ALPHA_IRI}> {{
            ex:alice ex:name "Alice" .
        }}

        GRAPH <{BETA_IRI}> {{
            ex:bob ex:name "Bob" .
        }}
        "#,
    );
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed insert");

    let info = fluree
        .ledger_info(ledger_id)
        .execute()
        .await
        .expect("ledger_info");

    let iris = graph_iris(&info);

    assert!(
        iris.contains(&"urn:default".to_string()),
        "default graph must always appear; got {iris:?}",
    );
    assert!(
        iris.contains(&ALPHA_IRI.to_string()),
        "alpha must appear pre-index; got {iris:?}",
    );
    assert!(
        iris.contains(&BETA_IRI.to_string()),
        "beta must appear pre-index; got {iris:?}",
    );

    // System graphs are seeded into the registry by genesis and should
    // also be reported.
    let txn_meta = fluree_db_core::graph_registry::txn_meta_graph_iri(ledger_id);
    let config = fluree_db_core::graph_registry::config_graph_iri(ledger_id);
    assert!(
        iris.contains(&txn_meta),
        "txn-meta must appear pre-index; got {iris:?}",
    );
    assert!(
        iris.contains(&config),
        "config must appear pre-index; got {iris:?}",
    );

    // Pre-index, per-graph flake/size totals are best-effort: they may
    // be 0 because IndexStats.graphs is not populated until an index is
    // built. The point of this test is registry visibility, not stats.
    let entries = info["ledger"]["named-graphs"]
        .as_array()
        .expect("named-graphs is array");
    let alpha_entry = entries
        .iter()
        .find(|e| e["iri"].as_str() == Some(ALPHA_IRI))
        .expect("alpha entry present");
    assert!(
        alpha_entry["g-id"].as_u64().unwrap_or(0) >= 3,
        "user graphs must report a g-id in the user range (>=3); got {alpha_entry:?}",
    );
}

/// A graph registered by a commit **after** the last index build must
/// still appear in `info.ledger.named-graphs`. Pre-fix the binary index
/// store was frozen at the indexed `t`, so the post-index registration
/// was silently missing.
#[tokio::test]
async fn named_graphs_include_post_index_registrations() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/ledger-info-named-graphs/post-index:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Seed alpha and build an index at this t.
    let trig_alpha = format!(
        r#"
        @prefix ex: <http://example.org/> .
        GRAPH <{ALPHA_IRI}> {{ ex:alice ex:name "Alice" . }}
        "#,
    );
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig_alpha)
        .execute()
        .await
        .expect("seed alpha");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;

    let info_pre_beta = fluree
        .ledger_info(ledger_id)
        .execute()
        .await
        .expect("ledger_info after first index");
    let iris_pre = graph_iris(&info_pre_beta);
    assert!(iris_pre.contains(&ALPHA_IRI.to_string()));
    assert!(!iris_pre.contains(&BETA_IRI.to_string()));

    // Now register a brand new graph *without* rebuilding the index.
    let ledger = fluree.ledger(ledger_id).await.expect("reload ledger");
    let trig_beta = format!(
        r#"
        @prefix ex: <http://example.org/> .
        GRAPH <{BETA_IRI}> {{ ex:bob ex:name "Bob" . }}
        "#,
    );
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig_beta)
        .execute()
        .await
        .expect("seed beta post-index");

    let info_post_beta = fluree
        .ledger_info(ledger_id)
        .execute()
        .await
        .expect("ledger_info after post-index registration");
    let iris_post = graph_iris(&info_post_beta);
    assert!(
        iris_post.contains(&ALPHA_IRI.to_string()),
        "alpha must still appear; got {iris_post:?}",
    );
    assert!(
        iris_post.contains(&BETA_IRI.to_string()),
        "beta must appear even though it was registered after the last \
         index build; got {iris_post:?}",
    );
}
