//! Named-graph placement for tests of the fast paths and union lanes.

#![allow(dead_code)]

use fluree_db_api::Fluree;

/// TriG pinning every user named graph of `ledger_id` to trees of its own,
/// where a graph this small would be pooled. The fast paths and union lanes
/// that read a graph's leaf metadata decline a pooled graph, so a test that
/// asserts one of them runs over small named graphs adds this to its first
/// commit. Only a build that reads the ledger config (`Fluree::reindex`, the
/// background indexer) applies it.
pub fn dedicated_named_graphs_trig(ledger_id: &str) -> String {
    const F: &str = "https://ns.flur.ee/db#";
    let config = fluree_db_core::graph_registry::config_graph_iri(ledger_id);
    format!(
        "GRAPH <{config}> {{ <urn:config:main> a <{F}LedgerConfig> ; \
         <{F}indexingDefaults> [ <{F}graphPlacement> <{F}PlacementDedicated> ] . }}\n"
    )
}

/// [`dedicated_named_graphs_trig`] as a commit of its own, for a fixture
/// that seeds by JSON-LD.
pub async fn pin_named_graphs_dedicated(fluree: &Fluree, ledger_id: &str) {
    let handle = fluree.ledger_cached(ledger_id).await.expect("ledger");
    fluree
        .stage(&handle)
        .upsert_turtle(&dedicated_named_graphs_trig(ledger_id))
        .execute()
        .await
        .expect("pin named graphs dedicated");
}
