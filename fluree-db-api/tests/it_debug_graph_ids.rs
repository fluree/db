#![cfg(feature = "native")]

use fluree_db_api::FlureeBuilder;
use fluree_db_core::ContentStore;
use serde_json::json;

use std::sync::Arc;
mod support;

use support::{genesis_ledger, start_background_indexer_local, trigger_index_and_wait};

#[tokio::test]
#[ignore]
async fn debug_graph_ids_after_named_graph_index() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/debug-graph-ids:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            let trig = r#"
                @prefix ex: <http://example.org/> .

                GRAPH <http://example.org/graphs/audit> {
                    ex:log1 ex:action "user created" .
                }
            "#;

            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("tx");

            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            let ns = fluree
                .nameservice()
                .lookup(ledger_id)
                .await
                .expect("ns lookup")
                .expect("ledger exists");
            let root_id = ns.index_head_id.expect("index head id");

            let cs = fluree.content_store(ledger_id);
            let bytes = cs.get(&root_id).await.expect("fetch root");
            let root =
                fluree_db_binary_index::format::index_root::IndexRoot::decode(&bytes).unwrap();

            println!("root.graph_iris = {:?}", root.graph_iris);
            println!("root.named_graphs = {:?}", root.named_graphs);

            // Also sanity-check querying the audit graph through the public API.
            let audit_alias = format!("{ledger_id}#http://example.org/graphs/audit");
            let q = json!({
                "@context": {"ex": "http://example.org/"},
                "from": &audit_alias,
                "select": ["?action"],
                "where": {"@id": "ex:log1", "ex:action": "?action"}
            });
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
            let r = fluree.query_connection(&q).await.expect("query");
            let jsonld = r.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            println!("query result = {jsonld}");
        })
        .await;
}
