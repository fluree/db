//! Rebuilds and incremental builds size leaves by `leaf_rows` alone:
//! shrinking the leaflets keeps one leaf per order at this scale, where
//! sizing leaves by leaflet count wrote several. The pooled tree is the
//! exception: its leaves hold about one leaflet each.

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::tx::IndexingMode;
use fluree_db_api::{Fluree, FlureeBuilder, IndexerConfig, TriggerIndexOptions};
use fluree_db_binary_index::format::branch::read_branch_from_bytes;
use fluree_db_binary_index::format::index_root::IndexRoot;
use fluree_db_core::{ContentId, ContentStore};

const GRAPH_IRI: &str = "http://example.org/graphs/g";

fn subjects(range: std::ops::Range<u32>) -> String {
    range
        .map(|i| format!("ex:s{i} ex:n {i} .\n"))
        .collect::<String>()
}

/// `(label, leaf count)` for every order of every graph in the root.
async fn leaf_counts(fluree: &Fluree, ledger_id: &str, root: &ContentId) -> Vec<(String, usize)> {
    let cs = fluree.content_store(&fluree.storage_namespace(ledger_id).await.unwrap());
    let root = IndexRoot::decode(&cs.get(root).await.unwrap()).unwrap();
    let mut counts: Vec<(String, usize)> = root
        .default_graph_orders
        .iter()
        .map(|o| (format!("g0 {:?}", o.order), o.leaves.len()))
        .collect();
    for graph in &root.named_graphs {
        for (order, branch_cid) in &graph.orders {
            let branch = read_branch_from_bytes(&cs.get(branch_cid).await.unwrap()).unwrap();
            counts.push((format!("g{} {order:?}", graph.g_id), branch.leaves.len()));
        }
    }
    for (order, branch_cid) in root.pool.iter().flat_map(|p| &p.orders) {
        let branch = read_branch_from_bytes(&cs.get(branch_cid).await.unwrap()).unwrap();
        counts.push((format!("pool {order:?}"), branch.leaves.len()));
    }
    counts
}

#[tokio::test(flavor = "current_thread")]
async fn smaller_leaflets_keep_one_leaf_through_rebuild_and_incremental() {
    let mut fluree = FlureeBuilder::memory().build_memory();
    let config = IndexerConfig::default()
        .with_leaflet_rows(10)
        .with_incremental_enabled(true)
        .with_incremental_max_commits(10_000);
    let (local, handle) = support::start_background_indexer_for(&fluree, config);
    fluree.set_indexing_mode(IndexingMode::Background(handle));

    local
        .run_until(async move {
            let ledger_id = "it/leaf-target-wiring:main";
            let prefix = "@prefix ex: <http://example.org/> .\n";

            // 300 rows per order; leaves of 10 × 10 rows would be three.
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();
            let seed = format!("{prefix}{}", subjects(0..300));
            fluree
                .stage_owned(ledger)
                .insert_turtle(&seed)
                .execute()
                .await
                .expect("seed insert");
            let rebuilt = fluree
                .trigger_index(ledger_id, TriggerIndexOptions::default())
                .await
                .expect("full build")
                .root_id
                .expect("root");
            let counts = leaf_counts(&fluree, ledger_id, &rebuilt).await;
            assert!(counts.iter().all(|(_, n)| *n == 1), "rebuild: {counts:?}");

            // An incremental merge into the default graph's leaves, and the
            // pool's first build, for the new named graph.
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            let more = format!(
                "{prefix}{}GRAPH <{GRAPH_IRI}> {{\n{}}}\n",
                subjects(300..600),
                subjects(0..300)
            );
            fluree
                .stage_owned(ledger)
                .insert_turtle(&more)
                .execute()
                .await
                .expect("second insert");
            let merged = fluree
                .trigger_index(ledger_id, TriggerIndexOptions::default())
                .await
                .expect("incremental build")
                .root_id
                .expect("root");
            let counts = leaf_counts(&fluree, ledger_id, &merged).await;
            // The default graph, txn-meta (g1) and the pool.
            assert_eq!(counts.len(), 13, "every tree in every order: {counts:?}");
            let (pool, own): (Vec<_>, Vec<_>) = counts
                .iter()
                .partition(|(label, _)| label.starts_with("pool"));
            assert!(own.iter().all(|(_, n)| *n == 1), "incremental: {own:?}");
            // A pooled leaf holds about one leaflet: 300 rows in leaflets of 10.
            assert!(
                pool.iter().all(|(_, n)| *n >= 15),
                "pooled leaves: {pool:?}"
            );
        })
        .await;
}
