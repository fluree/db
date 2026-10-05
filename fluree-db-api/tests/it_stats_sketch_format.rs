#![cfg(feature = "native")]
//! The persisted HLL stats sketch is read by exactly one consumer: the next
//! incremental build. When that read fails the build reseeds from the base
//! root, which keeps counts exact but holds NDV at a floor, so most stats
//! tests cannot tell a working read site from a broken one. These tests
//! compare NDV, not counts, on workloads where the floor and the true
//! estimate diverge.

use crate::support;
use crate::support::hooked_storage::{HookedStorage, IndexWriteCounts};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::tx::IndexingMode;
use fluree_db_api::{Fluree, IndexerConfig, LedgerState, NameServiceMode, TriggerIndexOptions};
use fluree_db_binary_index::format::index_root::IndexRoot;
use fluree_db_connection::config::ConnectionConfig;
use fluree_db_core::index_stats::GraphPropertyStatEntry;
use fluree_db_core::{ContentId, ContentStore, StorageRead, StorageWrite};
use fluree_db_indexer::stats::HllSketchBlob;
use fluree_db_nameservice::memory::MemoryNameService;
use serde_json::json;
use std::sync::Arc;
use tokio::task::LocalSet;

type Storage = HookedStorage<IndexWriteCounts>;

fn indexer_config() -> IndexerConfig {
    IndexerConfig::small()
        .with_leaflet_rows(10)
        .with_leaflets_per_leaf(2)
        .with_incremental_enabled(true)
        .with_incremental_max_commits(10_000)
}

fn setup() -> (Fluree, Storage, LocalSet) {
    let storage = HookedStorage::new(IndexWriteCounts::default());
    let nameservice = MemoryNameService::new();
    let mut fluree = Fluree::new(
        ConnectionConfig::memory(),
        storage.clone(),
        NameServiceMode::ReadWrite(Arc::new(nameservice.clone())),
    );
    let (local, handle) = support::start_background_indexer_local(
        fluree_db_core::StorageBackend::Managed(Arc::new(storage.clone())),
        Arc::new(nameservice),
        indexer_config(),
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle));
    (fluree, storage, local)
}

/// One subject per value, so each value is a new distinct object and subject.
async fn insert_values(
    fluree: &Fluree,
    ledger: LedgerState,
    values: std::ops::Range<u32>,
) -> LedgerState {
    let tx = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": values
            .map(|i| json!({ "@id": format!("ex:s{i}"), "ex:val": i }))
            .collect::<Vec<_>>()
    });
    fluree.insert(ledger, &tx).await.expect("insert").ledger
}

/// Index to head; returns the root and how many index leaves the build wrote.
async fn index(fluree: &Fluree, storage: &Storage, ledger_id: &str) -> (IndexRoot, u64) {
    let before = storage.hooks().snapshot_counts().0;
    let res = fluree
        .trigger_index(ledger_id, TriggerIndexOptions::default())
        .await
        .expect("trigger_index");
    let leaves = storage.hooks().snapshot_counts().0 - before;
    let root_id = res.root_id.expect("root id");
    (load_root(fluree, ledger_id, &root_id).await, leaves)
}

/// A full rebuild of the same ledger: the sketch regenerated from the data.
async fn reindex(fluree: &Fluree, storage: &Storage, ledger_id: &str) -> (IndexRoot, u64) {
    let before = storage.hooks().snapshot_counts().0;
    let res = fluree
        .reindex(
            ledger_id,
            ReindexOptions::default().with_indexer_config(indexer_config()),
        )
        .await
        .expect("reindex");
    let leaves = storage.hooks().snapshot_counts().0 - before;
    (load_root(fluree, ledger_id, &res.root_id).await, leaves)
}

async fn load_root(fluree: &Fluree, ledger_id: &str, root_id: &ContentId) -> IndexRoot {
    let bytes = fluree
        .content_store(ledger_id)
        .get(root_id)
        .await
        .expect("root bytes");
    IndexRoot::decode(&bytes).expect("decode root")
}

/// `ex:val`'s stats: the only property in the default graph.
fn val_stat(root: &IndexRoot) -> GraphPropertyStatEntry {
    let graphs = root
        .stats
        .as_ref()
        .and_then(|s| s.graphs.as_ref())
        .expect("per-graph stats");
    let default_graph = graphs.iter().find(|g| g.g_id == 0).expect("default graph");
    assert_eq!(
        default_graph.properties.len(),
        1,
        "only ex:val in the default graph"
    );
    default_graph.properties[0].clone()
}

async fn sketch_address(storage: &Storage, root: &IndexRoot) -> String {
    let cid = root.sketch_ref.as_ref().expect("root has a sketch");
    let suffix = format!("{}.hll", cid.digest_hex());
    storage
        .list_prefix("")
        .await
        .expect("list")
        .into_iter()
        .find(|a| a.ends_with(&suffix))
        .expect("sketch stored")
}

async fn sketch_bytes(storage: &Storage, root: &IndexRoot) -> Vec<u8> {
    let address = sketch_address(storage, root).await;
    storage.read_bytes(&address).await.expect("read sketch")
}

/// Replace the bytes stored at the root's sketch CID. Reads do not verify
/// digests, so the next incremental build reads these bytes.
async fn overwrite_sketch(storage: &Storage, root: &IndexRoot, bytes: &[u8]) {
    let address = sketch_address(storage, root).await;
    storage
        .write_bytes(&address, bytes)
        .await
        .expect("overwrite sketch");
}

/// The v1 JSON encoding of a sketch, as releases before v2 wrote it.
fn encode_v1(blob: &HllSketchBlob) -> Vec<u8> {
    let hex =
        |registers: &[u8]| -> String { registers.iter().map(|b| format!("{b:02x}")).collect() };
    let entries: Vec<_> = blob
        .entries
        .iter()
        .map(|e| {
            json!({
                "g_id": e.g_id,
                "p_id": e.p_id,
                "count": e.count,
                "values_hll": hex(e.values_hll.registers()),
                "subjects_hll": hex(e.subjects_hll.registers()),
                "last_modified_t": e.last_modified_t,
                "datatypes": e.datatypes,
            })
        })
        .collect();
    serde_json::to_vec(&json!({
        "version": 1,
        "index_t": blob.index_t,
        "entries": entries,
    }))
    .unwrap()
}

/// Asserts the build that produced `incremental` was incremental, and that
/// it carried the prior sketch's registers: its NDV must equal a full
/// rebuild's, not the base-root floor.
fn assert_matches_full_rebuild(
    incremental: (&IndexRoot, u64),
    full: (&IndexRoot, u64),
    floor: &GraphPropertyStatEntry,
) {
    let (incremental, incremental_leaves) = incremental;
    let (full, full_leaves) = full;
    assert!(
        incremental_leaves < full_leaves,
        "the build under test must be incremental: it wrote {incremental_leaves} leaves, \
         a full rebuild wrote {full_leaves}"
    );
    let got = val_stat(incremental);
    let want = val_stat(full);
    assert_eq!(got.count, want.count);
    assert!(
        want.ndv_values > floor.ndv_values && want.ndv_subjects > floor.ndv_subjects,
        "workload must move NDV past the base floor, or a fallback would pass: \
         floor {floor:?}, full {want:?}"
    );
    assert_eq!(
        (got.ndv_values, got.ndv_subjects),
        (want.ndv_values, want.ndv_subjects),
        "incremental NDV must equal a full rebuild's"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn incremental_build_carries_ndv_from_a_v2_sketch() {
    let (fluree, storage, local) = setup();
    local
        .run_until(async move {
            let ledger_id = "it/sketch-v2:main";
            let ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
            let ledger = insert_values(&fluree, ledger, 0..300).await;
            let (base, _) = index(&fluree, &storage, ledger_id).await;
            assert!(
                sketch_bytes(&storage, &base).await.starts_with(b"FHLL"),
                "full builds write v2"
            );

            insert_values(&fluree, ledger, 300..360).await;
            let incremental = index(&fluree, &storage, ledger_id).await;
            assert!(sketch_bytes(&storage, &incremental.0)
                .await
                .starts_with(b"FHLL"));
            let full = reindex(&fluree, &storage, ledger_id).await;

            assert_eq!(val_stat(&incremental.0).count, 360);
            assert_matches_full_rebuild(
                (&incremental.0, incremental.1),
                (&full.0, full.1),
                &val_stat(&base),
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn incremental_build_upgrades_a_v1_sketch() {
    let (fluree, storage, local) = setup();
    local
        .run_until(async move {
            let ledger_id = "it/sketch-v1:main";
            let ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
            let ledger = insert_values(&fluree, ledger, 0..300).await;
            let (base, _) = index(&fluree, &storage, ledger_id).await;

            // Stand in for a ledger last indexed by a v1 release.
            let blob =
                HllSketchBlob::from_bytes(&sketch_bytes(&storage, &base).await).expect("decode v2");
            overwrite_sketch(&storage, &base, &encode_v1(&blob)).await;
            assert_eq!(sketch_bytes(&storage, &base).await[0], b'{');

            insert_values(&fluree, ledger, 300..360).await;
            let incremental = index(&fluree, &storage, ledger_id).await;
            assert!(
                sketch_bytes(&storage, &incremental.0)
                    .await
                    .starts_with(b"FHLL"),
                "a build seeded from v1 writes v2"
            );
            let full = reindex(&fluree, &storage, ledger_id).await;
            assert_matches_full_rebuild(
                (&incremental.0, incremental.1),
                (&full.0, full.1),
                &val_stat(&base),
            );
        })
        .await;
}

/// Pins the degradation accepted for downgrade or mixed-version indexing: a
/// build that cannot read the prior sketch reseeds from the base root. Counts
/// stay exact; NDV loses every distinct value the unreadable sketch held, and
/// later readable builds do not recover them. Only a full rebuild does.
#[tokio::test(flavor = "current_thread")]
async fn unreadable_sketch_keeps_counts_exact_and_thins_ndv_until_rebuild() {
    let (fluree, storage, local) = setup();
    local
        .run_until(async move {
            let ledger_id = "it/sketch-fallback:main";
            let mut ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
            ledger = insert_values(&fluree, ledger, 0..100).await;
            let (mut root, _) = index(&fluree, &storage, ledger_id).await;

            for round in 1..=4u32 {
                ledger = insert_values(&fluree, ledger, round * 100..(round + 1) * 100).await;
                if round % 2 == 1 {
                    overwrite_sketch(&storage, &root, b"not a sketch").await;
                }
                (root, _) = index(&fluree, &storage, ledger_id).await;
                assert_eq!(
                    val_stat(&root).count,
                    u64::from((round + 1) * 100),
                    "round {round}: counts stay exact"
                );
            }

            let (full, _) = reindex(&fluree, &storage, ledger_id).await;
            let thinned = val_stat(&root);
            let truth = val_stat(&full);
            assert_eq!(thinned.count, truth.count);
            assert!(
                thinned.ndv_values * 2 < truth.ndv_values,
                "two unreadable sketches each discarded ~100 of 500 distinct values; \
                 thinned {} vs rebuilt {}",
                thinned.ndv_values,
                truth.ndv_values
            );
        })
        .await;
}
