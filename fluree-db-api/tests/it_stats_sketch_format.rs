#![cfg(feature = "native")]
//! The persisted HLL stats sketch is read by exactly one consumer: the next
//! incremental build. When that read fails the build reseeds from the base
//! root, which keeps counts exact but holds NDV at a floor, so most stats
//! tests cannot tell a working read site from a broken one. These tests
//! compare NDV, not just counts, on workloads where the floor and the true
//! estimate diverge.

use crate::support;
use crate::support::hooked_storage::{HookedStorage, IndexWriteCounts};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::tx::IndexingMode;
use fluree_db_api::{Fluree, IndexerConfig, LedgerState, NameServiceMode, TriggerIndexOptions};
use fluree_db_binary_index::format::index_root::IndexRoot;
use fluree_db_connection::config::ConnectionConfig;
use fluree_db_core::graph_registry::{DEFAULT_GRAPH_ID, FIRST_USER_GRAPH_ID};
use fluree_db_core::index_stats::GraphPropertyStatEntry;
use fluree_db_core::{ContentId, ContentStore, StorageRead, StorageWrite};
use fluree_db_indexer::stats::HllSketchBlob;
use fluree_db_nameservice::memory::MemoryNameService;
use serde_json::json;
use std::collections::BTreeMap;
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

type UserStats = BTreeMap<(u16, u32), (u64, u64, u64, i64, Vec<(u8, u64)>)>;

/// Per-(graph, property) stats for the default graph and every user named
/// graph: count, ndv values, ndv subjects, last modified t, datatypes.
fn user_stats(root: &IndexRoot) -> UserStats {
    root.stats
        .as_ref()
        .and_then(|s| s.graphs.as_ref())
        .expect("per-graph stats")
        .iter()
        .filter(|g| g.g_id == DEFAULT_GRAPH_ID || g.g_id >= FIRST_USER_GRAPH_ID)
        .flat_map(|g| {
            g.properties.iter().map(move |p| {
                (
                    (g.g_id, p.p_id),
                    (
                        p.count,
                        p.ndv_values,
                        p.ndv_subjects,
                        p.last_modified_t,
                        p.datatypes.clone(),
                    ),
                )
            })
        })
        .collect()
}

/// Asserts the build that produced `incremental` was incremental, and that
/// it carried the prior sketch's registers: every user-graph stat, NDV
/// included, must equal a full rebuild's rather than the base-root floor.
fn assert_matches_full_rebuild(
    incremental: (&IndexRoot, u64),
    full: (&IndexRoot, u64),
    base: &IndexRoot,
) {
    let (incremental, incremental_leaves) = incremental;
    let (full, full_leaves) = full;
    assert!(
        incremental_leaves < full_leaves,
        "the build under test must be incremental: it wrote {incremental_leaves} leaves, \
         a full rebuild wrote {full_leaves}"
    );
    let floor = user_stats(base);
    let want = user_stats(full);
    assert!(
        want.iter().any(|(key, (_, ndv_v, ndv_s, _, _))| {
            floor
                .get(key)
                .is_some_and(|(_, base_v, base_s, _, _)| ndv_v > base_v && ndv_s > base_s)
        }),
        "workload must move some NDV past the base floor, or a fallback would pass: \
         floor {floor:?}, full {want:?}"
    );
    assert_eq!(
        user_stats(incremental),
        want,
        "incremental stats must equal a full rebuild's"
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
            assert_matches_full_rebuild((&incremental.0, incremental.1), (&full.0, full.1), &base);
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
            assert_matches_full_rebuild((&incremental.0, incremental.1), (&full.0, full.1), &base);
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

async fn sparql_update(fluree: &Fluree, ledger_id: &str, update: String) {
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(&update)
        .commit()
        .await
        .expect("sparql update");
}

fn triples(range: std::ops::Range<u32>, triple: impl Fn(u32) -> String) -> String {
    range.map(triple).collect::<Vec<_>>().join("\n")
}

/// Retractions and named graphs through SPARQL UPDATE (the other tests use
/// JSON-LD inserts into the default graph). The novelty retracts part of one
/// property, every string of a mixed-datatype property, and all of another
/// property, so counts clamp to zero and datatype entries drop out while
/// registers, which never shrink, carry forward.
#[tokio::test(flavor = "current_thread")]
async fn incremental_build_with_retractions_in_named_graphs_matches_full_rebuild() {
    let (fluree, storage, local) = setup();
    local
        .run_until(async move {
            let ledger_id = "it/sketch-retract:main";
            fluree.create_ledger(ledger_id).await.expect("create");
            sparql_update(
                &fluree,
                ledger_id,
                format!(
                    "PREFIX ex: <http://example.org/>\nINSERT DATA {{\n{}\n\
                     GRAPH ex:g1 {{\n{}\n{}\n}}\n\
                     GRAPH ex:g2 {{\n{}\n{}\n}}\n}}",
                    triples(0..300, |i| format!("ex:s{i} ex:val {i} .")),
                    triples(0..200, |i| format!("ex:a{i} ex:val {i} .")),
                    triples(0..200, |i| format!("ex:a{i} ex:label \"a{i}\" .")),
                    triples(0..20, |i| format!("ex:b{i} ex:mixed {i} .")),
                    triples(20..40, |i| format!("ex:b{i} ex:mixed \"m{i}\" .")),
                ),
            )
            .await;
            let (base, _) = index(&fluree, &storage, ledger_id).await;
            assert!(
                user_stats(&base)
                    .keys()
                    .any(|&(g_id, _)| g_id >= FIRST_USER_GRAPH_ID),
                "named graphs must be in the base stats"
            );

            sparql_update(
                &fluree,
                ledger_id,
                format!(
                    "PREFIX ex: <http://example.org/>\nDELETE DATA {{\n{}\n\
                     GRAPH ex:g1 {{\n{}\n}}\n\
                     GRAPH ex:g2 {{\n{}\n}}\n}}",
                    triples(0..50, |i| format!("ex:s{i} ex:val {i} .")),
                    triples(0..200, |i| format!("ex:a{i} ex:label \"a{i}\" .")),
                    triples(20..40, |i| format!("ex:b{i} ex:mixed \"m{i}\" .")),
                ),
            )
            .await;
            sparql_update(
                &fluree,
                ledger_id,
                format!(
                    "PREFIX ex: <http://example.org/>\nINSERT DATA {{\n{}\n\
                     GRAPH ex:g1 {{\n{}\n}}\n}}",
                    triples(300..360, |i| format!("ex:s{i} ex:val {i} .")),
                    triples(200..260, |i| format!("ex:a{i} ex:val {i} .")),
                ),
            )
            .await;

            let incremental = index(&fluree, &storage, ledger_id).await;
            let full = reindex(&fluree, &storage, ledger_id).await;
            assert_matches_full_rebuild((&incremental.0, incremental.1), (&full.0, full.1), &base);

            // The retractions must have reached the compared stats.
            let stats = user_stats(&incremental.0);
            assert!(
                stats.values().any(|(count, ..)| *count == 310),
                "default-graph ex:val: 300 - 50 + 60 = 310 facts; {stats:?}"
            );
            assert!(
                stats
                    .values()
                    .any(|(count, .., datatypes)| *count == 20 && datatypes.len() == 1),
                "g2 ex:mixed keeps only its integer datatype; {stats:?}"
            );
        })
        .await;
}
