#![cfg(feature = "native")]
//! The statistics store (hot object and base segments) holds each
//! (graph, property) entry's HLL registers, and the next incremental build is
//! what reads them. A build that lost them would still keep counts exact but
//! hold NDV at the base root's floor, so most stats tests cannot tell a
//! working read site from a broken one. These tests compare NDV, not just
//! counts, on workloads where the floor and the true estimate diverge.

use crate::support;
use crate::support::hooked_storage::{HookedStorage, IndexWriteCounts};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::tx::IndexingMode;
use fluree_db_api::{
    CommitOpts, Fluree, IndexConfig, IndexerConfig, LedgerState, NameServiceMode,
    TriggerIndexOptions, TxnOpts,
};
use fluree_db_binary_index::format::index_root::IndexRoot;
use fluree_db_binary_index::format::stats_store::StoreObject;
use fluree_db_connection::config::ConnectionConfig;
use fluree_db_core::graph_registry::{DEFAULT_GRAPH_ID, FIRST_USER_GRAPH_ID};
use fluree_db_core::index_stats::GraphPropertyStatEntry;
use fluree_db_core::{ContentId, ContentStore, StorageRead, StorageWrite};
use fluree_db_indexer::hll::HllSketch256;
use fluree_db_indexer::StatsCompactionPolicy;
use fluree_db_nameservice::memory::MemoryNameService;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::task::LocalSet;

type Storage = HookedStorage<IndexWriteCounts>;

/// A tiny stats compaction policy, so every test here also runs the hot
/// object through compaction and segment splits.
fn indexer_config() -> IndexerConfig {
    IndexerConfig::small()
        .with_leaflet_rows(10)
        .with_leaf_rows(20)
        .with_incremental_enabled(true)
        .with_incremental_max_commits(10_000)
        .with_stats_compaction(StatsCompactionPolicy {
            hot_threshold_entries: 2,
            hot_ceiling_entries: 4,
            normal_steps: 1,
            segment_target_entries: 3,
        })
}

/// Commits never trigger a build, so the only builds are the ones a test
/// asks for and each build's leaf writes can be measured without a race.
fn no_auto_index() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: usize::MAX,
        reindex_max_bytes: usize::MAX,
    }
}

fn setup() -> (Fluree, Storage, LocalSet) {
    let (fluree, storage, _, local) = setup_with_nameservice(indexer_config());
    (fluree, storage, local)
}

fn setup_with_nameservice(config: IndexerConfig) -> (Fluree, Storage, MemoryNameService, LocalSet) {
    let storage = HookedStorage::new(IndexWriteCounts::default());
    let nameservice = MemoryNameService::new();
    let mut fluree = Fluree::new(
        ConnectionConfig::memory(),
        storage.clone(),
        NameServiceMode::ReadWrite(Arc::new(nameservice.clone())),
    );
    let (local, handle) = support::start_background_indexer_local(
        fluree_db_core::StorageBackend::Managed(Arc::new(storage.clone())),
        Arc::new(nameservice.clone()),
        config,
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle));
    (fluree, storage, nameservice, local)
}

/// A second process over the same storage and nameservice, with nothing
/// cached. Call inside the first one's `LocalSet`.
fn restart(storage: &Storage, nameservice: &MemoryNameService, config: IndexerConfig) -> Fluree {
    let mut fluree = Fluree::new(
        ConnectionConfig::memory(),
        storage.clone(),
        NameServiceMode::ReadWrite(Arc::new(nameservice.clone())),
    );
    let (worker, handle) = fluree_db_api::BackgroundIndexerWorker::new(
        fluree_db_core::StorageBackend::Managed(Arc::new(storage.clone())),
        Arc::new(nameservice.clone()),
        config,
    );
    tokio::task::spawn_local(worker.run());
    fluree.set_indexing_mode(IndexingMode::Background(handle));
    fluree
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
    fluree
        .insert_with_opts(
            ledger,
            &tx,
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto_index(),
        )
        .await
        .expect("insert")
        .ledger
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

/// A full rebuild of the same ledger: the store regenerated from the data.
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
        .content_store(&fluree.storage_namespace(ledger_id).await.unwrap())
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

/// The storage address of a stats store object.
async fn object_address(storage: &Storage, cid: &ContentId) -> String {
    let suffix = format!("{}.hll", cid.address_digest());
    storage
        .list_prefix("")
        .await
        .expect("list")
        .into_iter()
        .find(|a| a.ends_with(&suffix))
        .expect("stats store object stored")
}

async fn object_bytes(storage: &Storage, cid: &ContentId) -> Vec<u8> {
    let address = object_address(storage, cid).await;
    storage
        .read_bytes(&address)
        .await
        .expect("read stats object")
}

/// Replace the bytes stored at `cid`. Reads do not verify digests, so the
/// next incremental build reads these bytes.
async fn overwrite_object(storage: &Storage, cid: &ContentId, bytes: &[u8]) {
    let address = object_address(storage, cid).await;
    storage
        .write_bytes(&address, bytes)
        .await
        .expect("overwrite stats object");
}

/// Each user-graph entry's (values, subjects) estimates from the store `root`
/// references: hot entries over segment entries, drop markers applied.
async fn store_estimates(
    storage: &Storage,
    root: &IndexRoot,
) -> BTreeMap<(fluree_db_core::GraphId, u32), (u64, u64)> {
    let store = root.stats_store.as_ref().expect("root has a stats store");
    let mut objects = Vec::new();
    for segment in &store.segments {
        objects.push(StoreObject::decode(&object_bytes(storage, &segment.cid).await).unwrap());
    }
    let hot = match &store.hot {
        Some(cid) => Some(StoreObject::decode(&object_bytes(storage, cid).await).unwrap()),
        None => None,
    };
    let markers = hot.as_ref().map(|h| h.markers.clone()).unwrap_or_default();
    objects.extend(hot);
    let mut out = BTreeMap::new();
    for e in objects.into_iter().flat_map(|o| o.entries) {
        let s = &e.summary;
        let user = s.key.g_id == DEFAULT_GRAPH_ID || s.key.g_id >= FIRST_USER_GRAPH_ID;
        if !user || markers.iter().any(|m| m.hides(s)) {
            continue;
        }
        out.insert(
            (s.key.g_id, s.key.p_id),
            (
                HllSketch256::from_bytes(&e.values).estimate(),
                HllSketch256::from_bytes(&e.subjects).estimate(),
            ),
        );
    }
    out
}

type UserStats = BTreeMap<(fluree_db_core::GraphId, u32), (u64, u64, u64, i64, Vec<(u8, u64)>)>;

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
/// it carried the store's registers: every user-graph stat, NDV
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

/// A full build writes every entry to base segments; an incremental build
/// reads the touched entries back and writes them to the hot object, so its
/// NDV matches a full rebuild's.
#[tokio::test(flavor = "current_thread")]
async fn incremental_build_carries_ndv_through_the_store() {
    let (fluree, storage, local) = setup();
    local
        .run_until(async move {
            let ledger_id = "it/store-carry:main";
            let ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id).await;
            let ledger = insert_values(&fluree, ledger, 0..300).await;
            let (base, _) = index(&fluree, &storage, ledger_id).await;
            let store = base
                .stats_store
                .as_ref()
                .expect("full builds write the store");
            assert!(!store.segments.is_empty() && store.hot.is_none());
            assert!(base.sketch_ref.is_none());

            insert_values(&fluree, ledger, 300..360).await;
            let incremental = index(&fluree, &storage, ledger_id).await;
            let store = incremental.0.stats_store.as_ref().expect("store");
            assert!(
                store.hot.is_some(),
                "an incremental build writes the hot object"
            );
            let full = reindex(&fluree, &storage, ledger_id).await;

            assert_eq!(val_stat(&incremental.0).count, 360);
            assert_matches_full_rebuild((&incremental.0, incremental.1), (&full.0, full.1), &base);
        })
        .await;
}

/// A store object the next build cannot read fails the incremental build,
/// and the indexer rebuilds in full: counts and NDV come out exact rather
/// than reseeded from the base root's floor.
#[tokio::test(flavor = "current_thread")]
async fn unreadable_store_object_falls_back_to_a_full_rebuild() {
    let (fluree, storage, local) = setup();
    local
        .run_until(async move {
            let ledger_id = "it/store-unreadable:main";
            let mut ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id).await;
            ledger = insert_values(&fluree, ledger, 0..100).await;
            let (root, full_leaves) = index(&fluree, &storage, ledger_id).await;
            let segment = root.stats_store.as_ref().unwrap().segments[0].cid.clone();
            overwrite_object(&storage, &segment, b"not a stats object").await;

            insert_values(&fluree, ledger, 100..400).await;
            let (rebuilt, leaves) = index(&fluree, &storage, ledger_id).await;
            assert!(
                leaves >= full_leaves,
                "the build must have fallen back to a full rebuild: {leaves} leaves, \
                 the first full build {full_leaves}"
            );
            let (full, _) = reindex(&fluree, &storage, ledger_id).await;
            assert_eq!(val_stat(&rebuilt).count, 400);
            assert_eq!(user_stats(&rebuilt), user_stats(&full));
        })
        .await;
}

async fn sparql_update(fluree: &Fluree, ledger_id: &str, update: String) {
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(&update)
        .index_config(no_auto_index())
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
                     GRAPH ex:g2 {{\n{}\n{}\n}}\n\
                     GRAPH ex:g3 {{\n{}\n}}\n}}",
                    triples(0..300, |i| format!("ex:s{i} ex:val {i} .")),
                    triples(0..200, |i| format!("ex:a{i} ex:val {i} .")),
                    triples(0..200, |i| format!("ex:a{i} ex:label \"a{i}\" .")),
                    triples(0..20, |i| format!("ex:b{i} ex:mixed {i} .")),
                    triples(20..40, |i| format!("ex:b{i} ex:mixed \"m{i}\" .")),
                    // Never touched again: its leaves separate an incremental
                    // build from a full rebuild, since the novelty below
                    // reaches most leaves of the other graphs.
                    triples(0..600, |i| format!("ex:c{i} ex:val {i} .")),
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

/// What a build's NDV was before untouched entries kept their base
/// estimates: each entry's estimate from the registers the build persisted,
/// floored at the base root's. Every user-graph entry of `root` must equal
/// it, so keeping a base estimate never changes what a build publishes. The
/// root and its store hold the same entries, and every base entry outside a
/// `dropped` graph is still there.
async fn assert_ndv_is_the_floored_estimate(
    storage: &Storage,
    base: &IndexRoot,
    root: &IndexRoot,
    dropped: Option<fluree_db_core::GraphId>,
    step: &str,
) {
    let estimates = store_estimates(storage, root).await;
    let floor = user_stats(base);
    let got = user_stats(root);
    assert!(
        got.keys().eq(estimates.keys()),
        "{step}: root and store entries differ: root {:?}, store {:?}",
        got.keys().collect::<Vec<_>>(),
        estimates.keys().collect::<Vec<_>>()
    );
    for key in floor.keys().filter(|&&(g_id, _)| Some(g_id) != dropped) {
        assert!(got.contains_key(key), "{step}: {key:?} lost its entry");
    }
    for (key, (_, ndv_v, ndv_s, _, _)) in &got {
        let (est_v, est_s) = estimates[key];
        let (floor_v, floor_s) = floor.get(key).map_or((0, 0), |(_, v, s, _, _)| (*v, *s));
        assert_eq!(
            (*ndv_v, *ndv_s),
            (est_v.max(floor_v), est_s.max(floor_s)),
            "{step}: {key:?}"
        );
    }
}

fn graph_id(root: &IndexRoot, iri: &str) -> Option<fluree_db_core::GraphId> {
    let slot = root.graph_iris.iter().position(|g| g == iri)?;
    Some(slot as fluree_db_core::GraphId + 1)
}

/// Each window touches one or two of eight (graph, property) entries, so
/// most entries keep their base NDV; the windows cover assertions,
/// retractions, zero-net churn, reassertions and a dropped graph.
#[tokio::test(flavor = "current_thread")]
async fn untouched_entries_keep_the_ndv_a_full_estimate_would_give() {
    let (fluree, storage, local) = setup();
    local
        .run_until(async move {
            let ledger_id = "it/sketch-carry:main";
            fluree.create_ledger(ledger_id).await.expect("create");
            let update = |body: String| {
                let fluree = &fluree;
                async move {
                    sparql_update(
                        fluree,
                        ledger_id,
                        format!("PREFIX ex: <http://example.org/>\n{body}"),
                    )
                    .await;
                }
            };
            update(format!(
                "INSERT DATA {{\n{}\n{}\n\
                 GRAPH ex:g1 {{\n{}\n{}\n}}\n\
                 GRAPH ex:g2 {{\n{}\n{}\n}}\n\
                 GRAPH ex:g3 {{\n{}\n{}\n}}\n}}",
                triples(0..200, |i| format!("ex:s{i} ex:val {i} .")),
                triples(0..200, |i| format!("ex:s{i} ex:label \"s{i}\" .")),
                triples(0..150, |i| format!("ex:a{i} ex:val {i} .")),
                triples(0..150, |i| format!("ex:a{i} ex:label \"a{i}\" .")),
                triples(0..100, |i| format!("ex:b{i} ex:val {i} .")),
                triples(0..100, |i| format!("ex:b{i} ex:label \"b{i}\" .")),
                triples(0..80, |i| format!("ex:c{i} ex:val {i} .")),
                triples(0..80, |i| format!("ex:c{i} ex:label \"c{i}\" .")),
            ))
            .await;
            let (mut base, full_leaves) = index(&fluree, &storage, ledger_id).await;
            assert_eq!(user_stats(&base).len(), 8, "{:?}", user_stats(&base));
            let g3 = graph_id(&base, "http://example.org/g3");
            assert!(g3.is_some());
            // A full rebuild writes every leaf; a build that falls back to one
            // would pass the NDV comparison without keeping any base NDV.
            let assert_incremental = |leaves: u64, step: &str| {
                assert!(
                    leaves < full_leaves,
                    "{step}: the build must be incremental: it wrote {leaves} leaves, \
                     the full build {full_leaves}"
                );
            };

            let windows: Vec<(&str, Vec<String>)> = vec![
                (
                    "assertions",
                    vec![format!(
                        "INSERT DATA {{ GRAPH ex:g1 {{\n{}\n}} }}",
                        triples(150..220, |i| format!("ex:a{i} ex:val {i} ."))
                    )],
                ),
                (
                    "retractions",
                    vec![format!(
                        "DELETE DATA {{\n{}\n}}",
                        triples(0..60, |i| format!("ex:s{i} ex:val {i} ."))
                    )],
                ),
                (
                    "zero-net churn",
                    vec![
                        format!(
                            "DELETE DATA {{ GRAPH ex:g2 {{\n{}\n}} }}",
                            triples(0..30, |i| format!("ex:b{i} ex:label \"b{i}\" ."))
                        ),
                        format!(
                            "INSERT DATA {{ GRAPH ex:g2 {{\n{}\n}} }}",
                            triples(0..30, |i| format!("ex:b{i} ex:label \"b{i}\" ."))
                        ),
                    ],
                ),
                (
                    "reassertions",
                    vec![format!(
                        "INSERT DATA {{ GRAPH ex:g1 {{\n{}\n}} }}",
                        triples(0..40, |i| format!("ex:a{i} ex:label \"a{i}\" ."))
                    )],
                ),
                ("dropped graph", vec!["DROP GRAPH ex:g3".to_string()]),
            ];
            for (step, updates) in windows {
                for body in updates {
                    update(body).await;
                }
                let (root, leaves) = index(&fluree, &storage, ledger_id).await;
                assert_incremental(leaves, step);
                let dropped = (step == "dropped graph").then_some(g3).flatten();
                assert_ndv_is_the_floored_estimate(&storage, &base, &root, dropped, step).await;
                base = root;
            }
            assert!(
                user_stats(&base).keys().all(|&(g, _)| Some(g) != g3),
                "the dropped graph's stats are gone: {:?}",
                user_stats(&base)
            );
        })
        .await;
}

/// Counts and datatypes per (graph, property): what a full rebuild must
/// agree on exactly. (NDV can differ: a rebuild's HLLs observe history.)
type Counts = BTreeMap<(fluree_db_core::GraphId, u32), (u64, Vec<(u8, u64)>)>;

fn counts(root: &IndexRoot) -> Counts {
    user_stats(root)
        .into_iter()
        .map(|(k, (count, _, _, _, mut dts))| {
            dts.sort_unstable();
            (k, (count, dts))
        })
        .collect()
}

/// The store holds every entry the root does: a store that lost one, say to
/// a drop marker hiding what it should not, fails here.
async fn assert_store_covers_root(storage: &Storage, root: &IndexRoot, step: &str) {
    let estimates = store_estimates(storage, root).await;
    let root_keys: Vec<_> = user_stats(root).into_keys().collect();
    let store_keys: Vec<_> = estimates.into_keys().collect();
    assert_eq!(
        root_keys, store_keys,
        "{step}: root and store entries differ"
    );
}

/// A commit drops `g2` and writes it afresh at the same `t`, so the
/// recreated graph's entries sit exactly at the drop's floor next to its
/// marker. Later builds leave them in the hot object until a write of many
/// new graphs pushes it into compaction; a second process over the same
/// storage then writes `g2` again. The recreated entries stay visible
/// throughout; the old graph's never return.
#[tokio::test(flavor = "current_thread")]
async fn a_graph_recreated_at_its_drop_survives_compaction_and_restart() {
    // No compaction until the hot object passes 20 entries.
    let config = indexer_config().with_stats_compaction(StatsCompactionPolicy {
        hot_threshold_entries: 20,
        hot_ceiling_entries: 40,
        normal_steps: 1,
        segment_target_entries: 3,
    });
    let (fluree, storage, nameservice, local) = setup_with_nameservice(config.clone());
    local
        .run_until(async move {
            let ledger_id = "it/store-drop:main";
            fluree.create_ledger(ledger_id).await.expect("create");
            let graphs = |range: std::ops::RangeInclusive<u32>| -> String {
                range
                    .map(|g| {
                        format!(
                            "GRAPH ex:g{g} {{\n{}\n{}\n}}",
                            triples(0..20, |i| format!("ex:s{g}_{i} ex:val {i} .")),
                            triples(0..20, |i| format!("ex:s{g}_{i} ex:label \"l{g}_{i}\" ."))
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            let insert = |body: String| {
                format!("PREFIX ex: <http://example.org/>\nINSERT DATA {{\n{body}\n}}")
            };
            sparql_update(&fluree, ledger_id, insert(graphs(1..=4))).await;
            let (base, _) = index(&fluree, &storage, ledger_id).await;
            assert_store_covers_root(&storage, &base, "full build").await;

            sparql_update(
                &fluree,
                ledger_id,
                format!(
                    "PREFIX ex: <http://example.org/>\nDROP GRAPH ex:g2 ;\n\
                     INSERT DATA {{ GRAPH ex:g2 {{\n{}\n}} }}",
                    triples(0..5, |i| format!("ex:n{i} ex:val {} .", 1000 + i))
                ),
            )
            .await;
            let (dropped, _) = index(&fluree, &storage, ledger_id).await;
            let store = dropped.stats_store.as_ref().unwrap();
            assert_eq!(
                store.segments,
                base.stats_store.as_ref().unwrap().segments,
                "no compaction yet"
            );
            assert_store_covers_root(&storage, &dropped, "drop and recreate").await;

            for round in 0..2u32 {
                sparql_update(
                    &fluree,
                    ledger_id,
                    insert(format!(
                        "GRAPH ex:g1 {{ ex:r{round} ex:val {} . }}",
                        500 + round
                    )),
                )
                .await;
                let (root, _) = index(&fluree, &storage, ledger_id).await;
                assert_store_covers_root(&storage, &root, "a build that leaves g2 alone").await;
            }

            sparql_update(&fluree, ledger_id, insert(graphs(5..=16))).await;
            let (compacted, _) = index(&fluree, &storage, ledger_id).await;
            assert_ne!(
                compacted.stats_store.as_ref().unwrap().segments,
                base.stats_store.as_ref().unwrap().segments,
                "compaction must have rewritten base segments"
            );
            assert_store_covers_root(&storage, &compacted, "compaction").await;

            let restarted = restart(&storage, &nameservice, config);
            sparql_update(
                &restarted,
                ledger_id,
                insert("GRAPH ex:g2 { ex:n9 ex:val 2000 . }".to_string()),
            )
            .await;
            let (root, _) = index(&restarted, &storage, ledger_id).await;
            assert_store_covers_root(&storage, &root, "after restart").await;
            let (full, _) = reindex(&restarted, &storage, ledger_id).await;
            assert_eq!(counts(&root), counts(&full));
            let g2 = graph_id(&root, "http://example.org/g2").expect("g2");
            let g2_counts: Vec<_> = counts(&root)
                .into_iter()
                .filter(|((g, _), _)| *g == g2)
                .map(|(_, (count, _))| count)
                .collect();
            assert_eq!(
                g2_counts,
                vec![6],
                "only the recreated graph's six ex:val facts"
            );
        })
        .await;
}
