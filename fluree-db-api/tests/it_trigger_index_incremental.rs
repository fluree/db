#![cfg(feature = "native")]

mod support;

use async_trait::async_trait;
use fluree_db_api::tx::IndexingMode;
use fluree_db_api::{Fluree, IndexerConfig, NameServiceMode, TriggerIndexOptions};
use fluree_db_connection::config::ConnectionConfig;
use fluree_db_core::{ContentKind, ContentStore, MemoryStorage, StorageMethod};
use fluree_db_nameservice::memory::MemoryNameService;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
struct CountingStorage {
    inner: MemoryStorage,
    index_leaf_writes: Arc<AtomicU64>,
    index_branch_writes: Arc<AtomicU64>,
    index_root_writes: Arc<AtomicU64>,
}

impl CountingStorage {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            index_leaf_writes: Arc::new(AtomicU64::new(0)),
            index_branch_writes: Arc::new(AtomicU64::new(0)),
            index_root_writes: Arc::new(AtomicU64::new(0)),
        }
    }

    fn snapshot_counts(&self) -> (u64, u64, u64) {
        (
            self.index_leaf_writes.load(Ordering::Relaxed),
            self.index_branch_writes.load(Ordering::Relaxed),
            self.index_root_writes.load(Ordering::Relaxed),
        )
    }
}

#[async_trait]
impl fluree_db_core::StorageRead for CountingStorage {
    async fn read_bytes(&self, address: &str) -> fluree_db_core::error::Result<Vec<u8>> {
        self.inner.read_bytes(address).await
    }

    async fn exists(&self, address: &str) -> fluree_db_core::error::Result<bool> {
        self.inner.exists(address).await
    }

    async fn list_prefix(&self, prefix: &str) -> fluree_db_core::error::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    fn resolve_local_path(&self, address: &str) -> Option<std::path::PathBuf> {
        self.inner.resolve_local_path(address)
    }
}

impl CountingStorage {
    /// Increment the appropriate counter based on what kind of artifact the
    /// address points at. Used by both `write_bytes` and
    /// `content_write_bytes_with_hash` so writes are counted regardless of
    /// which entry point the indexer uses to upload a CAS blob.
    fn note_address(&self, address: &str) {
        if address.contains("/index/objects/leaves/") {
            self.index_leaf_writes.fetch_add(1, Ordering::Relaxed);
        } else if address.contains("/index/objects/branches/") {
            self.index_branch_writes.fetch_add(1, Ordering::Relaxed);
        } else if address.contains("/index/roots/") {
            self.index_root_writes.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[async_trait]
impl fluree_db_core::StorageWrite for CountingStorage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> fluree_db_core::error::Result<()> {
        self.note_address(address);
        self.inner.write_bytes(address, bytes).await
    }

    async fn delete(&self, address: &str) -> fluree_db_core::error::Result<()> {
        self.inner.delete(address).await
    }
}

impl StorageMethod for CountingStorage {
    fn storage_method(&self) -> &str {
        self.inner.storage_method()
    }
}

#[async_trait]
impl fluree_db_core::ContentAddressedWrite for CountingStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> fluree_db_core::error::Result<fluree_db_core::storage::ContentWriteResult> {
        let result = self
            .inner
            .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
            .await?;
        self.note_address(&result.address);
        Ok(result)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn trigger_index_second_run_uses_incremental_not_full_rebuild() {
    let storage = CountingStorage::new();
    let nameservice = MemoryNameService::new();

    let mut fluree: Fluree = Fluree::new(
        ConnectionConfig::memory(),
        storage.clone(),
        NameServiceMode::ReadWrite(Arc::new(nameservice.clone())),
    );

    // Use tiny leaflets/leaves so we can get multi-leaf indexes with small data.
    let indexer_cfg = IndexerConfig::small()
        .with_leaflet_rows(10)
        .with_leaflets_per_leaf(2)
        .with_incremental_enabled(true)
        .with_incremental_max_commits(10_000);

    let (local, handle) = support::start_background_indexer_local(
        fluree_db_core::StorageBackend::Managed(std::sync::Arc::new(storage.clone())),
        std::sync::Arc::new(nameservice.clone()),
        indexer_cfg,
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/trigger-index-incremental:main";
            let mut ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);

            // Seed enough facts to force multiple leaf files under the tiny leaf sizing.
            // 120 subjects × 1 property => 120 facts => ~6 leaves per order (20 rows per leaf).
            let tx = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "xsd": "http://www.w3.org/2001/XMLSchema#"
                },
                "@graph": (0..120).map(|i| {
                    json!({
                        "@id": format!("ex:person{i}"),
                        "@type": "ex:Person",
                        "ex:name": format!("Person {i}"),
                        // Two BigInt-typed properties to force per-predicate NumBig arenas.
                        // Values are unique per subject so the arenas are non-trivial.
                        "ex:amountA": { "@value": format!("1000000000000000000000000000{i}"), "@type": "xsd:integer" },
                        "ex:amountB": { "@value": format!("2000000000000000000000000000{i}"), "@type": "xsd:integer" }
                    })
                }).collect::<Vec<_>>()
            });

            let r1 = fluree.insert(ledger, &tx).await.expect("seed insert");
            ledger = r1.ledger;

            // First trigger builds the initial full index (no prior root).
            let before1 = storage.snapshot_counts();
            let res1 = fluree
                .trigger_index(ledger_id, TriggerIndexOptions::default())
                .await
                .expect("trigger_index #1");
            let after1 = storage.snapshot_counts();
            let delta1_leaf = after1.0 - before1.0;
            assert!(
                delta1_leaf >= 8,
                "expected full build to write many index leaves; delta={delta1_leaf}"
            );
            let root1 = res1.root_id.clone().expect("root id after first index");

            // Second tx: localized novelty that should touch only a few leaves.
            // Use same predicate set to avoid width promotion / rebuild fallback.
            let tx2 = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "xsd": "http://www.w3.org/2001/XMLSchema#"
                },
                "@id": "ex:person060",
                "ex:name": "Person 60 updated",
                // Update ONLY amountA to force only that predicate's NumBig arena to change.
                "ex:amountA": { "@value": "9999999999999999999999999999999", "@type": "xsd:integer" }
            });
            let r2 = fluree.insert(ledger, &tx2).await.expect("update insert");
            ledger = r2.ledger;

            let before2 = storage.snapshot_counts();
            let res2 = fluree
                .trigger_index(ledger_id, TriggerIndexOptions::default())
                .await
                .expect("trigger_index #2");
            let after2 = storage.snapshot_counts();
            let delta2_leaf = after2.0 - before2.0;
            // Second run should be incremental: it should write *far fewer* index leaves
            // than the initial full build.
            assert!(
                delta2_leaf < delta1_leaf,
                "expected incremental run to write fewer index leaves than full build; full_delta={delta1_leaf}, incr_delta={delta2_leaf}"
            );
            // Safety cap: a localized update should not rewrite "most" leaves.
            assert!(
                delta2_leaf <= 20,
                "expected incremental update to rewrite only a small number of leaves; delta={delta2_leaf}"
            );
            assert!(
                res2.index_t >= r2.receipt.t,
                "index_t should advance to cover latest commit"
            );
            let root2 = res2.root_id.clone().expect("root id after second index");
            assert_ne!(root1, root2, "root CID should change after update");

            // Sanity: both roots decode as IndexRoot and the second root remains queryable.
            let cs = fluree.content_store(ledger_id);
            let bytes1 = cs.get(&root1).await.expect("root1 bytes");
            let bytes2 = cs.get(&root2).await.expect("root2 bytes");
            let v1 = fluree_db_binary_index::format::index_root::IndexRoot::decode(&bytes1)
                .expect("decode root1");
            let v2 = fluree_db_binary_index::format::index_root::IndexRoot::decode(&bytes2)
                .expect("decode root2");
            assert!(
                !v1.default_graph_orders.is_empty() && !v2.default_graph_orders.is_empty(),
                "expected default graph routing to be present"
            );

            // NumBig arena predicate scoping: updating only `ex:amountA` should only
            // rewrite that predicate's numbig arena CID (and reuse `ex:amountB`).
            let find_p_id = |root: &fluree_db_binary_index::format::index_root::IndexRoot,
                             target_iri: &str|
             -> u32 {
                for (p_id, (ns_code, suffix)) in root.predicate_sids.iter().enumerate() {
                    let prefix = root
                        .namespace_codes
                        .get(ns_code)
                        .map(std::string::String::as_str)
                        .unwrap_or("");
                    let iri = format!("{prefix}{suffix}");
                    if iri == target_iri {
                        return p_id as u32;
                    }
                }
                panic!("predicate IRI not found in root predicate_sids: {target_iri}");
            };

            let amount_a_iri = "http://example.org/amountA";
            let amount_b_iri = "http://example.org/amountB";
            let p_amount_a = find_p_id(&v1, amount_a_iri);
            let p_amount_b = find_p_id(&v1, amount_b_iri);

            let numbig_cid = |root: &fluree_db_binary_index::format::index_root::IndexRoot,
                              g_id: u16,
                              p_id: u32|
             -> fluree_db_core::ContentId {
                let ga = root
                    .graph_arenas
                    .iter()
                    .find(|ga| ga.g_id == g_id)
                    .unwrap_or_else(|| panic!("missing graph_arenas entry for g_id={g_id}"));
                ga.numbig
                    .iter()
                    .find(|(pid, _)| *pid == p_id)
                    .map(|(_, cid)| cid.clone())
                    .unwrap_or_else(|| {
                        panic!("missing numbig arena for g_id={g_id}, p_id={p_id} (predicate likely not parsed as BigInt/Decimal)")
                    })
            };

            let nb1_a = numbig_cid(&v1, 0, p_amount_a);
            let nb1_b = numbig_cid(&v1, 0, p_amount_b);
            let nb2_a = numbig_cid(&v2, 0, p_amount_a);
            let nb2_b = numbig_cid(&v2, 0, p_amount_b);

            assert_ne!(
                nb1_a, nb2_a,
                "expected numbig CID to change for updated predicate amountA"
            );
            assert_eq!(
                nb1_b, nb2_b,
                "expected numbig CID to be reused for untouched predicate amountB"
            );

            // CID stability: only a small number of leaf blobs should change per order.
            let leaves_by_order = |root: &fluree_db_binary_index::format::index_root::IndexRoot| {
                let mut m: HashMap<
                    fluree_db_binary_index::format::run_record::RunSortOrder,
                    Vec<fluree_db_core::ContentId>,
                > = HashMap::new();
                for o in &root.default_graph_orders {
                    m.insert(
                        o.order,
                        o.leaves.iter().map(|e| e.leaf_cid.clone()).collect(),
                    );
                }
                m
            };
            let m1 = leaves_by_order(&v1);
            let m2 = leaves_by_order(&v2);

            let mut any_order_changed = false;
            for (order, cids1) in &m1 {
                let Some(cids2) = m2.get(order) else {
                    continue;
                };
                assert_eq!(
                    cids1.len(),
                    cids2.len(),
                    "expected same leaf count for order {order:?}"
                );
                if cids1.is_empty() {
                    continue;
                }

                let diffs = cids1
                    .iter()
                    .zip(cids2.iter())
                    .filter(|(a, b)| *a != *b)
                    .count();
                if diffs > 0 {
                    any_order_changed = true;
                }
                assert!(
                    diffs <= 2,
                    "expected at most 2 leaf CID changes for order {:?}, got {} (leaf_count={})",
                    order,
                    diffs,
                    cids1.len()
                );
            }
            assert!(
                any_order_changed,
                "expected at least one order to have a rewritten leaf CID after update"
            );

            let q = json!({
                "@context": { "ex":"http://example.org/" },
                "select": ["?name"],
                "where": { "@id": "ex:person060", "ex:name": "?name" }
            });
            let out = support::query_jsonld(&fluree, &ledger, &q)
                .await
                .expect("query");
            let rows = out.to_jsonld(&ledger.snapshot).expect("jsonld");
            assert!(
                rows.as_array().is_some_and(|a| !a.is_empty()),
                "expected query result after incremental index"
            );
        })
        .await;
}
