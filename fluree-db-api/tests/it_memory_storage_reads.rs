//! Memory storage already holds every index artifact in process memory, so a
//! warm query over it must not read anything back out of the store: the
//! readers borrow the resident bytes instead of copying them per open.

#![cfg(feature = "native")]

use crate::support;
use crate::support::hooked_storage::{HookedStorage, StorageHooks};
use async_trait::async_trait;
use fluree_db_api::tx::IndexingMode;
use fluree_db_api::{
    BackgroundIndexerWorker, Fluree, FlureeBuilder, IndexerConfig, LedgerManagerConfig,
    LedgerState, NameServiceMode, TriggerIndexOptions,
};
use fluree_db_core::StorageBackend;
use fluree_db_nameservice::memory::MemoryNameService;
use parking_lot::Mutex;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Default)]
struct CountReads {
    addresses: Mutex<Vec<String>>,
}

#[async_trait]
impl StorageHooks for CountReads {
    async fn before_read(&self, address: &str) {
        self.addresses.lock().push(address.to_owned());
    }
}

impl CountReads {
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.addresses.lock())
    }
}

fn people(from: usize, to: usize) -> serde_json::Value {
    let graph: Vec<_> = (from..to)
        .map(|i| {
            json!({
                "@id": format!("ex:p{i}"),
                "@type": "ex:Person",
                "ex:name": format!("Person {i}"),
                "ex:age": i % 90,
            })
        })
        .collect();
    json!({"@context": {"ex": "http://example.org/"}, "@graph": graph})
}

async fn query_people(fluree: &Fluree, ledger: &LedgerState) -> usize {
    let rows = support::query_jsonld_formatted(
        fluree,
        ledger,
        &json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["?s", "?name"],
            "where": {"@id": "?s", "@type": "ex:Person", "ex:name": "?name", "ex:age": 42}
        }),
    )
    .await
    .expect("query");
    rows.as_array().map(Vec::len).unwrap_or(0)
}

fn by_kind(reads: &[String]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for address in reads {
        let kind = address.rsplit('/').nth(1).unwrap_or("?").to_owned();
        *counts.entry(kind).or_default() += 1;
    }
    counts
}

#[tokio::test(flavor = "multi_thread")]
async fn warm_queries_over_memory_storage_read_nothing_from_the_store() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let indexer_config = IndexerConfig::small().with_data_dir(tmp.path().join("data"));
    let cache_dir = tmp.path().join("cache");
    let storage = HookedStorage::new(CountReads::default());
    let nameservice = MemoryNameService::new();
    let mut fluree: Fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig {
            cache_dir,
            ..LedgerManagerConfig::default()
        })
        .build_with(
            storage.clone(),
            NameServiceMode::ReadWrite(Arc::new(nameservice.clone())),
        );
    let (worker, handle) = BackgroundIndexerWorker::new(
        StorageBackend::Managed(Arc::new(storage.clone())),
        Arc::new(nameservice),
        indexer_config,
    );
    tokio::spawn(worker.run());
    fluree.set_indexing_mode(IndexingMode::Background(handle));

    let ledger_id = "it/memory-reads:main";
    let mut ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
    // A full build, then an incremental one, so both leaf shapes exist.
    for (from, to) in [(0, 3000), (3000, 3500)] {
        ledger = fluree
            .insert(ledger, &people(from, to))
            .await
            .expect("insert")
            .ledger;
        fluree
            .trigger_index(ledger_id, TriggerIndexOptions::default())
            .await
            .expect("trigger_index");
    }

    let ledger = fluree.ledger(ledger_id).await.expect("load");
    for _ in 0..2 {
        query_people(&fluree, &ledger).await;
    }
    storage.hooks().take();

    const QUERIES: usize = 20;
    for _ in 0..QUERIES {
        assert_eq!(query_people(&fluree, &ledger).await, 39);
    }
    let reads = storage.hooks().take();

    assert!(
        reads.is_empty(),
        "{QUERIES} warm queries read {} artifacts from memory storage ({:.1} per query): {:?}",
        reads.len(),
        reads.len() as f64 / QUERIES as f64,
        by_kind(&reads),
    );
}
