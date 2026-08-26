//! Real-path native tests for the wasm residency read tier.
//!
//! These drive REAL queries through a residency-mode storage backend — the
//! exact assembly a browser peer uses (`FlureeBuilder::memory().build_with(
//! storage, NameServiceMode::ReadOnly(...))`, storage behind
//! `StorageContentStore`) — with an initially empty resident tier, and
//! assert that:
//!
//! 1. every sync read miss is RECORDED in the store-level miss register (the
//!    load-bearing channel — the typed `NeedFetch` error is stringified by
//!    most query-crate wrappers and cannot be relied on above the operator
//!    boundary);
//! 2. a drain → concurrent-fetch → re-run loop (progress-terminated, the
//!    same shape `RetryBudget` implements) completes each query with results
//!    IDENTICAL to a plain native instance over the same data;
//! 3. the scan operator's in-frame retry consumes leaf misses without
//!    surfacing them to the outer loop (F7's operator-local await-and-retry);
//! 4. one-shot paths (fast paths, dir-only count walks, policy-filtered
//!    queries) recover through the outer loop.
//!
//! Every test asserts a positive "miss fired" marker; recovery is never
//! inferred from the absence of an error.
//!
//! Requires the `residency` feature on fluree-db-query/fluree-db-binary-index
//! (wired through this crate's dev-dependencies), which compiles the same
//! read arms wasm32 gets unconditionally.

mod support;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_api::{FlureeBuilder, GraphDb, NameServiceMode, QueryResult};
use fluree_db_binary_index::read::need_fetch::fetch_wants;
use fluree_db_core::storage::residency::{FetchKind, MissRegister, Want};
use fluree_db_core::storage::{
    ContentAddressedWrite, ContentStore, ContentWriteResult, MemoryStorage, StorageMethod,
    StorageRead, StorageWrite,
};
use fluree_db_core::ContentId;
use fluree_db_nameservice::memory::MemoryNameService;
use parking_lot::RwLock;
use serde_json::{json, Value as JsonValue};
use support::normalize_rows;

const LEDGER: &str = "test/residency:main";
const PEOPLE: u64 = 40;

// ============================================================================
// Residency-mode storage: async side reads the shared MemoryStorage and pins
// (fetch-pins contract); the sync resident tier starts empty; misses are
// counted and recorded in the register.
// ============================================================================

#[derive(Debug, Clone)]
struct ResidencyStorage {
    inner: MemoryStorage,
    /// Resident tier keyed by the CID's digest hex (the digest appears in
    /// every CAS address, so pin-on-read can key by it without knowing the
    /// namespace layout — exactly what a CID-addressed browser store does
    /// naturally).
    resident: Arc<RwLock<HashMap<String, Arc<[u8]>>>>,
    register: Arc<MissRegister>,
    misses: Arc<AtomicUsize>,
}

impl ResidencyStorage {
    fn over(inner: MemoryStorage) -> Self {
        Self {
            inner,
            resident: Arc::new(RwLock::new(HashMap::new())),
            register: Arc::new(MissRegister::new()),
            misses: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn miss_count(&self) -> usize {
        self.misses.load(Ordering::Relaxed)
    }

    fn pin(&self, address: &str, bytes: &[u8]) {
        if let Some(hex) = digest_hex_in(address) {
            self.resident
                .write()
                .insert(hex, Arc::from(bytes.to_vec().into_boxed_slice()));
        }
    }
}

/// Extract the 64-char lowercase-hex digest run embedded in a CAS address.
fn digest_hex_in(address: &str) -> Option<String> {
    let bytes = address.as_bytes();
    let mut run_start = 0usize;
    let mut run_len = 0usize;
    for (i, &b) in bytes.iter().enumerate() {
        let is_hex = b.is_ascii_digit() || (b'a'..=b'f').contains(&b);
        if is_hex {
            if run_len == 0 {
                run_start = i;
            }
            run_len += 1;
        } else {
            if run_len == 64 {
                return Some(address[run_start..run_start + 64].to_string());
            }
            run_len = 0;
        }
    }
    (run_len == 64).then(|| address[run_start..run_start + 64].to_string())
}

#[async_trait]
impl StorageRead for ResidencyStorage {
    async fn read_bytes(&self, address: &str) -> fluree_db_core::Result<Vec<u8>> {
        let bytes = self.inner.read_bytes(address).await?;
        // Fetch-pins contract: bytes served async become sync-resident.
        self.pin(address, &bytes);
        Ok(bytes)
    }

    async fn exists(&self, address: &str) -> fluree_db_core::Result<bool> {
        self.inner.exists(address).await
    }

    async fn list_prefix(&self, prefix: &str) -> fluree_db_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    fn resolve_cached_bytes(&self, id: &ContentId) -> Option<Arc<[u8]>> {
        let hit = self.resident.read().get(&id.digest_hex()).cloned();
        if hit.is_none() {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    fn miss_register(&self) -> Option<&MissRegister> {
        Some(&self.register)
    }
}

#[async_trait]
impl StorageWrite for ResidencyStorage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> fluree_db_core::Result<()> {
        self.inner.write_bytes(address, bytes).await
    }

    async fn delete(&self, address: &str) -> fluree_db_core::Result<()> {
        self.inner.delete(address).await
    }
}

#[async_trait]
impl ContentAddressedWrite for ResidencyStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: fluree_db_core::content_kind::ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> fluree_db_core::Result<ContentWriteResult> {
        self.inner
            .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
            .await
    }
}

impl StorageMethod for ResidencyStorage {
    fn storage_method(&self) -> &str {
        self.inner.storage_method()
    }
}

// ============================================================================
// Fixture: instance A (plain storage, read-write nameservice) seeds and
// indexes the ledger; residency-mode instances are built over the SAME
// shared MemoryStorage afterwards.
// ============================================================================

fn people_txn() -> JsonValue {
    let graph: Vec<JsonValue> = (0..PEOPLE)
        .map(|i| {
            json!({
                "@id": format!("ex:p{i}"),
                "@type": "ex:Item",
                "ex:name": format!("Person {i}"),
                "ex:level": i % 4
            })
        })
        .collect();
    json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": graph
    })
}

async fn build_indexed_fixture() -> (MemoryStorage, Arc<MemoryNameService>, fluree_db_api::Fluree) {
    let shared = MemoryStorage::new();
    let ns = Arc::new(MemoryNameService::new());
    let fluree_a = FlureeBuilder::memory().build_with(
        shared.clone(),
        NameServiceMode::ReadWrite(
            ns.clone() as Arc<dyn fluree_db_nameservice::NameServicePublisher>
        ),
    );
    let ledger0 = support::genesis_ledger_for_fluree(&fluree_a, LEDGER);
    let _ = fluree_a
        .insert(ledger0, &people_txn())
        .await
        .expect("seed people");
    support::rebuild_and_publish_index(&fluree_a, LEDGER).await;
    (shared, ns, fluree_a)
}

/// A fresh residency-mode instance over the shared storage — the browser
/// assembly path (`build_with` + `NameServiceMode::ReadOnly`), so the hook
/// forwarding through `StorageContentStore` is what the tests exercise.
fn residency_instance(
    shared: &MemoryStorage,
    ns: &Arc<MemoryNameService>,
) -> (fluree_db_api::Fluree, ResidencyStorage) {
    let storage = ResidencyStorage::over(shared.clone());
    let fluree = FlureeBuilder::memory().build_with(
        storage.clone(),
        NameServiceMode::ReadOnly(ns.clone() as Arc<dyn fluree_db_nameservice::NameServiceLookup>),
    );
    (fluree, storage)
}

/// A `ContentStore` over the same residency storage, for the recovery loop's
/// drain-and-fetch side (the browser driver holds the equivalent handle).
fn recovery_store(storage: &ResidencyStorage) -> impl ContentStore {
    fluree_db_core::storage::content_store_for(storage.clone(), LEDGER)
}

// ============================================================================
// The outer recovery loop: drain the register on ANY error, fetch the wants
// concurrently, require progress, re-run. (`RetryBudget` packages the same
// policy; the loop is written out here so tests can capture the want sets.)
// ============================================================================

struct Recovery {
    rounds: usize,
    wants: Vec<Want>,
}

async fn recover_once(
    cs: &dyn ContentStore,
    storage: &ResidencyStorage,
    recovery: &mut Recovery,
    err: &dyn std::fmt::Debug,
) -> bool {
    let wants = storage.register.drain();
    if wants.is_empty() {
        return false;
    }
    recovery.rounds += 1;
    assert!(
        recovery.rounds <= 64,
        "runaway recovery loop (last error: {err:?})"
    );
    let outcome = fetch_wants(cs, wants.clone(), 8).await;
    assert!(
        outcome.newly_resident > 0,
        "recovery round made no progress: {:?}",
        outcome.failures
    );
    recovery.wants.extend(wants);
    true
}

async fn query_with_recovery(
    fluree: &fluree_db_api::Fluree,
    db: &GraphDb,
    query: &JsonValue,
    storage: &ResidencyStorage,
) -> (QueryResult, Recovery) {
    let cs = recovery_store(storage);
    let mut recovery = Recovery {
        rounds: 0,
        wants: Vec::new(),
    };
    loop {
        match fluree.query(db, query).await {
            Ok(result) => return (result, recovery),
            Err(e) => {
                if !recover_once(&cs, storage, &mut recovery, &e).await {
                    panic!("non-residency query error: {e:?}");
                }
            }
        }
    }
}

async fn ledger_with_recovery(
    fluree: &fluree_db_api::Fluree,
    storage: &ResidencyStorage,
) -> fluree_db_api::LedgerState {
    let cs = recovery_store(storage);
    let mut recovery = Recovery {
        rounds: 0,
        wants: Vec::new(),
    };
    loop {
        match fluree.ledger(LEDGER).await {
            Ok(ledger) => return ledger,
            Err(e) => {
                if !recover_once(&cs, storage, &mut recovery, &e).await {
                    panic!("non-residency ledger-load error: {e:?}");
                }
            }
        }
    }
}

fn rows_of(result: &QueryResult, ledger: &fluree_db_api::LedgerState) -> Vec<JsonValue> {
    normalize_rows(&result.to_jsonld(&ledger.snapshot).expect("to_jsonld"))
}

// ============================================================================
// 1. Operator (scan) path: leaf misses are consumed IN-FRAME by the scan
//    operator's retry; only non-leaf wants (forward packs) reach the outer
//    loop.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn scan_query_recovers_and_leaf_misses_stay_in_frame() {
    support::assert_index_defaults();
    let (shared, ns, fluree_a) = build_indexed_fixture().await;

    let query = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "select": "?name",
        "where": [{ "@id": "?item", "ex:name": "?name" }]
    });

    // Ground truth from the plain native instance.
    let ledger_a = fluree_a.ledger(LEDGER).await.expect("ledger A");
    let db_a = GraphDb::from_ledger_state(&ledger_a);
    let expected = rows_of(
        &fluree_a.query(&db_a, &query).await.expect("query A"),
        &ledger_a,
    );
    assert_eq!(expected.len(), PEOPLE as usize, "fixture sanity");

    // Residency-mode instance, cold resident tier.
    let (fluree_b, storage) = residency_instance(&shared, &ns);
    let ledger_b = ledger_with_recovery(&fluree_b, &storage).await;
    let db_b = GraphDb::from_ledger_state(&ledger_b);
    let misses_before = storage.miss_count();

    let (result, recovery) = query_with_recovery(&fluree_b, &db_b, &query, &storage).await;

    assert!(
        storage.miss_count() > misses_before,
        "the residency path must actually miss (positive marker)"
    );
    assert_eq!(rows_of(&result, &ledger_b), expected, "identical results");
    // F7 in-frame proof: the scan operator drains and fetches its own leaf
    // wants; whatever reached the OUTER loop must not be index leaves.
    assert!(
        !recovery
            .wants
            .iter()
            .any(|w| w.kind == FetchKind::IndexLeaf),
        "leaf misses leaked to the outer loop — in-frame scan retry regressed: {:?}",
        recovery.wants
    );
}

// ============================================================================
// 2. Fast path (one-shot predicate COUNT): recovers through the outer loop.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn fast_path_count_recovers_through_outer_loop() {
    support::assert_index_defaults();
    let (shared, ns, fluree_a) = build_indexed_fixture().await;

    let query = "SELECT (COUNT(?s) AS ?c) WHERE { ?s <http://example.org/ns/name> ?o }";

    let ledger_a = fluree_a.ledger(LEDGER).await.expect("ledger A");
    let db_a = GraphDb::from_ledger_state(&ledger_a);
    let expected = rows_of(
        &fluree_a.query(&db_a, query).await.expect("query A"),
        &ledger_a,
    );

    let (fluree_b, storage) = residency_instance(&shared, &ns);
    let ledger_b = ledger_with_recovery(&fluree_b, &storage).await;
    let db_b = GraphDb::from_ledger_state(&ledger_b);

    // Positive one-shot proof: the first attempt must surface the miss (fast
    // paths have no in-frame retry; if this starts succeeding, they gained
    // one and this test should be updated).
    let first = fluree_b.query(&db_b, query).await;
    assert!(
        first.is_err(),
        "expected the cold fast path to surface a residency miss on the first attempt"
    );
    assert!(
        !storage.register.is_empty(),
        "the miss must be recorded in the register before the retry loop runs"
    );

    let cs = recovery_store(&storage);
    let mut recovery = Recovery {
        rounds: 0,
        wants: Vec::new(),
    };
    let result = loop {
        match fluree_b.query(&db_b, query).await {
            Ok(result) => break result,
            Err(e) => {
                if !recover_once(&cs, &storage, &mut recovery, &e).await {
                    panic!("non-residency query error: {e:?}");
                }
            }
        }
    };
    assert!(
        recovery.rounds >= 1,
        "outer loop must have done the recovery"
    );
    assert!(
        recovery
            .wants
            .iter()
            .any(|w| w.kind == FetchKind::IndexLeaf),
        "fast-path leaf wants flow through the outer loop: {:?}",
        recovery.wants
    );
    assert_eq!(rows_of(&result, &ledger_b), expected, "identical count");
}

// ============================================================================
// 3. Dir-only walk (open_leaf_dir under the COUNT(DISTINCT ?s) fast path,
//    `count_distinct_subjects_for_predicate`): the miss fires inside the
//    leaflet-cache dir-load closure — the F2 stringification point — and
//    still registers and recovers. (A whole-graph COUNT(*) is answered from
//    index stats without touching leaves, so it cannot exercise this path.)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn count_distinct_dir_walk_recovers() {
    support::assert_index_defaults();
    let (shared, ns, fluree_a) = build_indexed_fixture().await;

    let query = "SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s <http://example.org/ns/name> ?o }";

    let ledger_a = fluree_a.ledger(LEDGER).await.expect("ledger A");
    let db_a = GraphDb::from_ledger_state(&ledger_a);
    let expected = rows_of(
        &fluree_a.query(&db_a, query).await.expect("query A"),
        &ledger_a,
    );

    let (fluree_b, storage) = residency_instance(&shared, &ns);
    let ledger_b = ledger_with_recovery(&fluree_b, &storage).await;
    let db_b = GraphDb::from_ledger_state(&ledger_b);
    let misses_before = storage.miss_count();

    let cs = recovery_store(&storage);
    let mut recovery = Recovery {
        rounds: 0,
        wants: Vec::new(),
    };
    let result = loop {
        match fluree_b.query(&db_b, query).await {
            Ok(result) => break result,
            Err(e) => {
                if !recover_once(&cs, &storage, &mut recovery, &e).await {
                    panic!("non-residency query error: {e:?}");
                }
            }
        }
    };

    assert!(storage.miss_count() > misses_before, "misses must fire");
    assert_eq!(rows_of(&result, &ledger_b), expected, "identical count");
    // The dir walk's wants are index leaves; whether they surface in-frame
    // or in the outer loop depends on which count plan fires, so the strong
    // assertion here is identical results plus the positive miss marker.
    let _ = recovery;
}

// ============================================================================
// 4. Policy-filtered query (f:query sub-query): the policy layer stringifies
//    errors (`QueryError::Policy(String)`), so recovery MUST come from the
//    register, not the error chain.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn policy_filtered_query_recovers_via_register() {
    support::assert_index_defaults();
    let (shared, ns, fluree_a) = build_indexed_fixture().await;

    let policy = json!([{
        "@id": "ex:levelPolicy",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "f:query": {
            "@type": "@json",
            "@value": {
                "@context": { "ex": "http://example.org/ns/" },
                "where": [{ "@id": "?$this", "ex:level": 0 }]
            }
        }
    }]);
    let query = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "from": LEDGER,
        "opts": { "policy": policy, "default-allow": false },
        "select": "?name",
        "where": [{ "@id": "?item", "@type": "ex:Item", "ex:name": "?name" }]
    });

    // Ground truth on the plain instance (10 of 40 people have level 0).
    let expected_result = fluree_a
        .query_connection(&query)
        .await
        .expect("policy query A");
    let ledger_a = fluree_a.ledger(LEDGER).await.expect("ledger A");
    let expected = normalize_rows(
        &expected_result
            .to_jsonld(&ledger_a.snapshot)
            .expect("to_jsonld"),
    );
    assert_eq!(expected.len(), (PEOPLE / 4) as usize, "fixture sanity");

    let (fluree_b, storage) = residency_instance(&shared, &ns);
    let cs = recovery_store(&storage);
    let mut recovery = Recovery {
        rounds: 0,
        wants: Vec::new(),
    };
    let misses_before = storage.miss_count();
    let result = loop {
        match fluree_b.query_connection(&query).await {
            Ok(result) => break result,
            Err(e) => {
                if !recover_once(&cs, &storage, &mut recovery, &e).await {
                    panic!("non-residency policy-query error: {e:?}");
                }
            }
        }
    };

    assert!(
        storage.miss_count() > misses_before,
        "the policy path must actually miss (positive marker)"
    );
    let ledger_b = ledger_with_recovery(&fluree_b, &storage).await;
    let rows = normalize_rows(&result.to_jsonld(&ledger_b.snapshot).expect("to_jsonld"));
    assert_eq!(rows, expected, "identical policy-filtered results");
}
