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
//! 2. the PRODUCTION retry loop at the query entry (`query_with_options`)
//!    absorbs every miss — a single direct `Fluree::query` call completes
//!    with results IDENTICAL to a plain native instance over the same data,
//!    scan misses handled in-frame by the operator and one-shot paths
//!    (fast paths, dir walks, policy sub-queries) by the entry loop;
//! 3. ledger LOAD prefetches novelty's overlay-translation miss sources
//!    (F8: reverse-dict leaves), so translation lookups are pure hits;
//! 4. FORMATTING — the second miss frame — recovers through its own loop.
//!    Encoded bindings are materialized late, through dictionary leaves the
//!    execution round often never touches, so a query that completed can
//!    still miss while being formatted.
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
use fluree_db_core::storage::residency::{MissRegister, Want};
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
                let recovered = recover_once(&cs, storage, &mut recovery, &e).await;
                assert!(recovered, "non-residency ledger-load error: {e:?}");
            }
        }
    }
}

fn rows_of(result: &QueryResult, ledger: &fluree_db_api::LedgerState) -> Vec<JsonValue> {
    normalize_rows(&result.to_jsonld(&ledger.snapshot).expect("to_jsonld"))
}

// ============================================================================
// 1. Operator (scan) path through the PRODUCTION loop: one direct query
//    call completes — leaf misses consumed in-frame by the scan operator,
//    forward-pack misses by the query-entry loop.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn scan_query_completes_through_production_loop() {
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

    // Residency-mode instance, cold resident tier: ONE direct call must
    // succeed — the production loop owns the recovery.
    let (fluree_b, storage) = residency_instance(&shared, &ns);
    let ledger_b = ledger_with_recovery(&fluree_b, &storage).await;
    let db_b = GraphDb::from_ledger_state(&ledger_b);
    let misses_before = storage.miss_count();

    let result = fluree_b
        .query(&db_b, &query)
        .await
        .expect("production retry loop must absorb every residency miss");

    assert!(
        storage.miss_count() > misses_before,
        "the residency path must actually miss (positive marker)"
    );
    assert!(
        storage.register.is_empty(),
        "every recorded want must have been drained by a retry frame"
    );
    assert_eq!(rows_of(&result, &ledger_b), expected, "identical results");
}

// ============================================================================
// 2. Fast path (one-shot predicate COUNT): the production loop at the query
//    entry absorbs the miss the one-shot path cannot retry in-frame.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn fast_path_count_completes_through_production_loop() {
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
    let misses_before = storage.miss_count();

    let result = fluree_b
        .query(&db_b, query)
        .await
        .expect("production retry loop must absorb the one-shot path's misses");

    assert!(
        storage.miss_count() > misses_before,
        "the residency path must actually miss (positive marker)"
    );
    assert!(storage.register.is_empty(), "wants drained");
    assert_eq!(rows_of(&result, &ledger_b), expected, "identical count");
}

// ============================================================================
// 3. Dir-only walk (open_leaf_dir under the COUNT(DISTINCT ?s) fast path,
//    `count_distinct_subjects_for_predicate`): the miss fires inside the
//    leaflet-cache dir-load closure — the F2 stringification point — and the
//    production loop still recovers it via the register. (A whole-graph
//    COUNT(*) is answered from index stats without touching leaves, so it
//    cannot exercise this path.)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn count_distinct_dir_walk_completes_through_production_loop() {
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

    let result = fluree_b
        .query(&db_b, query)
        .await
        .expect("production retry loop must absorb the dir-walk misses");

    assert!(storage.miss_count() > misses_before, "misses must fire");
    assert!(storage.register.is_empty(), "wants drained");
    assert_eq!(rows_of(&result, &ledger_b), expected, "identical count");
}

// ============================================================================
// 4. Policy-filtered query (f:query sub-query): the policy layer
//    stringifies errors (`QueryError::Policy(String)`), so the production
//    loop's recovery works only because the register, not the error chain,
//    carries the wants. `query_connection` routes single-ledger queries
//    through `query_with_options`, i.e. through the production loop.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn policy_filtered_query_completes_through_production_loop() {
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
    let misses_before = storage.miss_count();
    let result = fluree_b
        .query_connection(&query)
        .await
        .expect("production retry loop must absorb the policy path's misses");

    assert!(
        storage.miss_count() > misses_before,
        "the policy path must actually miss (positive marker)"
    );
    assert!(storage.register.is_empty(), "wants drained");
    let ledger_b = ledger_with_recovery(&fluree_b, &storage).await;
    let rows = normalize_rows(&result.to_jsonld(&ledger_b.snapshot).expect("to_jsonld"));
    assert_eq!(rows, expected, "identical policy-filtered results");
}

// ============================================================================
// 5. F8(b): a ledger with UNCOMMITTED-to-index novelty loads with the
//    reverse-dict prefetch, so overlay translation lookups are pure hits and
//    a query touching novelty completes through the production loop.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn novelty_ledger_prefetches_translation_and_completes() {
    support::assert_index_defaults();
    let (shared, ns, fluree_a) = build_indexed_fixture().await;

    // Commit MORE people AFTER the index build: they live only in novelty,
    // and their overlay translation must reverse-look-up the persisted dict
    // trees at query time.
    const EXTRA: u64 = 8;
    let ledger_head = fluree_a.ledger(LEDGER).await.expect("ledger A at index");
    let graph: Vec<JsonValue> = (0..EXTRA)
        .map(|i| {
            json!({
                "@id": format!("ex:novel{i}"),
                "@type": "ex:Item",
                "ex:name": format!("Novel {i}"),
                "ex:level": 0
            })
        })
        .collect();
    let txn = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": graph
    });
    let _ = fluree_a
        .insert(ledger_head, &txn)
        .await
        .expect("post-index novelty insert");

    let query = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "select": "?name",
        "where": [{ "@id": "?item", "ex:name": "?name" }]
    });

    // Ground truth (index + novelty) from the plain instance.
    let ledger_a2 = fluree_a.ledger(LEDGER).await.expect("ledger A + novelty");
    let db_a = GraphDb::from_ledger_state(&ledger_a2);
    let expected = rows_of(
        &fluree_a.query(&db_a, &query).await.expect("query A"),
        &ledger_a2,
    );
    assert_eq!(expected.len(), (PEOPLE + EXTRA) as usize, "fixture sanity");

    // Residency instance: LOAD runs the F8 reverse-leaf prefetch.
    let (fluree_b, storage) = residency_instance(&shared, &ns);
    let ledger_b = ledger_with_recovery(&fluree_b, &storage).await;

    // Direct proof the prefetch covered translation: a novelty subject's
    // reverse lookup — exactly what overlay translation performs per entry —
    // must be a pure hit (no new miss, no retry round).
    let provider = ledger_b
        .snapshot
        .range_provider
        .as_ref()
        .expect("indexed snapshot has a range provider");
    let brp = provider
        .as_any()
        .downcast_ref::<fluree_db_query::BinaryRangeProvider>()
        .expect("binary range provider");
    let store = brp.store();
    let dict_novelty = brp.dict_novelty();
    let (ns_code, suffix) = dict_novelty
        .subjects
        .iter_entries()
        .next()
        .map(|(ns_code, suffix)| (ns_code, suffix.to_string()))
        .expect("post-index commits must have populated dict novelty");
    let misses_before = storage.miss_count();
    let lookup = store.find_subject_id_by_parts(ns_code, &suffix);
    assert!(
        lookup.is_ok(),
        "prefetched reverse leaf must serve the translation lookup: {lookup:?}"
    );
    assert_eq!(
        storage.miss_count(),
        misses_before,
        "translation lookup after the load prefetch must be a pure hit          (F8: no retry round per reverse-dict leaf)"
    );

    // Full query (persisted + novelty rows) through the production loop.
    let db_b = GraphDb::from_ledger_state(&ledger_b);
    let result = fluree_b
        .query(&db_b, &query)
        .await
        .expect("production retry loop must absorb the misses");
    assert_eq!(
        rows_of(&result, &ledger_b),
        expected,
        "identical results including novelty rows"
    );
    assert!(storage.register.is_empty(), "wants drained");
}

// ============================================================================
// 6. A5: a beyond-gap head change (sleeping tab) takes the reload fallback
//    and still lands the manager at the record's watermarks — the engine
//    half of the "queryable at t" signal (the browser head sink fires its
//    callbacks unconditionally after notify, with the record's watermarks).
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn beyond_gap_head_change_reloads_at_record_watermarks() {
    use fluree_db_api::{NotifyResult, NsNotify};
    use fluree_db_nameservice::NameServiceLookup;

    support::assert_index_defaults();
    let (shared, ns, fluree_a) = build_indexed_fixture().await;

    // Residency instance with the ledger CACHED in its manager (the state
    // the SSE head sink notifies against).
    let (fluree_b, storage) = residency_instance(&shared, &ns);
    let _ = ledger_with_recovery(&fluree_b, &storage).await;
    let handle = fluree_b
        .ledger_cached(LEDGER)
        .await
        .expect("cache ledger in manager");
    drop(handle);

    // A sleeps-and-wakes gap: more commits than the incremental cap (5 on
    // native), each insert being one commit.
    const GAP: u64 = 8;
    let mut ledger_a = fluree_a.ledger(LEDGER).await.expect("ledger A");
    for i in 0..GAP {
        let txn = json!({
            "@context": { "ex": "http://example.org/ns/" },
            "@graph": [{
                "@id": format!("ex:wake{i}"),
                "@type": "ex:Item",
                "ex:name": format!("Wake {i}"),
                "ex:level": 1
            }]
        });
        ledger_a = fluree_a
            .insert(ledger_a, &txn)
            .await
            .expect("commit")
            .ledger;
    }

    let record = ns
        .lookup(LEDGER)
        .await
        .expect("ns lookup")
        .expect("record exists");
    let expected_t = record.commit_t;

    let mgr = fluree_b.ledger_manager().expect("manager");
    let t_before = mgr
        .current_t(LEDGER)
        .await
        .expect("ledger cached before the gap");
    assert!(
        expected_t - t_before > 5,
        "fixture sanity: gap {} must exceed the native incremental cap",
        expected_t - t_before
    );
    let result = mgr
        .notify(NsNotify {
            ledger_id: LEDGER.to_string(),
            record: Some(record),
        })
        .await
        .expect("notify");
    assert_eq!(
        result,
        NotifyResult::Reloaded,
        "beyond the incremental cap the fallback must be a full re-open"
    );
    assert_eq!(
        mgr.current_t(LEDGER).await,
        Some(expected_t),
        "reloaded state must sit at the record's commit watermark"
    );

    // And the re-opened state answers queries (through the production loop)
    // with every post-gap row present.
    let query = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "select": "?name",
        "where": [{ "@id": "?item", "ex:name": "?name" }]
    });
    let ledger_b = ledger_with_recovery(&fluree_b, &storage).await;
    assert_eq!(ledger_b.t(), expected_t, "fresh load at the same watermark");
    let db_b = GraphDb::from_ledger_state(&ledger_b);
    let result = fluree_b
        .query(&db_b, &query)
        .await
        .expect("production retry loop absorbs the cold re-open");
    assert_eq!(
        rows_of(&result, &ledger_b).len(),
        (PEOPLE + GAP) as usize,
        "all pre- and post-gap rows visible"
    );
}

// ============================================================================
// 7. A2: a within-gap head change takes incremental catch-up, runs the
//    novelty translation prefetch (call-site wiring; the prefetcher itself
//    is unit-pinned in fluree-db-binary-index), and the caught-up state
//    serves the new rows through the production loop.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn catch_up_applies_commits_and_prefetches_translation() {
    use fluree_db_api::{NotifyResult, NsNotify};
    use fluree_db_nameservice::NameServiceLookup;

    support::assert_index_defaults();
    let (shared, ns, fluree_a) = build_indexed_fixture().await;

    let (fluree_b, storage) = residency_instance(&shared, &ns);
    let _ = ledger_with_recovery(&fluree_b, &storage).await;
    let _ = fluree_b
        .ledger_cached(LEDGER)
        .await
        .expect("cache ledger in manager");

    // Two commits — within the incremental cap — introducing new subjects
    // and strings (novelty translation work).
    let mut ledger_a = fluree_a.ledger(LEDGER).await.expect("ledger A");
    for i in 0..2u64 {
        let txn = json!({
            "@context": { "ex": "http://example.org/ns/" },
            "@graph": [{
                "@id": format!("ex:live{i}"),
                "@type": "ex:Item",
                "ex:name": format!("Live {i}"),
                "ex:level": 2
            }]
        });
        ledger_a = fluree_a
            .insert(ledger_a, &txn)
            .await
            .expect("commit")
            .ledger;
    }

    let record = ns
        .lookup(LEDGER)
        .await
        .expect("ns lookup")
        .expect("record exists");
    let expected_t = record.commit_t;

    let mgr = fluree_b.ledger_manager().expect("manager");
    let result = mgr
        .notify(NsNotify {
            ledger_id: LEDGER.to_string(),
            record: Some(record),
        })
        .await
        .expect("notify");
    assert_eq!(
        result,
        NotifyResult::CommitsApplied { count: 2 },
        "within the cap the head change must catch up incrementally"
    );
    assert_eq!(mgr.current_t(LEDGER).await, Some(expected_t));

    // The caught-up state serves persisted + novelty rows through the
    // production loop, with the register drained afterwards. (On this
    // fixture scale the translation prefetch is a resident no-op — the
    // strong prefetcher pin is `residency_prefetch_covers_novelty_reverse_lookups`
    // in fluree-db-binary-index; this test pins the catch-up call-site
    // wiring and the end-to-end result.)
    let query = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "select": "?name",
        "where": [{ "@id": "?item", "ex:name": "?name" }]
    });
    let ledger_b = ledger_with_recovery(&fluree_b, &storage).await;
    assert_eq!(ledger_b.t(), expected_t);
    let db_b = GraphDb::from_ledger_state(&ledger_b);
    let result = fluree_b
        .query(&db_b, &query)
        .await
        .expect("production retry loop");
    assert_eq!(
        rows_of(&result, &ledger_b).len(),
        (PEOPLE + 2) as usize,
        "persisted + caught-up novelty rows visible"
    );
    assert!(storage.register.is_empty(), "wants drained");
}

// ============================================================================
// 8. FORMATTING is the SECOND residency frame, with a retry loop of its own.
//    Execution emits `Binding::Encoded*` for late materialization; those ids
//    are resolved to IRIs and literals only during formatting, through
//    dictionary and forward-pack leaves the execution round often never
//    touches. So a peer whose query round succeeded can still take its first
//    miss here — which is what `format::format_results_async`'s loop exists
//    for. Every other test in this file formats through the SYNC `to_jsonld`
//    (`rows_of`), so none of them can reach it.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn formatting_completes_through_its_own_residency_loop() {
    support::assert_index_defaults();
    let (shared, ns, fluree_a) = build_indexed_fixture().await;

    // Selecting the subject IRI, not just the literal, is what forces
    // materialization through a dictionary leaf during formatting.
    let query = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "select": ["?item", "?name"],
        "where": [{ "@id": "?item", "ex:name": "?name" }]
    });

    let ledger_a = fluree_a.ledger(LEDGER).await.expect("ledger A");
    let db_a = GraphDb::from_ledger_state(&ledger_a);
    let expected = rows_of(
        &fluree_a.query(&db_a, &query).await.expect("query A"),
        &ledger_a,
    );
    assert_eq!(expected.len(), PEOPLE as usize, "fixture sanity");

    // (a) The loop is load-bearing, not decorative: on a cold residency
    //     instance the production query round succeeds and the SYNC formatter
    //     — the one every other test here uses — still fails, not resident.
    let (fluree_c, storage_c) = residency_instance(&shared, &ns);
    let ledger_c = ledger_with_recovery(&fluree_c, &storage_c).await;
    let db_c = GraphDb::from_ledger_state(&ledger_c);
    let result_c = fluree_c.query(&db_c, &query).await.expect("query C");
    let sync_err = result_c
        .to_jsonld(&ledger_c.snapshot)
        .expect_err("formatting must miss where execution did not")
        .to_string();
    assert!(
        sync_err.contains("not resident"),
        "the formatting-time failure must be a residency miss: {sync_err}"
    );

    // (b) The async formatter absorbs it, on a FRESH instance so nothing (a)
    //     made resident can carry over.
    let (fluree_b, storage_b) = residency_instance(&shared, &ns);
    let ledger_b = ledger_with_recovery(&fluree_b, &storage_b).await;
    let db_b = GraphDb::from_ledger_state(&ledger_b);
    let result_b = fluree_b.query(&db_b, &query).await.expect("query B");
    let misses_before = storage_b.miss_count();

    let formatted = result_b
        .to_jsonld_async(db_b.as_graph_db_ref())
        .await
        .expect("the formatting retry loop must absorb every residency miss");

    assert!(
        storage_b.miss_count() > misses_before,
        "the miss must fire DURING formatting (positive marker), not before it"
    );
    assert!(
        storage_b.register.is_empty(),
        "every want recorded while formatting must have been drained"
    );
    assert_eq!(normalize_rows(&formatted), expected, "identical results");
}
