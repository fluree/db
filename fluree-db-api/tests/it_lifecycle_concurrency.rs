//! Audit 0.4 — concurrency / lifecycle regression tests.
//!
//! These are the safety net Phase 2 (`CoherentLedgerState` + `ArcSwap`) depends
//! on: they pin observable behavior across the refresh/commit lifecycle and the
//! `dict_novelty` Arc-identity contract documented on `LedgerState`. None of
//! these existed before; the lifecycle audit (§3.3) flagged the gap.
//!
//! Run with:
//!   cargo test -p fluree-db-api --test it_lifecycle_concurrency --features native

#![cfg(feature = "native")]

mod support;

use fluree_db_api::ledger_manager::RefreshOpts;
use fluree_db_api::{FlureeBuilder, ReindexOptions};
use serde_json::json;

/// One insert of `ex:item{k} ex:name "item{k}"`.
fn item_tx(k: i64) -> serde_json::Value {
    json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [{ "@id": format!("ex:item{k}"), "ex:name": format!("item{k}") }]
    })
}

/// Count the `?s ex:name ?o` subjects visible in `ledger` (one row per item).
async fn item_count(fluree: &fluree_db_api::Fluree, ledger: &fluree_db_api::LedgerState) -> usize {
    let sparql = "PREFIX ex: <http://example.org/> SELECT ?s WHERE { ?s ex:name ?o }";
    let rows = support::query_sparql(fluree, ledger, sparql)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    rows.as_array().map(std::vec::Vec::len).unwrap_or(0)
}

/// Detached-`dict_novelty` detection — the Arc-identity contract on `LedgerState`.
///
/// After `reindex` attaches a `BinaryRangeProvider` holding
/// `Arc::clone(dict_novelty)`, a later commit mutates `dict_novelty` via
/// `Arc::make_mut`. If the provider's copy silently detaches (the chronic
/// disappearing-properties bug), overlay translation can no longer resolve the
/// post-index novel subject, and a bound-subject query for it returns empty.
/// This pins that the post-index novel subject still resolves through the
/// attached provider (same namespace — so only the subject/string `dict_novelty`
/// is exercised, not the namespace table).
#[tokio::test]
async fn post_index_novelty_subject_resolves_through_attached_range_provider() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/lifecycle-detached-dictnov:main";

    // t=1: index a first subject in namespace `ex`.
    let l0 = fluree.create_ledger(ledger_id).await.unwrap();
    fluree
        .insert(
            l0,
            &json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [{ "@id": "ex:s1", "ex:name": "v1" }]
            }),
        )
        .await
        .unwrap();
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex at t=1");

    // Load the indexed state with the binary store + range provider attached
    // (the provider `Arc::clone`s `dict_novelty` at this point).
    let handle = fluree.ledger_cached(ledger_id).await.unwrap();
    let l_indexed = handle.snapshot().await.to_ledger_state();
    assert!(
        l_indexed.snapshot.range_provider.is_some(),
        "expected a binary range provider after reindex"
    );
    assert!(
        l_indexed.binary_store.is_some(),
        "expected a binary store after reindex"
    );

    // t=2: commit a NEW subject in the SAME namespace on top of the indexed
    // state. This is post-index novelty — `ex:s2`'s subject id lives only in
    // `dict_novelty`, and applying the commit mutates it via `Arc::make_mut`.
    let l2 = fluree
        .insert(
            l_indexed,
            &json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [{ "@id": "ex:s2", "ex:name": "v2" }]
            }),
        )
        .await
        .unwrap()
        .ledger;

    // Bound-subject query for the post-index novel subject. This drives the
    // binary-scan overlay path, which resolves `ex:s2` via `dict_novelty`. A
    // detached provider copy would miss it and return `[]`.
    let rows = support::query_sparql(
        &fluree,
        &l2,
        "PREFIX ex: <http://example.org/> SELECT ?v WHERE { ex:s2 ex:name ?v }",
    )
    .await
    .unwrap()
    .to_jsonld(&l2.snapshot)
    .unwrap();
    assert_eq!(
        rows,
        json!([["v2"]]),
        "post-index novel subject must resolve via the attached range provider \
         (detached dict_novelty would return [])"
    );

    // Sanity: the persisted (pre-index) subject still resolves too.
    let rows1 = support::query_sparql(
        &fluree,
        &l2,
        "PREFIX ex: <http://example.org/> SELECT ?v WHERE { ex:s1 ex:name ?v }",
    )
    .await
    .unwrap()
    .to_jsonld(&l2.snapshot)
    .unwrap();
    assert_eq!(rows1, json!([["v1"]]));
}

/// Query-during-refresh must never observe torn state.
///
/// Three concurrent actors over the same on-disk storage: a writer (connection
/// A) advancing the durable head, a refresher (connection B) repeatedly pulling
/// its cache up to the head, and a querier (connection B) reading concurrently.
/// The refresher and querier contend on B's `LedgerManager` lock. The invariant:
/// every atomically-taken snapshot is internally coherent — its item count
/// equals its own `t` (each commit adds exactly one item) — even while a refresh
/// is mid-flight swapping B's cached state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_during_refresh_never_sees_torn_state() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let ledger_id = "it/lifecycle-query-during-refresh:main";

    let fa = FlureeBuilder::file(path.clone())
        .build()
        .expect("build A (writer)");
    let fb = FlureeBuilder::file(path.clone())
        .build()
        .expect("build B (reader)");

    // Genesis + t=1 via A so B has something to cache.
    let l0 = fa.create_ledger(ledger_id).await.expect("create");
    let mut la = fa.insert(l0, &item_tx(1)).await.expect("A t=1").ledger;
    let hb = fb.ledger_cached(ledger_id).await.expect("B cache");

    const ROUNDS: i64 = 30;

    // Writer: advance the durable head t=2..=ROUNDS, yielding between commits.
    let writer = async {
        for k in 2..=ROUNDS {
            la = fa.insert(la, &item_tx(k)).await.expect("A insert").ledger;
            tokio::task::yield_now().await;
        }
        la.t()
    };

    // Refresher: repeatedly pull B's cache up to the durable head.
    let refresher = async {
        for _ in 0..ROUNDS * 2 {
            let _ = fb.refresh(ledger_id, RefreshOpts { min_t: None }).await;
            tokio::task::yield_now().await;
        }
    };

    // Querier: read B concurrently; every atomic snapshot must be coherent.
    let querier = async {
        let mut max_seen = 0usize;
        for _ in 0..ROUNDS * 2 {
            let lb = hb.snapshot().await.to_ledger_state();
            let t = usize::try_from(lb.t()).unwrap_or(0);
            let count = item_count(&fb, &lb).await;
            assert_eq!(
                count, t,
                "torn state during refresh: snapshot at t={t} has {count} items"
            );
            max_seen = max_seen.max(count);
            tokio::task::yield_now().await;
        }
        max_seen
    };

    let (head_t, (), _max_seen) = tokio::join!(writer, refresher, querier);

    // After the race, B can catch all the way up and see every item.
    fb.refresh(
        ledger_id,
        RefreshOpts {
            min_t: Some(head_t),
        },
    )
    .await
    .expect("final catch-up refresh");
    let lb = hb.snapshot().await.to_ledger_state();
    assert_eq!(
        item_count(&fb, &lb).await,
        usize::try_from(head_t).unwrap(),
        "after catching up, B must see all {head_t} committed items"
    );
}
