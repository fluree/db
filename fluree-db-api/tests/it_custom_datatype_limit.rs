//! Regression tests: a ledger with more than 241 distinct custom datatypes
//! must still index (issue #1751).
//!
//! The indexer used to reject any datatype dictionary id above `u8::MAX`.
//! Fifteen ids are reserved for built-in datatypes, so the 242nd custom
//! datatype overflowed. The insert succeeded and every later index attempt
//! failed. The storage format itself carries datatype ids in a 14-bit
//! `OType` payload, so the 8-bit limit was never a format constraint.
//!
//! The 14-bit limit is real, so a ledger that reaches it must refuse the
//! next new datatype at write time. An insert error is recoverable. A
//! ledger that accepted data it can never index is not.

#![cfg(feature = "native")]

use crate::support::{self, genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::{
    Base64Bytes, FlureeBuilder, GovernanceOptions, IndexConfig, LedgerHandle, PushCommitsRequest,
};
use fluree_db_core::commit::codec::{read_commit, write_commit};
use fluree_db_core::{plan_commit_transfer, DatatypeDictId, RuntimeSmallDicts, Sid};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

/// Comfortably past the old boundary of 241 custom datatypes.
const N: usize = 300;

/// Custom datatypes a ledger can hold: every datatype dict id from
/// `RESERVED_COUNT` through `MAX`.
const CAPACITY: usize = (DatatypeDictId::MAX - DatatypeDictId::RESERVED_COUNT + 1) as usize;

fn ctx() -> serde_json::Value {
    json!({"ex": "http://example.org/"})
}

/// One subject per custom datatype: `ex:s{i} ex:p "v{i}"^^ex:U{i}`.
fn insert_range(range: std::ops::Range<usize>) -> serde_json::Value {
    let graph: Vec<_> = range
        .map(|i| {
            json!({
                "@id": format!("ex:s{i}"),
                "ex:p": {"@value": format!("v{i}"), "@type": format!("ex:U{i}")}
            })
        })
        .collect();
    json!({"@context": ctx(), "@graph": graph})
}

fn expected_rows(range: std::ops::Range<usize>) -> serde_json::Value {
    let rows: Vec<_> = range
        .map(|i| {
            json!([
                format!("ex:s{i}"),
                {"@value": format!("v{i}"), "@type": format!("ex:U{i}")}
            ])
        })
        .collect();
    json!(rows)
}

async fn all_rows(fluree: &MemoryFluree, ledger: &MemoryLedger) -> serde_json::Value {
    let q = json!({
        "@context": ctx(),
        "select": ["?s", "?v"],
        "where": {"@id": "?s", "ex:p": "?v"}
    });
    support::query_jsonld(fluree, ledger, &q)
        .await
        .expect("query should succeed")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld")
}

async fn load_indexed(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let indexed = fluree.ledger(ledger_id).await.expect("load indexed ledger");
    assert!(
        indexed.snapshot.range_provider.is_some(),
        "expected binary range provider after indexing"
    );
    indexed
}

/// Full rebuild with 300 custom datatypes, read back through JSON-LD.
#[tokio::test]
async fn full_rebuild_past_u8_custom_datatype_limit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:rebuild";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    fluree
        .insert(ledger0, &insert_range(0..N))
        .await
        .expect("insert");

    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let indexed = load_indexed(&fluree, ledger_id).await;

    let rows = all_rows(&fluree, &indexed).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&expected_rows(0..N)),
        "every custom-typed literal must round-trip through the index"
    );

    // A bound object whose datatype id is above the old u8 limit must match
    // exactly its own subject.
    let q = json!({
        "@context": ctx(),
        "select": ["?s"],
        "where": {"@id": "?s", "ex:p": {"@value": "v299", "@type": "ex:U299"}}
    });
    let bound = support::query_jsonld(&fluree, &indexed, &q)
        .await
        .expect("bound query should succeed")
        .to_jsonld(&indexed.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&bound),
        normalize_rows(&json!([["ex:s299"]])),
        "bound custom-typed object must match one subject, got {bound}"
    );
}

/// SPARQL twin of the full-rebuild test.
#[tokio::test]
async fn full_rebuild_past_u8_custom_datatype_limit_sparql() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:sparql";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    fluree
        .insert(ledger0, &insert_range(0..N))
        .await
        .expect("insert");

    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let indexed = load_indexed(&fluree, ledger_id).await;

    let count_q = r"
        PREFIX ex: <http://example.org/>
        SELECT (COUNT(DISTINCT ?dt) AS ?n)
        WHERE { ?s ex:p ?v . BIND(DATATYPE(?v) AS ?dt) }
    ";
    let rows = support::query_sparql(&fluree, &indexed, count_q)
        .await
        .expect("sparql count should succeed")
        .to_jsonld(&indexed.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([[N]])),
        "every custom datatype must survive indexing, got {rows}"
    );

    let bound_q = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s
        WHERE { ?s ex:p "v299"^^ex:U299 }
    "#;
    let rows = support::query_sparql(&fluree, &indexed, bound_q)
        .await
        .expect("sparql bound query should succeed")
        .to_jsonld(&indexed.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([["ex:s299"]])),
        "bound custom-typed object must match one subject, got {rows}"
    );
}

/// Cross the old boundary on the second index cycle, so the overflow happens
/// in incremental indexing rather than in a full rebuild.
#[tokio::test]
async fn incremental_index_crosses_u8_custom_datatype_limit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:incremental";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let ledger1 = fluree
        .insert(ledger0, &insert_range(0..200))
        .await
        .expect("first insert")
        .ledger;
    support::rebuild_and_publish_index(&fluree, ledger_id).await;

    fluree
        .insert(ledger1, &insert_range(200..N))
        .await
        .expect("second insert");
    support::build_and_publish_index(&fluree, ledger_id).await;
    let indexed = load_indexed(&fluree, ledger_id).await;

    let rows = all_rows(&fluree, &indexed).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&expected_rows(0..N)),
        "custom-typed literals from both index cycles must round-trip"
    );
}

/// Assert that a write was refused for exceeding the datatype limit.
fn assert_datatype_limit_rejection<T, E: std::fmt::Display>(result: Result<T, E>, what: &str) {
    match result {
        Ok(_) => panic!("{what}: a write past the datatype limit was accepted"),
        Err(e) => assert!(
            e.to_string().contains("datatype limit"),
            "{what}: expected a datatype limit error, got: {e}"
        ),
    }
}

/// A new subject typed with a datatype the ledger already holds.
fn insert_known_datatype() -> serde_json::Value {
    json!({
        "@context": ctx(),
        "@id": "ex:known",
        "ex:p": {"@value": "known", "@type": "ex:U0"}
    })
}

/// The ledger is full and every existing datatype is still in novelty.
#[tokio::test]
async fn insert_past_datatype_limit_is_rejected_from_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:reject-novelty";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let full = fluree
        .insert(ledger0, &insert_range(0..CAPACITY))
        .await
        .expect("filling the datatype dictionary exactly is allowed")
        .ledger;

    assert_datatype_limit_rejection(
        fluree
            .insert(full.clone(), &insert_range(CAPACITY..CAPACITY + 1))
            .await,
        "new datatype over novelty",
    );
    fluree
        .insert(full, &insert_known_datatype())
        .await
        .expect("a datatype the ledger already holds is still accepted");

    support::rebuild_and_publish_index(&fluree, ledger_id).await;
}

/// Every existing datatype but the last is in the index. The last free ID
/// is still granted, so the index's datatypes are counted exactly, not
/// over-counted.
#[tokio::test]
async fn insert_past_datatype_limit_is_rejected_from_index() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:reject-indexed";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    fluree
        .insert(ledger0, &insert_range(0..CAPACITY - 1))
        .await
        .expect("filling all but one datatype id is allowed");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let indexed = load_indexed(&fluree, ledger_id).await;
    let full = fluree
        .insert(indexed, &insert_range(CAPACITY - 1..CAPACITY))
        .await
        .expect("the last datatype id is granted over the index")
        .ledger;

    assert_datatype_limit_rejection(
        fluree
            .insert(full.clone(), &insert_range(CAPACITY..CAPACITY + 1))
            .await,
        "new datatype over the index",
    );
    fluree
        .insert(full, &insert_known_datatype())
        .await
        .expect("a datatype the ledger already holds is still accepted");

    support::build_and_publish_index(&fluree, ledger_id).await;
}

/// The ledger is full in its index, but the state's runtime dictionary was
/// never seeded from that index. The limit still counts the indexed
/// datatypes rather than trusting the dictionary.
#[tokio::test]
async fn datatype_limit_counts_index_when_runtime_dict_is_unseeded() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:reject-unseeded";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    fluree
        .insert(ledger0, &insert_range(0..CAPACITY))
        .await
        .expect("filling the datatype dictionary exactly is allowed");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let mut unseeded = load_indexed(&fluree, ledger_id).await;
    unseeded.runtime_small_dicts = Arc::new(RuntimeSmallDicts::new());

    assert_datatype_limit_rejection(
        fluree
            .insert(unseeded.clone(), &insert_range(CAPACITY..CAPACITY + 1))
            .await,
        "new datatype over an unseeded runtime dictionary",
    );
    fluree
        .insert(unseeded, &insert_known_datatype())
        .await
        .expect("a datatype only the index holds is still known");
}

async fn sparql_update(
    fluree: &MemoryFluree,
    handle: &LedgerHandle,
    body: &str,
) -> fluree_db_api::Result<()> {
    fluree
        .stage(handle)
        .sparql_update(&format!("PREFIX ex: <http://example.org/> {body}"))
        .execute()
        .await
        .map(|_| ())
}

/// SPARQL twin of the insert rejection tests. A new datatype is refused
/// whether the update states it as data or computes it in a binding.
#[tokio::test]
async fn sparql_update_past_datatype_limit_is_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:reject-sparql";
    fluree.create_ledger(ledger_id).await.expect("create");
    let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
    fluree
        .stage(&handle)
        .insert(&insert_range(0..CAPACITY))
        .execute()
        .await
        .expect("filling the datatype dictionary exactly is allowed");

    assert_datatype_limit_rejection(
        sparql_update(
            &fluree,
            &handle,
            &format!(r#"INSERT DATA {{ ex:new ex:p "x"^^ex:U{CAPACITY} }}"#),
        )
        .await,
        "INSERT DATA with a new datatype",
    );
    assert_datatype_limit_rejection(
        sparql_update(
            &fluree,
            &handle,
            &format!(
                r#"INSERT {{ ex:derived ex:p ?v }}
                   WHERE {{ ex:s0 ex:p ?o BIND(STRDT("y", ex:U{CAPACITY}) AS ?v) }}"#
            ),
        )
        .await,
        "INSERT WHERE computing a new datatype",
    );

    sparql_update(
        &fluree,
        &handle,
        r#"INSERT DATA { ex:known ex:p "known"^^ex:U0 }"#,
    )
    .await
    .expect("INSERT DATA with a datatype the ledger holds is accepted");
    sparql_update(
        &fluree,
        &handle,
        r#"INSERT { ex:derived ex:p ?v }
           WHERE { ex:s0 ex:p ?o BIND(STRDT("y", ex:U1) AS ?v) }"#,
    )
    .await
    .expect("INSERT WHERE computing a datatype the ledger holds is accepted");
    assert_eq!(
        handle.t().await,
        3,
        "only the fill and the two known-datatype updates committed"
    );
}

/// A single transaction that brings more new datatypes than the ledger has
/// room for.
#[tokio::test]
async fn single_insert_past_datatype_limit_is_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "custom-dt-limit:reject-single");
    assert_datatype_limit_rejection(
        fluree.insert(ledger0, &insert_range(0..CAPACITY + 1)).await,
        "one transaction over the limit",
    );
}

/// Two writes on disjoint subjects race for the last free datatype id. The
/// one that commits second is staged before the first commits, so a check
/// made only at stage time would pass both. It must be refused instead of
/// being re-based over the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_inserts_cannot_jointly_pass_datatype_limit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:reject-concurrent";
    fluree.create_ledger(ledger_id).await.expect("create");
    let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
    fluree
        .stage(&handle)
        .insert(&insert_range(0..CAPACITY - 1))
        .execute()
        .await
        .expect("filling all but one datatype id is allowed");

    let (parked, release) = handle.gate_next_optimistic_stage_for_test();
    let behind = {
        let fluree = fluree.clone();
        let handle = handle.clone();
        tokio::spawn(async move {
            fluree
                .stage(&handle)
                .insert(&insert_range(CAPACITY..CAPACITY + 1))
                .execute()
                .await
                .map(|_| ())
        })
    };
    parked.await.expect("the write parks after staging");

    fluree
        .stage(&handle)
        .insert(&insert_range(CAPACITY - 1..CAPACITY))
        .execute()
        .await
        .expect("the write that takes the last datatype id commits");
    release.send(true).unwrap();
    assert_datatype_limit_rejection(
        behind.await.expect("task"),
        "write staged before the last id was taken",
    );

    support::rebuild_and_publish_index(&fluree, ledger_id).await;
}

/// A pushed commit, with the txn blob it references.
struct PushedCommit {
    bytes: Vec<u8>,
    txn_blob: Option<(String, Vec<u8>)>,
}

/// The commits on `ledger_id`'s line, oldest first.
async fn line_commits(fluree: &MemoryFluree, ledger_id: &str) -> Vec<PushedCommit> {
    let store = fluree.branched_content_store(ledger_id).await.unwrap();
    let head = fluree
        .ledger(ledger_id)
        .await
        .unwrap()
        .head_commit_id
        .expect("ledger has a head");
    let plan = plan_commit_transfer(store.as_ref(), &head, None)
        .await
        .unwrap()
        .expect("a full line");
    let mut out = Vec::new();
    for cid in &plan.lineage {
        let bytes = store.get(cid).await.unwrap();
        let txn_blob = match read_commit(&bytes).unwrap().txn {
            Some(txn) => Some((txn.to_string(), store.get(&txn).await.unwrap())),
            None => None,
        };
        out.push(PushedCommit { bytes, txn_blob });
    }
    out
}

async fn push(
    fluree: &MemoryFluree,
    ledger_id: &str,
    commits: &[&PushedCommit],
) -> fluree_db_api::Result<()> {
    let request = PushCommitsRequest {
        commits: commits
            .iter()
            .map(|c| Base64Bytes(c.bytes.clone()))
            .collect(),
        blobs: commits
            .iter()
            .filter_map(|c| c.txn_blob.clone())
            .map(|(cid, bytes)| (cid, Base64Bytes(bytes)))
            .collect::<HashMap<_, _>>(),
        missing_blobs: Vec::new(),
        merged_commits: Vec::new(),
    };
    let index_config = IndexConfig {
        reindex_min_bytes: 100_000,
        reindex_max_bytes: 1_000_000_000,
    };
    fluree
        .push_commits(
            ledger_id,
            request,
            &GovernanceOptions::default(),
            &index_config,
        )
        .await
        .map(|_| ())
}

/// A sender that does not enforce the datatype limit can push commits past
/// it. The receiver refuses them, whether the datatypes they collide with
/// arrived in the same push or are already in the ledger.
#[tokio::test]
async fn push_past_datatype_limit_is_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let sender = "custom-dt-limit:push-sender";
    let ledger0 = genesis_ledger(&fluree, sender);
    let full = fluree
        .insert(ledger0, &insert_range(0..CAPACITY))
        .await
        .expect("filling the datatype dictionary exactly is allowed")
        .ledger;
    fluree
        .insert(full, &insert_known_datatype())
        .await
        .expect("a datatype the ledger already holds is accepted");
    let [fill, known]: [PushedCommit; 2] = line_commits(&fluree, sender)
        .await
        .try_into()
        .unwrap_or_else(|_| panic!("the sender has two commits"));

    // What an older client could send: the second commit, retyped with a
    // datatype the ledger has no room for.
    let mut commit = read_commit(&known.bytes).unwrap();
    for flake in &mut commit.flakes {
        if flake.dt.name_str() == "U0" {
            flake.dt = Sid::new(flake.dt.namespace_code, format!("U{CAPACITY}"));
        }
    }
    let over = PushedCommit {
        bytes: write_commit(&commit, false, None).unwrap().bytes,
        txn_blob: known.txn_blob.clone(),
    };

    let control = "custom-dt-limit:push-control";
    fluree.create_ledger(control).await.unwrap();
    push(&fluree, control, &[&fill, &known])
        .await
        .expect("the sender's own commits are accepted");

    let same_push = "custom-dt-limit:push-same";
    fluree.create_ledger(same_push).await.unwrap();
    assert_datatype_limit_rejection(
        push(&fluree, same_push, &[&fill, &over]).await,
        "datatypes filled earlier in the same push",
    );

    let later_push = "custom-dt-limit:push-later";
    fluree.create_ledger(later_push).await.unwrap();
    push(&fluree, later_push, &[&fill])
        .await
        .expect("filling the datatype dictionary exactly is allowed");
    // Through the cached state push updated, as the server's writes are.
    let handle = fluree.ledger_cached(later_push).await.unwrap();
    assert_datatype_limit_rejection(
        fluree
            .stage(&handle)
            .insert(&insert_range(CAPACITY..CAPACITY + 1))
            .execute()
            .await,
        "a transaction over pushed datatypes",
    );
    assert_datatype_limit_rejection(
        push(&fluree, later_push, &[&over]).await,
        "datatypes already in the ledger",
    );
}

/// Import `count` distinct custom datatypes, `ex:s{i} ex:p "v{i}"^^ex:U{i}`,
/// into a new file-backed ledger.
async fn import_datatypes(
    fluree: &fluree_db_api::Fluree,
    data_dir: &std::path::Path,
    ledger_id: &str,
    count: usize,
) -> Result<(), String> {
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    for i in 0..count {
        ttl.push_str(&format!("ex:s{i} ex:p \"v{i}\"^^ex:U{i} .\n"));
    }
    let path = data_dir.join(format!("{count}.ttl"));
    std::fs::write(&path, ttl).unwrap();
    fluree
        .create(ledger_id)
        .import(&path)
        .threads(2)
        .memory_budget_mb(256)
        .execute()
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Bulk import assigns datatype IDs directly, not through a commit, so it
/// enforces the limit itself.
#[tokio::test]
async fn bulk_import_past_datatype_limit_is_rejected() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    assert_datatype_limit_rejection(
        import_datatypes(
            &fluree,
            data_dir.path(),
            "custom-dt-limit:import-over",
            CAPACITY + 1,
        )
        .await,
        "import over the limit",
    );

    let ledger_id = "custom-dt-limit:import-full";
    import_datatypes(&fluree, data_dir.path(), ledger_id, CAPACITY)
        .await
        .expect("importing exactly as many datatypes as fit is allowed");
    let imported = load_indexed(&fluree, ledger_id).await;
    let last = CAPACITY - 1;
    let q = json!({
        "@context": ctx(),
        "select": ["?v"],
        "where": {"@id": format!("ex:s{last}"), "ex:p": "?v"}
    });
    let rows = support::query_jsonld(&fluree, &imported, &q)
        .await
        .expect("query should succeed")
        .to_jsonld(&imported.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([[{"@value": format!("v{last}"), "@type": format!("ex:U{last}")}]])),
        "the last datatype id round-trips through the imported index"
    );

    assert_datatype_limit_rejection(
        fluree
            .insert(imported, &insert_range(CAPACITY..CAPACITY + 1))
            .await,
        "a transaction over imported datatypes",
    );
}
