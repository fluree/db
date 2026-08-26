//! Graph-sync integration tests.
//!
//! `sync_named_graph` makes a named graph's contents exactly the payload,
//! committing only the delta: `current − payload` retracted, `payload −
//! current` asserted, unchanged facts untouched. An identical payload
//! produces no commit.

#![cfg(feature = "native")]

use crate::support::genesis_ledger;
use fluree_db_api::{FlureeBuilder, SyncGraphOpts};
use serde_json::{json, Value as JsonValue};

const ONT_IRI: &str = "http://example.org/graphs/ontology";
const OTHER_IRI: &str = "http://example.org/graphs/other";

fn payload_v1() -> JsonValue {
    json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:alice", "ex:name": "Alice", "ex:role": "engineer" },
            { "@id": "ex:bob", "ex:name": "Bob" }
        ]
    })
}

/// v2 = v1 with alice's role changed and bob's name dropped, carol added.
fn payload_v2() -> JsonValue {
    json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:alice", "ex:name": "Alice", "ex:role": "manager" },
            { "@id": "ex:carol", "ex:name": "Carol" }
        ]
    })
}

/// Seed a ledger with one default-graph triple and one triple in OTHER_IRI.
async fn seed(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> i64 {
    let ledger = genesis_ledger(fluree, ledger_id);
    let trig = format!(
        r#"
        @prefix ex: <http://example.org/> .
        ex:default-subject ex:p "default-graph-value" .
        GRAPH <{OTHER_IRI}> {{
            ex:zed ex:name "Zed" .
        }}
        "#,
    );
    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed insert");
    result.receipt.t
}

async fn rows_in_graph(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    graph_iri: Option<&str>,
) -> Vec<JsonValue> {
    let from = match graph_iri {
        Some(iri) => format!("{ledger_id}#{iri}"),
        None => ledger_id.to_string(),
    };
    let q = json!({
        "from": from,
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"}
    });
    let result = fluree.query_connection(&q).await.expect("query connection");
    let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
    let rows = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    rows.as_array().cloned().unwrap_or_default()
}

async fn count_in_graph(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    graph_iri: Option<&str>,
) -> usize {
    rows_in_graph(fluree, ledger_id, graph_iri).await.len()
}

/// `(default, ONT_IRI, OTHER_IRI)` row counts.
async fn graph_counts(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> (usize, usize, usize) {
    (
        count_in_graph(fluree, ledger_id, None).await,
        count_in_graph(fluree, ledger_id, Some(ONT_IRI)).await,
        count_in_graph(fluree, ledger_id, Some(OTHER_IRI)).await,
    )
}

#[tokio::test]
async fn first_sync_populates_a_new_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/first:main";
    let seed_t = seed(&fluree, ledger_id).await;

    let report = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect("first sync");

    assert_eq!(report.asserted, 3, "three payload triples asserted");
    assert_eq!(report.retracted, 0, "nothing to retract in a new graph");
    assert!(report.committed);
    assert_eq!(report.t, seed_t + 1);
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(ONT_IRI)).await, 3);
}

#[tokio::test]
async fn identical_resync_is_a_noop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/noop:main";
    seed(&fluree, ledger_id).await;

    let first = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect("first sync");
    assert!(first.committed);

    let second = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect("identical resync");
    assert_eq!(second.asserted, 0, "identical payload asserts nothing");
    assert_eq!(second.retracted, 0, "identical payload retracts nothing");
    assert!(
        !second.committed,
        "identical payload must not create a commit"
    );
    assert_eq!(second.t, first.t, "head t unchanged on a no-op sync");
}

#[tokio::test]
async fn delta_sync_commits_only_the_delta() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/delta:main";
    seed(&fluree, ledger_id).await;

    let first = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect("first sync");

    let second = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v2(), SyncGraphOpts::default())
        .await
        .expect("delta sync");

    // v1 → v2: alice role engineer→manager (1 retract + 1 assert), bob's
    // name + node removed (1 retract), carol added (1 assert). Alice's
    // unchanged ex:name must NOT appear in the commit.
    assert_eq!(second.asserted, 2, "role change + carol");
    assert_eq!(second.retracted, 2, "old role + bob");
    assert!(second.committed);
    assert_eq!(second.t, first.t + 1, "one commit for the whole delta");

    // The graph now equals payload v2 exactly.
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(ONT_IRI)).await, 3);
}

#[tokio::test]
async fn sync_does_not_touch_other_graphs() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/scoped:main";
    seed(&fluree, ledger_id).await;

    fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect("sync");
    fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v2(), SyncGraphOpts::default())
        .await
        .expect("delta sync");

    assert_eq!(count_in_graph(&fluree, ledger_id, Some(OTHER_IRI)).await, 1);
    assert_eq!(count_in_graph(&fluree, ledger_id, None).await, 1);
}

#[tokio::test]
async fn empty_payload_requires_allow_empty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/empty:main";
    seed(&fluree, ledger_id).await;
    fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect("sync");

    let empty = json!({ "@graph": [] });

    let err = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &empty, SyncGraphOpts::default())
        .await
        .expect_err("empty payload without allowEmpty must be rejected");
    assert!(
        err.to_string().contains("allowEmpty"),
        "error should name the opt-in: {err}"
    );

    let report = fluree
        .sync_named_graph(
            ledger_id,
            ONT_IRI,
            &empty,
            SyncGraphOpts {
                allow_empty: true,
                ..Default::default()
            },
        )
        .await
        .expect("empty sync with allowEmpty");
    assert_eq!(report.asserted, 0);
    assert_eq!(report.retracted, 3, "clears the whole graph");
    assert!(report.committed);
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(ONT_IRI)).await, 0);
}

#[tokio::test]
async fn dry_run_reports_the_delta_without_committing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/dryrun:main";
    seed(&fluree, ledger_id).await;
    let first = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect("sync");

    let dry = fluree
        .sync_named_graph(
            ledger_id,
            ONT_IRI,
            &payload_v2(),
            SyncGraphOpts {
                dry_run: true,
                ..Default::default()
            },
        )
        .await
        .expect("dry run");
    assert!(dry.dry_run);
    assert!(!dry.committed);
    assert_eq!(dry.asserted, 2);
    assert_eq!(dry.retracted, 2);
    assert_eq!(dry.t, first.t, "dry run must not advance t");

    // Nothing changed: the graph still equals payload v1.
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(ONT_IRI)).await, 3);

    // The real run matches the dry run's numbers.
    let real = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v2(), SyncGraphOpts::default())
        .await
        .expect("real run");
    assert_eq!(
        (real.asserted, real.retracted),
        (dry.asserted, dry.retracted)
    );
    assert!(real.committed);
}

#[tokio::test]
async fn blank_node_payload_resyncs_as_noop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/bnode:main";
    seed(&fluree, ledger_id).await;

    // An OWL-restriction-shaped payload: bnode-rooted structure under a
    // stable label. Sync skolemizes with a deterministic graph-scoped key,
    // so an identical resync must be a no-op.
    let payload = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            {
                "@id": "ex:Widget",
                "ex:restriction": {
                    "@id": "_:r1",
                    "ex:onProperty": { "@id": "ex:hasPart" },
                    "ex:minCount": 1
                }
            }
        ]
    });

    let first = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload, SyncGraphOpts::default())
        .await
        .expect("first sync");
    assert!(first.committed);

    let second = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload, SyncGraphOpts::default())
        .await
        .expect("resync");
    assert!(
        !second.committed,
        "stable-label bnode payload must not churn: {second:?}"
    );
}

#[tokio::test]
async fn payload_addressing_named_graphs_is_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/nested:main";
    seed(&fluree, ledger_id).await;

    let nested = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            {
                "@id": "http://example.org/graphs/inner",
                "@graph": [ { "@id": "ex:x", "ex:p": "v" } ]
            }
        ]
    });
    // The insert-shaped JSON-LD parse has no named-graph selector form, so
    // the nested-graph document fails parsing; if a future parser learns
    // one, `parse_sync_transaction`'s graph_delta guard rejects it with
    // "must not address named graphs". Either way: an error, no commit.
    fluree
        .sync_named_graph(ledger_id, ONT_IRI, &nested, SyncGraphOpts::default())
        .await
        .expect_err("payload-internal named graphs must be rejected");
}

#[tokio::test]
async fn system_graph_targets_are_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/system:main";
    seed(&fluree, ledger_id).await;

    let txn_meta_iri = format!("urn:fluree:{ledger_id}#txn-meta");
    let err = fluree
        .sync_named_graph(
            ledger_id,
            &txn_meta_iri,
            &payload_v1(),
            SyncGraphOpts::default(),
        )
        .await
        .expect_err("txn-meta graph must be rejected");
    assert!(err.to_string().contains("txn-meta"), "got: {err}");
}

/// A policy-gated sync takes the write-locked fast path
/// (`stage_under_lock`), which previously fell through to the JSON-like
/// insert path — staging the payload into the DEFAULT graph and leaving the
/// target untouched. Raft always uses that path.
#[tokio::test]
async fn policy_gated_sync_targets_the_named_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/policy-path:main";
    seed(&fluree, ledger_id).await;
    let handle = fluree.ledger_cached(ledger_id).await.expect("handle");
    let root = || fluree_db_api::PolicyContext::new(fluree_db_api::PolicyWrapper::root(), None);

    let v1 = payload_v1();
    let first = fluree
        .stage(&handle)
        .sync_graph(ONT_IRI, &v1)
        .policy(root())
        .execute()
        .await
        .expect("policy-gated sync");
    assert_eq!(first.receipt.assert_count, 3);
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(ONT_IRI)).await, 3);
    assert_eq!(
        count_in_graph(&fluree, ledger_id, None).await,
        1,
        "nothing may leak into the default graph"
    );

    let v2 = payload_v2();
    let second = fluree
        .stage(&handle)
        .sync_graph(ONT_IRI, &v2)
        .policy(root())
        .execute()
        .await
        .expect("policy-gated delta sync");
    assert_eq!(
        (second.receipt.assert_count, second.receipt.retract_count),
        (2, 2),
        "delta semantics must hold on the locked fast path"
    );
}

/// The consensus terminal (`build_commit`) must express a no-change sync
/// as `None` rather than failing with `EmptyTransaction` (which would
/// poison the queued request under Raft).
#[tokio::test]
async fn build_commit_reports_a_no_change_sync_as_none() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/build-commit-noop:main";
    seed(&fluree, ledger_id).await;
    fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect("first sync");
    let handle = fluree.ledger_cached(ledger_id).await.expect("handle");

    let v1 = payload_v1();
    let built = fluree
        .stage(&handle)
        .sync_graph(ONT_IRI, &v1)
        .build_commit()
        .await
        .expect("build_commit must not error on a no-change sync");
    assert!(built.is_none(), "identical payload must build no commit");

    let v2 = payload_v2();
    let built = fluree
        .stage(&handle)
        .sync_graph(ONT_IRI, &v2)
        .build_commit()
        .await
        .expect("build_commit");
    let (_guard, staged) = built.expect("a delta builds a commit");
    assert_eq!(staged.commit.flakes.len(), 4, "2 asserts + 2 retracts");
}

/// Target validation lives at staging, so every entry point (builder,
/// consensus applier, HTTP) meets it: malformed IRIs cannot be registered
/// as graphs, and the ledger's own system-graph IRIs are refused even when
/// the registry never seeded them.
#[tokio::test]
async fn staging_rejects_malformed_and_system_graph_targets() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/staging-guards:main";
    seed(&fluree, ledger_id).await;
    let handle = fluree.ledger_cached(ledger_id).await.expect("handle");
    let v1 = payload_v1();

    for bad in [
        "graphs/relative",
        "",
        "http://x.org/has space",
        "1abc:scheme",
    ] {
        let err = fluree
            .stage(&handle)
            .sync_graph(bad, &v1)
            .execute()
            .await
            .expect_err("malformed target must be rejected at staging");
        assert!(
            err.to_string().contains("sync target"),
            "unexpected error for {bad:?}: {err}"
        );
    }
    let config_iri = format!("urn:fluree:{ledger_id}#config");
    let err = fluree
        .stage(&handle)
        .sync_graph(&config_iri, &v1)
        .execute()
        .await
        .expect_err("system graph target must be rejected at staging");
    assert!(err.to_string().contains("reserved"), "got: {err}");
    assert_eq!(count_in_graph(&fluree, ledger_id, None).await, 1);
}

/// An identical resync must still be a no-op after the graph has been
/// indexed — the case the memory-backed tests cannot reach.
///
/// `scan_graph_flakes` reads through the range provider, and
/// `BinaryRangeProvider` materializes flakes with `g: None`
/// (`binary_range.rs:753`, `:1393`). The accumulator buckets on `flake.g`,
/// so without the explicit stamp in the sync wave the retractions land in a
/// different bucket than the payload's assertions, nothing cancels, and an
/// unchanged graph is retracted and re-asserted in full. Novelty-resident
/// flakes carry `g` already, which is why every other test here passes
/// either way.
#[tokio::test]
async fn identical_resync_is_a_noop_against_indexed_data() {
    use crate::support::{start_background_indexer_local, trigger_index_and_wait_outcome};

    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/sync-graph/indexed:main";
            fluree
                .create_ledger(ledger_id)
                .await
                .expect("create ledger");

            let first = fluree
                .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
                .await
                .expect("first sync");
            assert_eq!(first.asserted, 3);
            assert!(first.committed);

            // Push the graph into the persisted index, so the sync wave's
            // scan is served by the range provider rather than novelty.
            trigger_index_and_wait_outcome(&handle, ledger_id, first.t).await;
            let ledger = fluree.ledger(ledger_id).await.expect("reload ledger");
            assert!(
                ledger.snapshot.range_provider.is_some(),
                "graph must be index-resident for this test to mean anything"
            );

            let second = fluree
                .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
                .await
                .expect("resync after index");
            assert_eq!(second.retracted, 0, "indexed rows must still cancel");
            assert_eq!(second.asserted, 0, "identical payload asserts nothing");
            assert!(
                !second.committed,
                "identical resync of an indexed graph must not commit"
            );
            assert_eq!(second.t, first.t, "head t unchanged");

            // And a real delta over indexed data still commits only the delta.
            let third = fluree
                .sync_named_graph(ledger_id, ONT_IRI, &payload_v2(), SyncGraphOpts::default())
                .await
                .expect("delta sync after index");
            assert_eq!(third.retracted, 2, "bob's name and alice's old role");
            assert_eq!(third.asserted, 2, "alice's new role and carol's name");
            assert!(third.committed);
            assert_eq!(count_in_graph(&fluree, ledger_id, Some(ONT_IRI)).await, 3);
        })
        .await;
}

/// The whole-graph memory backstop: staging materializes the target
/// graph's current flakes, so a graph past the cap must fail loud (an
/// OOM kill otherwise, with no guard in between — `NoveltyWouldExceed`
/// only ever sees the netted delta). The limit is read per scan, so the
/// env changes below take effect immediately; nextest's process-per-test
/// isolation keeps them from leaking into other tests.
#[tokio::test]
async fn whole_graph_scan_backstop_fails_loud_before_materializing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/backstop:main";
    seed(&fluree, ledger_id).await;

    std::env::set_var("FLUREE_MAX_GRAPH_SCAN_FLAKES", "2");
    // First sync scans an EMPTY graph (0 <= 2): the cap bounds the scan,
    // not the payload, so populating past the cap succeeds.
    let report = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect("first sync scans an empty graph");
    assert_eq!(report.asserted, 3);

    // Now the graph holds 3 > 2: every whole-graph verb refuses.
    let err = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect_err("resync over the cap must fail loud");
    assert!(
        err.to_string().contains("FLUREE_MAX_GRAPH_SCAN_FLAKES"),
        "error names the knob: {err}"
    );

    // Dry run takes the same scan and must fail the same way.
    let dry = fluree
        .sync_named_graph(
            ledger_id,
            ONT_IRI,
            &payload_v1(),
            SyncGraphOpts {
                dry_run: true,
                ..Default::default()
            },
        )
        .await
        .expect_err("dry run must fail the way the real run would");
    assert!(dry.to_string().contains("FLUREE_MAX_GRAPH_SCAN_FLAKES"));

    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let clear = fluree
        .stage_owned(ledger)
        .txn(fluree_db_transact::Txn::clear_graph(ONT_IRI))
        .execute()
        .await
        .expect_err("CLEAR shares the scan and the backstop");
    assert!(clear.to_string().contains("FLUREE_MAX_GRAPH_SCAN_FLAKES"));

    // 0 disables; the identical resync is a no-op again.
    std::env::set_var("FLUREE_MAX_GRAPH_SCAN_FLAKES", "0");
    let report = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect("disabled cap syncs normally");
    assert!(!report.committed);
    std::env::remove_var("FLUREE_MAX_GRAPH_SCAN_FLAKES");
}

/// CLEAR / COPY / MOVE on an index-resident named graph — the same scan sync
/// rides, and broken the same two ways on `main` before this branch: the
/// scan issued `RangeTest::Ge`, which the V3 provider rejects, and once it
/// runs, index-decoded flakes carry `g: None` and route to the default
/// graph. The receipt then reports a commit while the target is untouched
/// (CLEAR), or the destination merges instead of replacing (COPY).
#[tokio::test]
async fn graph_management_verbs_work_on_indexed_data() {
    use crate::support::{start_background_indexer_local, trigger_index_and_wait_outcome};
    use fluree_db_transact::Txn;

    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/sync-graph/gmgmt-indexed:main";
            fluree
                .create_ledger(ledger_id)
                .await
                .expect("create ledger");
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            fluree
                .stage_owned(ledger)
                .upsert_turtle(&format!(
                    r#"
                    @prefix ex: <http://example.org/> .
                    ex:d ex:name "Default" .
                    GRAPH <{ONT_IRI}> {{ ex:alice ex:name "Alice" . ex:bob ex:name "Bob" . }}
                    GRAPH <{OTHER_IRI}> {{ ex:zed ex:name "Zed" . }}
                    "#
                ))
                .execute()
                .await
                .expect("seed");
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(
                ledger.snapshot.range_provider.is_some(),
                "must be index-resident"
            );

            assert_eq!(graph_counts(&fluree, ledger_id).await, (1, 2, 1));

            // CLEAR empties exactly the target.
            let r = fluree
                .stage_owned(ledger)
                .txn(Txn::clear_graph(ONT_IRI))
                .execute()
                .await
                .expect("CLEAR on indexed graph");
            assert_eq!(r.receipt.flake_count, 2);
            assert_eq!(
                graph_counts(&fluree, ledger_id).await,
                (1, 0, 1),
                "CLEAR emptied ont only"
            );

            // COPY replaces the destination with the source.
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            fluree
                .stage_owned(ledger)
                .txn(Txn::copy_graph(OTHER_IRI, ONT_IRI))
                .execute()
                .await
                .expect("COPY on indexed graphs");
            assert_eq!(
                graph_counts(&fluree, ledger_id).await,
                (1, 1, 1),
                "COPY replaced ont with other"
            );

            // Index again so MOVE's source and destination are both index-resident.
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            fluree
                .stage_owned(ledger)
                .txn(Txn::move_graph(OTHER_IRI, ONT_IRI))
                .execute()
                .await
                .expect("MOVE on indexed graphs");
            assert_eq!(
                graph_counts(&fluree, ledger_id).await,
                (1, 1, 0),
                "MOVE emptied the source"
            );
        })
        .await;
}
