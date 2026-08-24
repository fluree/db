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
