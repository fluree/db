//! Conflict retractions in a named graph read index-resident values.
//!
//! A take-source merge, a take-source revert and a take-branch rebase each
//! retract the losing side's current values on a conflicting `(subject,
//! predicate, graph)`. They read those values with a range read that
//! returns index-resident rows without their graph, then kept only the rows
//! whose graph equalled the key's. In a named graph that dropped every
//! indexed value, so the losing value survived next to the winning one
//! (novelty-resident values were read correctly). The read now goes through
//! the retraction resolver, which stamps the graph.

use crate::support;
use fluree_db_api::{CommitRef, ConflictStrategy, FlureeBuilder, MergePreviewOpts};
use serde_json::{json, Value as JsonValue};

const G1: &str = "http://example.org/g1";

fn set_value(v: &str) -> JsonValue {
    json!({
        "@context": {"ex": "http://example.org/"},
        "graph": G1,
        "where": {"@id": "ex:s", "ex:p": "?o"},
        "delete": {"@id": "ex:s", "ex:p": "?o"},
        "insert": {"@id": "ex:s", "ex:p": v}
    })
}

async fn values(fluree: &support::MemoryFluree, ledger_id: &str) -> Vec<String> {
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let q = format!(
        "SELECT ?o WHERE {{ GRAPH <{G1}> {{ <http://example.org/s> <http://example.org/p> ?o }} }}"
    );
    let r = support::query_sparql(fluree, &ledger, &q)
        .await
        .expect("query");
    let mut out: Vec<String> = r
        .to_jsonld(&ledger.snapshot)
        .expect("jsonld")
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| {
            let cell = row.as_array().and_then(|r| r.first()).unwrap_or(row);
            cell.as_str()
                .map(str::to_string)
                .unwrap_or_else(|| cell.to_string())
        })
        .collect();
    out.sort();
    out
}

async fn seed_graph(fluree: &support::MemoryFluree, ledger: fluree_db_api::LedgerState) {
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [{"@id": "ex:s", "@graph": G1, "ex:p": "orig"}]
            }),
        )
        .await
        .expect("seed");
}

/// Take-source merge: the target's value is retracted whether it is in
/// novelty or in the index, and the preview shows it either way.
#[tokio::test]
async fn take_source_merge_replaces_the_targets_named_graph_value() {
    for indexed in [false, true] {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("mydb").await.unwrap();
        seed_graph(&fluree, ledger).await;
        fluree
            .create_branch("mydb", "dev", None, None)
            .await
            .unwrap();
        let dev = fluree.ledger("mydb:dev").await.unwrap();
        fluree.update(dev, &set_value("frombranch")).await.unwrap();
        let main = fluree.ledger("mydb:main").await.unwrap();
        fluree.update(main, &set_value("frommain")).await.unwrap();
        if indexed {
            support::rebuild_and_publish_index(&fluree, "mydb:main").await;
        }

        let preview = fluree
            .merge_preview_with(
                "mydb",
                "dev",
                None,
                MergePreviewOpts {
                    include_conflict_details: true,
                    conflict_strategy: ConflictStrategy::TakeSource,
                    ..MergePreviewOpts::default()
                },
            )
            .await
            .unwrap();
        let detail = preview.conflicts.details.first().expect("one conflict");
        assert_eq!(
            detail.target_values.len(),
            1,
            "indexed={indexed}: the preview must show the target's value"
        );

        fluree
            .merge_branch("mydb", "dev", None, ConflictStrategy::TakeSource)
            .await
            .unwrap();
        assert_eq!(
            values(&fluree, "mydb:main").await,
            ["frombranch"],
            "indexed={indexed}"
        );
    }
}

/// Take-source revert: reverting the commit that wrote the value also
/// retracts the value HEAD holds now.
#[tokio::test]
async fn take_source_revert_retracts_the_indexed_head_value() {
    for indexed in [false, true] {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("mydb").await.unwrap();
        let r1 = fluree
            .insert(
                ledger,
                &json!({
                    "@context": {"ex": "http://example.org/"},
                    "@graph": [{"@id": "ex:anchor", "ex:name": "anchor"}]
                }),
            )
            .await
            .unwrap();
        let r2 = fluree
            .insert(
                r1.ledger,
                &json!({
                    "@context": {"ex": "http://example.org/"},
                    "@graph": [{"@id": "ex:s", "@graph": G1, "ex:p": "v1"}]
                }),
            )
            .await
            .unwrap();
        let v1_commit = r2.receipt.commit_id.clone();
        fluree.update(r2.ledger, &set_value("v2")).await.unwrap();
        if indexed {
            support::rebuild_and_publish_index(&fluree, "mydb:main").await;
        }
        fluree
            .revert_commits(
                "mydb",
                "main",
                vec![CommitRef::Exact(v1_commit)],
                ConflictStrategy::TakeSource,
            )
            .await
            .unwrap();
        assert!(
            values(&fluree, "mydb:main").await.is_empty(),
            "indexed={indexed}: the revert wins, so v2 goes too"
        );
    }
}

/// Take-branch rebase: the branch's value wins and the source's value,
/// index-resident on the source, is retracted.
#[tokio::test]
async fn take_branch_rebase_retracts_the_sources_indexed_value() {
    for indexed in [false, true] {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("mydb").await.unwrap();
        seed_graph(&fluree, ledger).await;
        fluree
            .create_branch("mydb", "dev", None, None)
            .await
            .unwrap();
        let dev = fluree.ledger("mydb:dev").await.unwrap();
        fluree.update(dev, &set_value("dev-val")).await.unwrap();
        let main = fluree.ledger("mydb:main").await.unwrap();
        fluree.update(main, &set_value("main-val")).await.unwrap();
        if indexed {
            support::rebuild_and_publish_index(&fluree, "mydb:main").await;
        }
        fluree
            .rebase_branch("mydb", "dev", ConflictStrategy::TakeBranch)
            .await
            .unwrap();
        assert_eq!(
            values(&fluree, "mydb:dev").await,
            ["dev-val"],
            "indexed={indexed}"
        );
    }
}
