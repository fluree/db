//! A value's relative lifetime counts from when it was first written, on
//! whichever branch: a merge or a rebase that brings it onto another line
//! does not restart it, and keeps a lifetime its transaction gave it.

use fluree_db_api::{ConflictStrategy, Fluree, FlureeBuilder};
use serde_json::{json, Value as JsonValue};
use std::time::Duration;

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"})
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn insert(fluree: &Fluree, ledger_id: &str, doc: JsonValue) {
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&doc)
        .commit()
        .await
        .expect("insert");
}

fn plain(id: &str) -> JsonValue {
    json!({"@context": ctx(), "@id": id, "ex:note": "plain"})
}

/// A value with a lifetime of its own.
fn per_value() -> JsonValue {
    json!({"@context": ctx(), "@id": "ex:s", "ex:v": {"@value": "x", "@ttl": 3600}})
}

/// A value with its transaction's default lifetime.
fn transaction_default() -> JsonValue {
    json!({"@context": ctx(), "@graph": [{"@id": "ex:s", "ex:v": "x"}], "f:ttl": 3600})
}

/// The branch's earliest deadline, after a pass discovers its commits.
async fn next_due(fluree: &Fluree, ledger_id: &str) -> i64 {
    let report = fluree
        .run_retention_at(ledger_id, now_ms())
        .await
        .expect("pass");
    assert_eq!(report.pending, 1, "{ledger_id}: {report:?}");
    fluree
        .retention_status(ledger_id)
        .await
        .expect("status")
        .registration
        .and_then(|r| r.next_due_ms)
        .expect("a deadline")
}

/// `name` with a `dev` branch that wrote `doc`, and a `main` that moved on
/// since, so neither a merge nor a rebase between them fast-forwards.
async fn diverged(fluree: &Fluree, name: &str, doc: JsonValue) -> (String, String) {
    fluree.create_ledger(name).await.unwrap();
    let (main, dev) = (format!("{name}:main"), format!("{name}:dev"));
    insert(fluree, &main, plain("ex:base")).await;
    fluree.create_branch(name, "dev", None, None).await.unwrap();
    insert(fluree, &dev, doc).await;
    insert(fluree, &main, plain("ex:main")).await;
    (main, dev)
}

#[tokio::test]
async fn a_merge_keeps_the_lifetime_a_value_began_on_its_branch() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (name, doc) in [
        ("it/life-merge-value", per_value()),
        ("it/life-merge-txn", transaction_default()),
    ] {
        let (main, dev) = diverged(&fluree, name, doc).await;
        let due = next_due(&fluree, &dev).await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        fluree
            .merge_branch(name, "dev", Some("main"), ConflictStrategy::default())
            .await
            .expect("merge");
        assert_eq!(next_due(&fluree, &main).await, due, "{name}");
    }
}

#[tokio::test]
async fn a_rebase_keeps_the_lifetime_a_value_began_with() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (name, doc) in [
        ("it/life-rebase-value", per_value()),
        ("it/life-rebase-txn", transaction_default()),
    ] {
        let (_, dev) = diverged(&fluree, name, doc).await;
        let due = next_due(&fluree, &dev).await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let report = fluree
            .rebase_branch(name, "dev", ConflictStrategy::default())
            .await
            .expect("rebase");
        assert_eq!(report.replayed, 1, "{name}");
        assert_eq!(next_due(&fluree, &dev).await, due, "{name}");
    }
}

/// A rebase rewrites the branch's line under its schedule: what the source
/// brought in below the schedule's watermark is still found.
#[tokio::test]
async fn a_rebase_brings_in_the_sources_lifetimes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it/life-rebase-below";
    let (main, dev) = diverged(&fluree, name, per_value()).await;
    // The branch's watermark ends above where the rebase puts the source's
    // commit.
    insert(&fluree, &dev, plain("ex:d1")).await;
    insert(&fluree, &dev, plain("ex:d2")).await;
    next_due(&fluree, &dev).await;
    insert(
        &fluree,
        &main,
        json!({"@context": ctx(), "@id": "ex:m", "ex:v": {"@value": "y", "@ttl": 7200}}),
    )
    .await;
    fluree
        .rebase_branch(name, "dev", ConflictStrategy::default())
        .await
        .expect("rebase");
    let report = fluree.run_retention_at(&dev, now_ms()).await.expect("pass");
    assert_eq!(report.pending, 2, "{report:?}");
}

/// Merged on through a second branch, a value still counts from its write.
#[tokio::test]
async fn a_lifetime_survives_a_merge_of_a_merge() {
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it/life-chain";
    let (main, dev) = diverged(&fluree, name, plain("ex:dev")).await;
    fluree
        .create_branch(name, "feature", Some("dev"), None)
        .await
        .unwrap();
    let feature = format!("{name}:feature");
    insert(&fluree, &feature, per_value()).await;
    insert(&fluree, &dev, plain("ex:dev-again")).await;
    let due = next_due(&fluree, &feature).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    fluree
        .merge_branch(name, "feature", Some("dev"), ConflictStrategy::default())
        .await
        .expect("merge into dev");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    fluree
        .merge_branch(name, "dev", Some("main"), ConflictStrategy::default())
        .await
        .expect("merge into main");
    assert_eq!(next_due(&fluree, &dev).await, due, "dev");
    assert_eq!(next_due(&fluree, &main).await, due, "main");
}

/// A lifetime a property policy gives counts from the value's write too.
#[tokio::test]
async fn a_property_lifetime_counts_from_the_write_across_a_merge() {
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it/life-policy";
    fluree.create_ledger(name).await.unwrap();
    let main = format!("{name}:main");
    fluree
        .graph(&main)
        .transact()
        .upsert_turtle(&format!(
            "@prefix f: <https://ns.flur.ee/db#> . @prefix ex: <http://example.org/> .
             GRAPH <urn:fluree:{main}#config> {{
               <urn:fluree:{main}:config:ledger> a f:LedgerConfig ;
                 f:retentionDefaults <urn:cfg:retention> .
               <urn:cfg:retention> a f:RetentionDefaults ; f:property <urn:cfg:reading> .
               <urn:cfg:reading> a f:RetentionProperty ; f:target ex:reading ; f:ttl 3600 .
             }}"
        ))
        .commit()
        .await
        .expect("config");
    fluree.create_branch(name, "dev", None, None).await.unwrap();
    let dev = format!("{name}:dev");
    insert(
        &fluree,
        &dev,
        json!({"@context": ctx(), "@id": "ex:s", "ex:reading": 1}),
    )
    .await;
    insert(&fluree, &main, plain("ex:main")).await;
    let due = next_due(&fluree, &dev).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    fluree
        .merge_branch(name, "dev", Some("main"), ConflictStrategy::default())
        .await
        .expect("merge");
    assert_eq!(next_due(&fluree, &main).await, due);
}
