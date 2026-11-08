//! `f:noHistory` and targeted removal: a `noHistory` property's history
//! leaves the index, and every read of it is answered from what remains; a
//! read that needs a removed occurrence fails with history-unavailable.

use fluree_db_api::{ApiError, Fluree, FlureeBuilder};
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"})
}

async fn ledger(fluree: &Fluree, name: &str) -> String {
    fluree.create_ledger(name).await.expect("create");
    format!("{name}:main")
}

async fn upsert(fluree: &Fluree, ledger_id: &str, doc: JsonValue) -> i64 {
    fluree
        .graph(ledger_id)
        .transact()
        .upsert(&doc)
        .commit()
        .await
        .expect("upsert")
        .receipt
        .t
}

async fn set_no_history(fluree: &Fluree, ledger_id: &str, predicate: &str, on: bool) -> i64 {
    fluree
        .graph(ledger_id)
        .transact()
        .upsert_turtle(&format!(
            "@prefix f: <https://ns.flur.ee/db#> . @prefix ex: <http://example.org/> .
             GRAPH <urn:fluree:{ledger_id}#config> {{
               <urn:fluree:{ledger_id}:config:ledger> a f:LedgerConfig ;
                 f:retentionDefaults <urn:cfg:retention> .
               <urn:cfg:retention> a f:RetentionDefaults ; f:property <urn:cfg:p> .
               <urn:cfg:p> a f:RetentionProperty ; f:target {predicate} ; f:noHistory {on} .
             }}"
        ))
        .commit()
        .await
        .expect("config")
        .receipt
        .t
}

/// `s p ?o` read from `from`; `Err` when refused.
async fn read(
    fluree: &Fluree,
    ledger_id: &str,
    from: JsonValue,
    s: &str,
    p: &str,
) -> Result<Vec<JsonValue>, ApiError> {
    let mut query = json!({
        "@context": ctx(),
        "from": from,
        "select": "?o",
        "where": {"@id": s, p: "?o"}
    });
    if let JsonValue::Object(map) = &mut query {
        if let Some(to) = map.get("from").and_then(|f| f.get("to")).cloned() {
            map.insert("to".into(), to);
            map.insert("from".into(), map["from"]["from"].clone());
        }
    }
    let results = fluree.query_connection(&query).await?;
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    Ok(results
        .to_jsonld(&ledger.snapshot)
        .unwrap()
        .as_array()
        .cloned()
        .unwrap_or_default())
}

fn unavailable(result: Result<Vec<JsonValue>, ApiError>) -> bool {
    matches!(result, Err(ApiError::HistoryUnavailable(_)))
}

async fn reindex(fluree: &Fluree, ledger_id: &str) {
    fluree
        .reindex(ledger_id, fluree_db_api::ReindexOptions::default())
        .await
        .expect("index");
}

/// The history rules `ledger_id`'s journal records.
async fn history_rules(
    fluree: &Fluree,
    ledger_id: &str,
) -> Vec<fluree_db_api::retention::journal::HistoryRule> {
    fluree
        .retention_status(ledger_id)
        .await
        .unwrap()
        .registration
        .map(|r| r.journal.history_rules)
        .unwrap_or_default()
}

#[tokio::test]
async fn a_no_history_property_is_read_from_what_the_index_keeps() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/nohist-counter").await;
    let t1 = upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 1, "ex:name": "C"}),
    )
    .await;
    upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 2}),
    )
    .await;
    set_no_history(&fluree, &id, "ex:count", true).await;
    let t4 = upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 3}),
    )
    .await;
    fluree.run_retention(&id).await.expect("pass");
    assert_eq!(history_rules(&fluree, &id).await.len(), 1);
    assert!(
        !fluree.history_suppressed(&id).await.unwrap(),
        "a rule alone leaves raw commits ordinary data"
    );
    let at = |t: i64| json!(format!("{id}@t:{t}"));
    // Until it is indexed, the counter's history is still there to read.
    assert_eq!(
        read(&fluree, &id, at(t1), "ex:c", "ex:count")
            .await
            .unwrap(),
        vec![json!(1)]
    );

    reindex(&fluree, &id).await;
    // The counter reads from the t its current value was asserted...
    assert!(read(&fluree, &id, at(t1), "ex:c", "ex:count")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        read(&fluree, &id, at(t4), "ex:c", "ex:count")
            .await
            .unwrap(),
        vec![json!(3)]
    );
    assert_eq!(
        read(&fluree, &id, json!(id), "ex:c", "ex:count")
            .await
            .unwrap(),
        vec![json!(3)]
    );
    // ...while other predicates keep their history, under a variable
    // predicate and a crawl too.
    assert_eq!(
        read(&fluree, &id, at(t1), "ex:c", "ex:name").await.unwrap(),
        vec![json!("C")]
    );
    assert_eq!(
        read(&fluree, &id, at(t1), "ex:c", "?p").await.unwrap(),
        vec![json!("C")]
    );
    let crawl = json!({"@context": ctx(), "select": {"ex:c": ["*"]}});
    for (t, expected) in [
        (t1, json!([{"@id": "ex:c", "ex:name": "C"}])),
        (t4, json!([{"@id": "ex:c", "ex:name": "C", "ex:count": 3}])),
    ] {
        let view = fluree.db_at_t(&id, t).await.unwrap();
        let rows = fluree.query(&view, &crawl).await.expect("crawl");
        let rows = rows.to_jsonld_async(view.as_graph_db_ref()).await.unwrap();
        assert_eq!(rows, expected);
    }
    // A history range lists only the current value's assertion.
    assert_eq!(
        read(
            &fluree,
            &id,
            json!({"from": format!("{id}@t:1"), "to": format!("{id}@t:latest")}),
            "ex:c",
            "ex:count"
        )
        .await
        .unwrap(),
        vec![json!(3)]
    );
    // SPARQL and export read the same way.
    let sparql = |t: i64| {
        format!(
            "PREFIX ex: <http://example.org/>\n\
             SELECT ?o FROM <{id}@t:{t}> WHERE {{ ex:c ex:count ?o }}"
        )
    };
    let ledger = fluree.ledger(&id).await.unwrap();
    for (t, expected) in [(t1, json!([])), (t4, json!([[3]]))] {
        let rows = fluree.query_connection_sparql(&sparql(t)).await.unwrap();
        assert_eq!(rows.to_jsonld(&ledger.snapshot).unwrap(), expected);
    }
    let mut out = Vec::new();
    fluree
        .export(&id)
        .format(fluree_db_api::export::ExportFormat::NTriples)
        .as_of(fluree_db_api::TimeSpec::AtT(t1))
        .write_to(&mut out)
        .await
        .expect("export");
    let out = String::from_utf8(out).unwrap();
    assert!(out.contains("\"C\""), "{out}");
    assert!(!out.contains("http://example.org/count"), "{out}");
}

#[tokio::test]
async fn disabling_no_history_reopens_only_what_it_kept() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/nohist-disable").await;
    let doc = |v: i64| json!({"@context": ctx(), "@id": "ex:c", "ex:count": v});
    let t1 = upsert(&fluree, &id, doc(1)).await;
    set_no_history(&fluree, &id, "ex:count", true).await;
    let t3 = upsert(&fluree, &id, doc(2)).await;
    fluree.run_retention(&id).await.expect("pass");
    let off = set_no_history(&fluree, &id, "ex:count", false).await;
    let t5 = upsert(&fluree, &id, doc(3)).await;
    upsert(&fluree, &id, doc(4)).await;
    fluree.run_retention(&id).await.expect("pass");
    let status = fluree.retention_status(&id).await.unwrap();
    let rule = &status.registration.as_ref().unwrap().journal.history_rules[0];
    assert_eq!(rule.until_t, Some(off), "{status:?}");
    reindex(&fluree, &id).await;
    let at = |t: i64| json!(format!("{id}@t:{t}"));
    // A value superseded while the rule was in force leaves the index.
    assert_eq!(
        read(&fluree, &id, at(t1), "ex:c", "ex:count")
            .await
            .unwrap(),
        Vec::<JsonValue>::new()
    );
    // One superseded after it ended keeps its whole span, its assertion
    // under the rule included; history written after it ended stays too.
    for (t, value) in [(t3, 2), (off, 2), (t5, 3)] {
        assert_eq!(
            read(&fluree, &id, at(t), "ex:c", "ex:count").await.unwrap(),
            vec![json!(value)],
            "t={t}"
        );
    }
}

/// A build before the worker's next pass does not know the rule has ended:
/// it leaves out only history through the watermark the worker persisted.
#[tokio::test]
async fn a_rebuild_ahead_of_the_worker_keeps_history_written_after_a_rule_ended() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/nohist-worker-lag").await;
    let doc = |v: i64| json!({"@context": ctx(), "@id": "ex:c", "ex:count": v});
    set_no_history(&fluree, &id, "ex:count", true).await;
    upsert(&fluree, &id, doc(1)).await;
    fluree.run_retention(&id).await.expect("pass");
    reindex(&fluree, &id).await;

    let off = set_no_history(&fluree, &id, "ex:count", false).await;
    let t2 = upsert(&fluree, &id, doc(2)).await;
    upsert(&fluree, &id, doc(3)).await;
    reindex(&fluree, &id).await;
    fluree.run_retention(&id).await.expect("pass");
    assert_eq!(history_rules(&fluree, &id).await[0].until_t, Some(off));
    assert_eq!(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t2}")),
            "ex:c",
            "ex:count"
        )
        .await
        .unwrap(),
        vec![json!(2)]
    );
}

#[tokio::test]
async fn ttl_composes_with_no_history() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/nohist-ttl").await;
    set_no_history(&fluree, &id, "ex:status", true).await;
    let t = upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:d", "ex:status": {"@value": "on", "@ttl": 60}}),
    )
    .await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    fluree.run_retention_at(&id, now).await.expect("pass");
    let report = fluree
        .run_retention_at(&id, now + 61_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 1, "{report:?}");
    // Expired from current state, and once a pass has covered the expiry
    // and it is indexed, from its history too.
    assert!(read(&fluree, &id, json!(id), "ex:d", "ex:status")
        .await
        .unwrap()
        .is_empty());
    fluree
        .run_retention_at(&id, now + 61_000)
        .await
        .expect("pass");
    reindex(&fluree, &id).await;
    assert!(read(
        &fluree,
        &id,
        json!(format!("{id}@t:{t}")),
        "ex:d",
        "ex:status"
    )
    .await
    .unwrap()
    .is_empty());
}

async fn insert(fluree: &Fluree, ledger_id: &str, doc: JsonValue) -> i64 {
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&doc)
        .commit()
        .await
        .expect("insert")
        .receipt
        .t
}

fn select(subject: &str, object: Option<JsonValue>) -> fluree_db_api::retention::RemovalRequest {
    fluree_db_api::retention::RemovalRequest {
        occurrences: vec![],
        matching: vec![fluree_db_api::retention::FactSelector {
            subject: format!("http://example.org/{subject}"),
            predicate: "http://example.org/email".into(),
            graph: None,
            object,
        }],
    }
}

#[tokio::test]
async fn removing_one_occurrence_preserves_the_others() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/removal-one").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:email": "a@old", "ex:name": "Alice"},
            {"@id": "ex:bob", "ex:email": "b@old"}
        ]}),
    )
    .await;
    upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@new"}),
    )
    .await;
    let receipt = fluree
        .remove_occurrences(&id, select("alice", Some(json!("a@old"))))
        .await
        .expect("remove");
    // The assertion and the retraction that ended it.
    assert_eq!(receipt.manifest.len(), 2, "{receipt:?}");
    assert_eq!(receipt.retracted_t, None, "it was no longer current");

    // Current state is unchanged.
    assert_eq!(
        read(&fluree, &id, json!(id), "ex:alice", "ex:email")
            .await
            .unwrap(),
        vec![json!("a@new")]
    );
    // A past read that could see the removed occurrence is refused...
    assert!(unavailable(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t1}")),
            "ex:alice",
            "ex:email"
        )
        .await
    ));
    // ...one about another subject, or another predicate, is answered.
    assert_eq!(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t1}")),
            "ex:bob",
            "ex:email"
        )
        .await
        .unwrap(),
        vec![json!("b@old")]
    );
    assert_eq!(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t1}")),
            "ex:alice",
            "ex:name"
        )
        .await
        .unwrap(),
        vec![json!("Alice")]
    );
}

#[tokio::test]
async fn removing_a_current_occurrence_retracts_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/removal-current").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@x", "ex:name": "Alice"}),
    )
    .await;
    let receipt = fluree
        .remove_occurrences(&id, select("alice", None))
        .await
        .expect("remove");
    let retracted = receipt.retracted_t.expect("retracted the current value");
    assert!(retracted > t1);
    assert!(read(&fluree, &id, json!(id), "ex:alice", "ex:email")
        .await
        .unwrap()
        .is_empty());
    assert!(unavailable(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t1}")),
            "ex:alice",
            "ex:email"
        )
        .await
    ));
    assert_eq!(
        read(&fluree, &id, json!(id), "ex:alice", "ex:name")
            .await
            .unwrap(),
        vec![json!("Alice")]
    );
    // A second request for the same occurrences adds nothing.
    let again = fluree
        .remove_occurrences(&id, select("alice", None))
        .await
        .expect("again");
    assert_eq!(again.retracted_t, None);
    assert_eq!(again.generation, receipt.generation);
    // Re-ingesting the value later is allowed and is a new occurrence.
    let t4 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@x"}),
    )
    .await;
    assert_eq!(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t4}")),
            "ex:alice",
            "ex:email"
        )
        .await
        .unwrap(),
        vec![json!("a@x")]
    );
}

/// The number of history entries in the default graph's index sidecars,
/// read straight from storage (as a raw index reader would, past any serving
/// check).
async fn indexed_history_entries(fluree: &Fluree, ledger_id: &str) -> usize {
    use fluree_db_core::ContentStore;
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .expect("record");
    let root_id = record.index_head_id.clone().expect("indexed");
    let store = fluree.content_store(&record.storage_namespace());
    let root_bytes = store.get(&root_id).await.unwrap();
    let root = fluree_db_binary_index::format::index_root::IndexRoot::decode(&root_bytes).unwrap();
    let mut entries = 0;
    for order in &root.default_graph_orders {
        for leaf in &order.leaves {
            let Some(cid) = &leaf.sidecar_cid else {
                continue;
            };
            let bytes = store.get(cid).await.unwrap();
            // FHS1: 8-byte header, then per leaflet a u32 count and 31-byte entries.
            let mut pos = 8;
            while pos + 4 <= bytes.len() {
                let n = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
                entries += n;
                pos += 4 + n * 31;
            }
        }
    }
    entries
}

#[tokio::test]
async fn no_history_compacts_the_index_history() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/nohist-compact").await;
    for n in 1..=3 {
        upsert(
            &fluree,
            &id,
            json!({"@context": ctx(), "@id": "ex:c", "ex:count": n}),
        )
        .await;
    }
    fluree
        .reindex(&id, fluree_db_api::ReindexOptions::default())
        .await
        .expect("index");
    let before = indexed_history_entries(&fluree, &id).await;
    assert!(
        before > 0,
        "the counter's past values are in the index history"
    );

    set_no_history(&fluree, &id, "ex:count", true).await;
    fluree.run_retention(&id).await.expect("pass");
    assert_eq!(
        indexed_history_entries(&fluree, &id).await,
        before,
        "recording the rule does not rebuild the index"
    );
    fluree
        .reindex(&id, fluree_db_api::ReindexOptions::default())
        .await
        .expect("index");
    assert_eq!(
        indexed_history_entries(&fluree, &id).await,
        0,
        "a rebuild leaves no history for the counter"
    );

    // A later change keeps its history until a pass covers it.
    upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 4}),
    )
    .await;
    fluree
        .reindex(&id, fluree_db_api::ReindexOptions::default())
        .await
        .expect("index");
    assert!(indexed_history_entries(&fluree, &id).await > 0);
    fluree.run_retention(&id).await.expect("pass");
    fluree
        .reindex(&id, fluree_db_api::ReindexOptions::default())
        .await
        .expect("index");
    assert_eq!(indexed_history_entries(&fluree, &id).await, 0);
    // Current state is intact.
    assert_eq!(
        read(&fluree, &id, json!(id), "ex:c", "ex:count")
            .await
            .unwrap(),
        vec![json!(4)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_builds_leave_suppressed_history_out() {
    use std::sync::Arc;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let fluree = Arc::new(
        FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .with_indexer_catchup_interval(Duration::from_millis(200))
            .build()
            .unwrap(),
    );
    fluree.start_maintenance(
        None,
        Some(fluree_db_api::retention::RetentionOptions {
            interval: Duration::from_millis(100),
            limits: fluree_db_api::retention::RetentionLimits {
                checkpoint_ms: 0,
                ..Default::default()
            },
            ..Default::default()
        }),
    );
    let id = ledger(&fluree, "it/nohist-incremental").await;
    upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 1}),
    )
    .await;
    set_no_history(&fluree, &id, "ex:count", true).await;
    let mut ready = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(r) = fluree.retention_status(&id).await.unwrap().registration {
            if !r.journal.history_rules.is_empty() {
                ready = true;
                break;
            }
        }
    }
    assert!(ready, "the worker records the rule");
    let mut head = 0;
    for n in 2..=5 {
        head = upsert(
            &fluree,
            &id,
            json!({"@context": ctx(), "@id": "ex:c", "ex:count": n}),
        )
        .await;
    }
    caught_up(&fluree, &id).await;
    // Once the worker has covered the changes, the next incremental build
    // over the counter's leaves keeps none of its history.
    let mut covered = false;
    for _ in 0..300 {
        let status = fluree.retention_status(&id).await.unwrap();
        if status.registration.is_some_and(|r| r.watermark_t >= head) {
            covered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(covered, "the worker persists its watermark");
    upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:name": "counter"}),
    )
    .await;
    caught_up(&fluree, &id).await;
    assert_eq!(indexed_history_entries(&fluree, &id).await, 0);
    assert_eq!(
        read(&fluree, &id, json!(id), "ex:c", "ex:count")
            .await
            .unwrap(),
        vec![json!(5)]
    );
}

/// Wait for the background indexer to reach the head.
async fn caught_up(fluree: &Fluree, ledger_id: &str) {
    for _ in 0..300 {
        let record = fluree
            .nameservice()
            .lookup(ledger_id)
            .await
            .unwrap()
            .unwrap();
        if record.index_t >= record.commit_t {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("the background indexer did not catch up");
}

/// Wait for the worker to record the ledger's one rule as in force, or ended.
async fn rule_in_force(fluree: &Fluree, ledger_id: &str, in_force: bool) {
    for _ in 0..300 {
        let rules = history_rules(fluree, ledger_id).await;
        if rules.len() == 1 && rules[0].until_t.is_none() == in_force {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("the worker did not record the rule");
}

/// Incremental builds keep a span that straddles a rule's end whole, as a
/// rebuild does (`disabling_no_history_reopens_only_what_it_kept`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_builds_keep_a_span_that_outlives_the_rule() {
    use std::sync::Arc;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let fluree = Arc::new(
        FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .with_indexer_catchup_interval(Duration::from_millis(200))
            .build()
            .unwrap(),
    );
    fluree.start_maintenance(
        None,
        Some(fluree_db_api::retention::RetentionOptions {
            interval: Duration::from_millis(100),
            ..Default::default()
        }),
    );
    let id = ledger(&fluree, "it/nohist-incremental-span").await;
    let doc = |v: i64| json!({"@context": ctx(), "@id": "ex:c", "ex:count": v});
    let t1 = upsert(&fluree, &id, doc(1)).await;
    caught_up(&fluree, &id).await;
    set_no_history(&fluree, &id, "ex:count", true).await;
    rule_in_force(&fluree, &id, true).await;
    let t3 = upsert(&fluree, &id, doc(2)).await;
    caught_up(&fluree, &id).await;
    set_no_history(&fluree, &id, "ex:count", false).await;
    rule_in_force(&fluree, &id, false).await;
    let t5 = upsert(&fluree, &id, doc(3)).await;
    caught_up(&fluree, &id).await;

    let at = |t: i64| json!(format!("{id}@t:{t}"));
    assert_eq!(
        read(&fluree, &id, at(t1), "ex:c", "ex:count")
            .await
            .unwrap(),
        Vec::<JsonValue>::new()
    );
    for (t, value) in [(t3, 2), (t5, 3)] {
        assert_eq!(
            read(&fluree, &id, at(t), "ex:c", "ex:count").await.unwrap(),
            vec![json!(value)],
            "t={t}"
        );
    }
}

/// As `incremental_builds_keep_a_span_that_outlives_the_rule`, in a named
/// graph with trees of its own. Its history entries carry no graph, and the
/// ones a build derives from novelty do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_builds_keep_a_span_that_outlives_the_rule_in_a_dedicated_graph() {
    use std::sync::Arc;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let fluree = Arc::new(
        FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .with_indexer_catchup_interval(Duration::from_millis(200))
            .build()
            .unwrap(),
    );
    fluree.start_maintenance(
        None,
        Some(fluree_db_api::retention::RetentionOptions {
            interval: Duration::from_millis(100),
            ..Default::default()
        }),
    );
    let id = ledger(&fluree, "it/nohist-dedicated-span").await;
    fluree
        .graph(&id)
        .transact()
        .upsert_turtle(&format!(
            "@prefix f: <https://ns.flur.ee/db#> .
             GRAPH <urn:fluree:{id}#config> {{
               <urn:fluree:{id}:config:ledger> a f:LedgerConfig ;
                 f:indexingDefaults [ f:graphPlacement f:PlacementDedicated ] .
             }}"
        ))
        .commit()
        .await
        .expect("placement");
    let write = |v: i64| {
        let fluree = Arc::clone(&fluree);
        let id = id.clone();
        async move {
            fluree
                .graph(&id)
                .transact()
                .upsert_turtle(&format!(
                    "@prefix ex: <http://example.org/> .
                     GRAPH ex:g {{ ex:c ex:count {v} . }}"
                ))
                .commit()
                .await
                .expect("write")
                .receipt
                .t
        }
    };
    let t1 = write(1).await;
    caught_up(&fluree, &id).await;
    set_no_history(&fluree, &id, "ex:count", true).await;
    rule_in_force(&fluree, &id, true).await;
    let t3 = write(2).await;
    caught_up(&fluree, &id).await;
    set_no_history(&fluree, &id, "ex:count", false).await;
    rule_in_force(&fluree, &id, false).await;
    let t5 = write(3).await;
    caught_up(&fluree, &id).await;

    let at = |t: i64| json!({"@id": id, "t": t, "graph": "http://example.org/g"});
    assert_eq!(
        read(&fluree, &id, at(t1), "ex:c", "ex:count")
            .await
            .unwrap(),
        Vec::<JsonValue>::new()
    );
    for (t, value) in [(t3, 2), (t5, 3)] {
        assert_eq!(
            read(&fluree, &id, at(t), "ex:c", "ex:count").await.unwrap(),
            vec![json!(value)],
            "t={t}"
        );
    }
}

/// A file-backed instance whose background indexer builds each commit, and
/// whose retention passes run only when called.
fn indexing_without_background_passes(dir: &std::path::Path) -> std::sync::Arc<Fluree> {
    use std::time::Duration;
    let fluree = std::sync::Arc::new(
        FlureeBuilder::file(dir.to_string_lossy().to_string())
            .with_indexer_catchup_interval(Duration::from_secs(3600))
            .build()
            .unwrap(),
    );
    fluree.start_maintenance(
        None,
        Some(fluree_db_api::retention::RetentionOptions {
            interval: Duration::from_secs(3600),
            ..Default::default()
        }),
    );
    fluree
}

async fn transact_turtle(fluree: &Fluree, ledger_id: &str, turtle: &str) -> i64 {
    transact_turtle_with(fluree, ledger_id, turtle, true).await
}

/// `turtle` committed, with an index build triggered when `index`.
async fn transact_turtle_with(fluree: &Fluree, ledger_id: &str, turtle: &str, index: bool) -> i64 {
    let graph = fluree.graph(ledger_id);
    let turtle = format!(
        "@prefix f: <https://ns.flur.ee/db#> . @prefix ex: <http://example.org/> .
         {turtle}"
    );
    let mut txn = graph.transact().upsert_turtle(&turtle);
    if !index {
        txn = txn.index_config(fluree_db_api::IndexConfig {
            reindex_min_bytes: usize::MAX,
            reindex_max_bytes: usize::MAX,
        });
    }
    txn.commit().await.expect("transact").receipt.t
}

/// `ex:count` keeps no history, except in `ex:g2`.
async fn no_history_except_in_g2(fluree: &Fluree, ledger_id: &str) {
    transact_turtle(
        fluree,
        ledger_id,
        &format!(
            "GRAPH <urn:fluree:{ledger_id}#config> {{
               <urn:fluree:{ledger_id}:config:ledger> a f:LedgerConfig ;
                 f:retentionDefaults <urn:cfg:retention> ;
                 f:graphOverrides <urn:cfg:g2> .
               <urn:cfg:retention> a f:RetentionDefaults ; f:property <urn:cfg:p> .
               <urn:cfg:p> a f:RetentionProperty ; f:target ex:count ; f:noHistory true .
               <urn:cfg:g2> a f:GraphConfig ; f:targetGraph ex:g2 ;
                 f:retentionDefaults [ f:property [
                   a f:RetentionProperty ; f:target ex:count ; f:noHistory false ] ] .
             }}"
        ),
    )
    .await;
}

/// `ex:c ex:count v` in `ex:g1` and `ex:g2`, both small enough to pool.
async fn count_in_both(fluree: &Fluree, ledger_id: &str, v: i64) -> i64 {
    count_in_both_with(fluree, ledger_id, v, true).await
}

async fn count_in_both_with(fluree: &Fluree, ledger_id: &str, v: i64, index: bool) -> i64 {
    transact_turtle_with(
        fluree,
        ledger_id,
        &format!("GRAPH ex:g1 {{ ex:c ex:count {v} . }} GRAPH ex:g2 {{ ex:c ex:count {v} . }}"),
        index,
    )
    .await
}

/// `(rows, history)` the index records for pooled `ex:{graph}`; `None` when
/// it has trees of its own.
async fn pool_member(fluree: &Fluree, ledger_id: &str, graph: &str) -> Option<(u32, u32)> {
    use fluree_db_core::ContentStore;
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .expect("record");
    let store = fluree.content_store(&record.storage_namespace());
    let bytes = store
        .get(&record.index_head_id.expect("indexed"))
        .await
        .unwrap();
    let root = fluree_db_binary_index::format::index_root::IndexRoot::decode(&bytes).unwrap();
    let iri = format!("http://example.org/{graph}");
    let g_id = root.graph_iris.iter().position(|g| *g == iri)? as u32 + 1;
    root.pool?.member(g_id).map(|m| (m.rows, m.history))
}

/// `ex:c`'s counts as of `t` in `ex:g1` and in `ex:g2`.
async fn counts_at(fluree: &Fluree, ledger_id: &str, t: i64) -> (Vec<JsonValue>, Vec<JsonValue>) {
    let at = |graph: &str| json!({"@id": ledger_id, "t": t, "graph": format!("http://example.org/{graph}")});
    (
        read(fluree, ledger_id, at("g1"), "ex:c", "ex:count")
            .await
            .unwrap(),
        read(fluree, ledger_id, at("g2"), "ex:c", "ex:count")
            .await
            .unwrap(),
    )
}

/// `[subject, count]` rows as of `t` in `ex:g1` and in `ex:g2`, by a range
/// on the count: a pooled graph reads it from the pool's POST order, where
/// GSPO serves [`counts_at`].
async fn count_range_at(
    fluree: &Fluree,
    ledger_id: &str,
    t: i64,
) -> (Vec<JsonValue>, Vec<JsonValue>) {
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let rows = |graph: &str| {
        let query = json!({
            "@context": ctx(),
            "from": {"@id": ledger_id, "t": t, "graph": format!("http://example.org/{graph}")},
            "select": ["?s", "?o"],
            "where": [{"@id": "?s", "ex:count": "?o"}, ["filter", "(>= ?o 0)"]]
        });
        let ledger = &ledger;
        async move {
            let result = fluree.query_connection(&query).await.unwrap();
            result
                .to_jsonld(&ledger.snapshot)
                .unwrap()
                .as_array()
                .cloned()
                .unwrap_or_default()
        }
    };
    (rows("g1").await, rows("g2").await)
}

/// Pooled graphs share their trees, and a rule leaves out history only in
/// the graphs it covers: from GSPO at each build, from a delta run the spans
/// its window closes, and from the other orders' base when the delta folds
/// into it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pooled_builds_leave_out_only_the_covered_graphs_history() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = indexing_without_background_passes(dir.path());
    let id = ledger(&fluree, "it/nohist-pool").await;
    no_history_except_in_g2(&fluree, &id).await;
    let t1 = count_in_both(&fluree, &id, 1).await;
    fluree.run_retention(&id).await.expect("pass");
    caught_up(&fluree, &id).await;
    // One build, after a pass has covered both changes.
    let t2 = count_in_both_with(&fluree, &id, 2, false).await;
    let t3 = count_in_both_with(&fluree, &id, 3, false).await;
    fluree.run_retention(&id).await.expect("pass");
    let built = fluree_db_api::admin::MaintainIndexOptions {
        compact: false,
        ..Default::default()
    };
    fluree.maintain_index(&id, built).await.expect("build");

    assert_eq!(pool_member(&fluree, &id, "g1").await.map(|m| m.1), Some(0));
    assert_eq!(pool_member(&fluree, &id, "g2").await.map(|m| m.1), Some(4));
    assert_eq!(counts_at(&fluree, &id, t1).await, (vec![], vec![json!(1)]));
    assert_eq!(
        counts_at(&fluree, &id, t3).await,
        (vec![json!(3)], vec![json!(3)])
    );
    // The delta run holds `ex:g1`'s value 2 neither asserted nor retracted.
    assert_eq!(
        count_range_at(&fluree, &id, t2).await,
        (vec![], vec![json!(["ex:c", 2])])
    );

    let maintained = fluree
        .maintain_index(&id, Default::default())
        .await
        .expect("maintain");
    assert!(maintained.compacted);
    assert_eq!(
        count_range_at(&fluree, &id, t1).await,
        (vec![], vec![json!(["ex:c", 1])])
    );
}

/// A graph leaving the pool takes its pooled history to trees of its own,
/// less what its rule covers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_graph_leaving_the_pool_leaves_out_what_its_rule_covers() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = indexing_without_background_passes(dir.path());
    let id = ledger(&fluree, "it/nohist-promote").await;
    no_history_except_in_g2(&fluree, &id).await;
    let t1 = count_in_both(&fluree, &id, 1).await;
    fluree.run_retention(&id).await.expect("pass");
    caught_up(&fluree, &id).await;
    count_in_both(&fluree, &id, 2).await;
    let t3 = count_in_both(&fluree, &id, 3).await;
    caught_up(&fluree, &id).await;
    // Built ahead of a pass, the pool still holds `ex:g1`'s history.
    assert_eq!(pool_member(&fluree, &id, "g1").await.map(|m| m.1), Some(4));

    fluree.run_retention(&id).await.expect("pass");
    transact_turtle(
        &fluree,
        &id,
        &format!(
            "GRAPH <urn:fluree:{id}#config> {{
               <urn:fluree:{id}:config:ledger>
                 f:indexingDefaults [ f:graphPlacement f:PlacementDedicated ] .
             }}"
        ),
    )
    .await;
    caught_up(&fluree, &id).await;
    assert_eq!(pool_member(&fluree, &id, "g1").await, None);
    assert_eq!(pool_member(&fluree, &id, "g2").await, None);
    assert_eq!(counts_at(&fluree, &id, t1).await, (vec![], vec![json!(1)]));
    assert_eq!(
        counts_at(&fluree, &id, t3).await,
        (vec![json!(3)], vec![json!(3)])
    );
}

/// A rebuild pools the graphs, and leaves out history only in the ones the
/// rule covers.
#[tokio::test]
async fn a_rebuild_leaves_out_only_the_covered_pooled_graphs_history() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/nohist-pool-rebuild").await;
    no_history_except_in_g2(&fluree, &id).await;
    let t1 = count_in_both(&fluree, &id, 1).await;
    count_in_both(&fluree, &id, 2).await;
    count_in_both(&fluree, &id, 3).await;
    fluree.run_retention(&id).await.expect("pass");
    reindex(&fluree, &id).await;

    assert_eq!(pool_member(&fluree, &id, "g1").await.map(|m| m.1), Some(0));
    assert_eq!(pool_member(&fluree, &id, "g2").await.map(|m| m.1), Some(4));
    assert_eq!(counts_at(&fluree, &id, t1).await, (vec![], vec![json!(1)]));
    assert_eq!(
        count_range_at(&fluree, &id, t1).await,
        (vec![], vec![json!(["ex:c", 1])])
    );
}

/// As `a_rebuild_ahead_of_the_worker_keeps_history_written_after_a_rule_ended`,
/// for incremental builds. Passes run only when called, so the background
/// indexer builds before the pass that ends the rule.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_builds_ahead_of_the_worker_keep_history_written_after_a_rule_ended() {
    use std::sync::Arc;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let fluree = Arc::new(
        FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .with_indexer_catchup_interval(Duration::from_secs(3600))
            .build()
            .unwrap(),
    );
    fluree.start_maintenance(
        None,
        Some(fluree_db_api::retention::RetentionOptions {
            interval: Duration::from_secs(3600),
            ..Default::default()
        }),
    );
    let id = ledger(&fluree, "it/nohist-incremental-worker-lag").await;
    let doc = |v: i64| json!({"@context": ctx(), "@id": "ex:c", "ex:count": v});
    set_no_history(&fluree, &id, "ex:count", true).await;
    upsert(&fluree, &id, doc(1)).await;
    fluree.run_retention(&id).await.expect("pass");
    caught_up(&fluree, &id).await;

    let off = set_no_history(&fluree, &id, "ex:count", false).await;
    let t2 = upsert(&fluree, &id, doc(2)).await;
    upsert(&fluree, &id, doc(3)).await;
    caught_up(&fluree, &id).await;
    fluree.run_retention(&id).await.expect("pass");
    assert_eq!(history_rules(&fluree, &id).await[0].until_t, Some(off));
    assert_eq!(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t2}")),
            "ex:c",
            "ex:count"
        )
        .await
        .unwrap(),
        vec![json!(2)]
    );
}

/// A full-text index built before a removal could still match the removed
/// value: searches of it are refused until it has indexed the removal, and
/// searches of its earlier snapshots stay refused.
#[tokio::test]
async fn a_search_index_behind_a_removal_is_not_served() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/removal-search").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "@type": "ex:Doc", "ex:email": "secret rust"},
            {"@id": "ex:bob", "@type": "ex:Doc", "ex:email": "plain rust"}
        ]}),
    )
    .await;
    let index_query = json!({
        "@context": ctx(),
        "where": [{"@id": "?x", "@type": "ex:Doc", "ex:email": "?e"}],
        "select": {"?x": ["@id", "ex:email"]}
    });
    let created = fluree
        .create_full_text_index(fluree_db_api::Bm25CreateConfig::new(
            "removal-search",
            &id,
            index_query,
        ))
        .await
        .expect("create index");
    let search = |from: JsonValue| {
        json!({
            "@context": ctx(),
            "from": from,
            "where": [{
                "f:graphSource": &created.graph_source_id,
                "f:searchText": "secret",
                "f:searchLimit": 10,
                "f:searchResult": {"f:resultId": "?doc"}
            }],
            "select": ["?doc"]
        })
    };
    let hits = |result: fluree_db_api::QueryResult| -> usize {
        result.batches.iter().map(fluree_db_api::Batch::len).sum()
    };
    assert_eq!(
        hits(fluree.query_connection(&search(json!(id))).await.unwrap()),
        1
    );

    let receipt = fluree
        .remove_occurrences(&id, select("alice", Some(json!("secret rust"))))
        .await
        .expect("remove");
    assert!(receipt.retracted_t.is_some());
    assert!(matches!(
        fluree.query_connection(&search(json!(id))).await,
        Err(ApiError::HistoryUnavailable(_))
    ));

    fluree
        .sync_bm25_index(&created.graph_source_id)
        .await
        .expect("sync");
    assert_eq!(
        hits(fluree.query_connection(&search(json!(id))).await.unwrap()),
        0,
        "the synced index no longer holds the removed value"
    );
    assert!(matches!(
        fluree
            .query_connection(&search(json!(format!("{id}@t:{t1}"))))
            .await,
        Err(ApiError::HistoryUnavailable(_))
    ));
}

/// `f:noHistory` refuses no search: a full-text index of the property is
/// served while it lags the head, and as of an earlier point.
#[tokio::test]
async fn a_search_index_of_a_no_history_property_is_served() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/nohist-search").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "@type": "ex:Doc", "ex:status": "draft rust"}),
    )
    .await;
    let built = set_no_history(&fluree, &id, "ex:status", true).await;
    fluree.run_retention(&id).await.expect("pass");
    let created = fluree
        .create_full_text_index(fluree_db_api::Bm25CreateConfig::new(
            "nohist-search",
            &id,
            json!({
                "@context": ctx(),
                "where": [{"@id": "?x", "@type": "ex:Doc", "ex:status": "?s"}],
                "select": {"?x": ["@id", "ex:status"]}
            }),
        ))
        .await
        .expect("create index");
    upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:status": "final rust"}),
    )
    .await;
    for from in [json!(id), json!(format!("{id}@t:{built}"))] {
        let result = fluree
            .query_connection(&json!({
                "@context": ctx(),
                "from": from,
                "where": [{
                    "f:graphSource": &created.graph_source_id,
                    "f:searchText": "rust",
                    "f:searchLimit": 10,
                    "f:searchResult": {"f:resultId": "?doc"}
                }],
                "select": ["?doc"]
            }))
            .await
            .expect("served");
        let hits: usize = result.batches.iter().map(fluree_db_api::Batch::len).sum();
        assert_eq!(hits, 1, "{from}");
    }
}

/// A full-text index that reads none of a removal's properties cannot hold
/// what it removed: it is served behind the removal, as of now and earlier.
/// One whose indexing query may read anything (`*`, or reasoning, which
/// infers from properties its patterns do not name) is refused.
#[tokio::test]
async fn a_search_index_that_never_read_a_removed_property_is_served() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/removal-other-search").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "@type": "ex:Doc", "ex:email": "secret", "ex:bio": "rust"},
            {"@id": "ex:bob", "@type": "ex:Doc", "ex:bio": "rust"}
        ]}),
    )
    .await;
    let index = |name: &str, select: JsonValue, reasoning: &str| {
        fluree.create_full_text_index(fluree_db_api::Bm25CreateConfig::new(
            name,
            &id,
            json!({
                "@context": ctx(),
                "where": [{"@id": "?x", "@type": "ex:Doc"}],
                "select": {"?x": select},
                "reasoning": reasoning
            }),
        ))
    };
    let bio = index("removal-bio", json!(["@id", "ex:bio"]), "none")
        .await
        .expect("create index")
        .graph_source_id;
    let everything = index("removal-everything", json!(["*"]), "none")
        .await
        .expect("create index")
        .graph_source_id;
    let inferred = index("removal-inferred", json!(["@id", "ex:bio"]), "owl2rl")
        .await
        .expect("create index")
        .graph_source_id;
    let search = |source: &str, from: JsonValue| {
        json!({
            "@context": ctx(),
            "from": from,
            "where": [{
                "f:graphSource": source,
                "f:searchText": "rust",
                "f:searchLimit": 10,
                "f:searchResult": {"f:resultId": "?doc"}
            }],
            "select": ["?doc"]
        })
    };
    let hits = |result: fluree_db_api::QueryResult| -> usize {
        result.batches.iter().map(fluree_db_api::Batch::len).sum()
    };

    fluree
        .remove_occurrences(&id, select("alice", Some(json!("secret"))))
        .await
        .expect("remove");
    for from in [json!(id), json!(format!("{id}@t:{t1}"))] {
        let result = fluree.query_connection(&search(&bio, from.clone())).await;
        assert_eq!(hits(result.expect("served")), 2, "{from}");
        for refused in [&everything, &inferred] {
            assert!(
                matches!(
                    fluree
                        .query_connection(&search(refused, from.clone()))
                        .await,
                    Err(ApiError::HistoryUnavailable(_))
                ),
                "{refused} {from}"
            );
        }
    }
}

/// The SPARQL twin of the removal refusal: a `FROM <ledger@t:N>` read that
/// could see a removed occurrence is refused; another subject's is answered.
#[tokio::test]
async fn sparql_time_travel_honors_a_removal() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/removal-sparql").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:email": "a@old"},
            {"@id": "ex:bob", "ex:email": "b@old"}
        ]}),
    )
    .await;
    fluree
        .remove_occurrences(&id, select("alice", Some(json!("a@old"))))
        .await
        .expect("remove");
    let sparql = |subject: &str| {
        format!(
            "PREFIX ex: <http://example.org/>\n\
             SELECT ?e FROM <{id}@t:{t1}> WHERE {{ ex:{subject} ex:email ?e }}"
        )
    };
    assert!(matches!(
        fluree.query_connection_sparql(&sparql("alice")).await,
        Err(ApiError::HistoryUnavailable(_))
    ));
    let bob = fluree
        .query_connection_sparql(&sparql("bob"))
        .await
        .expect("another subject's history is served");
    assert_eq!(
        bob.batches
            .iter()
            .map(fluree_db_api::Batch::len)
            .sum::<usize>(),
        1
    );
}

/// A ledger archive carries the removal journal: a copy restored from it
/// refuses the same history its source did.
#[tokio::test]
async fn a_restored_archive_keeps_the_removal_journal() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let id = ledger(&fluree, "it/removal-archive").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:email": "a@old"},
            {"@id": "ex:bob", "ex:email": "b@old"}
        ]}),
    )
    .await;
    fluree
        .remove_occurrences(&id, select("alice", Some(json!("a@old"))))
        .await
        .expect("remove");

    let mut archive = Vec::new();
    fluree
        .archive_ledger(&id, false, &mut archive)
        .await
        .expect("archive");
    let restored = fluree
        .restore_ledger("it/removal-restored", &mut archive.as_slice())
        .await
        .expect("restore")
        .ledger_id;

    let at_t1 = json!(format!("{restored}@t:{t1}"));
    assert!(unavailable(
        read(&fluree, &restored, at_t1.clone(), "ex:alice", "ex:email").await
    ));
    assert_eq!(
        read(&fluree, &restored, at_t1, "ex:bob", "ex:email")
            .await
            .unwrap(),
        vec![json!("b@old")]
    );
}

/// A transitive path reads past its starting subject: removing `bob knows
/// secret` refuses an earlier `alice knows+ ?o`, in SPARQL and JSON-LD,
/// though the path starts at alice.
#[tokio::test]
async fn a_property_path_through_a_removed_occurrence_is_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/removal-path").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:knows": {"@id": "ex:secret"}}
        ]}),
    )
    .await;
    fluree
        .remove_occurrences(
            &id,
            fluree_db_api::retention::RemovalRequest {
                occurrences: vec![],
                matching: vec![fluree_db_api::retention::FactSelector {
                    subject: "http://example.org/bob".into(),
                    predicate: "http://example.org/knows".into(),
                    graph: None,
                    object: Some(json!({"@id": "http://example.org/secret"})),
                }],
            },
        )
        .await
        .expect("remove");

    let sparql = format!(
        "PREFIX ex: <http://example.org/>\n\
         SELECT ?o FROM <{id}@t:{t1}> WHERE {{ ex:alice ex:knows+ ?o }}"
    );
    assert!(matches!(
        fluree.query_connection_sparql(&sparql).await,
        Err(ApiError::HistoryUnavailable(_))
    ));
    let jsonld = json!({
        "@context": {"ex": "http://example.org/", "knowsPlus": {"@path": "ex:knows+"}},
        "from": format!("{id}@t:{t1}"),
        "select": "?o",
        "where": {"@id": "ex:alice", "knowsPlus": "?o"}
    });
    assert!(matches!(
        fluree.query_connection(&jsonld).await,
        Err(ApiError::HistoryUnavailable(_))
    ));
}

/// A policy enabled and disabled between two worker passes still covers what
/// it was in force for: the pass records each transition at its commit.
#[tokio::test]
async fn a_policy_enabled_and_disabled_between_passes_still_covers_its_interval() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/nohist-transient").await;
    let t1 = upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 1}),
    )
    .await;
    // The worker sees the ledger before the policy, so the policy arrives
    // between passes.
    fluree.run_retention(&id).await.ok();
    let enabled_t = set_no_history(&fluree, &id, "ex:count", true).await;
    upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 2}),
    )
    .await;
    let disabled_t = set_no_history(&fluree, &id, "ex:count", false).await;
    fluree.run_retention(&id).await.expect("pass");

    let journal = fluree
        .retention_status(&id)
        .await
        .unwrap()
        .registration
        .expect("registered")
        .journal;
    assert_eq!(journal.history_rules.len(), 1, "{journal:?}");
    let rule = &journal.history_rules[0];
    assert_eq!(rule.enabled_t, enabled_t);
    assert_eq!(rule.until_t, Some(disabled_t));
    // The value it superseded while in force leaves the index.
    reindex(&fluree, &id).await;
    assert!(read(
        &fluree,
        &id,
        json!(format!("{id}@t:{t1}")),
        "ex:c",
        "ex:count"
    )
    .await
    .unwrap()
    .is_empty());
    // Current state stays.
    assert_eq!(
        read(&fluree, &id, json!(id), "ex:c", "ex:count")
            .await
            .unwrap(),
        vec![json!(2)]
    );
}

/// A cluster replica that cannot confirm its journal is current refuses a
/// ledger's history even when it holds no retention registration for it: it
/// may not have applied the first one yet.
#[tokio::test]
async fn an_unconfirmed_replica_refuses_history_of_a_ledger_it_does_not_know_is_enabled() {
    struct Partitioned;
    #[async_trait::async_trait]
    impl fluree_db_api::retention::replicas::ReplicaSync for Partitioned {
        async fn sync_reads(&self) -> Result<(), String> {
            Err("no leader".into())
        }
        async fn await_replicas(&self, _: std::time::Duration) -> Vec<String> {
            Vec::new()
        }
    }
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/replica-unregistered").await;
    let t1 = upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 1}),
    )
    .await;
    upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 2}),
    )
    .await;
    fluree.set_retention_replicas(std::sync::Arc::new(Partitioned));
    assert!(fluree
        .retention_status(&id)
        .await
        .unwrap()
        .registration
        .is_none());
    assert!(matches!(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t1}")),
            "ex:c",
            "ex:count"
        )
        .await,
        Err(ApiError::ReplicaNotCurrent(_))
    ));
    // Current state is still served, and so is a view held past a commit:
    // it is checked against the journal this replica holds.
    assert_eq!(
        read(&fluree, &id, json!(id), "ex:c", "ex:count")
            .await
            .unwrap(),
        vec![json!(2)]
    );
    let held = fluree.db(&id).await.unwrap();
    upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 3}),
    )
    .await;
    let count =
        json!({"@context": ctx(), "select": "?o", "where": {"@id": "ex:c", "ex:count": "?o"}});
    let rows = fluree.query(&held, &count).await.expect("held view");
    assert_eq!(rows.to_jsonld(&held.snapshot).unwrap(), json!([2]));
}

/// An archive made before a removal, restored after its source is purged,
/// still refuses the removed history: the lineage's journal checkpoint lives
/// outside the ledger and the archive, and restore merges it in.
#[tokio::test]
async fn a_pre_removal_archive_restored_after_a_purge_keeps_the_removal() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let name = "it/removal-old-archive";
    let id = ledger(&fluree, name).await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:email": "a@old"},
            {"@id": "ex:bob", "ex:email": "b@old"}
        ]}),
    )
    .await;

    let mut archive = Vec::new();
    fluree
        .archive_ledger(&id, false, &mut archive)
        .await
        .expect("archive before the removal");
    fluree
        .remove_occurrences(&id, select("alice", Some(json!("a@old"))))
        .await
        .expect("remove");
    fluree
        .drop_ledger(name, fluree_db_api::DropMode::Hard)
        .await
        .expect("purge");

    let restored = fluree
        .restore_ledger("it/removal-old-archive-restored", &mut archive.as_slice())
        .await
        .expect("restore")
        .ledger_id;
    let at_t1 = json!(format!("{restored}@t:{t1}"));
    assert!(unavailable(
        read(&fluree, &restored, at_t1.clone(), "ex:alice", "ex:email").await
    ));
    assert_eq!(
        read(&fluree, &restored, at_t1, "ex:bob", "ex:email")
            .await
            .unwrap(),
        vec![json!("b@old")]
    );
    // The value the archive still held as current is retracted.
    assert!(
        read(&fluree, &restored, json!(restored), "ex:alice", "ex:email")
            .await
            .unwrap()
            .is_empty()
    );
}

fn email_query() -> JsonValue {
    json!({"@context": ctx(), "select": "?o", "where": {"@id": "ex:alice", "ex:email": "?o"}})
}

/// A view opened before a removal, whether from the cache or from a ledger
/// state the application holds, with or without a ledger cache, does not
/// serve the removed value: it is behind the head the removal committed, so
/// it reads history.
#[tokio::test]
async fn a_view_held_across_a_removal_refuses_the_removed_value() {
    for fluree in [
        FlureeBuilder::memory().build_memory(),
        FlureeBuilder::memory()
            .without_ledger_caching()
            .build_memory(),
    ] {
        let id = ledger(&fluree, "it/removal-held-view").await;
        insert(
            &fluree,
            &id,
            json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
        )
        .await;
        let view = fluree.db(&id).await.unwrap();
        let state = fluree.ledger(&id).await.unwrap();
        let held = fluree_db_api::GraphDb::from_ledger_state(&state);
        let rows = fluree.query(&view, &email_query()).await.expect("at head");
        assert_eq!(rows.to_jsonld(&view.snapshot).unwrap(), json!(["a@old"]));
        fluree
            .remove_occurrences(&id, select("alice", None))
            .await
            .expect("remove");
        for view in [&view, &held] {
            assert!(matches!(
                fluree.query(view, &email_query()).await,
                Err(ApiError::HistoryUnavailable(_))
            ));
        }
        // Nor once the cache has let the ledger go.
        fluree.disconnect_ledger(&id).await;
        for view in [&view, &held] {
            assert!(matches!(
                fluree.query(view, &email_query()).await,
                Err(ApiError::HistoryUnavailable(_))
            ));
        }
        assert!(read(&fluree, &id, json!(id), "ex:alice", "ex:email")
            .await
            .unwrap()
            .is_empty());

        // `f:noHistory` refuses nothing: a held view answers with the
        // value it holds.
        let counted = ledger(&fluree, "it/held-view-counter").await;
        set_no_history(&fluree, &counted, "ex:count", true).await;
        let doc = |v: i64| json!({"@context": ctx(), "@id": "ex:c", "ex:count": v});
        upsert(&fluree, &counted, doc(1)).await;
        fluree.run_retention(&counted).await.expect("pass");
        let view = fluree.db(&counted).await.unwrap();
        upsert(&fluree, &counted, doc(2)).await;
        let count =
            json!({"@context": ctx(), "select": "?o", "where": {"@id": "ex:c", "ex:count": "?o"}});
        let rows = fluree.query(&view, &count).await.expect("held view");
        assert_eq!(rows.to_jsonld(&view.snapshot).unwrap(), json!([1]));
    }
    let fluree = FlureeBuilder::memory().build_memory();

    // A view held across an ordinary commit of a ledger without retention
    // still answers as of its point.
    let plain = ledger(&fluree, "it/held-view-plain").await;
    insert(
        &fluree,
        &plain,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@plain"}),
    )
    .await;
    let view = fluree.db(&plain).await.unwrap();
    insert(
        &fluree,
        &plain,
        json!({"@context": ctx(), "@id": "ex:bob", "ex:email": "b@plain"}),
    )
    .await;
    let rows = fluree.query(&view, &email_query()).await.expect("query");
    assert_eq!(rows.to_jsonld(&view.snapshot).unwrap(), json!(["a@plain"]));
}

/// A copy restored from an archive made before `f:noHistory` was enabled
/// inherits the rule from the lineage's checkpoint, and keeps it when its
/// own retention pass rediscovers the older policy its commits hold.
#[tokio::test]
async fn a_restored_copy_keeps_an_inherited_rule_through_rediscovery() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let id = ledger(&fluree, "it/inherited-rule").await;
    let doc = |v: i64| json!({"@context": ctx(), "@id": "ex:c", "ex:count": v});
    upsert(&fluree, &id, doc(1)).await;
    upsert(&fluree, &id, doc(2)).await;
    let mut archive = Vec::new();
    fluree
        .archive_ledger(&id, false, &mut archive)
        .await
        .expect("archive");
    set_no_history(&fluree, &id, "ex:count", true).await;
    fluree.run_retention(&id).await.expect("pass");

    let copy = fluree
        .restore_ledger("it/inherited-rule-copy", &mut archive.as_slice())
        .await
        .expect("restore")
        .ledger_id;
    let inherited = history_rules(&fluree, &copy).await;
    assert_eq!(inherited.len(), 1, "{inherited:?}");
    fluree.run_retention(&copy).await.expect("pass on the copy");
    assert_eq!(history_rules(&fluree, &copy).await, inherited);
}

fn files_under(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}

/// A checkpoint that cannot be read is not taken for an absent one: the
/// restore that needs it fails rather than serving what it may suppress.
#[tokio::test]
async fn an_unreadable_checkpoint_fails_the_restore() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let name = "it/corrupt-checkpoint";
    let id = ledger(&fluree, name).await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    let mut archive = Vec::new();
    fluree
        .archive_ledger(&id, false, &mut archive)
        .await
        .expect("archive");
    fluree
        .remove_occurrences(&id, select("alice", None))
        .await
        .expect("remove");
    let objects = files_under(&dir.path().join("@retention/journal"));
    assert!(!objects.is_empty());
    for object in objects {
        std::fs::write(object, b"not a journal").unwrap();
    }
    fluree
        .drop_ledger(name, fluree_db_api::DropMode::Hard)
        .await
        .expect("purge");
    let copy = "it/corrupt-checkpoint-copy";
    assert!(fluree
        .restore_ledger(copy, &mut archive.as_slice())
        .await
        .is_err());
    assert!(fluree.ledger(&format!("{copy}:main")).await.is_err());
}

/// A removal whose checkpoint cannot be published reports failure, and a
/// retry publishes it: a removal reported complete survives its ledger.
#[cfg(unix)]
#[tokio::test]
async fn a_removal_is_not_complete_until_its_checkpoint_is_published() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let name = "it/unwritable-checkpoint";
    let id = ledger(&fluree, name).await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    let mut archive = Vec::new();
    fluree
        .archive_ledger(&id, false, &mut archive)
        .await
        .expect("archive");
    let journal_dir = dir.path().join("@retention/journal");
    std::fs::create_dir_all(&journal_dir).unwrap();
    std::fs::set_permissions(&journal_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let failed = fluree.remove_occurrences(&id, select("alice", None)).await;
    std::fs::set_permissions(&journal_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(failed.is_err(), "reported complete without its checkpoint");
    fluree
        .remove_occurrences(&id, select("alice", None))
        .await
        .expect("retry");

    fluree
        .drop_ledger(name, fluree_db_api::DropMode::Hard)
        .await
        .expect("purge");
    let copy = fluree
        .restore_ledger("it/unwritable-checkpoint-copy", &mut archive.as_slice())
        .await
        .expect("restore")
        .ledger_id;
    assert!(unavailable(
        read(
            &fluree,
            &copy,
            json!(format!("{copy}@t:{t1}")),
            "ex:alice",
            "ex:email"
        )
        .await
    ));
}

/// Copies of one lineage removing at once each add to the checkpoint; none
/// overwrites another's removal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removals_on_copies_at_once_all_reach_the_checkpoint() {
    const COPIES: usize = 8;
    let dir = tempfile::tempdir().unwrap();
    let fluree = std::sync::Arc::new(
        FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .build()
            .unwrap(),
    );
    let name = "it/concurrent-checkpoint";
    let id = ledger(&fluree, name).await;
    let people: Vec<JsonValue> = (0..COPIES)
        .map(|i| json!({"@id": format!("ex:p{i}"), "ex:email": format!("p{i}@old")}))
        .collect();
    let t1 = insert(&fluree, &id, json!({"@context": ctx(), "@graph": people})).await;
    let mut archive = Vec::new();
    fluree
        .archive_ledger(&id, false, &mut archive)
        .await
        .expect("archive");
    let mut copies = Vec::new();
    for i in 0..COPIES {
        let copy = format!("it/concurrent-checkpoint-{i}");
        fluree
            .restore_ledger(&copy, &mut archive.as_slice())
            .await
            .expect("restore");
        copies.push(copy);
    }
    let removals = copies.iter().enumerate().map(|(i, copy)| {
        let fluree = std::sync::Arc::clone(&fluree);
        let copy = format!("{copy}:main");
        tokio::spawn(async move {
            fluree
                .remove_occurrences(&copy, select(&format!("p{i}"), None))
                .await
        })
    });
    for removal in futures::future::join_all(removals).await {
        removal.unwrap().expect("remove");
    }
    for copy in copies.iter().map(String::as_str).chain([name]) {
        fluree
            .drop_ledger(copy, fluree_db_api::DropMode::Hard)
            .await
            .expect("purge");
    }

    let restored = fluree
        .restore_ledger("it/concurrent-checkpoint-last", &mut archive.as_slice())
        .await
        .expect("restore")
        .ledger_id;
    let at_t1 = json!(format!("{restored}@t:{t1}"));
    for i in 0..COPIES {
        assert!(
            unavailable(
                read(
                    &fluree,
                    &restored,
                    at_t1.clone(),
                    &format!("ex:p{i}"),
                    "ex:email"
                )
                .await
            ),
            "copy {i}'s removal was lost"
        );
    }
}

/// A restore of an archive from before a removal is not served until the
/// removed value it holds as current state is retracted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restore_serves_nothing_before_it_retracts_removed_values() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = std::sync::Arc::new(
        FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .build()
            .unwrap(),
    );
    let id = ledger(&fluree, "it/restore-race").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    let mut archive = Vec::new();
    fluree
        .archive_ledger(&id, false, &mut archive)
        .await
        .expect("archive");
    fluree
        .remove_occurrences(&id, select("alice", None))
        .await
        .expect("remove");

    let restoring = {
        let fluree = std::sync::Arc::clone(&fluree);
        tokio::spawn(async move {
            fluree
                .restore_ledger("it/restore-race-copy", &mut archive.as_slice())
                .await
        })
    };
    let copy = "it/restore-race-copy:main";
    let mut leaked = false;
    while !restoring.is_finished() {
        if let Ok(rows) = read(&fluree, copy, json!(copy), "ex:alice", "ex:email").await {
            leaked |= !rows.is_empty();
        }
        tokio::task::yield_now().await;
    }
    let restored = restoring.await.unwrap().expect("restore");
    assert!(!leaked, "the removed value was served during the restore");
    assert_eq!(restored.commit_t, t1 + 1, "the head is the retraction");
    assert!(read(&fluree, copy, json!(copy), "ex:alice", "ex:email")
        .await
        .unwrap()
        .is_empty());
}

/// A pass whose checkpoint cannot be published fails, and a later pass
/// publishes the journal it left unpublished.
#[cfg(unix)]
#[tokio::test]
async fn a_later_pass_publishes_a_journal_an_earlier_one_could_not() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let name = "it/unwritable-pass-checkpoint";
    let id = ledger(&fluree, name).await;
    let doc = |v: i64| json!({"@context": ctx(), "@id": "ex:c", "ex:count": v});
    upsert(&fluree, &id, doc(1)).await;
    upsert(&fluree, &id, doc(2)).await;
    let mut archive = Vec::new();
    fluree
        .archive_ledger(&id, false, &mut archive)
        .await
        .expect("archive");
    set_no_history(&fluree, &id, "ex:count", true).await;
    let journal_dir = dir.path().join("@retention/journal");
    std::fs::create_dir_all(&journal_dir).unwrap();
    std::fs::set_permissions(&journal_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let failed = fluree.run_retention(&id).await;
    std::fs::set_permissions(&journal_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        failed.is_err(),
        "a pass reported complete without its checkpoint"
    );
    fluree.run_retention(&id).await.expect("later pass");

    fluree
        .drop_ledger(name, fluree_db_api::DropMode::Hard)
        .await
        .expect("purge");
    let copy = fluree
        .restore_ledger(
            "it/unwritable-pass-checkpoint-copy",
            &mut archive.as_slice(),
        )
        .await
        .expect("restore")
        .ledger_id;
    assert_eq!(history_rules(&fluree, &copy).await.len(), 1);
}

/// A restore retracts a removed occurrence the archive holds as current, but
/// not the same value asserted again after the removal.
#[tokio::test]
async fn a_restore_keeps_a_value_reasserted_after_its_removal() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let id = ledger(&fluree, "it/reasserted").await;
    let doc = json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"});
    let t1 = insert(&fluree, &id, doc.clone()).await;
    fluree
        .remove_occurrences(&id, select("alice", None))
        .await
        .expect("remove");
    let t3 = insert(&fluree, &id, doc).await;
    let mut archive = Vec::new();
    fluree
        .archive_ledger(&id, false, &mut archive)
        .await
        .expect("archive");

    let restored = fluree
        .restore_ledger("it/reasserted-copy", &mut archive.as_slice())
        .await
        .expect("restore");
    let copy = restored.ledger_id;
    assert_eq!(restored.commit_t, t3, "nothing was retracted");
    assert_eq!(
        read(&fluree, &copy, json!(copy), "ex:alice", "ex:email")
            .await
            .unwrap(),
        vec![json!("a@old")]
    );
    assert!(unavailable(
        read(
            &fluree,
            &copy,
            json!(format!("{copy}@t:{t1}")),
            "ex:alice",
            "ex:email"
        )
        .await
    ));
}

/// A view of a ledger since purged, or replaced by another under its name,
/// does not serve what the dropped ledger had removed: its journal went
/// with it, so the view is refused. With and without a ledger cache, and on
/// an instance no event bus follows.
#[tokio::test]
async fn a_view_of_a_purged_or_replaced_ledger_is_refused() {
    use fluree_db_api::{ConnectionConfig, NameServiceMode};
    let dir = tempfile::tempdir().unwrap();
    let path = |sub: &str| dir.path().join(sub).to_string_lossy().to_string();
    let untracked = Fluree::new(
        ConnectionConfig::memory(),
        fluree_db_core::MemoryStorage::new(),
        NameServiceMode::ReadWrite(std::sync::Arc::new(
            fluree_db_nameservice::memory::MemoryNameService::new(),
        )),
    );
    for (what, fluree) in [
        (
            "cached",
            FlureeBuilder::file(path("cached")).build().unwrap(),
        ),
        (
            "uncached",
            FlureeBuilder::file(path("uncached"))
                .without_ledger_caching()
                .build()
                .unwrap(),
        ),
        ("untracked", untracked),
    ] {
        purged_or_replaced(&fluree, what).await;
    }
}

async fn purged_or_replaced(fluree: &Fluree, what: &str) {
    let name = "it/purged-view";
    let id = ledger(fluree, name).await;
    let t1 = insert(
        fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    insert(
        fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:bob", "ex:email": "b@old"}),
    )
    .await;
    let head = fluree.db(&id).await.unwrap();
    let at_t1 = fluree.db_at_t(&id, t1).await.unwrap();
    fluree
        .remove_occurrences(&id, select("alice", None))
        .await
        .expect("remove");
    for view in [&head, &at_t1] {
        assert!(matches!(
            fluree.query(view, &email_query()).await,
            Err(ApiError::HistoryUnavailable(_))
        ));
    }

    fluree
        .drop_ledger(name, fluree_db_api::DropMode::Hard)
        .await
        .expect("purge");
    for view in [&head, &at_t1] {
        assert!(
            fluree.query(view, &email_query()).await.is_err(),
            "a view of the purged ledger was served ({what})"
        );
    }
    ledger(fluree, name).await;
    for view in [&head, &at_t1] {
        assert!(
            fluree.query(view, &email_query()).await.is_err(),
            "a view of the replaced ledger was served ({what})"
        );
    }

    // A branch dropped and created again under the same name: the same
    // ledger, another branch.
    insert(
        fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    fluree
        .create_branch(name, "dev", None, None)
        .await
        .expect("branch");
    let dev = format!("{name}:dev");
    let on_dev = fluree.db(&dev).await.unwrap();
    fluree
        .remove_occurrences(&dev, select("alice", None))
        .await
        .expect("remove on the branch");
    fluree.drop_branch(name, "dev").await.expect("drop branch");
    assert!(
        fluree.query(&on_dev, &email_query()).await.is_err(),
        "a view of the dropped branch was served ({what})"
    );
    fluree
        .create_branch(name, "dev", None, None)
        .await
        .expect("branch again");
    assert!(
        fluree.query(&on_dev, &email_query()).await.is_err(),
        "a view of the replaced branch was served ({what})"
    );
}

/// A drop this instance did not make is seen too: one made by another
/// instance over the same nameservice and storage, found by the record's
/// fence, and one announced on the event bus, as a cluster replicates it.
#[tokio::test]
async fn a_view_is_refused_after_a_drop_made_elsewhere() {
    use fluree_db_api::{ConnectionConfig, NameServiceMode};
    let storage = fluree_db_core::MemoryStorage::new();
    let ns = std::sync::Arc::new(fluree_db_nameservice::memory::MemoryNameService::new());
    let instance = || {
        Fluree::new(
            ConnectionConfig::memory(),
            storage.clone(),
            NameServiceMode::ReadWrite(ns.clone()),
        )
    };
    let (here, elsewhere) = (instance(), instance());
    let name = "it/dropped-elsewhere";
    let id = ledger(&here, name).await;
    insert(
        &here,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    // A branch dropped and created again elsewhere: the same ledger instance,
    // another branch record.
    here.create_branch(name, "dev", None, None)
        .await
        .expect("branch");
    let dev = format!("{name}:dev");
    let held = here.db(&dev).await.unwrap();
    here.remove_occurrences(&dev, select("alice", None))
        .await
        .expect("remove");
    elsewhere
        .drop_branch(name, "dev")
        .await
        .expect("drop elsewhere");
    elsewhere
        .create_branch(name, "dev", None, None)
        .await
        .expect("branch again elsewhere");
    assert!(
        here.query(&held, &email_query()).await.is_err(),
        "a view of a branch replaced elsewhere was served"
    );

    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/dropped-replicated").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    let held = fluree.db(&id).await.unwrap();
    fluree
        .query(&held, &email_query())
        .await
        .expect("before the drop");
    fluree
        .event_bus()
        .notify(fluree_db_nameservice::NameServiceEvent::LedgerRetracted {
            ledger_id: fluree_db_api::LedgerId::parse(&id).unwrap(),
            instance: None,
        });
    assert!(
        fluree.query(&held, &email_query()).await.is_err(),
        "a view of a ledger dropped on another node was served"
    );
}

/// A view behind its ledger's head is served while the ledger suppresses
/// nothing, and refused once a removal makes it hold a removed value.
#[tokio::test]
async fn a_view_behind_the_head_is_checked_again_after_a_removal() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/behind-then-removed").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    let held = fluree.db(&id).await.unwrap();
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:bob", "ex:email": "b@old"}),
    )
    .await;
    let rows = fluree.query(&held, &email_query()).await.expect("behind");
    assert_eq!(rows.to_jsonld(&held.snapshot).unwrap(), json!(["a@old"]));
    fluree
        .remove_occurrences(&id, select("alice", None))
        .await
        .expect("remove");
    assert!(matches!(
        fluree.query(&held, &email_query()).await,
        Err(ApiError::HistoryUnavailable(_))
    ));
}

/// A branch holds its source's history, so it refuses what its source's
/// journal removed from the moment it exists.
#[tokio::test]
async fn a_branch_made_after_a_removal_refuses_the_same_history() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/removal-branch-after").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    fluree
        .remove_occurrences(&id, select("alice", Some(json!("a@old"))))
        .await
        .expect("remove");
    fluree
        .create_branch("it/removal-branch-after", "dev", None, None)
        .await
        .expect("branch");
    let dev = "it/removal-branch-after:dev";
    assert!(unavailable(
        read(
            &fluree,
            dev,
            json!(format!("{dev}@t:{t1}")),
            "ex:alice",
            "ex:email"
        )
        .await
    ));
}

/// A dropped ledger, restored, serves its history as it did before the
/// drop: its removal journal comes back with it, on memory and on file
/// storage, with a restart between the drop and the restore and after it.
#[tokio::test]
async fn a_restored_ledger_keeps_its_removal_history() {
    for storage in ["memory", "file", "file restarted"] {
        let dir = tempfile::tempdir().unwrap();
        let open = || match storage {
            "memory" => FlureeBuilder::memory().build_memory(),
            _ => FlureeBuilder::file(dir.path().to_string_lossy().to_string())
                .without_indexing()
                .build()
                .unwrap(),
        };
        let mut fluree = open();
        let id = ledger(&fluree, "it/restore-journal").await;
        let t1 = insert(
            &fluree,
            &id,
            json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
        )
        .await;
        fluree
            .remove_occurrences(&id, select("alice", Some(json!("a@old"))))
            .await
            .expect("remove");
        let at_t1 = json!(format!("{id}@t:{t1}"));
        let dropped = fluree
            .drop_ledger("it/restore-journal", fluree_db_api::DropMode::Soft)
            .await
            .unwrap();
        if storage == "file restarted" {
            drop(fluree);
            fluree = open();
        }
        fluree
            .restore_dropped(&dropped.instance.unwrap().to_string())
            .await
            .unwrap();
        assert!(
            unavailable(read(&fluree, &id, at_t1.clone(), "ex:alice", "ex:email").await),
            "{storage}"
        );
        if storage != "memory" {
            drop(fluree);
            let reopened = open();
            assert!(
                unavailable(read(&reopened, &id, at_t1, "ex:alice", "ex:email").await),
                "{storage}, reopened"
            );
        }
    }
}

/// An entry from before drops kept configs restores its ledger with the
/// removal journal its lineage's checkpoint holds.
#[tokio::test]
async fn a_restore_of_an_entry_without_configs_takes_the_journal_from_the_checkpoint() {
    use fluree_db_nameservice::DroppedLedger;
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/restore-legacy").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    fluree
        .remove_occurrences(&id, select("alice", Some(json!("a@old"))))
        .await
        .expect("remove");
    let instance = fluree
        .drop_ledger("it/restore-legacy", fluree_db_api::DropMode::Soft)
        .await
        .unwrap()
        .instance
        .unwrap();
    let store = fluree.publisher().unwrap();
    let held = store.get_dropped(&instance).await.unwrap().unwrap();
    let legacy = DroppedLedger {
        configs: None,
        ..held.value
    };
    store
        .cas_dropped(&instance, Some(held.version), Some(&legacy))
        .await
        .unwrap();

    fluree.restore_dropped(&instance.to_string()).await.unwrap();
    let at_t1 = json!(format!("{id}@t:{t1}"));
    assert!(unavailable(
        read(&fluree, &id, at_t1, "ex:alice", "ex:email").await
    ));
}

/// A soft-dropped ledger, restored, refuses what another copy of it
/// removed while it was dropped: the lineage's checkpoint is merged into
/// the configs its drop kept, and the value retracted where still current.
#[tokio::test]
async fn a_restored_ledger_takes_removals_made_on_another_copy_meanwhile() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let source = ledger(&fluree, "it/restore-copy-source").await;
    let t1 = insert(
        &fluree,
        &source,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    let mut archive = Vec::new();
    fluree
        .archive_ledger(&source, false, &mut archive)
        .await
        .unwrap();
    let copy = fluree
        .restore_ledger("it/restore-copy-other", &mut archive.as_slice())
        .await
        .unwrap()
        .ledger_id;
    let instance = fluree
        .drop_ledger("it/restore-copy-source", fluree_db_api::DropMode::Soft)
        .await
        .unwrap()
        .instance
        .unwrap();
    fluree
        .remove_occurrences(&copy, select("alice", Some(json!("a@old"))))
        .await
        .expect("remove");

    fluree.restore_dropped(instance.as_str()).await.unwrap();
    let at_t1 = json!(format!("{source}@t:{t1}"));
    assert!(unavailable(
        read(&fluree, &source, at_t1, "ex:alice", "ex:email").await
    ));
    assert!(
        read(&fluree, &source, json!(source), "ex:alice", "ex:email")
            .await
            .unwrap()
            .is_empty()
    );
}

/// A restored ledger that retracted a value itself, later than the copy
/// that removed it did, refuses the value up to its own retraction: the
/// entries merged from the copy end at the copy's retraction.
#[tokio::test]
async fn a_restored_ledger_journals_its_own_retraction_of_a_removed_value() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let source = ledger(&fluree, "it/restore-own-retraction").await;
    insert(
        &fluree,
        &source,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    let mut archive = Vec::new();
    fluree
        .archive_ledger(&source, false, &mut archive)
        .await
        .unwrap();
    let copy = fluree
        .restore_ledger("it/restore-own-retraction-copy", &mut archive.as_slice())
        .await
        .unwrap()
        .ledger_id;
    let t2 = insert(
        &fluree,
        &source,
        json!({"@context": ctx(), "@id": "ex:bob", "ex:email": "b@old"}),
    )
    .await;
    fluree
        .graph(&source)
        .transact()
        .update(&json!({
            "@context": ctx(),
            "delete": {"@id": "ex:alice", "ex:email": "a@old"}
        }))
        .commit()
        .await
        .unwrap();
    let instance = fluree
        .drop_ledger("it/restore-own-retraction", fluree_db_api::DropMode::Soft)
        .await
        .unwrap()
        .instance
        .unwrap();
    fluree
        .remove_occurrences(&copy, select("alice", Some(json!("a@old"))))
        .await
        .expect("remove");

    fluree.restore_dropped(instance.as_str()).await.unwrap();
    let at_t2 = json!(format!("{source}@t:{t2}"));
    assert!(unavailable(
        read(&fluree, &source, at_t2, "ex:alice", "ex:email").await
    ));
}

/// A ledger dropped and created again under its name is answered from its
/// own removal journal, not one held for the dropped ledger at the same
/// generation.
#[tokio::test]
async fn a_recreated_ledger_honors_its_own_removals() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let name = "it/removal-recreated";
    let id = ledger(&fluree, name).await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:bob", "ex:email": "b@old"}),
    )
    .await;
    fluree
        .remove_occurrences(&id, select("bob", Some(json!("b@old"))))
        .await
        .expect("remove");
    let at_t1 = json!(format!("{id}@t:{t1}"));
    assert!(unavailable(
        read(&fluree, &id, at_t1.clone(), "ex:bob", "ex:email").await
    ));
    fluree
        .drop_ledger(name, fluree_db_api::DropMode::Hard)
        .await
        .expect("drop");

    let id = ledger(&fluree, name).await;
    let again = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    assert_eq!(again, t1);
    fluree
        .remove_occurrences(&id, select("alice", Some(json!("a@old"))))
        .await
        .expect("remove");
    assert!(unavailable(
        read(&fluree, &id, at_t1, "ex:alice", "ex:email").await
    ));
}

/// A removal made before any retention pass registers the ledger without
/// passing over a `noHistory` policy already committed, even one already
/// disabled: the pass after it still finds the policy's interval.
#[tokio::test]
async fn a_removal_before_the_first_pass_keeps_an_earlier_policy() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/nohist-removal-first").await;
    upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 1}),
    )
    .await;
    set_no_history(&fluree, &id, "ex:count", true).await;
    upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 2}),
    )
    .await;
    set_no_history(&fluree, &id, "ex:count", false).await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    fluree
        .remove_occurrences(&id, select("alice", Some(json!("a@old"))))
        .await
        .expect("remove");

    fluree.run_retention(&id).await.expect("pass");
    let status = fluree.retention_status(&id).await.unwrap();
    let journal = &status.registration.as_ref().unwrap().journal;
    assert_eq!(journal.history_rules.len(), 1, "{status:?}");
    assert!(journal.history_rules[0].until_t.is_some(), "{status:?}");
}

/// A branch made at a point before a removal holds the removed value as
/// current, which history suppression does not cover: it is retracted as
/// the branch is made, and the retraction journaled, as a restore does.
#[tokio::test]
async fn a_branch_made_before_a_removal_holds_no_removed_value() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/removal-past-branch").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    let t2 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:bob", "ex:email": "b@old"}),
    )
    .await;
    fluree
        .remove_occurrences(&id, select("alice", Some(json!("a@old"))))
        .await
        .expect("remove");
    let made = fluree
        .create_branch(
            "it/removal-past-branch",
            "past",
            None,
            Some(fluree_db_api::TimeSpec::AtT(t2)),
        )
        .await
        .expect("branch");
    assert_eq!(made.commit_t, t2 + 1, "the retraction");
    let past = "it/removal-past-branch:past";
    assert_eq!(
        read(&fluree, past, json!(past), "ex:alice", "ex:email")
            .await
            .unwrap(),
        Vec::<JsonValue>::new()
    );
    assert_eq!(
        read(&fluree, past, json!(past), "ex:bob", "ex:email")
            .await
            .unwrap(),
        vec![json!("b@old")]
    );
    // Its own retraction is journaled: a past read of the branch that
    // could see the value is refused.
    assert!(unavailable(
        read(
            &fluree,
            past,
            json!(format!("{past}@t:{t2}")),
            "ex:alice",
            "ex:email"
        )
        .await
    ));
}
