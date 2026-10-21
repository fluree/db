//! `f:noHistory` and targeted removal: reads that need suppressed history
//! fail with history-unavailable; current reads, and reads of other
//! predicates, are answered as before.

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

#[tokio::test]
async fn no_history_refuses_reads_that_need_the_counters_past() {
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
    // Before processing, history is still served.
    assert_eq!(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t1}")),
            "ex:c",
            "ex:count"
        )
        .await
        .unwrap(),
        vec![json!(1)]
    );
    let report = fluree.run_retention(&id).await.expect("pass");
    assert!(!report.inactive, "{report:?}");
    let status = fluree.retention_status(&id).await.unwrap();
    let journal = &status.registration.as_ref().unwrap().journal;
    assert_eq!(journal.history_rules.len(), 1, "{status:?}");

    // Current state is answered.
    assert_eq!(
        read(&fluree, &id, json!(id), "ex:c", "ex:count")
            .await
            .unwrap(),
        vec![json!(3)]
    );
    assert_eq!(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t4}")),
            "ex:c",
            "ex:count"
        )
        .await
        .unwrap(),
        vec![json!(3)],
        "a pin at the head is a current read"
    );
    // A past point that reads the counter is refused...
    assert!(unavailable(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t1}")),
            "ex:c",
            "ex:count"
        )
        .await
    ));
    // ...a past point that reads another predicate is not.
    assert_eq!(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t1}")),
            "ex:c",
            "ex:name"
        )
        .await
        .unwrap(),
        vec![json!("C")]
    );
    // A variable predicate could read the counter.
    assert!(unavailable(
        read(&fluree, &id, json!(format!("{id}@t:{t1}")), "ex:c", "?p").await
    ));
    // A history range over the counter is refused.
    assert!(unavailable(
        read(
            &fluree,
            &id,
            json!({"from": format!("{id}@t:1"), "to": format!("{id}@t:latest")}),
            "ex:c",
            "ex:count"
        )
        .await
    ));
}

#[tokio::test]
async fn disabling_no_history_reopens_only_what_it_kept() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/nohist-disable").await;
    let t1 = upsert(
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
    fluree.run_retention(&id).await.expect("pass");
    let off = set_no_history(&fluree, &id, "ex:count", false).await;
    let t5 = upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 3}),
    )
    .await;
    upsert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:c", "ex:count": 4}),
    )
    .await;
    fluree.run_retention(&id).await.expect("pass");
    let status = fluree.retention_status(&id).await.unwrap();
    let rule = &status.registration.as_ref().unwrap().journal.history_rules[0];
    assert_eq!(rule.until_t, Some(off), "{status:?}");
    // Suppressed history stays suppressed.
    assert!(unavailable(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t1}")),
            "ex:c",
            "ex:count"
        )
        .await
    ));
    // History kept after the rule ended is served again.
    assert_eq!(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t5}")),
            "ex:c",
            "ex:count"
        )
        .await
        .unwrap(),
        vec![json!(3)]
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
    // Expired from current state, and its history is not served either.
    assert!(read(&fluree, &id, json!(id), "ex:d", "ex:status")
        .await
        .unwrap()
        .is_empty());
    assert!(unavailable(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t}")),
            "ex:d",
            "ex:status"
        )
        .await
    ));
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
    let mut backfilled = false;
    for _ in 0..200 {
        let status = fluree.retention_status(&id).await.unwrap();
        let r = status.registration.unwrap();
        if r.history_backfilled >= r.journal.generation && r.journal.generation > 0 {
            backfilled = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(backfilled, "the backfill rebuild did not finish");
    assert_eq!(
        indexed_history_entries(&fluree, &id).await,
        0,
        "the backfill left no history for the counter"
    );

    // Later changes leave no history in the next build either.
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
    // The worker records the rule and rebuilds once.
    let mut ready = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(r) = fluree.retention_status(&id).await.unwrap().registration {
            if r.journal.generation > 0 && r.history_backfilled >= r.journal.generation {
                ready = true;
                break;
            }
        }
    }
    assert!(ready, "the rule and its backfill");
    // Incremental builds over later changes add no history for the counter.
    for n in 2..=5 {
        upsert(
            &fluree,
            &id,
            json!({"@context": ctx(), "@id": "ex:c", "ex:count": n}),
        )
        .await;
    }
    let mut indexed = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let record = fluree.nameservice().lookup(&id).await.unwrap().unwrap();
        if record.index_t >= record.commit_t {
            indexed = true;
            break;
        }
    }
    assert!(indexed, "the background indexer caught up");
    assert_eq!(indexed_history_entries(&fluree, &id).await, 0);
    assert_eq!(
        read(&fluree, &id, json!(id), "ex:c", "ex:count")
            .await
            .unwrap(),
        vec![json!(5)]
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
    // The value it superseded while in force is not served.
    assert!(unavailable(
        read(
            &fluree,
            &id,
            json!(format!("{id}@t:{t1}")),
            "ex:c",
            "ex:count"
        )
        .await
    ));
    // Current state is.
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
        assert!(read(&fluree, &id, json!(id), "ex:alice", "ex:email")
            .await
            .unwrap()
            .is_empty());

        // Under `f:noHistory`, a held view's value superseded since is
        // history too.
        let counted = ledger(&fluree, "it/held-view-counter").await;
        set_no_history(&fluree, &counted, "ex:count", true).await;
        let doc = |v: i64| json!({"@context": ctx(), "@id": "ex:c", "ex:count": v});
        upsert(&fluree, &counted, doc(1)).await;
        fluree.run_retention(&counted).await.expect("pass");
        let view = fluree.db(&counted).await.unwrap();
        upsert(&fluree, &counted, doc(2)).await;
        let count =
            json!({"@context": ctx(), "select": "?o", "where": {"@id": "ex:c", "ex:count": "?o"}});
        assert!(matches!(
            fluree.query(&view, &count).await,
            Err(ApiError::HistoryUnavailable(_))
        ));
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
    let t1 = upsert(&fluree, &id, doc(1)).await;
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
    let at_t1 = json!(format!("{copy}@t:{t1}"));
    assert!(unavailable(
        read(&fluree, &copy, at_t1.clone(), "ex:c", "ex:count").await
    ));
    fluree.run_retention(&copy).await.expect("pass on the copy");
    assert!(unavailable(
        read(&fluree, &copy, at_t1, "ex:c", "ex:count").await
    ));
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
    let t1 = upsert(&fluree, &id, doc(1)).await;
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
    assert!(unavailable(
        read(
            &fluree,
            &copy,
            json!(format!("{copy}@t:{t1}")),
            "ex:c",
            "ex:count"
        )
        .await
    ));
}
