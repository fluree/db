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
