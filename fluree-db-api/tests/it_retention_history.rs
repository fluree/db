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
