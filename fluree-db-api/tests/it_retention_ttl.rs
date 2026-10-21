//! TTL from committed policy and metadata: per-value instructions (JSON-LD
//! `@ttl` / `@expiresAt`, SPARQL annotations), transaction defaults, and
//! `f:retentionDefaults` property policies, expired by background passes.
//!
//! Passes run explicitly with `run_retention_at(ledger, now_ms)`, so a test
//! moves the clock instead of sleeping.

use fluree_db_api::{Fluree, FlureeBuilder};
use serde_json::{json, Value as JsonValue};

const HOUR_MS: i64 = 3_600_000;

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "f": "https://ns.flur.ee/db#",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn ledger(fluree: &Fluree, name: &str) -> String {
    fluree.create_ledger(name).await.expect("create");
    format!("{name}:main")
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

/// The objects of `s p ?o` in `from` (a ledger id, optionally `@t:n`).
async fn objects(fluree: &Fluree, ledger_id: &str, from: &str, s: &str, p: &str) -> Vec<JsonValue> {
    let query = json!({
        "@context": ctx(),
        "from": from,
        "select": "?o",
        "where": {"@id": s, p: "?o"}
    });
    let results = fluree.query_connection(&query).await.expect("query");
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let mut rows: Vec<JsonValue> = results
        .to_jsonld(&ledger.snapshot)
        .expect("jsonld")
        .as_array()
        .cloned()
        .unwrap_or_default();
    rows.sort_by_key(ToString::to_string);
    rows
}

/// The number of `rdf:reifies` links in the default graph.
async fn link_count(fluree: &Fluree, ledger_id: &str) -> usize {
    let query = format!(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         SELECT ?r FROM <{ledger_id}> WHERE {{ ?r rdf:reifies ?t }}"
    );
    let results = fluree
        .query_connection_sparql(&query)
        .await
        .expect("sparql");
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    results
        .to_jsonld(&ledger.snapshot)
        .expect("jsonld")
        .as_array()
        .map_or(0, Vec::len)
}

#[tokio::test]
async fn jsonld_ttl_expires_the_value_and_keeps_its_history() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-jsonld").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({
            "@context": ctx(),
            "@id": "ex:device1",
            "ex:status": {"@value": "online", "@ttl": 3600},
            "ex:name": "Device 1"
        }),
    )
    .await;
    let start = now_ms();

    let report = fluree.run_retention_at(&id, start).await.expect("pass");
    assert_eq!(report.pending, 1, "{report:?}");
    assert_eq!(report.committed_t, None);
    assert_eq!(
        objects(&fluree, &id, &id, "ex:device1", "ex:status").await,
        vec![json!("online")]
    );

    let report = fluree
        .run_retention_at(&id, start + HOUR_MS + 1_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 1, "{report:?}");
    let expired_t = report.committed_t.expect("an expiration commit");
    assert!(expired_t > t1);

    // The value is gone from current state, and with it the TTL-only
    // annotation; other values of the subject remain.
    assert!(objects(&fluree, &id, &id, "ex:device1", "ex:status")
        .await
        .is_empty());
    assert_eq!(
        objects(&fluree, &id, &id, "ex:device1", "ex:name").await,
        vec![json!("Device 1")]
    );
    assert_eq!(link_count(&fluree, &id).await, 0);
    // History keeps it.
    assert_eq!(
        objects(
            &fluree,
            &id,
            &format!("{id}@t:{t1}"),
            "ex:device1",
            "ex:status"
        )
        .await,
        vec![json!("online")]
    );

    // The next pass discovers the expiration and has nothing left.
    let report = fluree
        .run_retention_at(&id, start + HOUR_MS + 2_000)
        .await
        .expect("pass");
    assert_eq!(report.pending, 0, "{report:?}");
    assert_eq!(report.committed_t, None);
    let status = fluree.retention_status(&id).await.expect("status");
    assert!(status.active);
    let registration = status.registration.expect("registration");
    assert_eq!(registration.expired_total, 1);
    assert_eq!(registration.pending, 0);
}

#[tokio::test]
async fn expires_at_sets_an_absolute_deadline() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-expires-at").await;
    insert(
        &fluree,
        &id,
        json!({
            "@context": ctx(),
            "@id": "ex:offer",
            "ex:price": {"@value": 10, "@expiresAt": "2031-01-01T02:00:00+02:00"}
        }),
    )
    .await;
    let deadline = 1_924_992_000_000; // 2031-01-01T00:00:00Z
    let report = fluree
        .run_retention_at(&id, deadline - 1)
        .await
        .expect("pass");
    assert_eq!((report.pending, report.expired), (1, 0));
    let report = fluree.run_retention_at(&id, deadline).await.expect("pass");
    assert_eq!(report.expired, 1, "{report:?}");
    assert!(objects(&fluree, &id, &id, "ex:offer", "ex:price")
        .await
        .is_empty());
}

#[tokio::test]
async fn a_ledger_without_retention_input_is_never_registered() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-none").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:a", "ex:status": {"@value": "x", "@annotation": {"ex:source": "y"}}}),
    )
    .await;
    let report = fluree.run_retention(&id).await.expect("pass");
    assert!(report.inactive, "{report:?}");
    let status = fluree.retention_status(&id).await.expect("status");
    assert!(!status.active);
}

#[tokio::test]
async fn property_ttl_from_config_applies_to_later_assertions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-property").await;
    // Asserted before the policy: never scheduled.
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:old", "ex:reading": 1}),
    )
    .await;
    fluree
        .graph(&id)
        .transact()
        .upsert_turtle(&format!(
            "@prefix f: <https://ns.flur.ee/db#> . @prefix ex: <http://example.org/> .
             GRAPH <urn:fluree:{id}#config> {{
               <urn:fluree:{id}:config:ledger> a f:LedgerConfig ;
                 f:retentionDefaults <urn:cfg:retention> .
               <urn:cfg:retention> a f:RetentionDefaults ; f:property <urn:cfg:reading> .
               <urn:cfg:reading> a f:RetentionProperty ; f:target ex:reading ; f:ttlSeconds 60 .
             }}"
        ))
        .commit()
        .await
        .expect("config");
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:new", "ex:reading": 2, "ex:label": "kept"}),
    )
    .await;
    let start = now_ms();
    let report = fluree.run_retention_at(&id, start).await.expect("pass");
    assert_eq!(report.pending, 1, "{report:?}");
    let report = fluree
        .run_retention_at(&id, start + 61_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 1, "{report:?}");
    assert!(objects(&fluree, &id, &id, "ex:new", "ex:reading")
        .await
        .is_empty());
    assert_eq!(
        objects(&fluree, &id, &id, "ex:new", "ex:label").await,
        vec![json!("kept")]
    );
    assert_eq!(
        objects(&fluree, &id, &id, "ex:old", "ex:reading").await,
        vec![json!(1)]
    );
}

#[tokio::test]
async fn reasserting_an_unchanged_value_does_not_renew_but_an_explicit_ttl_does() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-renew").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:s", "ex:status": {"@value": "on", "@ttl": 60}}),
    )
    .await;
    let start = now_ms();
    fluree.run_retention_at(&id, start).await.expect("pass");

    // A plain reassertion: no renewal.
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:s", "ex:status": "on"}),
    )
    .await;
    let report = fluree.run_retention_at(&id, start).await.expect("pass");
    assert_eq!(report.pending, 1);
    let due = fluree
        .retention_status(&id)
        .await
        .unwrap()
        .registration
        .unwrap()
        .next_due_ms
        .unwrap();
    assert!(due <= start + 61_000, "a reassertion moved the deadline");

    // An explicit TTL on the unchanged value renews it.
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:s", "ex:status": {"@value": "on", "@ttl": 7200}}),
    )
    .await;
    let report = fluree
        .run_retention_at(&id, start + 61_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 0, "renewed: {report:?}");
    assert_eq!(report.pending, 1);
    assert_eq!(
        objects(&fluree, &id, &id, "ex:s", "ex:status").await,
        vec![json!("on")]
    );
    // The superseded annotation is retired: one link left.
    let report = fluree
        .run_retention_at(&id, start + 62_000)
        .await
        .expect("pass");
    assert!(
        report.committed_t.is_none() || report.retired > 0,
        "{report:?}"
    );
    let _ = fluree.run_retention_at(&id, start + 63_000).await;
    assert_eq!(link_count(&fluree, &id).await, 1);

    // TTL 0 cancels.
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:s", "ex:status": {"@value": "on", "@ttl": 0}}),
    )
    .await;
    let report = fluree
        .run_retention_at(&id, start + 3 * HOUR_MS)
        .await
        .expect("pass");
    assert_eq!(report.expired, 0, "{report:?}");
    assert_eq!(report.pending, 0, "{report:?}");
    assert_eq!(
        objects(&fluree, &id, &id, "ex:s", "ex:status").await,
        vec![json!("on")]
    );
}

#[tokio::test]
async fn sparql_annotation_and_transaction_default_schedule_too() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-sparql").await;
    fluree
        .graph(&id)
        .transact()
        .sparql_update(
            "PREFIX ex: <http://example.org/> PREFIX f: <https://ns.flur.ee/db#>
             INSERT DATA { ex:d ex:status \"online\" {| f:ttlSeconds 60 |} . }",
        )
        .commit()
        .await
        .expect("sparql ttl");
    // A transaction default applies to the commit's new assertions.
    insert(
        &fluree,
        &id,
        json!({
            "@context": ctx(),
            "@graph": [{"@id": "ex:e", "ex:temp": 21}],
            "f:ttlSeconds": 120
        }),
    )
    .await;
    let start = now_ms();
    let report = fluree.run_retention_at(&id, start).await.expect("pass");
    assert_eq!(report.pending, 2, "{report:?}");
    let report = fluree
        .run_retention_at(&id, start + 61_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 1, "{report:?}");
    let report = fluree
        .run_retention_at(&id, start + 121_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 1, "{report:?}");
    assert!(objects(&fluree, &id, &id, "ex:d", "ex:status")
        .await
        .is_empty());
    assert!(objects(&fluree, &id, &id, "ex:e", "ex:temp")
        .await
        .is_empty());
}

#[tokio::test]
async fn independent_deadlines_and_typed_values() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-multi").await;
    insert(
        &fluree,
        &id,
        json!({
            "@context": ctx(),
            "@id": "ex:s",
            "ex:label": [
                {"@value": "chat", "@language": "fr", "@ttl": 60},
                {"@value": "chat", "@language": "en", "@ttl": 120}
            ],
            "ex:when": {"@value": "2026-01-01", "@type": "xsd:date", "@ttl": 60}
        }),
    )
    .await;
    let start = now_ms();
    let report = fluree.run_retention_at(&id, start).await.expect("pass");
    assert_eq!(report.pending, 3, "{report:?}");
    let report = fluree
        .run_retention_at(&id, start + 61_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 2, "{report:?}");
    let labels = objects(&fluree, &id, &id, "ex:s", "ex:label").await;
    assert_eq!(labels.len(), 1, "{labels:?}");
    assert!(labels[0].to_string().contains("en"), "{labels:?}");
    assert!(objects(&fluree, &id, &id, "ex:s", "ex:when")
        .await
        .is_empty());
}

#[tokio::test]
async fn retract_and_reassert_starts_a_new_occurrence() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-reassert").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:s", "ex:v": {"@value": "a", "@ttl": 60}}),
    )
    .await;
    fluree
        .graph(&id)
        .transact()
        .update(&json!({"@context": ctx(), "delete": {"@id": "ex:s", "ex:v": "a"}}))
        .commit()
        .await
        .expect("retract");
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:s", "ex:v": "a"}),
    )
    .await;
    let start = now_ms();
    let report = fluree
        .run_retention_at(&id, start + HOUR_MS)
        .await
        .expect("pass");
    // The old schedule ended with the retraction; the plain reassertion has
    // no TTL of its own.
    assert_eq!((report.pending, report.expired), (0, 0), "{report:?}");
    assert_eq!(
        objects(&fluree, &id, &id, "ex:s", "ex:v").await,
        vec![json!("a")]
    );
}

#[tokio::test]
async fn malformed_ttl_inputs_are_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-invalid").await;
    for bad in [
        json!({"@value": "x", "@ttl": -1}),
        json!({"@value": "x", "@ttl": 1.5}),
        json!({"@value": "x", "@ttl": "3600"}),
        json!({"@value": "x", "@ttl": "P1M"}),
        json!({"@value": "x", "@ttl": null}),
        json!({"@value": "x", "@ttl": 60, "@expiresAt": "2030-01-01T00:00:00Z"}),
        json!({"@value": "x", "@expiresAt": "2030-01-01"}),
        json!({"@value": "x", "@expiresAt": "2030-01-01T00:00:00"}),
        json!({"@value": "x", "@ttl": 60, "@annotation": {"f:ttlSeconds": 30}}),
    ] {
        let result = fluree
            .graph(&id)
            .transact()
            .insert(&json!({"@context": ctx(), "@id": "ex:s", "ex:v": bad.clone()}))
            .commit()
            .await;
        assert!(result.is_err(), "accepted {bad}");
    }
    // Annotation form: wrong types, two instructions on one annotation.
    for bad in [
        "{| f:ttlSeconds \"3600\" |}",
        "{| f:ttlSeconds -5 |}",
        "{| f:ttlSeconds 1.5 |}",
        "{| f:ttlSeconds 60 ; f:expiresAt \"2030-01-01T00:00:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> |}",
    ] {
        let result = fluree
            .graph(&id)
            .transact()
            .sparql_update(&format!(
                "PREFIX ex: <http://example.org/> PREFIX f: <https://ns.flur.ee/db#>
                 INSERT DATA {{ ex:s ex:v \"x\" {bad} . }}"
            ))
            .commit()
            .await;
        assert!(result.is_err(), "accepted {bad}");
    }
    // A transaction default with a calendar duration.
    let result = fluree
        .graph(&id)
        .transact()
        .insert(&json!({"@context": ctx(), "@graph": [{"@id": "ex:s", "ex:v": 1}], "f:ttlSeconds": "P1Y"}))
        .commit()
        .await;
    assert!(result.is_err());
    // A TTL predicate outside an annotation is not an instruction.
    let result = fluree
        .graph(&id)
        .transact()
        .insert(&json!({"@context": ctx(), "@id": "ex:s", "f:ttlSeconds": 60}))
        .commit()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn retention_config_is_validated() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-config-guard").await;
    let config = |entry: &str| {
        format!(
            "@prefix f: <https://ns.flur.ee/db#> . @prefix ex: <http://example.org/> .
             GRAPH <urn:fluree:{id}#config> {{
               <urn:fluree:{id}:config:ledger> a f:LedgerConfig ;
                 f:retentionDefaults <urn:cfg:retention> .
               <urn:cfg:retention> a f:RetentionDefaults ; {entry}
             }}"
        )
    };
    for (bad, why) in [
        (
            "f:property [ a f:RetentionProperty ; f:target ex:c ; f:noHistory true ] .",
            "not supported yet",
        ),
        (
            "f:property [ a f:RetentionProperty ; f:target ex:c ; f:ttlSeconds -1 ] .",
            "invalid value",
        ),
        (
            "f:property [ a f:RetentionProperty ; f:target ex:c ; f:ttlSeconds \"60\" ] .",
            "invalid value",
        ),
        (
            "f:property [ a f:RetentionProperty ; f:ttlSeconds 60 ] .",
            "without an IRI f:target",
        ),
        (
            "f:property [ a f:RetentionProperty ; f:target ex:c ; f:ttlSeconds 60 ] , \
             [ a f:RetentionProperty ; f:target ex:c ; f:ttlSeconds 30 ] .",
            "several f:RetentionProperty entries",
        ),
        ("f:overrideControl f:OverrideNone .", "f:overrideControl"),
        ("f:ttlSeconds 60 .", "belongs on an f:RetentionProperty"),
        (
            "f:property [ a f:RetentionProperty ; f:target f:ttlSeconds ; f:ttlSeconds 60 ] .",
            "system predicate",
        ),
    ] {
        let err = fluree
            .graph(&id)
            .transact()
            .upsert_turtle(&config(bad))
            .commit()
            .await
            .expect_err(bad);
        assert!(err.to_string().contains(why), "{bad}: {err}");
    }
    fluree
        .graph(&id)
        .transact()
        .upsert_turtle(&config(
            "f:property [ a f:RetentionProperty ; f:target ex:c ; f:ttlSeconds 0 ; f:noHistory false ] .",
        ))
        .commit()
        .await
        .expect("a valid config");
}

async fn cypher(fluree: &Fluree, ledger_id: &str, statement: &str) -> fluree_db_api::Result<()> {
    let mut txn = fluree
        .begin_cypher_transaction(
            ledger_id,
            fluree_db_api::GovernanceOptions::default(),
            fluree_db_api::TrackingOptions::default(),
        )
        .await?;
    fluree
        .cypher_transaction_write(&mut txn, statement, None)
        .await?;
    fluree.commit_cypher_transaction(txn).await.map(|_| ())
}

async fn cypher_rows(fluree: &Fluree, ledger_id: &str, query: &str) -> usize {
    let db = fluree.db(ledger_id).await.unwrap();
    let result = fluree.query_cypher(&db, query).await.unwrap();
    result.to_cypher_typed_table(&db).await.unwrap().1.len()
}

#[tokio::test]
async fn cypher_wrappers_schedule_scalar_properties() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-cypher").await;
    cypher(
        &fluree,
        &id,
        r#"CREATE (:Device {id: "d1", status: fluree.ttl("online", 60), model: "x"})"#,
    )
    .await
    .expect("create with fluree.ttl");
    cypher(
        &fluree,
        &id,
        r#"MATCH (d:Device {id: "d1"}) SET d.mode = fluree.expiresAt("eco", datetime("2031-01-01T00:00:00Z"))"#,
    )
    .await
    .expect("set with fluree.expiresAt");
    // A wrapper is a write value only.
    let db = fluree.db(&id).await.unwrap();
    assert!(fluree
        .query_cypher(&db, "MATCH (d:Device) RETURN fluree.ttl(d.status, 5)")
        .await
        .is_err());
    assert!(cypher(
        &fluree,
        &id,
        r#"MATCH (d:Device) SET d.status = fluree.ttl("on", "P1M")"#
    )
    .await
    .is_err());
    assert!(cypher(
        &fluree,
        &id,
        r#"MATCH (d:Device) SET d.mode = fluree.expiresAt("x", "2031-01-01T00:00:00")"#
    )
    .await
    .is_err());

    let start = now_ms();
    let report = fluree.run_retention_at(&id, start).await.expect("pass");
    assert_eq!(report.pending, 2, "{report:?}");
    let report = fluree
        .run_retention_at(&id, start + 61_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 1, "{report:?}");
    assert_eq!(
        cypher_rows(
            &fluree,
            &id,
            "MATCH (d:Device) WHERE d.status IS NOT NULL RETURN d"
        )
        .await,
        0
    );
    assert_eq!(
        cypher_rows(
            &fluree,
            &id,
            "MATCH (d:Device) WHERE d.model = 'x' RETURN d"
        )
        .await,
        1
    );
    let report = fluree
        .run_retention_at(&id, 1_924_992_000_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 1, "{report:?}");
    assert_eq!(
        cypher_rows(
            &fluree,
            &id,
            "MATCH (d:Device) WHERE d.mode IS NOT NULL RETURN d"
        )
        .await,
        0
    );
}

/// Commit a JSON-LD insert without the API's post-staging checks: the way
/// annotations written before TTL support reach a ledger. No
/// `f:retentionControl` marker.
async fn commit_unmarked(fluree: &Fluree, ledger_id: &str, doc: &JsonValue) {
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::parse_transaction(
        doc,
        fluree_db_transact::TxnType::Insert,
        fluree_db_transact::TxnOpts::default(),
        &mut ns,
        ledger_id,
    )
    .expect("parse");
    let (view, ns) =
        fluree_db_transact::stage(ledger, txn, ns, fluree_db_transact::StageOptions::new())
            .await
            .expect("stage");
    fluree
        .commit_staged(
            view,
            ns,
            &fluree.default_index_config(),
            fluree_db_api::CommitOpts::default(),
        )
        .await
        .expect("commit");
}

#[tokio::test]
async fn annotations_written_before_the_activation_boundary_are_not_instructions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-boundary").await;
    commit_unmarked(
        &fluree,
        &id,
        &json!({
            "@context": ctx(),
            "@id": "ex:s",
            "ex:v": {"@value": "old", "@annotation": {"f:ttlSeconds": 1}}
        }),
    )
    .await;
    let report = fluree
        .run_retention_at(&id, now_ms() + HOUR_MS)
        .await
        .expect("pass");
    assert!(
        report.inactive,
        "an unmarked annotation activated: {report:?}"
    );

    // A marked write activates the ledger from its own commit: the earlier
    // annotation still is not an instruction.
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:s", "ex:w": {"@value": "new", "@ttl": 1}}),
    )
    .await;
    let report = fluree
        .run_retention_at(&id, now_ms() + HOUR_MS)
        .await
        .expect("pass");
    assert_eq!(report.expired, 1, "{report:?}");
    assert_eq!(
        objects(&fluree, &id, &id, "ex:s", "ex:v").await,
        vec![json!("old")]
    );
    assert!(objects(&fluree, &id, &id, "ex:s", "ex:w").await.is_empty());
}

#[tokio::test]
async fn ttl_on_a_value_shared_by_several_annotations_is_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-shared").await;
    fluree
        .graph(&id)
        .transact()
        .sparql_update(
            "PREFIX ex: <http://example.org/>
             INSERT DATA { ex:a ex:knows ex:b {| ex:since 2020 |} .
                           ex:a ex:knows ex:b {| ex:since 2021 |} . }",
        )
        .commit()
        .await
        .expect("two relationships on one triple");
    let err = fluree
        .graph(&id)
        .transact()
        .sparql_update(
            "PREFIX ex: <http://example.org/> PREFIX f: <https://ns.flur.ee/db#>
             INSERT DATA { ex:a ex:knows ex:b {| f:ttlSeconds 60 |} . }",
        )
        .commit()
        .await
        .expect_err("TTL on a shared triple");
    assert!(err.to_string().contains("share"), "{err}");

    // One annotation plus a TTL on the same reifier is an unambiguous value.
    fluree
        .graph(&id)
        .transact()
        .sparql_update(
            "PREFIX ex: <http://example.org/> PREFIX f: <https://ns.flur.ee/db#>
             INSERT DATA { ex:a ex:likes ex:b {| ex:since 2020 ; f:ttlSeconds 60 |} . }",
        )
        .commit()
        .await
        .expect("TTL with provenance on one annotation");
}

#[tokio::test]
async fn named_graph_values_expire_in_their_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-named-graph").await;
    insert(
        &fluree,
        &id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:s", "@graph": "ex:g1", "ex:v": {"@value": "in-g1", "@ttl": 60}},
                {"@id": "ex:s", "ex:v": "in-default"}
            ]
        }),
    )
    .await;
    let start = now_ms();
    let report = fluree
        .run_retention_at(&id, start + 61_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 1, "{report:?}");
    assert_eq!(
        objects(&fluree, &id, &id, "ex:s", "ex:v").await,
        vec![json!("in-default")]
    );
    let query = json!({
        "@context": ctx(),
        "from": id,
        "select": "?o",
        "where": {"@graph": "ex:g1", "@id": "ex:s", "ex:v": "?o"}
    });
    let in_g1 = fluree.query_connection(&query).await.expect("query");
    let ledger = fluree.ledger(&id).await.unwrap();
    assert_eq!(
        in_g1.to_jsonld(&ledger.snapshot).unwrap(),
        json!([]),
        "the named-graph value expired"
    );
}

#[tokio::test]
async fn the_schedule_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    let start = now_ms();
    let id = {
        let fluree = FlureeBuilder::file(path.clone())
            .without_indexing()
            .build()
            .expect("fluree");
        let id = ledger(&fluree, "it/ttl-restart").await;
        insert(
            &fluree,
            &id,
            json!({"@context": ctx(), "@id": "ex:s", "ex:v": {"@value": "x", "@ttl": 60}}),
        )
        .await;
        let report = fluree.run_retention_at(&id, start).await.expect("pass");
        assert_eq!(report.pending, 1);
        id
    };
    let fluree = FlureeBuilder::file(path)
        .without_indexing()
        .build()
        .expect("reopen");
    let registration = fluree
        .retention_status(&id)
        .await
        .expect("status")
        .registration
        .expect("still registered");
    assert!(registration.schedule.is_some(), "{registration:?}");
    assert_eq!(registration.pending, 1);
    let report = fluree
        .run_retention_at(&id, start + 61_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 1, "{report:?}");
    assert_eq!(report.discovered_commits, 0, "rebuilt instead of loaded");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_background_worker_expires_on_its_own() {
    use std::sync::Arc;
    use std::time::Duration;
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = Arc::new(
        FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .with_indexer_catchup_interval(Duration::from_millis(200))
            .build()
            .expect("fluree"),
    );
    assert!(fluree.start_maintenance(
        None,
        Some(fluree_db_api::retention::RetentionOptions {
            interval: Duration::from_millis(100),
            ..Default::default()
        })
    ));
    let id = ledger(&fluree, "it/ttl-worker").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:s", "ex:v": {"@value": "x", "@ttl": 1}, "ex:k": "kept"}),
    )
    .await;
    let mut gone = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if objects(&fluree, &id, &id, "ex:s", "ex:v").await.is_empty() {
            gone = true;
            break;
        }
    }
    let status = fluree.retention_status(&id).await.expect("status");
    assert!(gone, "not expired by the worker: {status:?}");
    assert!(status.active && status.maintenance_running, "{status:?}");
    assert_eq!(
        objects(&fluree, &id, &id, "ex:s", "ex:k").await,
        vec![json!("kept")]
    );
}

#[tokio::test]
async fn setting_a_lifetime_needs_permission_to_retract_the_value() {
    use fluree_db_api::policy_builder::build_policy_context_from_opts;
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-permission").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:victim", "ex:status": "online"}),
    )
    .await;
    // A TTL instruction through `@reifies` does not restate the value, so
    // the write itself never touches ex:status.
    let claim = json!({
        "@context": ctx(),
        "@id": "ex:claim",
        "f:ttlSeconds": 1,
        "@reifies": {"@id": "ex:victim", "ex:status": "online"}
    });
    let state = fluree.ledger(&id).await.unwrap();
    let policy = build_policy_context_from_opts(
        &state.snapshot,
        state.novelty.as_ref(),
        Some(state.novelty.as_ref()),
        state.t(),
        &fluree_db_api::GovernanceOptions {
            policy: Some(json!([{
                "@id": "http://example.org/denyStatus",
                "f:required": true,
                "f:onProperty": [{"@id": "http://example.org/status"}],
                "f:action": "f:modify",
                "f:allow": false
            }])),
            default_allow: Some(true),
            ..Default::default()
        },
        &[0],
    )
    .await
    .expect("policy");
    let refused = fluree
        .stage_owned(state)
        .insert(&claim)
        .policy(policy)
        .execute()
        .await;
    assert!(
        refused.is_err(),
        "a TTL on a value the identity may not retract was accepted"
    );
    // An identity that may modify the value renews it.
    insert(&fluree, &id, claim).await;
    let report = fluree
        .run_retention_at(&id, now_ms() + 2_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 1, "{report:?}");
}

#[tokio::test]
async fn a_batch_verified_at_an_older_head_is_not_committed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-race").await;
    let t = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:s", "ex:v": {"@value": "x", "@ttl": 60}}),
    )
    .await;
    let state = fluree.ledger(&id).await.unwrap();
    let stored = fluree_db_core::range_with_overlay(
        &state.snapshot,
        0,
        state.novelty.as_ref(),
        fluree_db_core::IndexType::Spot,
        fluree_db_core::RangeTest::Eq,
        fluree_db_core::RangeMatch::subject(
            state.snapshot.encode_iri("http://example.org/s").unwrap(),
        ),
        fluree_db_core::RangeOptions::default(),
    )
    .await
    .unwrap();
    let retract = fluree_db_core::Flake {
        op: false,
        ..stored[0].clone()
    };
    let batch = fluree_db_api::retention::RetentionBatch {
        expected_t: t,
        retractions: vec![retract],
        expired: 1,
        retired: 0,
    };
    // A renewal lands first.
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:s", "ex:v": {"@value": "x", "@ttl": 7200}}),
    )
    .await;
    let raced = fluree.commit_retention_batch(&id, batch.clone()).await;
    assert!(
        raced
            .as_ref()
            .is_err_and(fluree_db_api::ApiError::is_commit_conflict),
        "{raced:?}"
    );
    assert_eq!(
        fluree_db_api::retention::outcome_of(raced).unwrap(),
        fluree_db_api::retention::SubmitOutcome::HeadMoved
    );
    // The recomputed pass honors the renewal.
    let report = fluree
        .run_retention_at(&id, now_ms() + 61_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 0, "{report:?}");
    assert_eq!(
        objects(&fluree, &id, &id, "ex:s", "ex:v").await,
        vec![json!("x")]
    );
}

#[tokio::test]
async fn a_duplicate_batch_does_not_commit_twice() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-duplicate").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:s", "ex:v": {"@value": "x", "@ttl": 60}}),
    )
    .await;
    let start = now_ms();
    let first = fluree
        .run_retention_at(&id, start + 61_000)
        .await
        .expect("pass");
    let t = first.committed_t.expect("expired");
    // Running again (a retry after a lost response) commits nothing more.
    for _ in 0..2 {
        let again = fluree
            .run_retention_at(&id, start + 61_000)
            .await
            .expect("pass");
        assert_eq!(again.committed_t, None, "{again:?}");
    }
    let ledger = fluree.ledger(&id).await.unwrap();
    assert_eq!(ledger.t(), t);
}

#[tokio::test]
async fn overdue_work_is_reported_when_no_worker_runs() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-no-worker").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:s", "ex:v": {"@value": "x", "@expiresAt": "2020-01-01T00:00:00Z"}}),
    )
    .await;
    // A pass as of before the deadline schedules it without expiring it.
    let report = fluree
        .run_retention_at(&id, 1_500_000_000_000)
        .await
        .expect("pass");
    assert_eq!((report.pending, report.expired), (1, 0));
    let status = fluree.retention_status(&id).await.expect("status");
    assert!(!status.maintenance_running, "{status:?}");
    assert_eq!(status.overdue, Some(1), "{status:?}");
}

#[tokio::test]
async fn policy_changes_apply_to_later_assertions_only() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-policy-change").await;
    let config = |ttl: u64| {
        format!(
            "@prefix f: <https://ns.flur.ee/db#> . @prefix ex: <http://example.org/> .
             GRAPH <urn:fluree:{id}#config> {{
               <urn:fluree:{id}:config:ledger> a f:LedgerConfig ;
                 f:retentionDefaults <urn:cfg:retention> .
               <urn:cfg:retention> a f:RetentionDefaults ; f:property <urn:cfg:p> .
               <urn:cfg:p> a f:RetentionProperty ; f:target ex:reading ; f:ttlSeconds {ttl} .
             }}"
        )
    };
    let set = |ttl: u64| {
        let fluree = &fluree;
        let text = config(ttl);
        let id = id.clone();
        async move {
            fluree
                .graph(&id)
                .transact()
                .upsert_turtle(&text)
                .commit()
                .await
                .expect("config");
        }
    };
    set(60).await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:a", "ex:reading": 1}),
    )
    .await;
    // Turning the duration off does not cancel the existing schedule.
    set(0).await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:b", "ex:reading": 2}),
    )
    .await;
    let report = fluree
        .run_retention_at(&id, now_ms() + HOUR_MS)
        .await
        .expect("pass");
    assert_eq!(report.expired, 1, "{report:?}");
    assert!(objects(&fluree, &id, &id, "ex:a", "ex:reading")
        .await
        .is_empty());
    assert_eq!(
        objects(&fluree, &id, &id, "ex:b", "ex:reading").await,
        vec![json!(2)]
    );
}

#[tokio::test]
async fn sharing_that_appears_after_scheduling_leaves_the_value_in_place() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-shared-later").await;
    fluree
        .graph(&id)
        .transact()
        .sparql_update(
            "PREFIX ex: <http://example.org/> PREFIX f: <https://ns.flur.ee/db#>
             INSERT DATA { ex:a ex:knows ex:b {| ex:since 2020 ; f:ttlSeconds 60 |} . }",
        )
        .commit()
        .await
        .expect("ttl on one relationship");
    // A second relationship on the same triple, written without a TTL.
    fluree
        .graph(&id)
        .transact()
        .sparql_update(
            "PREFIX ex: <http://example.org/>
             INSERT DATA { ex:a ex:knows ex:b {| ex:since 2021 |} . }",
        )
        .commit()
        .await
        .expect("a parallel relationship");
    let report = fluree
        .run_retention_at(&id, now_ms() + 61_000)
        .await
        .expect("pass");
    assert_eq!(report.expired, 0, "{report:?}");
    assert!(
        report.problems.iter().any(|p| p.contains("shared")),
        "{report:?}"
    );
    assert_eq!(
        objects(&fluree, &id, &id, "ex:a", "ex:knows").await.len(),
        1
    );
}

#[tokio::test]
async fn expiry_keeps_the_commit_log() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = ledger(&fluree, "it/ttl-log").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:s", "ex:v": {"@value": "x", "@ttl": 60}}),
    )
    .await;
    let report = fluree
        .run_retention_at(&id, now_ms() + 61_000)
        .await
        .expect("pass");
    let t = report.committed_t.expect("expired");
    let log = fluree.commit_log(&id, None).await.expect("commit log");
    assert_eq!(log.0.len() as i64, t, "every commit is still in the log");
}
