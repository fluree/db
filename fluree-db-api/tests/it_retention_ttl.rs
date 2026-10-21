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
