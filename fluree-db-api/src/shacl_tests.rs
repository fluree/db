use crate::{ApiError, FlureeBuilder};
use fluree_db_transact::TransactError;
use serde_json::{json, Value as JsonValue};

fn default_context() -> JsonValue {
    json!({
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "sh": "http://www.w3.org/ns/shacl#",
        "schema": "http://schema.org/",
        "skos": "http://www.w3.org/2008/05/skos#",
        "wiki": "https://www.wikidata.org/wiki/",
        "f": "https://ns.flur.ee/db#"
    })
}

fn shacl_context() -> JsonValue {
    json!([default_context(), {"ex": "http://example.org/ns/"}])
}

fn assert_shacl_violation(err: ApiError, expected: &str) {
    match err {
        ApiError::Transact(TransactError::ShaclViolation(message)) => {
            assert!(
                message.contains(expected),
                "expected violation to contain '{expected}', got: {message}"
            );
        }
        other => panic!("expected SHACL violation, got {other:?}"),
    }
}

#[tokio::test]
async fn shacl_cardinality_constraints() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:UserShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:name"},
            "sh:minCount": 1,
            "sh:maxCount": 1,
            "sh:datatype": {"@id": "xsd:string"}
        }]
    });

    let ledger_ok = fluree
        .create_ledger("shacl/cardinality-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:john",
                "@type": "ex:User",
                "schema:name": "John"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": "?name",
        "where": {"@id": "ex:john", "schema:name": "?name"}
    });
    let db = crate::GraphDb::from_ledger_state(&ledger_ok);
    let result = fluree.query(&db, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.snapshot).unwrap();
    assert_eq!(jsonld, json!(["John"]));

    let ledger_min = fluree
        .create_ledger("shacl/cardinality-min:main")
        .await
        .unwrap();
    let ledger_min = fluree.upsert(ledger_min, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_min,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alex",
                "@type": "ex:User"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "Expected at least 1 value(s) but found 0");

    let ledger_max = fluree
        .create_ledger("shacl/cardinality-max:main")
        .await
        .unwrap();
    let ledger_max = fluree.upsert(ledger_max, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_max,
            &json!({
                "@context": context.clone(),
                "@id": "ex:brian",
                "@type": "ex:User",
                "schema:name": ["Brian", "Bri"]
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "Expected at most 1 value(s) but found 2");
}

#[tokio::test]
async fn shacl_datatype_constraints() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:UserShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:name"},
            "sh:datatype": {"@id": "xsd:string"}
        }]
    });

    let ledger_ok = fluree
        .create_ledger("shacl/datatype-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:john",
                "@type": "ex:User",
                "schema:name": "John"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": "?name",
        "where": {"@id": "ex:john", "schema:name": "?name"}
    });
    let db = crate::GraphDb::from_ledger_state(&ledger_ok);
    let result = fluree.query(&db, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.snapshot).unwrap();
    assert_eq!(jsonld, json!(["John"]));

    let ledger_bad = fluree
        .create_ledger("shacl/datatype-bad:main")
        .await
        .unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:john",
                "@type": "ex:User",
                "schema:name": { "@value": 42, "@type": "xsd:integer" }
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "Expected datatype");
}

#[tokio::test]
async fn shacl_range_constraints() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:AgeShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:age"},
            "sh:minExclusive": 1,
            "sh:maxInclusive": 100
        }]
    });

    let ledger_min = fluree.create_ledger("shacl/range-min:main").await.unwrap();
    let ledger_min = fluree.upsert(ledger_min, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_min,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:age": 1
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "must be greater than");

    let ledger_max = fluree.create_ledger("shacl/range-max:main").await.unwrap();
    let ledger_max = fluree.upsert(ledger_max, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_max,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:age": 101
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "exceeds maximum");
}

#[tokio::test]
async fn shacl_length_constraints() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:NameShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:name"},
            "sh:minLength": 4,
            "sh:maxLength": 10
        }]
    });

    let ledger_min = fluree.create_ledger("shacl/length-min:main").await.unwrap();
    let ledger_min = fluree.upsert(ledger_min, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_min,
            &json!({
                "@context": context.clone(),
                "@id": "ex:al",
                "@type": "ex:User",
                "schema:name": "Al"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "less than minimum");

    let ledger_max = fluree.create_ledger("shacl/length-max:main").await.unwrap();
    let ledger_max = fluree.upsert(ledger_max, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_max,
            &json!({
                "@context": context.clone(),
                "@id": "ex:jean-claude",
                "@type": "ex:User",
                "schema:name": "Jean-Claude"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "exceeds maximum");
}

#[tokio::test]
async fn shacl_pattern_constraints() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:GreetingShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "ex:greeting"},
            "sh:pattern": "hello .* world"
        }]
    });

    let ledger_ok = fluree.create_ledger("shacl/pattern-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "ex:greeting": "hello big world"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": "?greeting",
        "where": {"@id": "ex:alice", "ex:greeting": "?greeting"}
    });
    let db = crate::GraphDb::from_ledger_state(&ledger_ok);
    let result = fluree.query(&db, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.snapshot).unwrap();
    assert_eq!(jsonld, json!(["hello big world"]));

    let ledger_bad = fluree
        .create_ledger("shacl/pattern-bad:main")
        .await
        .unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "ex:greeting": "goodbye world"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "does not match pattern");
}

#[tokio::test]
async fn shacl_has_value_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:RoleShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:role"},
            "sh:hasValue": "admin"
        }]
    });

    let ledger_ok = fluree
        .create_ledger("shacl/has-value-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:role": "admin"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": "?role",
        "where": {"@id": "ex:alice", "schema:role": "?role"}
    });
    let db = crate::GraphDb::from_ledger_state(&ledger_ok);
    let result = fluree.query(&db, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.snapshot).unwrap();
    assert_eq!(jsonld, json!(["admin"]));

    let ledger_bad = fluree
        .create_ledger("shacl/has-value-bad:main")
        .await
        .unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:role": "user"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "Required value");
}

#[tokio::test]
async fn shacl_node_kind_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:HomepageShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:homepage"},
            "sh:nodeKind": {"@id": "sh:IRI"}
        }]
    });

    let ledger_ok = fluree
        .create_ledger("shacl/node-kind-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:homepage": {"@id": "ex:homepage"}
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": "?home",
        "where": {"@id": "ex:alice", "schema:homepage": "?home"}
    });
    let db = crate::GraphDb::from_ledger_state(&ledger_ok);
    let result = fluree.query(&db, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.snapshot).unwrap();
    assert_eq!(jsonld, json!(["ex:homepage"]));

    let ledger_bad = fluree
        .create_ledger("shacl/node-kind-bad:main")
        .await
        .unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:homepage": "not a valid IRI"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "Expected node kind");
}

// =============================================================================
// sh:closed + sh:ignoredProperties tests
// =============================================================================

#[tokio::test]
async fn shacl_closed_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Shape that only allows name and age properties
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:PersonShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Person"},
        "sh:closed": true,
        "sh:property": [
            {
                "@id": "ex:pshape1",
                "sh:path": {"@id": "schema:name"}
            },
            {
                "@id": "ex:pshape2",
                "sh:path": {"@id": "schema:age"}
            }
        ]
    });

    // Valid: only uses declared properties
    let ledger_ok = fluree.create_ledger("shacl/closed-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:Person",
                "schema:name": "Alice",
                "schema:age": 30
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": "?name",
        "where": {"@id": "ex:alice", "schema:name": "?name"}
    });
    let db = crate::GraphDb::from_ledger_state(&ledger_ok);
    let result = fluree.query(&db, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.snapshot).unwrap();
    assert_eq!(jsonld, json!(["Alice"]));

    // Invalid: uses undeclared property (schema:email)
    let ledger_bad = fluree.create_ledger("shacl/closed-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:bob",
                "@type": "ex:Person",
                "schema:name": "Bob",
                "schema:email": "bob@example.org"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "not allowed by closed shape");
}

#[tokio::test]
async fn shacl_closed_with_ignored_properties() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Shape that allows name, plus ignores rdf:type
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:PersonShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Person"},
        "sh:closed": true,
        "sh:ignoredProperties": { "@list": [{"@id": "rdf:type"}] },
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:name"}
        }]
    });

    // Valid: rdf:type is ignored even though not declared
    let ledger_ok = fluree
        .create_ledger("shacl/closed-ignored-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let _ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:Person",
                "schema:name": "Alice"
            }),
        )
        .await
        .unwrap()
        .ledger;
}

// =============================================================================
// sh:pattern with sh:flags tests
// =============================================================================

#[tokio::test]
async fn shacl_pattern_with_flags() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Shape with case-insensitive pattern (sh:flags "i")
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:GreetingShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Message"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "ex:text"},
            "sh:pattern": "hello",
            "sh:flags": "i"
        }]
    });

    // Valid: "HELLO" matches "hello" with case-insensitive flag
    let ledger_ok = fluree
        .create_ledger("shacl/pattern-flags-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:msg1",
                "@type": "ex:Message",
                "ex:text": "HELLO WORLD"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": "?text",
        "where": {"@id": "ex:msg1", "ex:text": "?text"}
    });
    let db = crate::GraphDb::from_ledger_state(&ledger_ok);
    let result = fluree.query(&db, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.snapshot).unwrap();
    assert_eq!(jsonld, json!(["HELLO WORLD"]));

    // Invalid: "goodbye" doesn't match pattern
    let ledger_bad = fluree
        .create_ledger("shacl/pattern-flags-bad:main")
        .await
        .unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:msg2",
                "@type": "ex:Message",
                "ex:text": "GOODBYE WORLD"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "does not match pattern");
}

// =============================================================================
// sh:in list semantics tests
// =============================================================================

#[tokio::test]
async fn shacl_in_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Shape with sh:in list of allowed values
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:StatusShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Task"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "ex:status"},
            "sh:in": { "@list": ["pending", "active", "completed"] }
        }]
    });

    // Valid: "active" is in the allowed list
    let ledger_ok = fluree.create_ledger("shacl/in-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:task1",
                "@type": "ex:Task",
                "ex:status": "active"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": "?status",
        "where": {"@id": "ex:task1", "ex:status": "?status"}
    });
    let db = crate::GraphDb::from_ledger_state(&ledger_ok);
    let result = fluree.query(&db, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.snapshot).unwrap();
    assert_eq!(jsonld, json!(["active"]));

    // Invalid: "cancelled" is not in the allowed list
    let ledger_bad = fluree.create_ledger("shacl/in-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:task2",
                "@type": "ex:Task",
                "ex:status": "cancelled"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "not in the allowed set");
}

// =============================================================================
// sh:equals constraint tests
// =============================================================================

#[tokio::test]
async fn shacl_equals_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Shape where startDate must equal endDate (for single-day events)
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:SingleDayEventShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:SingleDayEvent"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "ex:startDate"},
            "sh:equals": {"@id": "ex:endDate"}
        }]
    });

    // Valid: startDate equals endDate
    let ledger_ok = fluree.create_ledger("shacl/equals-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:event1",
                "@type": "ex:SingleDayEvent",
                "ex:startDate": "2024-01-15",
                "ex:endDate": "2024-01-15"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": "?date",
        "where": {"@id": "ex:event1", "ex:startDate": "?date"}
    });
    let db = crate::GraphDb::from_ledger_state(&ledger_ok);
    let result = fluree.query(&db, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.snapshot).unwrap();
    assert_eq!(jsonld, json!(["2024-01-15"]));

    // Invalid: startDate does not equal endDate
    let ledger_bad = fluree.create_ledger("shacl/equals-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:event2",
                "@type": "ex:SingleDayEvent",
                "ex:startDate": "2024-01-15",
                "ex:endDate": "2024-01-16"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "does not equal");
}

// =============================================================================
// sh:disjoint constraint tests
// =============================================================================

#[tokio::test]
async fn shacl_disjoint_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // primaryEmail values must not overlap with secondaryEmail values.
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:UserEmailShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape_disjoint",
            "sh:path": {"@id": "ex:primaryEmail"},
            "sh:disjoint": {"@id": "ex:secondaryEmail"}
        }]
    });

    // Valid: primary and secondary emails are different.
    let ledger_ok = fluree
        .create_ledger("shacl/disjoint-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "ex:primaryEmail": "alice@example.org",
                "ex:secondaryEmail": "alice-alt@example.org"
            }),
        )
        .await
        .expect("disjoint email sets should pass");

    // Invalid: overlapping emails.
    let ledger_bad = fluree
        .create_ledger("shacl/disjoint-bad:main")
        .await
        .unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:bob",
                "@type": "ex:User",
                "ex:primaryEmail": "bob@example.org",
                "ex:secondaryEmail": "bob@example.org"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "disjoint");
}

// =============================================================================
// sh:lessThan constraint tests
// =============================================================================

#[tokio::test]
async fn shacl_less_than_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // startYear must be strictly less than endYear.
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:EventRangeShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Event"},
        "sh:property": [{
            "@id": "ex:pshape_lt",
            "sh:path": {"@id": "ex:startYear"},
            "sh:lessThan": {"@id": "ex:endYear"}
        }]
    });

    // Valid: 2020 < 2024.
    let ledger_ok = fluree.create_ledger("shacl/lt-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:conf1",
                "@type": "ex:Event",
                "ex:startYear": 2020,
                "ex:endYear": 2024
            }),
        )
        .await
        .expect("startYear strictly less than endYear should pass");

    // Invalid: 2025 >= 2024.
    let ledger_bad = fluree.create_ledger("shacl/lt-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:conf2",
                "@type": "ex:Event",
                "ex:startYear": 2025,
                "ex:endYear": 2024
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "not less than");

    // Invalid: equal is also a violation of strict sh:lessThan.
    let ledger_eq = fluree.create_ledger("shacl/lt-eq:main").await.unwrap();
    let ledger_eq = fluree.upsert(ledger_eq, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_eq,
            &json!({
                "@context": context.clone(),
                "@id": "ex:conf3",
                "@type": "ex:Event",
                "ex:startYear": 2024,
                "ex:endYear": 2024
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "not less than");
}

// =============================================================================
// sh:lessThanOrEquals constraint tests
// =============================================================================

#[tokio::test]
async fn shacl_less_than_or_equals_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:BudgetShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Budget"},
        "sh:property": [{
            "@id": "ex:pshape_lte",
            "sh:path": {"@id": "ex:spent"},
            "sh:lessThanOrEquals": {"@id": "ex:cap"}
        }]
    });

    // Valid: spent <= cap (including equal).
    let ledger_ok = fluree.create_ledger("shacl/lte-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:b1",
                "@type": "ex:Budget",
                "ex:spent": 100,
                "ex:cap": 100
            }),
        )
        .await
        .expect("spent == cap should pass under sh:lessThanOrEquals");

    // Invalid: spent > cap.
    let ledger_bad = fluree.create_ledger("shacl/lte-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:b2",
                "@type": "ex:Budget",
                "ex:spent": 150,
                "ex:cap": 100
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "not less than or equal");
}

// =============================================================================
// sh:class constraint tests
// =============================================================================

#[tokio::test]
async fn shacl_class_constraint_direct_type() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Each ex:author value must be an instance of ex:Person.
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:BookShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Book"},
        "sh:property": [{
            "@id": "ex:pshape_class",
            "sh:path": {"@id": "ex:author"},
            "sh:class": {"@id": "ex:Person"}
        }]
    });

    // Valid: author is declared as ex:Person.
    let ledger_ok = fluree.create_ledger("shacl/class-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    fluree
        .upsert(
            ledger_ok,
            &json!([
                {
                    "@context": context.clone(),
                    "@id": "ex:alice",
                    "@type": "ex:Person"
                },
                {
                    "@context": context.clone(),
                    "@id": "ex:book1",
                    "@type": "ex:Book",
                    "ex:author": {"@id": "ex:alice"}
                }
            ]),
        )
        .await
        .expect("author of type ex:Person should pass");

    // Invalid: author has no rdf:type at all.
    let ledger_untyped = fluree
        .create_ledger("shacl/class-untyped:main")
        .await
        .unwrap();
    let ledger_untyped = fluree
        .upsert(ledger_untyped, &shape_txn)
        .await
        .unwrap()
        .ledger;
    let err = fluree
        .upsert(
            ledger_untyped,
            &json!([
                {
                    "@context": context.clone(),
                    "@id": "ex:ghost"
                },
                {
                    "@context": context.clone(),
                    "@id": "ex:book2",
                    "@type": "ex:Book",
                    "ex:author": {"@id": "ex:ghost"}
                }
            ]),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "not an instance of class");

    // Invalid: author is typed, but as the wrong class.
    let ledger_wrong = fluree
        .create_ledger("shacl/class-wrong:main")
        .await
        .unwrap();
    let ledger_wrong = fluree
        .upsert(ledger_wrong, &shape_txn)
        .await
        .unwrap()
        .ledger;
    let err = fluree
        .upsert(
            ledger_wrong,
            &json!([
                {
                    "@context": context.clone(),
                    "@id": "ex:acme",
                    "@type": "ex:Organization"
                },
                {
                    "@context": context.clone(),
                    "@id": "ex:book3",
                    "@type": "ex:Book",
                    "ex:author": {"@id": "ex:acme"}
                }
            ]),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "not an instance of class");

    // Invalid: literal value cannot be an instance of any class.
    let ledger_literal = fluree
        .create_ledger("shacl/class-literal:main")
        .await
        .unwrap();
    let ledger_literal = fluree
        .upsert(ledger_literal, &shape_txn)
        .await
        .unwrap()
        .ledger;
    let err = fluree
        .upsert(
            ledger_literal,
            &json!({
                "@context": context.clone(),
                "@id": "ex:book4",
                "@type": "ex:Book",
                "ex:author": "just a string"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "literal");
}

#[tokio::test]
async fn shacl_class_constraint_subclass_reasoning() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Seed: Novelist rdfs:subClassOf Person, and ex:pratchett is a Novelist.
    // Shape requires ex:author to be ex:Person. Instance of subclass should pass.
    let seed = json!([
        {
            "@context": context.clone(),
            "@id": "ex:Novelist",
            "@type": "rdfs:Class",
            "rdfs:subClassOf": {"@id": "ex:Person"}
        },
        {
            "@context": context.clone(),
            "@id": "ex:pratchett",
            "@type": "ex:Novelist"
        },
        {
            "@context": context.clone(),
            "@id": "ex:BookShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": {"@id": "ex:Book"},
            "sh:property": [{
                "@id": "ex:pshape_class_sub",
                "sh:path": {"@id": "ex:author"},
                "sh:class": {"@id": "ex:Person"}
            }]
        }
    ]);

    let ledger = fluree
        .create_ledger("shacl/class-subclass:main")
        .await
        .unwrap();
    let ledger = fluree.upsert(ledger, &seed).await.unwrap().ledger;

    // Valid: Novelist is a subclass of Person, so the constraint is satisfied
    // via RDFS subclass reasoning (cached on the snapshot's SchemaHierarchy).
    fluree
        .upsert(
            ledger,
            &json!({
                "@context": context.clone(),
                "@id": "ex:disc_world",
                "@type": "ex:Book",
                "ex:author": {"@id": "ex:pratchett"}
            }),
        )
        .await
        .expect("author of subclass (Novelist ⊑ Person) should pass sh:class ex:Person");
}

/// Regression: `sh:class` subclass reasoning must work when the subject under
/// validation lives in a named graph but the `rdfs:subClassOf` edge lives in
/// the schema (default) graph.
///
/// `validate_staged_nodes` partitions subjects by graph and builds a
/// `GraphDbRef` scoped to each subject's graph. Before this fix, the subclass
/// BFS walked via that same graph-scoped ref, so a schema-graph subClassOf
/// edge was invisible when validating a subject in a named graph —
/// silently producing a violation. The walk now always queries the default
/// graph for `rdfs:subClassOf`, which matches how `SchemaHierarchy` is built.
#[tokio::test]
async fn shacl_class_constraint_subclass_reasoning_cross_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Schema + shape in the default graph. Data (including the value's
    // rdf:type) will go in a named graph below.
    let seed = json!([
        {
            "@context": context.clone(),
            "@id": "ex:Novelist",
            "@type": "rdfs:Class",
            "rdfs:subClassOf": {"@id": "ex:Person"}
        },
        {
            "@context": context.clone(),
            "@id": "ex:BookShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": {"@id": "ex:Book"},
            "sh:property": [{
                "@id": "ex:pshape_class_xgraph",
                "sh:path": {"@id": "ex:author"},
                "sh:class": {"@id": "ex:Person"}
            }]
        }
    ]);
    let ledger = fluree
        .create_ledger("shacl/class-subclass-xgraph:main")
        .await
        .unwrap();
    let ledger = fluree.upsert(ledger, &seed).await.unwrap().ledger;

    // Data in a named graph. `ex:pratchett` is typed `ex:Novelist` here;
    // the `Novelist ⊑ Person` edge remains in the default graph.
    let trig = r"
        @prefix ex: <http://example.org/ns/> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <http://example.org/ns/data> {
            ex:pratchett rdf:type ex:Novelist .
            ex:disc_world rdf:type ex:Book ;
                          ex:author ex:pratchett .
        }
    ";

    // Route via the builder so TriG `GRAPH` blocks are parsed (the top-level
    // `upsert_turtle` uses the plain-Turtle parser which rejects `GRAPH`).
    fluree
        .stage_owned(ledger)
        .upsert_turtle(trig)
        .execute()
        .await
        .expect(
            "sh:class subclass reasoning must cross graph boundaries: \
             subject in named graph, subClassOf edge in schema graph",
        );
}

// =============================================================================
// sh:targetSubjectsOf tests (staged write path)
// =============================================================================

/// Shape targets subjects-of(ex:ssn) — any node that has an ex:ssn value
/// must also have an ex:name. Pre-fix, the cache didn't index `SubjectsOf`,
/// so staged-path validation missed this target entirely.
#[tokio::test]
async fn shacl_target_subjects_of() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:HasSsnShape",
        "@type": "sh:NodeShape",
        "sh:targetSubjectsOf": {"@id": "ex:ssn"},
        "sh:property": [{
            "@id": "ex:pshape_ssn",
            "sh:path": {"@id": "ex:name"},
            "sh:minCount": 1
        }]
    });

    // Valid: subject has ex:ssn AND ex:name.
    let ledger_ok = fluree.create_ledger("shacl/tso-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "ex:ssn": "123-45-6789",
                "ex:name": "Alice"
            }),
        )
        .await
        .expect("subject with ex:ssn and ex:name should pass");

    // Invalid: subject has ex:ssn but no ex:name — must still be a focus.
    let ledger_bad = fluree.create_ledger("shacl/tso-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:bob",
                "ex:ssn": "987-65-4321"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "at least 1");
}

/// Regression: `sh:targetSubjectsOf` hints must be built from ASSERT flakes
/// only. Retractions shouldn't trigger the target — the predicate is being
/// removed from the post-transaction view, so the shape no longer applies.
///
/// Pre-fix, the staged validator recorded every flake's predicate into the
/// outbound-hints map regardless of `flake.op`. A retraction of
/// `(alice, ex:ssn, ..)` would make alice look like a `targetSubjectsOf(ex:ssn)`
/// focus, fire the shape, and (because retractions also removed `ex:name`)
/// produce a spurious `minCount` violation.
#[tokio::test]
async fn shacl_target_subjects_of_ignores_retractions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Shape: subjects of ex:ssn must have at least one ex:name.
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:HasSsnShape",
        "@type": "sh:NodeShape",
        "sh:targetSubjectsOf": {"@id": "ex:ssn"},
        "sh:property": [{
            "@id": "ex:pshape_ssn_retract",
            "sh:path": {"@id": "ex:name"},
            "sh:minCount": 1
        }]
    });

    let ledger = fluree
        .create_ledger("shacl/tso-retract:main")
        .await
        .unwrap();
    let ledger = fluree.upsert(ledger, &shape_txn).await.unwrap().ledger;

    // Seed: alice has ex:ssn AND ex:name (shape initially satisfied).
    let ledger = fluree
        .upsert(
            ledger,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "ex:ssn": "123-45-6789",
                "ex:name": "Alice"
            }),
        )
        .await
        .expect("seed must succeed — shape is satisfied")
        .ledger;

    // Retract both ex:ssn and ex:name in a single transaction.
    // Post-state: alice has nothing. Shape's SubjectsOf(ex:ssn) should NOT
    // fire on alice for this transaction — ssn is being removed, not added.
    fluree
        .update(
            ledger,
            &json!({
                "@context": context.clone(),
                "delete": {
                    "@id": "ex:alice",
                    "ex:ssn": "123-45-6789",
                    "ex:name": "Alice"
                }
            }),
        )
        .await
        .expect(
            "retraction-only transaction must not trigger sh:targetSubjectsOf \
             on the retracting subject — the post-state has no ex:ssn",
        );
}

// =============================================================================
// sh:targetObjectsOf tests (staged write path)
// =============================================================================

/// Shape targets objects-of(ex:employer) — any node referenced as an
/// ex:employer must have an ex:name. Pre-fix, the staged path didn't add
/// ref-objects as focus nodes, so the referenced company was never validated.
#[tokio::test]
async fn shacl_target_objects_of() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:EmployerShape",
        "@type": "sh:NodeShape",
        "sh:targetObjectsOf": {"@id": "ex:employer"},
        "sh:property": [{
            "@id": "ex:pshape_employer",
            "sh:path": {"@id": "ex:name"},
            "sh:minCount": 1
        }]
    });

    // Valid: referenced company has ex:name.
    let ledger_ok = fluree.create_ledger("shacl/too-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    fluree
        .upsert(
            ledger_ok,
            &json!([
                {
                    "@context": context.clone(),
                    "@id": "ex:acme",
                    "ex:name": "Acme Corp"
                },
                {
                    "@context": context.clone(),
                    "@id": "ex:alice",
                    "ex:employer": {"@id": "ex:acme"}
                }
            ]),
        )
        .await
        .expect("referenced employer with ex:name should pass");

    // Invalid: referenced company has no ex:name. The object-ref (ex:opaque)
    // must be pulled in as a focus node by the staged validator.
    let ledger_bad = fluree.create_ledger("shacl/too-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!([
                {
                    "@context": context.clone(),
                    "@id": "ex:opaque"
                },
                {
                    "@context": context.clone(),
                    "@id": "ex:bob",
                    "ex:employer": {"@id": "ex:opaque"}
                }
            ]),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "at least 1");
}

/// Regression: `sh:targetSubjectsOf` must fire on a focus when the
/// triggering edge exists in the **base state** and this txn only edits
/// another property. Pre-fix, the validator built target hints from staged
/// assert flakes only, so a txn that retracted `ex:name` on a subject
/// whose `ex:ssn` was already present in the base DB would miss the
/// `targetSubjectsOf(ex:ssn)` shape — a false negative.
#[tokio::test]
async fn shacl_target_subjects_of_fires_on_base_state_edge() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:HasSsnShapeBase",
        "@type": "sh:NodeShape",
        "sh:targetSubjectsOf": {"@id": "ex:ssn"},
        "sh:property": [{
            "@id": "ex:pshape_ssn_base",
            "sh:path": {"@id": "ex:name"},
            "sh:minCount": 1
        }]
    });

    let ledger = fluree.create_ledger("shacl/tso-base:main").await.unwrap();
    let ledger = fluree.upsert(ledger, &shape_txn).await.unwrap().ledger;

    // Seed: alice has ex:ssn AND ex:name — shape satisfied.
    let ledger = fluree
        .upsert(
            ledger,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "ex:ssn": "123-45-6789",
                "ex:name": "Alice"
            }),
        )
        .await
        .expect("seed: shape initially satisfied")
        .ledger;

    // Retract ONLY ex:name. ex:ssn persists in the base state, so alice
    // must still be treated as a targetSubjectsOf(ex:ssn) focus.
    let err = fluree
        .update(
            ledger,
            &json!({
                "@context": context.clone(),
                "delete": {"@id": "ex:alice", "ex:name": "Alice"}
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "at least 1");
}

/// Regression: `sh:targetObjectsOf` must fire on a focus when the inbound
/// edge exists in the **base state** and this txn only edits another
/// property on the target. Pre-fix, the Ref-object-as-focus expansion
/// relied on the txn asserting the inbound edge, so a txn that only
/// retracted `ex:name` on an already-referenced node would miss the
/// `targetObjectsOf(ex:employer)` shape.
#[tokio::test]
async fn shacl_target_objects_of_fires_on_base_state_edge() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:EmployerShapeBase",
        "@type": "sh:NodeShape",
        "sh:targetObjectsOf": {"@id": "ex:employer"},
        "sh:property": [{
            "@id": "ex:pshape_employer_base",
            "sh:path": {"@id": "ex:name"},
            "sh:minCount": 1
        }]
    });

    let ledger = fluree.create_ledger("shacl/too-base:main").await.unwrap();
    let ledger = fluree.upsert(ledger, &shape_txn).await.unwrap().ledger;

    // Seed: acme has ex:name AND is referenced as bob's employer.
    let ledger = fluree
        .upsert(
            ledger,
            &json!([
                {"@context": context.clone(), "@id": "ex:acme", "ex:name": "Acme Corp"},
                {"@context": context.clone(), "@id": "ex:bob",
                 "ex:employer": {"@id": "ex:acme"}}
            ]),
        )
        .await
        .expect("seed: shape initially satisfied")
        .ledger;

    // Retract ONLY acme's ex:name. The (bob, ex:employer, acme) edge
    // persists in the base state, so acme is still a
    // targetObjectsOf(ex:employer) focus in the post-txn view.
    let err = fluree
        .update(
            ledger,
            &json!({
                "@context": context.clone(),
                "delete": {"@id": "ex:acme", "ex:name": "Acme Corp"}
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "at least 1");
}

/// Regression: `sh:targetSubjectsOf` / `sh:targetObjectsOf` applicability
/// must be determined against the **per-graph** view of the post-txn state.
/// If a subject has different predicates in different graphs, matching in
/// graph A must not leak into graph B.
///
/// Pre-fix, hints were keyed by focus `Sid` alone. In a multi-graph
/// transaction that wrote `ex:alice ex:ssn` in graph A and only
/// `ex:alice ex:hobby` in graph B, the validator would fire
/// `sh:targetSubjectsOf(ex:ssn)` on alice in graph B — where she has no
/// `ex:ssn` — producing a spurious `minCount` violation on `ex:name`.
#[tokio::test]
async fn shacl_target_subjects_of_does_not_leak_across_graphs() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:HasSsnShapeXG",
        "@type": "sh:NodeShape",
        "sh:targetSubjectsOf": {"@id": "ex:ssn"},
        "sh:property": [{
            "@id": "ex:pshape_ssn_xgraph",
            "sh:path": {"@id": "ex:name"},
            "sh:minCount": 1
        }]
    });

    let ledger = fluree.create_ledger("shacl/tso-xgraph:main").await.unwrap();
    let ledger = fluree.upsert(ledger, &shape_txn).await.unwrap().ledger;

    // TriG: alice gets ex:ssn AND ex:name in graph A (shape satisfied there).
    // In graph B, alice has only ex:hobby — no ex:ssn, so SubjectsOf(ex:ssn)
    // must NOT fire on alice in graph B. Keying hints by (GraphId, Sid)
    // ensures the graph-B hint set is {ex:hobby}, not {ex:ssn, ex:name, ex:hobby}.
    let trig = r#"
        @prefix ex: <http://example.org/ns/> .

        GRAPH <http://example.org/ns/graphA> {
            ex:alice ex:ssn "123" ;
                     ex:name "Alice" .
        }
        GRAPH <http://example.org/ns/graphB> {
            ex:alice ex:hobby "reading" .
        }
    "#;

    fluree
        .stage_owned(ledger)
        .upsert_turtle(trig)
        .execute()
        .await
        .expect(
            "cross-graph write must not leak sh:targetSubjectsOf hints: \
             alice in graph B has no ex:ssn, so the shape must not fire there",
        );
}

// =============================================================================
// Logical constraint tests (sh:not, sh:and, sh:or, sh:xone)
// =============================================================================

#[tokio::test]
async fn shacl_not_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Define a "forbidden" shape and a main shape that uses sh:not
    let shapes_txn = json!([
        {
            "@context": context.clone(),
            "@id": "ex:ForbiddenShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:forbidden_pshape",
                "sh:path": {"@id": "ex:status"},
                "sh:hasValue": "banned"
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:UserShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": {"@id": "ex:User"},
            "sh:not": {"@id": "ex:ForbiddenShape"}
        }
    ]);

    // Valid: user with status "active" (not "banned")
    let ledger_ok = fluree.create_ledger("shacl/not-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shapes_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "ex:status": "active"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": "?status",
        "where": {"@id": "ex:alice", "ex:status": "?status"}
    });
    let db = crate::GraphDb::from_ledger_state(&ledger_ok);
    let result = fluree.query(&db, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.snapshot).unwrap();
    assert_eq!(jsonld, json!(["active"]));

    // Invalid: user with status "banned" matches the forbidden shape
    let ledger_bad = fluree.create_ledger("shacl/not-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shapes_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:bob",
                "@type": "ex:User",
                "ex:status": "banned"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "sh:not");
}

#[tokio::test]
async fn shacl_and_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Two shapes that must both be satisfied
    let shapes_txn = json!([
        {
            "@context": context.clone(),
            "@id": "ex:HasNameShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:name_pshape",
                "sh:path": {"@id": "schema:name"},
                "sh:minCount": 1
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:HasEmailShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:email_pshape",
                "sh:path": {"@id": "schema:email"},
                "sh:minCount": 1
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:UserShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": {"@id": "ex:User"},
            "sh:and": { "@list": [
                {"@id": "ex:HasNameShape"},
                {"@id": "ex:HasEmailShape"}
            ]}
        }
    ]);

    // Valid: has both name and email
    let ledger_ok = fluree.create_ledger("shacl/and-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shapes_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:email": "alice@example.org"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": "?name",
        "where": {"@id": "ex:alice", "schema:name": "?name"}
    });
    let db = crate::GraphDb::from_ledger_state(&ledger_ok);
    let result = fluree.query(&db, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.snapshot).unwrap();
    assert_eq!(jsonld, json!(["Alice"]));

    // Invalid: missing email (only has name)
    let ledger_bad = fluree.create_ledger("shacl/and-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shapes_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:bob",
                "@type": "ex:User",
                "schema:name": "Bob"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "sh:and");
}

#[tokio::test]
async fn shacl_or_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Two shapes - at least one must be satisfied
    let shapes_txn = json!([
        {
            "@context": context.clone(),
            "@id": "ex:HasPhoneShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:phone_pshape",
                "sh:path": {"@id": "ex:phone"},
                "sh:minCount": 1
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:HasEmailShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:email_pshape",
                "sh:path": {"@id": "schema:email"},
                "sh:minCount": 1
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:ContactShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": {"@id": "ex:Contact"},
            "sh:or": { "@list": [
                {"@id": "ex:HasPhoneShape"},
                {"@id": "ex:HasEmailShape"}
            ]}
        }
    ]);

    // Valid: has email (satisfies one option)
    let ledger_ok = fluree.create_ledger("shacl/or-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shapes_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:Contact",
                "schema:email": "alice@example.org"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": "?email",
        "where": {"@id": "ex:alice", "schema:email": "?email"}
    });
    let db = crate::GraphDb::from_ledger_state(&ledger_ok);
    let result = fluree.query(&db, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.snapshot).unwrap();
    assert_eq!(jsonld, json!(["alice@example.org"]));

    // Invalid: has neither phone nor email
    let ledger_bad = fluree.create_ledger("shacl/or-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shapes_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:bob",
                "@type": "ex:Contact",
                "schema:name": "Bob"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "sh:or");
}

#[tokio::test]
async fn shacl_xone_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Two shapes - exactly one must be satisfied (not both, not neither)
    let shapes_txn = json!([
        {
            "@context": context.clone(),
            "@id": "ex:PersonalAccountShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:personal_pshape",
                "sh:path": {"@id": "ex:personalId"},
                "sh:minCount": 1
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:BusinessAccountShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:business_pshape",
                "sh:path": {"@id": "ex:businessId"},
                "sh:minCount": 1
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:AccountShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": {"@id": "ex:Account"},
            "sh:xone": { "@list": [
                {"@id": "ex:PersonalAccountShape"},
                {"@id": "ex:BusinessAccountShape"}
            ]}
        }
    ]);

    // Valid: has only personalId (exactly one shape matches)
    let ledger_ok = fluree.create_ledger("shacl/xone-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shapes_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:acct1",
                "@type": "ex:Account",
                "ex:personalId": "P12345"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": "?id",
        "where": {"@id": "ex:acct1", "ex:personalId": "?id"}
    });
    let db = crate::GraphDb::from_ledger_state(&ledger_ok);
    let result = fluree.query(&db, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.snapshot).unwrap();
    assert_eq!(jsonld, json!(["P12345"]));

    // Invalid: has both personalId AND businessId (both shapes match)
    let ledger_both = fluree.create_ledger("shacl/xone-both:main").await.unwrap();
    let ledger_both = fluree
        .upsert(ledger_both, &shapes_txn)
        .await
        .unwrap()
        .ledger;
    let err = fluree
        .upsert(
            ledger_both,
            &json!({
                "@context": context.clone(),
                "@id": "ex:acct2",
                "@type": "ex:Account",
                "ex:personalId": "P12345",
                "ex:businessId": "B67890"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "sh:xone");

    // Invalid: has neither (no shapes match)
    let ledger_none = fluree.create_ledger("shacl/xone-none:main").await.unwrap();
    let ledger_none = fluree
        .upsert(ledger_none, &shapes_txn)
        .await
        .unwrap()
        .ledger;
    let err = fluree
        .upsert(
            ledger_none,
            &json!({
                "@context": context.clone(),
                "@id": "ex:acct3",
                "@type": "ex:Account",
                "schema:name": "Anonymous"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "sh:xone");
}

#[tokio::test]
async fn shacl_or_with_inline_anonymous_shapes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = json!([default_context(), {
        "skosxl": "http://www.w3.org/2008/05/skos-xl#",
        "ex": "http://example.org/ns/"
    }]);

    // Shape with sh:or containing inline anonymous constraint shapes:
    // the value of skosxl:literalForm must be either rdf:langString or xsd:string.
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:LabelShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "skosxl:Label"},
        "sh:property": [{
            "sh:path": {"@id": "skosxl:literalForm"},
            "sh:minCount": 1,
            "sh:maxCount": 1,
            "sh:or": { "@list": [
                {"sh:datatype": {"@id": "rdf:langString"}},
                {"sh:datatype": {"@id": "xsd:string"}}
            ]}
        }]
    });

    // Valid: plain string value (matches xsd:string)
    let ledger_ok = fluree
        .create_ledger("shacl/or-inline-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let _ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:label1",
                "@type": "skosxl:Label",
                "skosxl:literalForm": "hello"
            }),
        )
        .await
        .unwrap()
        .ledger;

    // Invalid: integer value (matches neither rdf:langString nor xsd:string)
    let ledger_bad = fluree
        .create_ledger("shacl/or-inline-bad:main")
        .await
        .unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:label2",
                "@type": "skosxl:Label",
                "skosxl:literalForm": 42
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "sh:or");
}

#[tokio::test]
async fn shacl_and_with_inline_anonymous_shapes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = json!([default_context(), {
        "ex": "http://example.org/ns/"
    }]);

    // Shape requiring sh:and: value must be BOTH xsd:string AND have minLength 3
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:StrictNameShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Entity"},
        "sh:property": [{
            "sh:path": {"@id": "schema:name"},
            "sh:and": { "@list": [
                {"sh:datatype": {"@id": "xsd:string"}},
                {"sh:minLength": 3}
            ]}
        }]
    });

    // Valid: string with length >= 3
    let ledger_ok = fluree
        .create_ledger("shacl/and-inline-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let _ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:e1",
                "@type": "ex:Entity",
                "schema:name": "Alice"
            }),
        )
        .await
        .unwrap()
        .ledger;

    // Invalid: string with length < 3 (violates minLength)
    let ledger_bad = fluree
        .create_ledger("shacl/and-inline-bad:main")
        .await
        .unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:e2",
                "@type": "ex:Entity",
                "schema:name": "Al"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "sh:and");
}
