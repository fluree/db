//! The witnessed-DELETE lane must actually fire, and stand down when the
//! ledger can hold a list position.
//!
//! A DELETE whose template a WHERE triple witnesses (`DELETE { ?s ex:p ?o }
//! WHERE { ?s ex:p ?o }`) retracts its rows as decoded, with no stored-fact
//! read, when neither the index nor novelty holds an `@list` position; on a
//! list-bearing ledger the rows read their slots for list positions. The
//! answers agree either way, so only the `delete_witnessed` routing stamp
//! can tell which lane ran. Both directions are asserted on the same ledger:
//! `MustFire` while it is list-free, `MustNotFire` after one `@list` lands.
//!
//! Its own `[[test]]` binary, like `it_upsert_skip_fires`: tracing's
//! callsite-interest cache is process-global, so a sibling test in a bundled
//! binary can leave the stamp's callsite disabled.

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/ns/"})
}

fn witnessed_delete(subject: &str) -> JsonValue {
    json!({
        "@context": ctx(),
        "where": {"@id": subject, "ex:tag": "?o"},
        "delete": {"@id": subject, "ex:tag": "?o"}
    })
}

#[tokio::test(flavor = "current_thread")]
async fn delete_witnessed_lane_fires_only_on_a_list_free_ledger() {
    let (store, _guard) = span_capture::init_test_tracing();
    let site = fluree_db_transact::DELETE_WITNESSED_SITE;
    let outcomes = |from: usize| -> Vec<String> {
        store.find_events("fast-path outcome")[from..]
            .iter()
            .filter(|e| e.fields.get("site").map(String::as_str) == Some(site))
            .filter_map(|e| e.fields.get("outcome").cloned())
            .collect()
    };

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .create_ledger("it/delete-witness-routing:main")
        .await
        .expect("create");
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:a", "ex:tag": "x"},
                    {"@id": "ex:b", "ex:tag": "y"},
                    {"@id": "ex:c", "ex:tag": "z"}
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;
    assert!(!ledger.novelty.has_list_meta);

    // MustFire: list-free.
    let before = store.find_events("fast-path outcome").len();
    let r = fluree
        .update(ledger, &witnessed_delete("ex:a"))
        .await
        .expect("witnessed delete");
    assert_eq!(r.receipt.retract_count, 1);
    assert_eq!(
        outcomes(before),
        ["proceed"],
        "a witnessed delete on a list-free ledger must take the zero-read lane"
    );

    // A DELETE DATA-shaped delete has no witness and stamps nothing.
    let before = store.find_events("fast-path outcome").len();
    let r = fluree
        .update(
            r.ledger,
            &json!({"@context": ctx(), "delete": {"@id": "ex:b", "ex:tag": "y"}}),
        )
        .await
        .expect("constant delete");
    assert_eq!(r.receipt.retract_count, 1);
    assert!(outcomes(before).is_empty(), "no witness, no stamp");

    // One @list on the same ledger turns the lane off.
    let ledger = fluree
        .insert(
            r.ledger,
            &json!({"@context": ctx(), "@id": "ex:l", "ex:items": {"@list": ["a", "b"]}}),
        )
        .await
        .expect("list")
        .ledger;
    assert!(ledger.novelty.has_list_meta);

    // MustNotFire: list-bearing.
    let before = store.find_events("fast-path outcome").len();
    let r = fluree
        .update(ledger, &witnessed_delete("ex:c"))
        .await
        .expect("witnessed delete, list-bearing");
    assert_eq!(r.receipt.retract_count, 1, "the delete still retracts");
    assert_eq!(
        outcomes(before),
        ["fallback:gate_declined"],
        "a list-bearing ledger must read list positions"
    );
}

/// A value bound by the sole triple of an OPTIONAL, or of one UNION branch,
/// is the decode of that triple's fact, so its row takes the witnessed lane:
/// the JSON-LD and SPARQL shapes, and Cypher `SET` and `DETACH DELETE`,
/// which read the old value through an OPTIONAL. A UNION whose other branch
/// binds the object from another predicate stays on the matched lane.
#[tokio::test(flavor = "current_thread")]
async fn optional_and_union_rows_take_the_witnessed_lane() {
    let (store, _guard) = span_capture::init_test_tracing();
    let site = fluree_db_transact::DELETE_WITNESSED_SITE;
    let outcomes = |from: usize| -> Vec<String> {
        store.find_events("fast-path outcome")[from..]
            .iter()
            .filter(|e| e.fields.get("site").map(String::as_str) == Some(site))
            .filter_map(|e| e.fields.get("outcome").cloned())
            .collect()
    };

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .create_ledger("it/delete-witness-optional:main")
        .await
        .expect("create");
    let mut ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:a", "@type": "ex:Node", "ex:tag": "x"},
                    {"@id": "ex:b", "@type": "ex:Node", "ex:tag": "y"},
                    {"@id": "ex:c", "@type": "ex:Node", "ex:tag": "z"},
                    {"@id": "ex:d", "@type": "ex:Node", "ex:tag": "w", "ex:other": "v"},
                    {"@id": "ex:e", "@type": "ex:Node", "ex:tag": "u"}
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;

    let cases: Vec<(&str, JsonValue, &str)> = vec![
        (
            "JSON-LD OPTIONAL",
            json!({
                "@context": ctx(),
                "where": [{"@id": "ex:a", "@type": "ex:Node"},
                          ["optional", {"@id": "ex:a", "ex:tag": "?o"}]],
                "delete": {"@id": "ex:a", "ex:tag": "?o"}
            }),
            "proceed",
        ),
        (
            "JSON-LD UNION",
            json!({
                "@context": ctx(),
                "where": [["union", {"@id": "ex:b", "ex:tag": "?o"}, {"@id": "ex:b", "@type": "?t"}]],
                "delete": {"@id": "ex:b", "ex:tag": "?o"}
            }),
            "proceed",
        ),
    ];
    for (what, txn, expected) in cases {
        let before = store.find_events("fast-path outcome").len();
        let r = fluree.update(ledger, &txn).await.expect(what);
        assert_eq!(r.receipt.retract_count, 1, "{what}");
        assert_eq!(outcomes(before), [expected], "{what}");
        ledger = r.ledger;
    }

    // The other branch binds the object from another predicate: matched.
    let before = store.find_events("fast-path outcome").len();
    let r = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [["union", {"@id": "ex:d", "ex:tag": "?o"}, {"@id": "ex:d", "ex:other": "?o"}]],
                "delete": {"@id": "ex:d", "ex:tag": "?o"}
            }),
        )
        .await
        .expect("UNION binding the object twice");
    assert_eq!(r.receipt.retract_count, 1, "the delete still retracts");
    assert!(
        outcomes(before).is_empty(),
        "a UNION binding the object twice is not a witness"
    );
    ledger = r.ledger;

    for (what, stmt) in [
        (
            "Cypher SET",
            "MATCH (n:`http://example.org/ns/Node`) WHERE n.`http://example.org/ns/tag` = 'z' \
             SET n.`http://example.org/ns/tag` = 'zz'",
        ),
        (
            "Cypher DETACH DELETE",
            "MATCH (n:`http://example.org/ns/Node`) WHERE n.`http://example.org/ns/tag` = 'u' \
             DETACH DELETE n",
        ),
    ] {
        let before = store.find_events("fast-path outcome").len();
        let r = fluree.transact_cypher(ledger, stmt).await.expect(what);
        assert!(r.receipt.retract_count >= 1, "{what}");
        assert!(
            outcomes(before).iter().any(|o| o == "proceed"),
            "{what}: {:?}",
            outcomes(before)
        );
        ledger = r.ledger;
    }
}
