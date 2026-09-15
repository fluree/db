//! Standalone because lane overrides and the fast-path switch are process-global.
#![cfg(feature = "native")]
mod support;

use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, ReindexOptions};
use fluree_db_ledger::LedgerState;
use serde_json::{json, Value};

const EVENT: &str = "nested annotation subject chain engaged";
const BODY: &str = "<< << ?s ?p ?o >> ?d ?e >> ?t ?u";
const PREFIX: &str = "PREFIX ex: <http://example.org/> ";

struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
        std::env::remove_var("FLUREE_ANNOTATION_LANE");
    }
}

async fn check(
    fluree: &Fluree,
    ledger: &LedgerState,
    query: &str,
    count: usize,
    route: bool,
) -> Value {
    let mut first = None;
    for disabled in [false, true] {
        set_fast_paths_disabled(disabled);
        let (spans, guard) = support::span_capture::init_test_tracing();
        let value = support::query_sparql(fluree, ledger, &format!("{PREFIX}{query}"))
            .await
            .unwrap()
            .to_sparql_json(&ledger.snapshot)
            .unwrap();
        if query.contains("AS ?total") {
            assert_eq!(
                value["results"]["bindings"][0]["total"]["value"],
                count.to_string(),
                "{query}: {value}"
            );
        } else {
            assert_eq!(
                value["results"]["bindings"].as_array().unwrap().len(),
                count,
                "{query}"
            );
        }
        assert_eq!(
            spans.has_event(EVENT),
            route && !disabled,
            "routing: {query}"
        );
        if let Some(previous) = &first {
            assert_eq!(previous, &value, "ordered/count output: {query}");
        } else {
            first = Some(value);
        }
        drop(guard);
    }
    set_fast_paths_disabled(false);
    first.unwrap()
}

async fn check_json(
    fluree: &Fluree,
    ledger: &LedgerState,
    query: &Value,
    count: usize,
    route: bool,
    connection: bool,
) {
    for disabled in [false, true] {
        set_fast_paths_disabled(disabled);
        let (spans, guard) = support::span_capture::init_test_tracing();
        let result = if connection {
            fluree.query_connection(query).await.unwrap()
        } else {
            support::query_jsonld(fluree, ledger, query).await.unwrap()
        };
        let value = result.to_sparql_json(&ledger.snapshot).unwrap();
        assert_eq!(
            value["results"]["bindings"][0]["total"]["value"],
            count.to_string(),
            "{query}: {value}"
        );
        assert_eq!(
            spans.has_event(EVENT),
            route && !disabled,
            "JSON routing: {query}"
        );
        drop(guard);
    }
    set_fast_paths_disabled(false);
}

#[tokio::test(flavor = "current_thread")]
async fn nested_annotations_preserve_matches_multiplicity_and_fallbacks() {
    assert!(std::env::var_os("FLUREE_ANNOTATION_LANE").is_none());
    assert!(std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none());
    let _reset = Reset;
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let alias = "nested/chain:main";
    let ledger = fluree.create_ledger(alias).await.unwrap();
    let ledger = fluree.insert(ledger, &json!({"@context":{"ex":"http://example.org/"},"@graph":[
        {"@id":"ex:a","ex:p":{"@id":"ex:b","@annotation":{"@id":"ex:inner","ex:d":{"@id":"ex:e"}}}},
        {"@id":"ex:c","ex:p":{"@id":"ex:d","@annotation":{"@id":"ex:other","ex:d":{"@id":"ex:f"}}}}
    ]})).await.unwrap().ledger;
    let count_query = format!("SELECT (COUNT(*) AS ?total) WHERE {{ {BODY} }}");
    check(&fluree, &ledger, &count_query, 0, false).await;
    fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .unwrap();
    let indexed = fluree.ledger(alias).await.unwrap();
    check(&fluree, &indexed, &count_query, 0, true).await;

    // Two reifiers of the SAME annotation edge, with overlapping body values.
    // The count is 3 + 2 = 5, not the number of edges, reifiers, or distinct values.
    let ledger = fluree
        .insert(
            indexed,
            &json!({"@context":{"ex":"http://example.org/"},"@id":"ex:inner",
        "ex:d":{"@id":"ex:e","@annotation":{"@id":"ex:outer1","ex:t":[1,2],"ex:label":"outer"}}}),
        )
        .await
        .unwrap()
        .ledger;
    let ledger = fluree
        .insert(
            ledger,
            &json!({"@context":{"ex":"http://example.org/"},"@id":"ex:inner",
        "ex:d":{"@id":"ex:e","@annotation":{"@id":"ex:outer2","ex:t":[1,2]}}}),
        )
        .await
        .unwrap()
        .ledger;
    check(&fluree, &ledger, &count_query, 5, false).await;
    fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .unwrap();
    let indexed = fluree.ledger(alias).await.unwrap();
    let positive_t = indexed.snapshot.t;
    check(&fluree, &indexed, &count_query, 5, true).await;
    check(
        &fluree,
        &indexed,
        &format!("SELECT (COUNT(DISTINCT ?u) AS ?total) WHERE {{ {BODY} }}"),
        3,
        true,
    )
    .await;
    let rows = check(
        &fluree,
        &indexed,
        &format!("SELECT ?t ?u WHERE {{ {BODY} }} ORDER BY ?t ?u"),
        5,
        true,
    )
    .await;
    assert_eq!(
        rows["results"]["bindings"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["u"]["value"] == "1")
            .count(),
        2
    );
    check(
        &fluree,
        &indexed,
        &format!("SELECT ?t ?u WHERE {{ {BODY} }} ORDER BY ?t ?u LIMIT 2 OFFSET 1"),
        2,
        true,
    )
    .await;
    // Reading the wildcard predicate prevents chain elision.
    check(
        &fluree,
        &indexed,
        &format!("SELECT ?d ?u WHERE {{ {BODY} }} ORDER BY ?d ?u"),
        5,
        false,
    )
    .await;
    // Selective and repeated-variable outer shapes keep their existing choices.
    check(
        &fluree,
        &indexed,
        "SELECT (COUNT(*) AS ?total) WHERE { << << ?s ?p ?o >> ex:d ?e >> ?t ?u }",
        5,
        false,
    )
    .await;
    check(
        &fluree,
        &indexed,
        "SELECT (COUNT(*) AS ?total) WHERE { << << ?s ?p ?o >> ?d ex:e >> ?t ?u }",
        5,
        false,
    )
    .await;
    check(
        &fluree,
        &indexed,
        "SELECT (COUNT(*) AS ?total) WHERE { << << ?s ?p ?o >> ?d ?d >> ?t ?u }",
        0,
        false,
    )
    .await;
    // The normal fixed-subject arena use case is not a nested reifier source.
    check(
        &fluree,
        &indexed,
        "SELECT (COUNT(*) AS ?total) WHERE { << ex:a ex:p ex:b >> ex:d ?e }",
        1,
        false,
    )
    .await;

    let json_query = json!({"@context":{"ex":"http://example.org/"},"select":["(as (count *) ?total)"],"where":[
        {"@id":"?s","?p":{"@id":"?o","@annotation":{"@id":"?inner"}}},
        {"@id":"?inner","?d":{"@id":"?e","@annotation":{"@id":"?outer"}}},
        {"@id":"?outer","?t":"?u"}
    ]});
    check_json(&fluree, &indexed, &json_query, 5, true, false).await;
    check_json(&fluree, &ledger, &json_query, 5, false, false).await;

    // Explicit lane overrides take precedence over the new preference.
    for lane in ["arena", "chain"] {
        std::env::set_var("FLUREE_ANNOTATION_LANE", lane);
        check(&fluree, &indexed, &count_query, 5, false).await;
    }
    std::env::remove_var("FLUREE_ANNOTATION_LANE");

    let mut restricted = json_query.clone();
    restricted["from"] = json!(alias);
    restricted["opts"] = json!({"default-allow":true,"policy":[{
        "@id":"ex:hideD","f:required":true,"f:action":"f:view",
        "f:onProperty":[{"@id":"http://example.org/d"}],"f:allow":false
    }]});
    check_json(&fluree, &indexed, &restricted, 0, false, true).await;

    let other_alias = "nested/empty:main";
    let other = fluree.create_ledger(other_alias).await.unwrap();
    fluree
        .insert(
            other,
            &json!({"@id":"http://example.org/unrelated","http://example.org/value":1}),
        )
        .await
        .unwrap();
    let mut dataset = json_query.clone();
    dataset["from"] = json!([alias, other_alias]);
    // Exercise the multi-ledger fallback with a fixed body predicate and distinct
    // values; single-ledger assertions above pin row multiplicity separately.
    dataset["select"] = json!(["(as (count-distinct ?u) ?total)"]);
    dataset["where"][2] = json!({"@id":"?outer","ex:t":"?u"});
    check_json(&fluree, &indexed, &dataset, 2, false, true).await;
    check(
        &fluree,
        &indexed,
        &format!("SELECT (COUNT(*) AS ?total) WHERE {{ GRAPH <{alias}> {{ {BODY} }} }}"),
        5,
        false,
    )
    .await;

    // Remove the annotated base edge: cascade must remove both outer reifiers.
    let removed = fluree
        .update(
            indexed,
            &json!({"@context":{"ex":"http://example.org/"},
        "delete":[{"@id":"ex:inner","ex:d":{"@id":"ex:e"}}]}),
        )
        .await
        .unwrap()
        .ledger;
    check(&fluree, &removed, &count_query, 0, false).await;
    fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .unwrap();
    let head = fluree.ledger(alias).await.unwrap();
    check(&fluree, &head, &count_query, 0, true).await;
    let mut past = json_query.clone();
    past["from"] = json!({"@id":alias,"t":positive_t});
    check_json(&fluree, &head, &past, 5, false, true).await;

    // History has different row semantics. Compare the existing history path in
    // both modes and assert this current-state preference never engages.
    past["from"] = json!(format!("{alias}@t:1"));
    past["to"] = json!(format!("{alias}@t:latest"));
    let mut history = None;
    for disabled in [false, true] {
        set_fast_paths_disabled(disabled);
        let (spans, guard) = support::span_capture::init_test_tracing();
        let value = fluree
            .query_connection(&past)
            .await
            .unwrap()
            .to_sparql_json(&head.snapshot)
            .unwrap();
        assert!(!spans.has_event(EVENT));
        if let Some(previous) = &history {
            assert_eq!(previous, &value);
        }
        history = Some(value);
        drop(guard);
    }
}
