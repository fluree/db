//! A property-join star applies every FILTER collected with it.
//!
//! The block inlines the FILTERs it can evaluate inside the join. It used to
//! drop the rest: one containing EXISTS (never inlined, it needs the async
//! path) or reading a variable nothing binds. The star then returned rows the
//! FILTER rejects. The sequential join chain always applied them.

use crate::support::genesis_ledger;
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

const LEDGER_ID: &str = "property-join-filters:main";
const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n";

/// Six claims; payers alternate `p0`/`p1`, ages run 10..=15, only `c1` is
/// flagged.
async fn seed(fluree: &fluree_db_api::Fluree) {
    let graph: Vec<JsonValue> = (0..6)
        .map(|i| {
            let mut claim = json!({
                "@id": format!("ex:c{i}"),
                "ex:payer": {"@id": format!("ex:p{}", i % 2)},
                "ex:age": 10 + i,
                "ex:name": format!("claim {i}"),
            });
            if i == 1 {
                claim["ex:flagged"] = json!(true);
            }
            claim
        })
        .collect();
    fluree
        .insert(
            genesis_ledger(fluree, LEDGER_ID),
            &json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": graph}),
        )
        .await
        .expect("seed insert");
}

fn sparql(body: &str) -> String {
    format!(
        "{PREFIX}{}",
        body.replacen("WHERE", &format!("FROM <{LEDGER_ID}> WHERE"), 1)
    )
}

/// The `?c` values, sorted; asserts the star ran as a property join.
async fn claims(fluree: &fluree_db_api::Fluree, body: &str) -> Vec<String> {
    let view = fluree.db(LEDGER_ID).await.expect("view");
    let plan = fluree
        .explain_sparql(&view, &sparql(body))
        .await
        .expect("explain");
    assert!(
        plan["plan"]["physical"]
            .to_string()
            .contains("PropertyJoinOperator"),
        "precondition: the star should plan as a property join: {plan}"
    );

    let result = fluree
        .query_from()
        .sparql(&sparql(body))
        .execute_tracked()
        .await
        .expect("query should succeed");
    let mut out: Vec<String> = result.result["results"]["bindings"]
        .as_array()
        .expect("bindings array")
        .iter()
        .map(|row| row["c"]["value"].as_str().expect("?c bound").to_string())
        .collect();
    out.sort();
    out
}

fn claim(i: usize) -> String {
    format!("http://example.org/ns/c{i}")
}

#[tokio::test]
async fn filter_with_exists_applies_to_a_property_join_star() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;

    let body = "SELECT ?c WHERE {
      ?c ex:payer ex:p1 ; ex:age ?a ; ex:name ?n .
      FILTER(?a > 14 || EXISTS { ?c ex:flagged true })
    }";
    assert_eq!(claims(&fluree, body).await, vec![claim(1), claim(5)]);
}

#[tokio::test]
async fn filter_on_a_var_nothing_binds_applies_to_a_property_join_star() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;

    let body = "SELECT ?c WHERE {
      ?c ex:payer ex:p1 ; ex:age ?a ; ex:name ?n .
      FILTER(?unbound = 1)
    }";
    assert_eq!(claims(&fluree, body).await, Vec::<String>::new());
}

#[tokio::test]
async fn jsonld_filter_on_a_var_nothing_binds_applies_to_a_property_join_star() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;

    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "from": LEDGER_ID,
        "select": ["?c"],
        "where": [
            {"@id": "?c", "ex:payer": {"@id": "ex:p1"}, "ex:age": "?a", "ex:name": "?n"},
            ["filter", "(= ?unbound 1)"]
        ]
    });
    let view = fluree.db(LEDGER_ID).await.expect("view");
    let plan = fluree.explain(&view, &query).await.expect("explain");
    assert!(
        plan["plan"]["physical"]
            .to_string()
            .contains("PropertyJoinOperator"),
        "precondition: the star should plan as a property join: {plan}"
    );

    let result = fluree
        .query_from()
        .jsonld(&query)
        .execute_tracked()
        .await
        .expect("query should succeed");
    assert_eq!(result.result, json!([]));
}
