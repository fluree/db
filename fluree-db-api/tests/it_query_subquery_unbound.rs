//! Materialized subqueries must join compatible mappings, including unbound
//! outer keys, without discarding duplicate solutions or changing inner slices.
use crate::support::{assert_index_defaults, genesis_ledger, rebuild_and_publish_index};
use fluree_db_api::{Fluree, FlureeBuilder};
use serde_json::{json, Value};

const LEDGER: &str = "subquery-unbound:main";

async fn seed() -> Fluree {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, LEDGER);
    fluree
        .insert(
            ledger,
            &json!({
                "@context":{"ex":"http://example.org/"},
                "@graph":[
                    {"@id":"ex:a", "ex:price":10},
                    {"@id":"ex:b", "ex:price":20},
                    {"@id":"ex:left", "ex:selected":{"@id":"ex:a"}}
                ]
            }),
        )
        .await
        .unwrap();
    fluree
}

async fn check(fluree: &Fluree, query: &str, expected: Value) {
    // These fixtures use simple SELECT-variable lists. Compare in that order;
    // the SPARQL JSON renderer may order head.vars differently.
    let vars: Vec<_> = query
        .split_once("WHERE")
        .unwrap()
        .0
        .split_whitespace()
        .skip(1)
        .map(|var| var.strip_prefix('?').unwrap())
        .collect();
    let query = format!("PREFIX ex: <http://example.org/> {query}").replacen(
        "WHERE",
        &format!("FROM <{LEDGER}> WHERE"),
        1,
    );
    let response = fluree
        .query_from()
        .sparql(&query)
        .track_all()
        .execute_tracked()
        .await
        .unwrap();
    assert_eq!(response.status, 200, "{response:?}");
    let result = response.result;
    let mut rows: Vec<Vec<String>> = result["results"]["bindings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            vars.iter()
                .map(|var| {
                    let value = row[*var]["value"].as_str().unwrap_or("UNDEF");
                    value
                        .strip_prefix("http://example.org/")
                        .unwrap_or(value)
                        .to_owned()
                })
                .collect()
        })
        .collect();
    rows.sort();
    assert_eq!(json!(rows), expected, "{query}");
}

#[tokio::test]
async fn grouped_subquery_preserves_unbound_keys_and_duplicates() {
    let fluree = seed().await;
    for indexed in [false, true] {
        if indexed {
            rebuild_and_publish_index(&fluree, LEDGER).await;
        }
        check(
            &fluree,
            "SELECT ?p ?n WHERE {
            VALUES ?p { UNDEF ex:a }
            { SELECT ?p (SUM(?price) AS ?n) WHERE { ?p ex:price ?price } GROUP BY ?p }
        }",
            json!([["a", "10"], ["a", "10"], ["b", "20"]]),
        )
        .await;
        check(
            &fluree,
            "SELECT ?p ?n WHERE {
            VALUES ?p { UNDEF UNDEF }
            { SELECT ?p (SUM(?price) AS ?n) WHERE { ?p ex:price ?price } GROUP BY ?p }
        }",
            json!([["a", "10"], ["a", "10"], ["b", "20"], ["b", "20"]]),
        )
        .await;
        check(
            &fluree,
            "SELECT ?p ?n WHERE {
            VALUES ?p { ex:a ex:a ex:missing }
            { SELECT ?p (SUM(?price) AS ?n) WHERE { ?p ex:price ?price } GROUP BY ?p }
        }",
            json!([["a", "10"], ["a", "10"]]),
        )
        .await;
    }
}

#[tokio::test]
async fn grouped_subquery_restricts_partially_bound_composite_keys() {
    let fluree = seed().await;
    for indexed in [false, true] {
        if indexed {
            rebuild_and_publish_index(&fluree, LEDGER).await;
        }
        check(&fluree, "SELECT ?p ?price ?n WHERE {
            VALUES (?p ?price) { (UNDEF 10) (ex:a UNDEF) (UNDEF UNDEF) (ex:missing UNDEF) (ex:a 20) }
            { SELECT ?p ?price (COUNT(*) AS ?n) WHERE { ?p ex:price ?price } GROUP BY ?p ?price }
        }", json!([["a","10","1"],["a","10","1"],["a","10","1"],["b","20","1"]])).await;
        // The aggregate output is reconciled after the key lookup. Expanding
        // an unbound product must still reject a conflicting bound total.
        check(
            &fluree,
            "SELECT ?p ?n WHERE {
            VALUES (?p ?n) { (UNDEF 10) (ex:a 20) (UNDEF UNDEF) }
            { SELECT ?p (SUM(?price) AS ?n) WHERE { ?p ex:price ?price } GROUP BY ?p }
        }",
            json!([["a", "10"], ["a", "10"], ["b", "20"]]),
        )
        .await;
    }
}

#[tokio::test]
async fn grouped_subquery_distinguishes_union_unbound_from_optional_poisoned() {
    let fluree = seed().await;
    for indexed in [false, true] {
        if indexed {
            rebuild_and_publish_index(&fluree, LEDGER).await;
        }
        for (outer, expected) in [
            // Preserve the engine's existing Poisoned contract for failed
            // OPTIONAL bindings: they must not fan out like an actual UNDEF.
            (
                "VALUES ?row { ex:left ex:right } OPTIONAL { ?row ex:selected ?p }",
                json!([["left", "a", "10"]]),
            ),
            (
                "{ VALUES (?row ?p) { (ex:left ex:a) } } UNION { VALUES ?row { ex:right } }",
                json!([
                    ["left", "a", "10"],
                    ["right", "a", "10"],
                    ["right", "b", "20"]
                ]),
            ),
        ] {
            // Materialize the outer solutions independently so this test does
            // not depend on placement of a bare OPTIONAL relative to the join.
            check(
                &fluree,
                &format!(
                    "SELECT ?row ?p ?n WHERE {{ {{ SELECT ?row ?p WHERE {{ {outer} }} LIMIT 10 }}
                {{ SELECT ?p (SUM(?price) AS ?n) WHERE {{ ?p ex:price ?price }} GROUP BY ?p }}
            }}"
                ),
                expected,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn unbound_keys_preserve_inner_slice_and_empty_results() {
    let fluree = seed().await;
    for indexed in [false, true] {
        if indexed {
            rebuild_and_publish_index(&fluree, LEDGER).await;
        }
        check(
            &fluree,
            "SELECT ?p WHERE {
            VALUES ?p { UNDEF ex:a ex:b }
            { SELECT DISTINCT ?p WHERE { ?p ex:price ?price } ORDER BY ?p LIMIT 1 OFFSET 1 }
        }",
            json!([["b"], ["b"]]),
        )
        .await;
        check(&fluree, "SELECT ?p ?n WHERE {
            VALUES ?p { UNDEF ex:a }
            { SELECT ?p (COUNT(*) AS ?n) WHERE { ?p ex:price ?price FILTER(?price > 100) } GROUP BY ?p }
        }", json!([])).await;
    }
}
