//! Avoid a second final deduplication pass when distinct counts consume a chain.

use crate::support::{genesis_ledger, normalize_rows};
use fluree_db_api::{FlureeBuilder, QueryInput, ReindexOptions};
use serde_json::{json, Value};

fn count_ops(node: &Value, op: &str) -> usize {
    usize::from(node["op"] == op)
        + node["children"].as_array().map_or(0, |children| {
            children
                .iter()
                .map(|child| count_ops(&child["node"], op))
                .sum()
        })
}

fn aggregate_input(node: &Value) -> Option<&Value> {
    if node["op"] == "GroupAggregateOperator" {
        return Some(&node["children"][0]["node"]);
    }
    node["children"]
        .as_array()?
        .iter()
        .find_map(|child| aggregate_input(&child["node"]))
}

#[tokio::test]
async fn distinct_chain_counts_preserve_results_and_intermediate_dedup() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "aggregate-distinct:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let ledger = fluree.insert(ledger, &json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:p1", "ex:livesIn": {"@id": "ex:cityA"},
             "ex:knows": [{"@id": "ex:f1"}, {"@id": "ex:f2"}]},
            {"@id": "ex:p2", "ex:livesIn": {"@id": "ex:cityA"}, "ex:knows": {"@id": "ex:f1"}},
            {"@id": "ex:p3", "ex:livesIn": {"@id": "ex:cityB"}, "ex:knows": {"@id": "ex:f2"}},
            {"@id": "ex:f1", "ex:knows": [{"@id": "ex:x"}, {"@id": "ex:y"}, {"@id": "ex:p1"}]},
            {"@id": "ex:f2", "ex:knows": {"@id": "ex:x"}}
        ]
    })).await.unwrap().ledger;
    let before_update = ledger.t();
    let body = "?p ex:livesIn ?city ; ex:knows ?f . ?f ex:knows ?fof";
    let query = format!("PREFIX ex: <http://example.org/> SELECT ?city (COUNT(DISTINCT ?fof) AS ?n) WHERE {{ {body} }} GROUP BY ?city");
    let expected = json!([["ex:cityA", 3], ["ex:cityB", 1]]);

    for indexed in [false, true] {
        if indexed {
            fluree
                .reindex(ledger_id, ReindexOptions::default())
                .await
                .unwrap();
        }
        let view = fluree.db(ledger_id).await.unwrap();
        for (query, expected, intermediate) in [
            (query.clone(), expected.clone(), Some(1)),
            (
                query.replace(body, &format!("{body} FILTER(?fof != ?p && ?fof != ex:y)")),
                json!([["ex:cityA", 2], ["ex:cityB", 1]]),
                Some(0),
            ),
            (
                format!("{query} HAVING(COUNT(DISTINCT ?fof) > 1) ORDER BY DESC(?n) LIMIT 1"),
                json!([["ex:cityA", 3]]),
                Some(1),
            ),
            (
                query.replace(
                    "(COUNT(DISTINCT ?fof) AS ?n)",
                    "(COUNT(DISTINCT ?fof) AS ?n) (COUNT(*) AS ?rows)",
                ),
                json!([["ex:cityA", 3, 7], ["ex:cityB", 1, 1]]),
                None,
            ),
            (
                query.replace(
                    "(COUNT(DISTINCT ?fof) AS ?n)",
                    "(COUNT(DISTINCT ?fof) AS ?n) (COUNT(DISTINCT ?f) AS ?friends)",
                ),
                json!([["ex:cityA", 3, 2], ["ex:cityB", 1, 1]]),
                None,
            ),
            (
                query
                    .replace("SELECT ?city", "SELECT")
                    .replace("GROUP BY ?city", "")
                    .replace(body, &format!("{body} FILTER(false)")),
                json!([[0]]),
                None,
            ),
        ] {
            let result = fluree
                .query(&view, QueryInput::Sparql(&query))
                .await
                .unwrap();
            assert_eq!(
                normalize_rows(&result.to_jsonld(&view.snapshot).unwrap()),
                normalize_rows(&expected),
                "indexed={indexed}: {query}"
            );
            if let Some(intermediate) = intermediate {
                let plan = fluree.explain_sparql(&view, &query).await.unwrap();
                let physical = &plan["plan"]["physical"];
                assert_ne!(
                    aggregate_input(physical).unwrap()["op"],
                    "DistinctOperator",
                    "{physical}"
                );
                assert_eq!(
                    count_ops(physical, "DistinctOperator"),
                    intermediate,
                    "{physical}"
                );
            }
        }
    }

    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:f2", "ex:knows": {"@id": "ex:z"}
            }),
        )
        .await
        .unwrap();
    let view = fluree.db(ledger_id).await.unwrap();
    let result = fluree
        .query(&view, QueryInput::Sparql(&query))
        .await
        .unwrap();
    assert_eq!(
        normalize_rows(&result.to_jsonld(&view.snapshot).unwrap()),
        normalize_rows(&json!([["ex:cityA", 4], ["ex:cityB", 2]]))
    );
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .unwrap();
    let historical = fluree.db_at_t(ledger_id, before_update).await.unwrap();
    let result = fluree
        .query(&historical, QueryInput::Sparql(&query))
        .await
        .unwrap();
    assert_eq!(
        normalize_rows(&result.to_jsonld(&historical.snapshot).unwrap()),
        normalize_rows(&expected)
    );
}
