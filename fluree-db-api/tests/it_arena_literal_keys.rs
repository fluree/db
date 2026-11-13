//! A vector literal read from the index is a handle into its predicate's
//! arena, numbered per graph and predicate. The equality surfaces (DISTINCT,
//! GROUP BY, COUNT(DISTINCT)) key it by its value, as they key the same
//! literal read from novelty: two predicates' first handles are two vectors,
//! and one vector stored twice is one, whatever handles it has.

use fluree_db_api::{FlureeBuilder, GraphDb, ReindexOptions};
use serde_json::json;

const TTL: &str = r#"@prefix ex: <http://example.org/> .
@prefix f: <https://ns.flur.ee/db#> .
ex:a ex:v1 "[1.0, 0.0]"^^f:embeddingVector .
ex:b ex:v2 "[0.0, 1.0]"^^f:embeddingVector .
ex:c ex:w "[0.5, 0.5]"^^f:embeddingVector .
ex:d ex:w "[0.5, 0.5]"^^f:embeddingVector .
"#;

const P: &str = "PREFIX ex: <http://example.org/>\n";

async fn sparql_rows(fluree: &fluree_db_api::Fluree, ledger: &str, query: &str) -> Vec<String> {
    let handle = fluree.ledger_cached(ledger).await.expect("ledger");
    let view = GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state());
    let json = view
        .query(fluree)
        .sparql(query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut rows: Vec<String> = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| {
            let mut cells: Vec<String> = b
                .as_object()
                .expect("row")
                .iter()
                .map(|(k, v)| format!("{k}={}", v["value"].as_str().unwrap_or_default()))
                .collect();
            cells.sort();
            cells.join(" ")
        })
        .collect();
    rows.sort();
    rows
}

/// Each case's answer is the same before the ledger is indexed (novelty
/// decodes every value) and after (the index binds arena handles).
#[tokio::test]
async fn indexed_vectors_key_by_value() {
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    let ledger = "it/arena-literal-keys:main";
    fluree.create_ledger(ledger).await.expect("create");
    let handle = fluree.ledger_cached(ledger).await.expect("cache");
    fluree
        .stage(&handle)
        .insert_turtle(TTL)
        .execute()
        .await
        .expect("seed");
    let both = "{ ?s ex:v1 ?v } UNION { ?s ex:v2 ?v }";
    let cases = [
        // Two predicates' first handles hold two different vectors.
        (
            format!("{P}SELECT DISTINCT ?v WHERE {{ {both} }}"),
            vec!["v=[0.0,1.0]", "v=[1.0,0.0]"],
        ),
        (
            format!("{P}SELECT ?v (COUNT(*) AS ?n) WHERE {{ {both} }} GROUP BY ?v"),
            vec!["n=1 v=[0.0,1.0]", "n=1 v=[1.0,0.0]"],
        ),
        (
            format!("{P}SELECT (COUNT(DISTINCT ?v) AS ?n) WHERE {{ {both} }}"),
            vec!["n=2"],
        ),
        // One vector stored on two subjects of one predicate: two handles,
        // one value.
        (
            format!("{P}SELECT DISTINCT ?v WHERE {{ ?s ex:w ?v }}"),
            vec!["v=[0.5,0.5]"],
        ),
    ];
    let mut failures = Vec::new();
    for indexed in [false, true] {
        if indexed {
            fluree
                .reindex(ledger, ReindexOptions::default())
                .await
                .expect("index");
            fluree.disconnect_ledger(ledger).await;
        }
        for (query, want) in &cases {
            let got = sparql_rows(&fluree, ledger, query).await;
            if got != *want {
                failures.push(format!(
                    "indexed {indexed}: {query}\n  got {got:?}\n  want {want:?}"
                ));
            }
        }
        // The same keying on JSON-LD.
        let q = json!({
            "@context": {"ex": "http://example.org/"},
            "from": ledger,
            "selectDistinct": ["?v"],
            "where": [["union", {"@id": "?s", "ex:v1": "?v"}, {"@id": "?s", "ex:v2": "?v"}]]
        });
        let rows = fluree
            .query_connection(&q)
            .await
            .expect("JSON-LD query")
            .row_count();
        if rows != 2 {
            failures.push(format!(
                "indexed {indexed}: JSON-LD selectDistinct: {rows} rows, want 2"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
