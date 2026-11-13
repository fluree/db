//! A GRAPH scope streams its inner answer as the inner subplan produces it,
//! rather than holding a parent row's whole answer and handing it on as one
//! batch. Observable as the answer arriving in batches no larger than the
//! batch size, on SPARQL and on its JSON-LD twin.

use crate::support::genesis_ledger_for_fluree;
use fluree_db_api::{FlureeBuilder, GraphDb};
use serde_json::json;

const ROWS: usize = 2_500;
const BATCH_SIZE: usize = 1_000;
const G: &str = "http://example.org/g";

#[tokio::test]
async fn a_named_graph_scope_streams_its_answer() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/graph-scope-streams:main";
    let mut trig = format!("@prefix ex: <http://example.org/> .\nGRAPH <{G}> {{\n");
    for i in 0..ROWS {
        trig.push_str(&format!("  ex:s{i} ex:p {i} .\n"));
    }
    trig.push_str("}\n");
    let result = fluree
        .insert_turtle(genesis_ledger_for_fluree(&fluree, id).await, &trig)
        .await
        .expect("insert the named graph");
    let db = GraphDb::from_ledger_state(&result.ledger);

    let sparql = format!(
        "PREFIX ex: <http://example.org/>\nSELECT ?s ?o WHERE {{ GRAPH <{G}> {{ ?s ex:p ?o }} }}"
    );
    let jsonld = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s", "?o"],
        "where": [["graph", G, {"@id": "?s", "ex:p": "?o"}]],
    });
    for (surface, result) in [
        ("SPARQL", fluree.query(&db, sparql.as_str()).await),
        ("JSON-LD", fluree.query(&db, &jsonld).await),
    ] {
        let result = result.unwrap_or_else(|e| panic!("{surface}: {e}"));
        let sizes: Vec<usize> = result
            .batches
            .iter()
            .map(fluree_db_api::Batch::len)
            .collect();
        assert_eq!(sizes.iter().sum::<usize>(), ROWS, "{surface}: {sizes:?}");
        assert!(
            sizes.len() > 1 && sizes.iter().all(|&n| n <= BATCH_SIZE),
            "{surface}: the GRAPH scope's answer came in batches of {sizes:?}"
        );
    }
}
