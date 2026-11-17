//! Shared by the reasoning lane suites (`it_reasoning_batched_probe`,
//! `it_reasoning_lane_cache`).

use fluree_db_api::{Fluree, GraphDb, QueryInput};
use serde_json::{json, Value};

pub const SPARQL_PREFIXES: &str = "PREFIX ex: <http://example.org/ns/>\n\
     PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n";

/// The nested-loop join's subject- and object-lane flush spans.
pub const SUBJECT_LANE: &str = "join_flush_batched_binary";
pub const OBJECT_LANE: &str = "join_flush_batched_object_binary";

pub fn ctx() -> Value {
    json!({
        "ex": "http://example.org/ns/",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
    })
}

/// An OWL 2 RL SPARQL query over `body`.
pub fn sparql(body: &str) -> String {
    format!("# PRAGMA reasoning: owl2rl\n{SPARQL_PREFIXES}{body}")
}

/// The query's rows, sorted.
pub async fn query_rows(fluree: &Fluree, view: &GraphDb, input: QueryInput<'_>) -> Vec<Value> {
    let result = fluree.query(view, input).await.expect("reasoning query");
    let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
    sorted(super::normalize_rows(&jsonld))
}

pub fn sorted(mut rows: Vec<Value>) -> Vec<Value> {
    rows.sort_by_key(ToString::to_string);
    rows
}
