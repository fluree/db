//! Integration tests for AgentJson output format.
//!
//! Verifies envelope structure, schema extraction, byte-budget truncation,
//! and resume query generation against real query results.

mod support;

use fluree_db_api::{AgentJsonContext, FlureeBuilder, FormatterConfig};
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, query_sparql, MemoryFluree, MemoryLedger};

fn ctx() -> JsonValue {
    json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

async fn seed_data() -> (MemoryFluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/agent-json:test");

    let tx = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "@type": "schema:Person", "schema:name": "Alice", "schema:age": {"@value": 30, "@type": "xsd:long"}},
            {"@id": "ex:bob", "@type": "schema:Person", "schema:name": "Bob", "schema:age": {"@value": 25, "@type": "xsd:long"}},
            {"@id": "ex:charlie", "@type": "schema:Person", "schema:name": "Charlie", "schema:age": {"@value": 35, "@type": "xsd:long"}}
        ]
    });

    let committed = fluree.insert(ledger0, &tx).await.expect("seed data");
    (fluree, committed.ledger)
}

// ============================================================================
// Envelope structure
// ============================================================================

#[tokio::test]
async fn agent_json_envelope_structure() {
    let (fluree, ledger) = seed_data().await;

    let sparql = "PREFIX schema: <http://schema.org/> \
                  SELECT ?name ?age WHERE { ?s schema:name ?name ; schema:age ?age }";

    let result = query_sparql(&fluree, &ledger, sparql).await.unwrap();
    let json = result
        .to_agent_json(&ledger.snapshot)
        .expect("format agent json");

    let obj = json.as_object().expect("envelope should be object");

    // Required fields
    assert!(obj.contains_key("schema"), "missing schema");
    assert!(obj.contains_key("rows"), "missing rows");
    assert!(obj.contains_key("rowCount"), "missing rowCount");
    assert!(obj.contains_key("hasMore"), "missing hasMore");
    assert!(obj.contains_key("t"), "missing t for single-ledger");

    // hasMore should be false (no truncation)
    assert_eq!(obj["hasMore"], json!(false));

    // rowCount should match rows array length
    let rows = obj["rows"].as_array().unwrap();
    assert_eq!(obj["rowCount"], json!(rows.len()));
    assert_eq!(rows.len(), 3);

    // No message or resume when not truncated
    assert!(!obj.contains_key("message"));
    assert!(!obj.contains_key("resume"));
}

// ============================================================================
// Schema extraction
// ============================================================================

#[tokio::test]
async fn agent_json_schema_types() {
    let (fluree, ledger) = seed_data().await;

    let sparql = "PREFIX schema: <http://schema.org/> \
                  SELECT ?s ?name ?age WHERE { ?s schema:name ?name ; schema:age ?age }";

    let result = query_sparql(&fluree, &ledger, sparql).await.unwrap();
    let json = result
        .to_agent_json(&ledger.snapshot)
        .expect("format agent json");

    let schema = json["schema"].as_object().expect("schema should be object");

    // ?s is a URI
    assert_eq!(schema["?s"], json!("uri"));
    // SPARQL queries without @context return full datatype IRIs
    assert_eq!(
        schema["?name"],
        json!("http://www.w3.org/2001/XMLSchema#string")
    );
    assert_eq!(
        schema["?age"],
        json!("http://www.w3.org/2001/XMLSchema#long")
    );
}

// ============================================================================
// Row format (native JSON values)
// ============================================================================

#[tokio::test]
async fn agent_json_native_values() {
    let (fluree, ledger) = seed_data().await;

    let sparql = "PREFIX schema: <http://schema.org/> \
                  SELECT ?name ?age WHERE { ?s schema:name ?name ; schema:age ?age } \
                  ORDER BY ?name LIMIT 1";

    let result = query_sparql(&fluree, &ledger, sparql).await.unwrap();
    let json = result
        .to_agent_json(&ledger.snapshot)
        .expect("format agent json");

    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);

    let row = rows[0].as_object().unwrap();
    // Name should be a bare string (inferable type)
    assert_eq!(row["?name"], json!("Alice"));
    // Age should be a bare number (inferable type)
    assert_eq!(row["?age"], json!(30));
}

// ============================================================================
// Byte-budget truncation
// ============================================================================

#[tokio::test]
async fn agent_json_truncation() {
    let (fluree, ledger) = seed_data().await;

    let sparql = "PREFIX schema: <http://schema.org/> \
                  SELECT ?name ?age WHERE { ?s schema:name ?name ; schema:age ?age } \
                  ORDER BY ?name";

    let result = query_sparql(&fluree, &ledger, sparql).await.unwrap();

    // Use a tiny byte budget to force truncation
    let config = FormatterConfig::agent_json().with_max_bytes(50);
    let json = result
        .to_agent_json_with_config(&ledger.snapshot, &config)
        .expect("format with truncation");

    let obj = json.as_object().unwrap();
    assert_eq!(obj["hasMore"], json!(true));
    assert!(obj.contains_key("message"));

    let row_count = obj["rowCount"].as_u64().unwrap() as usize;
    let rows = obj["rows"].as_array().unwrap();
    assert_eq!(rows.len(), row_count);
    assert!(
        row_count < 3,
        "should have fewer than 3 rows due to truncation"
    );
}

// ============================================================================
// Resume query with context
// ============================================================================

#[tokio::test]
async fn agent_json_resume_query() {
    let (fluree, ledger) = seed_data().await;

    let sparql = "PREFIX schema: <http://schema.org/> \
                  SELECT ?name ?age \
                  WHERE { ?s schema:name ?name ; schema:age ?age } \
                  ORDER BY ?name";

    let result = query_sparql(&fluree, &ledger, sparql).await.unwrap();

    // Simulate single-FROM context with the SPARQL text containing a FROM clause
    // (the query itself doesn't need FROM for single-ledger, but the resume
    // generator needs it in the sparql_text to produce a resume query)
    let sparql_with_from = "PREFIX schema: <http://schema.org/> \
                            SELECT ?name ?age FROM <it/agent-json:test> \
                            WHERE { ?s schema:name ?name ; schema:age ?age } \
                            ORDER BY ?name";

    let config = FormatterConfig::agent_json()
        .with_max_bytes(50)
        .with_agent_json_context(AgentJsonContext {
            sparql_text: Some(sparql_with_from.to_string()),
            from_count: 1,
            iso_timestamp: Some("2026-03-26T14:30:00Z".to_string()),
            ..Default::default()
        });

    let json = result
        .to_agent_json_with_config(&ledger.snapshot, &config)
        .expect("format with resume");

    let obj = json.as_object().unwrap();
    assert_eq!(obj["hasMore"], json!(true));
    assert!(obj.contains_key("resume"), "should have resume query");
    assert!(obj.contains_key("iso"), "should have iso timestamp");

    let resume = obj["resume"].as_str().unwrap();
    assert!(resume.contains("@t:"), "resume should pin with @t:");
    assert!(resume.contains("OFFSET"), "resume should have OFFSET");
    assert!(resume.contains("LIMIT"), "resume should have LIMIT");
}

// ============================================================================
// No truncation — no budget
// ============================================================================

#[tokio::test]
async fn agent_json_no_budget_returns_all() {
    let (fluree, ledger) = seed_data().await;

    let sparql = "PREFIX schema: <http://schema.org/> \
                  SELECT ?name WHERE { ?s schema:name ?name } ORDER BY ?name";

    let result = query_sparql(&fluree, &ledger, sparql).await.unwrap();
    let json = result
        .to_agent_json(&ledger.snapshot)
        .expect("format agent json");

    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(json["hasMore"], json!(false));
    assert!(!json.as_object().unwrap().contains_key("message"));
}

// ============================================================================
// ISO timestamp present when context provided
// ============================================================================

#[tokio::test]
async fn agent_json_iso_timestamp() {
    let (fluree, ledger) = seed_data().await;

    let sparql = "PREFIX schema: <http://schema.org/> \
                  SELECT ?name WHERE { ?s schema:name ?name }";

    let result = query_sparql(&fluree, &ledger, sparql).await.unwrap();

    let config = FormatterConfig::agent_json().with_agent_json_context(AgentJsonContext {
        sparql_text: None,
        from_count: 1,
        iso_timestamp: Some("2026-03-26T14:30:00Z".to_string()),
        ..Default::default()
    });

    let json = result
        .to_agent_json_with_config(&ledger.snapshot, &config)
        .expect("format with iso");

    assert_eq!(json["iso"], json!("2026-03-26T14:30:00Z"));
    assert!(json["t"].is_number(), "single-ledger should have t");
}

// ============================================================================
// Multi-ledger: no t field
// ============================================================================

#[tokio::test]
async fn agent_json_multi_ledger_no_t() {
    let (fluree, ledger) = seed_data().await;

    let sparql = "PREFIX schema: <http://schema.org/> \
                  SELECT ?name WHERE { ?s schema:name ?name }";

    let result = query_sparql(&fluree, &ledger, sparql).await.unwrap();

    let config = FormatterConfig::agent_json().with_agent_json_context(AgentJsonContext {
        sparql_text: None,
        from_count: 2, // simulate multi-ledger
        iso_timestamp: Some("2026-03-26T14:30:00Z".to_string()),
        ..Default::default()
    });

    let json = result
        .to_agent_json_with_config(&ledger.snapshot, &config)
        .expect("format multi-ledger");

    // Multi-ledger: no t field
    assert!(
        !json.as_object().unwrap().contains_key("t"),
        "multi-ledger should not have t"
    );
    assert_eq!(json["iso"], json!("2026-03-26T14:30:00Z"));
}
