//! Integration tests for R2RML graph sources.
//!
//! These tests verify that GRAPH patterns correctly execute against R2RML
//! graph sources backed by Iceberg tables.
//!
//! Test categories:
//! - Unit tests: R2RML parsing, compilation, term materialization
//! - Engine-level E2E: Full query execution with mocked providers (no external infra)
//! - External E2E: Real Polaris/MinIO integration (requires infrastructure)

mod support;

use async_trait::async_trait;
use fluree_db_iceberg::io::batch::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType};
use fluree_db_query::error::{QueryError, Result as QueryResult};
use fluree_db_query::r2rml::{ColumnBatchStream, R2rmlProvider, R2rmlTableProvider, ScanFilter};
use fluree_db_r2rml::loader::R2rmlLoader;
use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Additional imports for engine-level E2E tests
use fluree_db_api::{
    execute, ContextConfig, ExecutableQuery, FlureeBuilder, ParsedContext, Pattern, VarRegistry,
};
use fluree_db_core::{FlakeValue, GraphDbRef, NoOverlay, Tracker};
use fluree_db_query::ir::triple::{Ref, Term, TriplePattern};
use fluree_db_query::ir::GraphName;
use fluree_db_query::ir::{
    AggregateFn, AggregateSpec, Expression, Function, Grouping, InputSemantics, Query, QueryOutput,
};
use fluree_db_query::var_registry::VarId;
use support::genesis_ledger;

fn r2rml_test_config<'a, P: R2rmlProvider + R2rmlTableProvider>(
    tracker: &'a Tracker,
    provider: &'a P,
) -> ContextConfig<'a, 'a> {
    ContextConfig {
        tracker: Some(tracker),
        r2rml: Some((provider, provider)),
        ..Default::default()
    }
}

// =============================================================================
// Mock R2RML Provider for Testing
// =============================================================================

/// A mock R2RML provider that returns pre-configured data.
///
/// This allows testing the R2RML query integration without requiring
/// external Iceberg/Polaris/MinIO infrastructure.
#[derive(Debug)]
struct MockR2rmlProvider {
    /// Pre-compiled R2RML mapping
    mapping: Arc<CompiledR2rmlMapping>,
    /// Pre-built column batches to return from scan_table
    batches: Vec<ColumnBatch>,
}

impl MockR2rmlProvider {
    fn new(mapping: CompiledR2rmlMapping, batches: Vec<ColumnBatch>) -> Self {
        Self {
            mapping: Arc::new(mapping),
            batches,
        }
    }
}

#[async_trait]
impl R2rmlProvider for MockR2rmlProvider {
    async fn has_r2rml_mapping(&self, _graph_source_id: &str) -> bool {
        true
    }

    async fn compiled_mapping(
        &self,
        _graph_source_id: &str,
        _as_of_t: Option<i64>,
    ) -> QueryResult<Arc<CompiledR2rmlMapping>> {
        Ok(Arc::clone(&self.mapping))
    }
}

#[async_trait]
impl R2rmlTableProvider for MockR2rmlProvider {
    async fn scan_table(
        &self,
        _graph_source_id: &str,
        _table_name: &str,
        _projection: &[String],
        _filters: &[ScanFilter],
        _as_of_t: Option<i64>,
    ) -> QueryResult<ColumnBatchStream> {
        Ok(vec_batch_stream(self.batches.clone()))
    }
}

/// Wrap pre-built batches as a `ColumnBatchStream` for the mock providers.
fn vec_batch_stream(batches: Vec<ColumnBatch>) -> ColumnBatchStream {
    use futures::StreamExt;
    Box::pin(futures::stream::iter(batches).map(Ok))
}

// =============================================================================
// Test Fixtures
// =============================================================================

/// Simple R2RML mapping for airlines table.
const AIRLINE_MAPPING_TTL: &str = r#"
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .

<http://example.org/mapping#AirlineMapping> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "openflights.airlines" ] ;
    rr:subjectMap [
        rr:template "http://example.org/airline/{id}" ;
        rr:class ex:Airline
    ] ;
    rr:predicateObjectMap [
        rr:predicate ex:name ;
        rr:objectMap [ rr:column "name" ]
    ] ;
    rr:predicateObjectMap [
        rr:predicate ex:country ;
        rr:objectMap [ rr:column "country" ]
    ] ;
    rr:predicateObjectMap [
        rr:predicate ex:iata ;
        rr:objectMap [ rr:column "iata" ]
    ] .
"#;

/// Create a sample column batch representing airline data.
fn sample_airline_batch() -> ColumnBatch {
    let schema = BatchSchema::new(vec![
        FieldInfo {
            name: "id".to_string(),
            field_type: FieldType::Int64,
            nullable: false,
            field_id: 1,
        },
        FieldInfo {
            name: "name".to_string(),
            field_type: FieldType::String,
            nullable: true,
            field_id: 2,
        },
        FieldInfo {
            name: "country".to_string(),
            field_type: FieldType::String,
            nullable: true,
            field_id: 3,
        },
        FieldInfo {
            name: "iata".to_string(),
            field_type: FieldType::String,
            nullable: true,
            field_id: 4,
        },
    ]);

    let columns = vec![
        Column::Int64(vec![Some(1), Some(2), Some(3)]),
        Column::String(vec![
            Some("Delta Air Lines".to_string()),
            Some("United Airlines".to_string()),
            Some("American Airlines".to_string()),
        ]),
        Column::String(vec![
            Some("United States".to_string()),
            Some("United States".to_string()),
            Some("United States".to_string()),
        ]),
        Column::String(vec![
            Some("DL".to_string()),
            Some("UA".to_string()),
            Some("AA".to_string()),
        ]),
    ];

    ColumnBatch::new(Arc::new(schema), columns).unwrap()
}

/// Compile the airline mapping from Turtle.
fn compile_airline_mapping() -> CompiledR2rmlMapping {
    R2rmlLoader::from_turtle(AIRLINE_MAPPING_TTL)
        .expect("Failed to parse R2RML Turtle")
        .compile()
        .expect("Failed to compile R2RML mapping")
}

// =============================================================================
// Unit-Level Tests (Mock Provider)
// =============================================================================

/// Test that the R2RML loader can parse and compile the airline mapping.
#[test]
fn test_r2rml_mapping_compilation() {
    let mapping = compile_airline_mapping();

    // Should have one TriplesMap
    assert_eq!(mapping.triples_maps.len(), 1);

    // Should be indexed by class
    let airline_maps = mapping.find_maps_for_class("http://example.org/Airline");
    assert_eq!(airline_maps.len(), 1);

    // Should be indexed by predicate
    let name_maps = mapping.find_maps_for_predicate("http://example.org/name");
    assert_eq!(name_maps.len(), 1);
}

/// Test that mock provider returns expected data.
#[tokio::test]
async fn test_mock_r2rml_provider() {
    let mapping = compile_airline_mapping();
    let batch = sample_airline_batch();
    let provider = MockR2rmlProvider::new(mapping, vec![batch]);

    // Test has_r2rml_mapping
    assert!(provider.has_r2rml_mapping("test-gs:main").await);

    // Test compiled_mapping
    let loaded = provider
        .compiled_mapping("test-gs:main", Some(0))
        .await
        .unwrap();
    assert_eq!(loaded.triples_maps.len(), 1);

    // Test scan_table
    use futures::StreamExt;
    let batches: Vec<ColumnBatch> = provider
        .scan_table("test-gs:main", "openflights.airlines", &[], &[], Some(0))
        .await
        .unwrap()
        .map(|b| b.unwrap())
        .collect()
        .await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows, 3);
}

// =============================================================================
// Integration Tests with Fluree
// =============================================================================

// Note: Full GRAPH query integration tests require the nameservice to be
// configured with an Iceberg graph source. These tests are marked as ignored until
// we have proper test infrastructure for registering mock graph sources.

/// Test that R2RML materialization produces correct RDF terms.
#[test]
fn test_r2rml_term_materialization() {
    use fluree_db_r2rml::mapping::{ObjectMap, SubjectMap};
    use fluree_db_r2rml::materialize::{
        materialize_object_from_batch, materialize_subject_from_batch, RdfTerm,
    };

    let batch = sample_airline_batch();

    // Test subject materialization
    let subject_map = SubjectMap::template("http://example.org/airline/{id}");
    let subject = materialize_subject_from_batch(&subject_map, &batch, 0).unwrap();
    assert!(subject.is_some());
    match subject.unwrap() {
        RdfTerm::Iri(iri) => assert_eq!(iri, "http://example.org/airline/1"),
        _ => panic!("Expected IRI"),
    }

    // Test object materialization (column value)
    let object_map = ObjectMap::column("name");
    let object = materialize_object_from_batch(&object_map, &batch, 0).unwrap();
    assert!(object.is_some());
    match object.unwrap() {
        RdfTerm::Literal { value, .. } => assert_eq!(value, "Delta Air Lines"),
        _ => panic!("Expected Literal"),
    }

    // Second row
    let subject2 = materialize_subject_from_batch(&subject_map, &batch, 1).unwrap();
    match subject2.unwrap() {
        RdfTerm::Iri(iri) => assert_eq!(iri, "http://example.org/airline/2"),
        _ => panic!("Expected IRI"),
    }
}

/// Test materialization handles null values correctly.
#[test]
fn test_r2rml_null_handling() {
    use fluree_db_r2rml::mapping::SubjectMap;
    use fluree_db_r2rml::materialize::materialize_subject_from_batch;

    // Create batch with null in template column
    let schema = BatchSchema::new(vec![FieldInfo {
        name: "id".to_string(),
        field_type: FieldType::Int64,
        nullable: true,
        field_id: 1,
    }]);
    let columns = vec![Column::Int64(vec![Some(1), None, Some(3)])];
    let batch = ColumnBatch::new(Arc::new(schema), columns).unwrap();

    let subject_map = SubjectMap::template("http://example.org/{id}");

    // Row 0: non-null id
    let result0 = materialize_subject_from_batch(&subject_map, &batch, 0).unwrap();
    assert!(result0.is_some());

    // Row 1: null id - should produce None (skip row)
    let result1 = materialize_subject_from_batch(&subject_map, &batch, 1).unwrap();
    assert!(
        result1.is_none(),
        "Null template column should produce None"
    );

    // Row 2: non-null id
    let result2 = materialize_subject_from_batch(&subject_map, &batch, 2).unwrap();
    assert!(result2.is_some());
}

// =============================================================================
// End-to-End Tests (Requires Polaris/MinIO)
// =============================================================================

/// End-to-end test with real Polaris REST catalog and S3/MinIO.
///
/// This test requires:
/// - Polaris REST catalog (default: localhost:8182)
/// - S3/MinIO with OpenFlights Parquet data
/// - Tables: openflights/airlines, openflights/airports, openflights/routes
///
/// Environment variables:
/// - ICEBERG_E2E=1 to enable the test
/// - ICEBERG_CATALOG_URI (default: http://localhost:8182)
/// - ICEBERG_WAREHOUSE (default: openflights)
/// - S3_ENDPOINT (optional, for MinIO)
/// - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (for S3 auth)
///
/// Run with: ICEBERG_E2E=1 cargo test e2e_r2rml_query_iceberg_table -- --nocapture
#[tokio::test]
#[ignore = "Requires external Polaris/MinIO infrastructure. Set ICEBERG_E2E=1 to run."]
async fn e2e_r2rml_query_iceberg_table() {
    if std::env::var("ICEBERG_E2E").is_err() {
        eprintln!("Skipping E2E test (set ICEBERG_E2E=1 to run)");
        return;
    }

    // R2RML mapping for airlines table
    const AIRLINES_R2RML: &str = r#"
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix ex: <http://example.org/> .

        <http://example.org/mapping#AirlineMapping>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "openflights/airlines" ] ;
            rr:subjectMap [
                rr:template "http://example.org/airline/{id}" ;
                rr:class ex:Airline
            ] ;
            rr:predicateObjectMap [
                rr:predicate ex:name ;
                rr:objectMap [ rr:column "name" ]
            ] ;
            rr:predicateObjectMap [
                rr:predicate ex:country ;
                rr:objectMap [ rr:column "country" ]
            ] .
    "#;

    // Configuration from environment
    // Default to Polaris (8182) with OAuth2 auth
    let catalog_uri = std::env::var("ICEBERG_CATALOG_URI")
        .unwrap_or_else(|_| "http://localhost:8182/api/catalog".to_string());
    let warehouse =
        std::env::var("ICEBERG_WAREHOUSE").unwrap_or_else(|_| "openflights".to_string());
    // OAuth2 credential for Polaris: client_id:client_secret
    let oauth2_credential = std::env::var("ICEBERG_OAUTH2_CREDENTIAL")
        .ok()
        .or_else(|| Some("root:s3cr3t".to_string())); // Default Polaris credentials

    eprintln!("E2E Test Configuration:");
    eprintln!("  Catalog URI: {catalog_uri}");
    eprintln!("  Warehouse: {warehouse}");
    eprintln!(
        "  OAuth2: {}",
        oauth2_credential
            .as_ref()
            .map(|c| c.split(':').next().unwrap_or("?"))
            .unwrap_or("none")
    );

    // Compile R2RML mapping
    let mapping = R2rmlLoader::from_turtle(AIRLINES_R2RML)
        .expect("Failed to parse R2RML")
        .compile()
        .expect("Failed to compile R2RML");

    eprintln!(
        "Compiled R2RML mapping with {} TriplesMap(s)",
        mapping.triples_maps.len()
    );

    // Create a custom provider that uses Iceberg directly
    let provider = IcebergDirectProvider {
        mapping: Arc::new(mapping),
        catalog_uri,
        warehouse,
        s3_endpoint: std::env::var("S3_ENDPOINT").ok(),
        s3_region: Some("us-east-1".to_string()),
        s3_path_style: true,
        oauth2_credential,
    };

    // Create Fluree instance and ledger
    let fluree = FlureeBuilder::memory().build_memory();
    let mut ledger = genesis_ledger(&fluree, "e2e-iceberg:main");

    // Register example.org namespace
    std::sync::Arc::make_mut(&mut ledger.snapshot)
        .insert_namespace_code(9_999, "http://example.org/".to_string())
        .unwrap();

    // Build query: SELECT ?airline ?name ?country WHERE { GRAPH <gs:main> { ?airline ex:name ?name ; ex:country ?country } }
    let mut vars = VarRegistry::new();
    let airline_var = vars.get_or_insert("?airline");
    let name_var = vars.get_or_insert("?name");
    let country_var = vars.get_or_insert("?country");

    // Register predicate IRIs
    let ex_name_sid = ledger
        .snapshot
        .encode_iri("http://example.org/name")
        .expect("namespace should be registered");
    let ex_country_sid = ledger
        .snapshot
        .encode_iri("http://example.org/country")
        .expect("namespace should be registered");

    let inner_patterns = vec![
        Pattern::Triple(TriplePattern::new(
            Ref::Var(airline_var),
            Ref::Sid(ex_name_sid),
            Term::Var(name_var),
        )),
        Pattern::Triple(TriplePattern::new(
            Ref::Var(airline_var),
            Ref::Sid(ex_country_sid),
            Term::Var(country_var),
        )),
    ];

    let graph_pattern = Pattern::Graph {
        name: GraphName::Iri("airlines-gs:main".into()),
        patterns: inner_patterns,
    };

    let mut parsed = Query::new(ParsedContext::default());
    parsed.patterns = vec![graph_pattern];
    parsed.output = QueryOutput::select_all(vec![airline_var, name_var, country_var]);

    let executable = ExecutableQuery::simple(parsed);
    let tracker = Tracker::disabled();

    eprintln!("Executing query against Iceberg...");

    // Execute query
    let result = execute(
        GraphDbRef::new(&ledger.snapshot, 0, &NoOverlay, ledger.t()),
        &vars,
        &executable,
        r2rml_test_config(&tracker, &provider),
    )
    .await;

    match result {
        Ok(batches) => {
            let total_rows: usize = batches.iter().map(fluree_db_api::Batch::len).sum();
            eprintln!("Query returned {total_rows} rows");

            // Print first few results
            for (batch_idx, batch) in batches.iter().enumerate() {
                for row_idx in 0..batch.len().min(5) {
                    if let (Some(name_col), Some(country_col)) =
                        (batch.column_by_idx(1), batch.column_by_idx(2))
                    {
                        eprintln!(
                            "  Row {}: name={:?}, country={:?}",
                            batch_idx * 1000 + row_idx,
                            &name_col[row_idx],
                            &country_col[row_idx]
                        );
                    }
                }
                if batch.len() > 5 {
                    eprintln!("  ... and {} more rows", batch.len() - 5);
                }
            }

            // OpenFlights has ~6000 airlines
            assert!(
                total_rows > 100,
                "Expected many airline rows, got {total_rows}"
            );
        }
        Err(e) => {
            eprintln!("Query failed: {e}");
            // Don't panic if it's a connection error - the infrastructure might not be running
            if e.to_string().contains("connection") || e.to_string().contains("Connection") {
                eprintln!("WARNING: Could not connect to Iceberg catalog - is it running?");
            } else {
                panic!("Query failed with unexpected error: {e}");
            }
        }
    }
}

/// End-to-end test using FlureeR2rmlProvider through the full Fluree API.
///
/// This test exercises the complete production flow:
/// 1. Store R2RML mapping in Fluree storage
/// 2. Create graph source using `create_r2rml_graph_source()` which registers in nameservice
/// 3. Query using `query_graph_source()` which uses `FlureeR2rmlProvider`
///
/// This test requires:
/// - Polaris REST catalog (default: localhost:8182)
/// - S3/MinIO with OpenFlights Parquet data
/// - Tables: openflights/airlines
///
/// Environment variables:
/// - ICEBERG_E2E=1 to enable the test
/// - ICEBERG_E2E_STRICT=1 to fail hard on connection errors (for CI)
/// - ICEBERG_CATALOG_URI (default: http://localhost:8182/api/catalog)
/// - ICEBERG_WAREHOUSE (default: openflights)
/// - ICEBERG_OAUTH2_CREDENTIAL (default: root:s3cr3t)
///
/// Run with: ICEBERG_E2E=1 cargo test e2e_fluree_r2rml_provider_full_flow -- --nocapture
/// Run strict: ICEBERG_E2E=1 ICEBERG_E2E_STRICT=1 cargo test e2e_fluree_r2rml_provider_full_flow -- --nocapture
#[tokio::test]
#[ignore = "Requires external Polaris/MinIO infrastructure. Set ICEBERG_E2E=1 to run."]
async fn e2e_fluree_r2rml_provider_full_flow() {
    use fluree_db_api::R2rmlCreateConfig;

    if std::env::var("ICEBERG_E2E").is_err() {
        eprintln!("Skipping E2E test (set ICEBERG_E2E=1 to run)");
        return;
    }

    // Strict mode: fail hard on connection errors (useful for CI)
    let strict_mode = std::env::var("ICEBERG_E2E_STRICT").is_ok();

    // Configuration from environment
    let catalog_uri = std::env::var("ICEBERG_CATALOG_URI")
        .unwrap_or_else(|_| "http://localhost:8182/api/catalog".to_string());
    let warehouse =
        std::env::var("ICEBERG_WAREHOUSE").unwrap_or_else(|_| "openflights".to_string());
    let oauth2_credential =
        std::env::var("ICEBERG_OAUTH2_CREDENTIAL").unwrap_or_else(|_| "root:s3cr3t".to_string());

    eprintln!("E2E FlureeR2rmlProvider Full Flow Test:");
    eprintln!("  Catalog URI: {catalog_uri}");
    eprintln!("  Warehouse: {warehouse}");

    // R2RML mapping for airlines table
    const AIRLINES_R2RML: &str = r#"
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix ex: <http://example.org/> .

        <http://example.org/mapping#AirlineMapping>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "openflights.airlines" ] ;
            rr:subjectMap [
                rr:template "http://example.org/airline/{id}" ;
                rr:class ex:Airline
            ] ;
            rr:predicateObjectMap [
                rr:predicate ex:name ;
                rr:objectMap [ rr:column "name" ]
            ] ;
            rr:predicateObjectMap [
                rr:predicate ex:country ;
                rr:objectMap [ rr:column "country" ]
            ] .
    "#;

    // Create Fluree instance
    let fluree = FlureeBuilder::memory().build_memory();

    // Create a ledger for query execution
    let mut ledger = genesis_ledger(&fluree, "e2e-provider:main");
    std::sync::Arc::make_mut(&mut ledger.snapshot)
        .insert_namespace_code(9_999, "http://example.org/".to_string())
        .unwrap();

    // Step 1: Store the R2RML mapping in Fluree storage
    eprintln!("Step 1: Storing R2RML mapping...");
    let mapping_address = "r2rml/airlines-e2e.ttl";
    fluree
        .admin_storage()
        .expect("managed backend")
        .write_bytes(mapping_address, AIRLINES_R2RML.as_bytes())
        .await
        .expect("Failed to store mapping");
    eprintln!("  Mapping stored at: {mapping_address}");

    // Step 2: Create graph source using create_r2rml_graph_source()
    eprintln!("Step 2: Creating R2RML graph source...");

    // Parse OAuth2 credentials
    let parts: Vec<&str> = oauth2_credential.split(':').collect();
    let token_url = format!("{catalog_uri}/v1/oauth/tokens");

    let mut config = R2rmlCreateConfig::new(
        "airlines-e2e",
        &catalog_uri,
        "openflights.airlines",
        AIRLINES_R2RML,
    )
    .with_warehouse(&warehouse)
    .with_mapping_media_type("text/turtle")
    .with_vended_credentials(true)
    .with_s3_path_style(true);

    // Set OAuth2 auth
    if parts.len() == 2 {
        config = config.with_auth_oauth2(&token_url, parts[0], parts[1]);
    }

    let gs_result = fluree.create_r2rml_graph_source(config).await;
    match &gs_result {
        Ok(result) => {
            eprintln!("  Graph source created: {}", result.graph_source_id);
            eprintln!("  Connection tested: {}", result.connection_tested);
            eprintln!("  Mapping validated: {}", result.mapping_validated);
            eprintln!("  TriplesMap count: {}", result.triples_map_count);
        }
        Err(e) => {
            eprintln!("  Graph source creation failed: {e}");
            let is_connection_error =
                e.to_string().contains("connection") || e.to_string().contains("Connection");

            if strict_mode {
                // In strict mode, fail hard on any error
                panic!("Graph source creation failed (strict mode): {e}");
            } else if is_connection_error {
                // In lenient mode, skip on connection errors
                eprintln!("WARNING: Could not connect to Iceberg catalog - skipping test");
                return;
            }
        }
    }

    // Graph source should be created even if connection test fails
    let gs_result = gs_result.expect("Graph source creation should succeed");
    assert!(gs_result.mapping_validated, "Mapping should be validated");
    assert_eq!(gs_result.triples_map_count, 1, "Should have 1 TriplesMap");

    // Step 3: Query using query_graph_source() which uses FlureeR2rmlProvider
    eprintln!("Step 3: Querying graph source...");

    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?airline", "?name", "?country"],
        "where": {
            "@graph": "airlines-e2e:main",
            "patterns": [
                {"@id": "?airline", "ex:name": "?name", "ex:country": "?country"}
            ]
        }
    });

    // Execute query via query_graph_source - this exercises the full FlureeR2rmlProvider path
    let result = fluree.query_graph_source(&ledger, &query).await;

    match result {
        Ok(query_result) => {
            let total_rows: usize = query_result
                .batches
                .iter()
                .map(fluree_db_api::Batch::len)
                .sum();
            eprintln!("  Query returned {total_rows} rows");

            // Print first few results
            for (i, batch) in query_result.batches.iter().enumerate() {
                for row_idx in 0..batch.len().min(3) {
                    if let (Some(name_col), Some(country_col)) =
                        (batch.column_by_idx(1), batch.column_by_idx(2))
                    {
                        eprintln!(
                            "  Row {}: name={:?}, country={:?}",
                            i * 1000 + row_idx,
                            &name_col[row_idx],
                            &country_col[row_idx]
                        );
                    }
                }
            }

            // OpenFlights has ~6000 airlines
            assert!(
                total_rows > 100,
                "Expected many airline rows, got {total_rows}"
            );
            eprintln!("SUCCESS: Full FlureeR2rmlProvider flow works!");
        }
        Err(e) => {
            let error_msg = e.to_string();
            eprintln!("  Query failed: {error_msg}");

            let is_connection_error = error_msg.contains("connection")
                || error_msg.contains("Connection")
                || error_msg.contains("catalog");

            if strict_mode {
                // In strict mode, fail hard on any error
                panic!("Query failed (strict mode): {e}");
            } else if is_connection_error {
                // In lenient mode, warn on connection errors
                eprintln!("WARNING: Could not connect to Iceberg - is infrastructure running?");
            } else {
                panic!("Query failed with unexpected error: {e}");
            }
        }
    }
}

/// Provider that queries Iceberg directly (without nameservice graph source registration).
#[derive(Debug)]
struct IcebergDirectProvider {
    mapping: Arc<CompiledR2rmlMapping>,
    catalog_uri: String,
    warehouse: String,
    s3_endpoint: Option<String>,
    s3_region: Option<String>,
    s3_path_style: bool,
    /// OAuth2 credentials for Polaris (client_id:client_secret)
    oauth2_credential: Option<String>,
}

#[async_trait]
impl R2rmlProvider for IcebergDirectProvider {
    async fn has_r2rml_mapping(&self, _graph_source_id: &str) -> bool {
        true
    }

    async fn compiled_mapping(
        &self,
        _graph_source_id: &str,
        _as_of_t: Option<i64>,
    ) -> QueryResult<Arc<CompiledR2rmlMapping>> {
        Ok(Arc::clone(&self.mapping))
    }
}

#[async_trait]
impl R2rmlTableProvider for IcebergDirectProvider {
    async fn scan_table(
        &self,
        _graph_source_id: &str,
        table_name: &str,
        projection: &[String],
        _filters: &[ScanFilter],
        _as_of_t: Option<i64>,
    ) -> QueryResult<ColumnBatchStream> {
        use fluree_db_iceberg::{
            auth::AuthConfig,
            catalog::{
                parse_table_identifier, RestCatalogClient, RestCatalogConfig, SendCatalogClient,
            },
            config_value::ConfigValue,
            io::{S3IcebergStorage, SendIcebergStorage, SendParquetReader},
            metadata::TableMetadata,
            scan::{ScanConfig, SendScanPlanner},
        };

        eprintln!("IcebergDirectProvider.scan_table: {table_name}");
        eprintln!("  projection: {projection:?}");

        // Create catalog client with OAuth2 auth if credentials provided
        let catalog_config = RestCatalogConfig {
            uri: self.catalog_uri.clone(),
            warehouse: Some(self.warehouse.clone()),
            ..Default::default()
        };

        // Use OAuth2 auth for Polaris, or no auth for Tabular
        let auth = if let Some(ref cred) = self.oauth2_credential {
            let parts: Vec<&str> = cred.split(':').collect();
            if parts.len() == 2 {
                let token_url = format!("{}/v1/oauth/tokens", self.catalog_uri);
                eprintln!("  Using OAuth2 auth with token_url: {token_url}");
                AuthConfig::OAuth2ClientCredentials {
                    token_url,
                    client_id: ConfigValue::literal(parts[0]),
                    client_secret: ConfigValue::literal(parts[1]),
                    scope: Some("PRINCIPAL_ROLE:ALL".to_string()),
                    audience: None,
                }
                .create_provider_arc()
                .map_err(|e| QueryError::Internal(format!("OAuth2 auth error: {e}")))?
            } else {
                AuthConfig::None
                    .create_provider_arc()
                    .map_err(|e| QueryError::Internal(format!("Auth error: {e}")))?
            }
        } else {
            AuthConfig::None
                .create_provider_arc()
                .map_err(|e| QueryError::Internal(format!("Auth error: {e}")))?
        };

        let catalog = RestCatalogClient::new(catalog_config, auth)
            .map_err(|e| QueryError::Internal(format!("Catalog error: {e}")))?;

        // Parse table identifier
        let table_id = parse_table_identifier(table_name)
            .map_err(|e| QueryError::Internal(format!("Table ID error: {e}")))?;

        eprintln!("  Loading table: {}.{}", table_id.namespace, table_id.table);

        // Load table metadata with vended credentials
        let load_response = catalog
            .load_table(&table_id, true)
            .await
            .map_err(|e| QueryError::Internal(format!("Load table error: {e}")))?;

        eprintln!("  Metadata location: {}", load_response.metadata_location);
        eprintln!(
            "  Has vended creds: {}",
            load_response.credentials.is_some()
        );

        // Create S3 storage
        let storage = if let Some(creds) = load_response.credentials {
            eprintln!("  Using vended credentials");
            eprintln!("    Endpoint from creds: {:?}", creds.endpoint);
            eprintln!("    Region from creds: {:?}", creds.region);

            // Override Docker internal hostname with localhost for test access
            // Polaris returns iceberg-minio:9000 (Docker network) but we need localhost:9000
            let endpoint = creds.endpoint.as_ref().map(|ep| {
                if ep.contains("iceberg-minio") {
                    ep.replace("iceberg-minio", "localhost")
                } else {
                    ep.clone()
                }
            });
            eprintln!("    Using endpoint: {endpoint:?}");

            // Workaround for AWS SDK TLS initialization issues:
            // Set environment variables and use the default chain instead of explicit credentials.
            // This avoids potential TLS cert parsing issues on some systems.
            std::env::set_var("AWS_ACCESS_KEY_ID", &creds.access_key_id);
            std::env::set_var("AWS_SECRET_ACCESS_KEY", &creds.secret_access_key);
            if let Some(ref token) = creds.session_token {
                std::env::set_var("AWS_SESSION_TOKEN", token);
            }

            S3IcebergStorage::from_default_chain(
                creds.region.as_deref(),
                endpoint.as_deref(),
                creds.path_style,
            )
            .await
            .map_err(|e| QueryError::Internal(format!("S3 storage error: {e}")))?
        } else {
            eprintln!("  Using default AWS credentials");
            S3IcebergStorage::from_default_chain(
                self.s3_region.as_deref(),
                self.s3_endpoint.as_deref(),
                self.s3_path_style,
            )
            .await
            .map_err(|e| QueryError::Internal(format!("S3 storage error: {e}")))?
        };

        // Read and parse metadata
        let metadata_bytes = storage
            .read(&load_response.metadata_location)
            .await
            .map_err(|e| QueryError::Internal(format!("Metadata read error: {e}")))?;

        let metadata = TableMetadata::from_json(&metadata_bytes)
            .map_err(|e| QueryError::Internal(format!("Metadata parse error: {e}")))?;

        let schema = metadata
            .current_schema()
            .ok_or_else(|| QueryError::Internal("No current schema".to_string()))?;

        eprintln!("  Schema fields: {:?}", schema.field_names());

        // Resolve projection to field IDs
        let projected_field_ids: Vec<i32> = if projection.is_empty() {
            schema
                .fields
                .iter()
                .filter(|f| !f.is_nested())
                .map(|f| f.id)
                .collect()
        } else {
            projection
                .iter()
                .filter_map(|name| schema.field_by_name(name).map(|f| f.id))
                .collect()
        };

        eprintln!("  Projected field IDs: {projected_field_ids:?}");

        // Create scan plan
        let scan_config = ScanConfig::new().with_projection(projected_field_ids);
        let planner = SendScanPlanner::new(&storage, &metadata, scan_config);
        let plan = planner
            .plan_scan()
            .await
            .map_err(|e| QueryError::Internal(format!("Scan plan error: {e}")))?;

        eprintln!(
            "  Scan plan: {} files, ~{} rows",
            plan.files_selected, plan.estimated_row_count
        );

        if plan.is_empty() {
            return Ok(vec_batch_stream(Vec::new()));
        }

        // Read Parquet files
        let reader = SendParquetReader::new(&storage);
        let mut all_batches = Vec::new();

        for task in &plan.tasks {
            eprintln!("  Reading: {}", task.data_file.file_path);
            let batches = reader
                .read_task(task)
                .await
                .map_err(|e| QueryError::Internal(format!("Parquet read error: {e}")))?;
            all_batches.extend(batches);
        }

        let total_rows: usize = all_batches.iter().map(|b| b.num_rows).sum();
        eprintln!(
            "  Loaded {} batches, {} total rows",
            all_batches.len(),
            total_rows
        );

        Ok(vec_batch_stream(all_batches))
    }
}

// =============================================================================
// Multi-Table / RefObjectMap Tests
// =============================================================================

/// R2RML mapping with RefObjectMap join between routes and airlines.
const ROUTES_MAPPING_TTL: &str = r#"
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .

<http://example.org/mapping#AirlineMapping> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "openflights.airlines" ] ;
    rr:subjectMap [
        rr:template "http://example.org/airline/{id}" ;
        rr:class ex:Airline
    ] ;
    rr:predicateObjectMap [
        rr:predicate ex:name ;
        rr:objectMap [ rr:column "name" ]
    ] .

<http://example.org/mapping#RouteMapping> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "openflights.routes" ] ;
    rr:subjectMap [
        rr:template "http://example.org/route/{airline_id}_{src_id}_{dst_id}" ;
        rr:class ex:Route
    ] ;
    rr:predicateObjectMap [
        rr:predicate ex:operatedBy ;
        rr:objectMap [
            rr:parentTriplesMap <http://example.org/mapping#AirlineMapping> ;
            rr:joinCondition [
                rr:child "airline_id" ;
                rr:parent "id"
            ]
        ]
    ] .
"#;

/// Test RefObjectMap parsing and compiled mapping indexes.
#[test]
fn test_ref_object_map_compilation() {
    let mapping = R2rmlLoader::from_turtle(ROUTES_MAPPING_TTL)
        .expect("Failed to parse R2RML")
        .compile()
        .expect("Failed to compile");

    // Debug: print what TriplesMap keys were found
    let keys: Vec<_> = mapping.triples_maps.keys().collect();
    eprintln!("Found TriplesMap keys: {keys:?}");

    // Should have two TriplesMap
    assert_eq!(
        mapping.triples_maps.len(),
        2,
        "Expected 2 TriplesMap, found: {keys:?}"
    );

    // RouteMapping should reference AirlineMapping
    // Full IRIs as written in Turtle: http://example.org/mapping#AirlineMapping
    let route_map = mapping
        .get("http://example.org/mapping#RouteMapping")
        .expect("RouteMapping not found");

    // Find the operatedBy predicate-object map
    let operated_by_pom = route_map
        .predicate_object_maps
        .iter()
        .find(|pom| pom.predicate_map.as_constant() == Some("http://example.org/operatedBy"))
        .expect("operatedBy POM not found");

    // It should have a RefObjectMap
    match &operated_by_pom.object_map {
        fluree_db_r2rml::mapping::ObjectMap::RefObjectMap(rom) => {
            assert_eq!(
                rom.parent_triples_map,
                "http://example.org/mapping#AirlineMapping"
            );
            assert_eq!(rom.join_conditions.len(), 1);
            assert_eq!(rom.join_conditions[0].child_column, "airline_id");
            assert_eq!(rom.join_conditions[0].parent_column, "id");
        }
        _ => panic!("Expected RefObjectMap for operatedBy"),
    }

    // Test find_maps_referencing index
    let referencing = mapping.find_maps_referencing("http://example.org/mapping#AirlineMapping");
    assert_eq!(referencing.len(), 1);
    assert_eq!(
        referencing[0].iri,
        "http://example.org/mapping#RouteMapping"
    );
}

// =============================================================================
// Engine-Level E2E Tests (Mock Providers, No External Infrastructure)
// =============================================================================
//
// These tests exercise the full query execution pipeline with GRAPH patterns:
// 1. GraphOperator detects R2RML graph source and rewrites triple patterns
// 2. R2rmlScanOperator loads mapping, scans tables, materializes terms
// 3. Variable binding/unification works end-to-end
//
// Uses MockR2rmlProvider to return test data without external infrastructure.

/// Engine-level E2E test: Execute GRAPH pattern query against R2RML graph source.
///
/// This test verifies:
/// - Pattern::Graph with concrete IRI triggers R2RML rewriting
/// - R2rmlScanOperator materializes subject IRIs from templates
/// - Object values are correctly extracted from columns
/// - Variable bindings match expected results
#[tokio::test]
async fn engine_e2e_graph_pattern_r2rml_scan() {
    // Create a minimal Fluree instance (we only need a Db for IRI encoding)
    let fluree = FlureeBuilder::memory().build_memory();
    let mut ledger = genesis_ledger(&fluree, "r2rml-e2e:main");

    // Register the example.org namespace prefix in this Db so the R2RML operator
    // can encode subject IRIs produced by rr:template. Without this, encode_iri()
    // returns None and the operator will skip all rows as "IRI not encodable".
    std::sync::Arc::make_mut(&mut ledger.snapshot)
        .insert_namespace_code(9_999, "http://example.org/".to_string())
        .unwrap();

    // Compile the airline mapping
    let mapping = compile_airline_mapping();
    let batch = sample_airline_batch();
    let provider = MockR2rmlProvider::new(mapping, vec![batch]);

    // Build a query with Pattern::Graph targeting our graph source
    // Equivalent SPARQL: SELECT ?s ?name WHERE { GRAPH <airlines-gs:main> { ?s ex:name ?name } }
    let mut vars = VarRegistry::new();
    let subject_var = vars.get_or_insert("?s");
    let name_var = vars.get_or_insert("?name");

    // Create triple pattern: ?s ex:name ?name
    // Use a real Sid for the predicate so the R2RML rewrite can apply predicate_filter
    // and we only materialize the ex:name predicate-object map (instead of all POMs).
    let ex_name_sid = ledger
        .snapshot
        .encode_iri("http://example.org/name")
        .expect("example.org namespace should be registered for Sid encoding");
    let inner_patterns = vec![Pattern::Triple(TriplePattern::new(
        Ref::Var(subject_var),
        Ref::Sid(ex_name_sid),
        Term::Var(name_var),
    ))];

    let graph_pattern = Pattern::Graph {
        name: GraphName::Iri("airlines-gs:main".into()),
        patterns: inner_patterns,
    };

    // Build Query with this pattern
    let mut parsed = Query::new(ParsedContext::default());
    parsed.patterns = vec![graph_pattern];
    parsed.output = QueryOutput::select_all(vec![subject_var, name_var]);

    let executable = ExecutableQuery::simple(parsed);
    let tracker = Tracker::disabled();

    // Execute with our mock R2RML provider
    let batches = execute(
        GraphDbRef::new(&ledger.snapshot, 0, &NoOverlay, ledger.t()),
        &vars,
        &executable,
        r2rml_test_config(&tracker, &provider),
    )
    .await
    .expect("Query execution should succeed");

    // Verify we got results - the R2RML pipeline should produce bindings
    eprintln!("Query returned {} batches", batches.len());
    for (i, batch) in batches.iter().enumerate() {
        eprintln!("  Batch {}: {} rows", i, batch.len());
    }

    // Assert: Query should produce results (not just execute without error)
    // With a concrete predicate (ex:name), we should get one row per input table row.
    let total_rows: usize = batches.iter().map(fluree_db_api::Batch::len).sum();
    assert!(
        total_rows > 0,
        "R2RML query should produce results; got {} batches with {} total rows",
        batches.len(),
        total_rows
    );
}

/// Engine-level E2E test: Verify R2RML provider is consulted for GRAPH patterns.
///
/// This test uses a custom provider that tracks method calls to verify
/// the execution pipeline correctly consults the R2RML provider.
#[tokio::test]
async fn engine_e2e_provider_method_calls() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    // Provider that tracks calls
    #[derive(Debug)]
    struct TrackingProvider {
        mapping: Arc<CompiledR2rmlMapping>,
        batches: Vec<ColumnBatch>,
        has_mapping_called: AtomicBool,
        compiled_mapping_called: AtomicUsize,
        scan_table_called: AtomicUsize,
    }

    #[async_trait]
    impl R2rmlProvider for TrackingProvider {
        async fn has_r2rml_mapping(&self, graph_source_id: &str) -> bool {
            eprintln!("has_r2rml_mapping called for: {graph_source_id}");
            self.has_mapping_called.store(true, Ordering::SeqCst);
            graph_source_id == "airlines-gs:main"
        }

        async fn compiled_mapping(
            &self,
            graph_source_id: &str,
            _as_of_t: Option<i64>,
        ) -> QueryResult<Arc<CompiledR2rmlMapping>> {
            eprintln!("compiled_mapping called for: {graph_source_id}");
            self.compiled_mapping_called.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::clone(&self.mapping))
        }
    }

    #[async_trait]
    impl R2rmlTableProvider for TrackingProvider {
        async fn scan_table(
            &self,
            graph_source_id: &str,
            table_name: &str,
            projection: &[String],
            _filters: &[ScanFilter],
            _as_of_t: Option<i64>,
        ) -> QueryResult<ColumnBatchStream> {
            eprintln!(
                "scan_table called: gs={graph_source_id}, table={table_name}, projection={projection:?}"
            );
            self.scan_table_called.fetch_add(1, Ordering::SeqCst);
            Ok(vec_batch_stream(self.batches.clone()))
        }
    }

    // Create tracking provider
    let mapping = compile_airline_mapping();
    let batch = sample_airline_batch();
    let provider = TrackingProvider {
        mapping: Arc::new(mapping),
        batches: vec![batch],
        has_mapping_called: AtomicBool::new(false),
        compiled_mapping_called: AtomicUsize::new(0),
        scan_table_called: AtomicUsize::new(0),
    };

    // Create minimal Fluree instance
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "r2rml-tracking:main");

    // Build query with GRAPH pattern
    let mut vars = VarRegistry::new();
    let subject_var = vars.get_or_insert("?s");

    let graph_pattern = Pattern::Graph {
        name: GraphName::Iri("airlines-gs:main".into()),
        patterns: vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(subject_var),
            Ref::Var(vars.get_or_insert("?p")),
            Term::Var(vars.get_or_insert("?o")),
        ))],
    };

    let mut parsed = Query::new(ParsedContext::default());
    parsed.patterns = vec![graph_pattern];
    parsed.output = QueryOutput::select_all(vec![subject_var]);

    let executable = ExecutableQuery::simple(parsed);
    let tracker = Tracker::disabled();

    // Execute query - should succeed
    let result = execute(
        GraphDbRef::new(&ledger.snapshot, 0, &NoOverlay, ledger.t()),
        &vars,
        &executable,
        r2rml_test_config(&tracker, &provider),
    )
    .await;

    // Assert execution succeeded
    assert!(
        result.is_ok(),
        "Query execution should succeed: {:?}",
        result.err()
    );

    // Verify provider methods were called in expected order
    assert!(
        provider.has_mapping_called.load(Ordering::SeqCst),
        "has_r2rml_mapping should have been called for the GRAPH pattern"
    );

    // Once R2RML graph source is detected, compiled_mapping and scan_table should be called
    assert!(
        provider.compiled_mapping_called.load(Ordering::SeqCst) > 0,
        "compiled_mapping should have been called to load the R2RML mapping"
    );

    assert!(
        provider.scan_table_called.load(Ordering::SeqCst) > 0,
        "scan_table should have been called to read Iceberg data"
    );

    eprintln!(
        "Provider calls: has_mapping={}, compiled_mapping={}, scan_table={}",
        provider.has_mapping_called.load(Ordering::SeqCst),
        provider.compiled_mapping_called.load(Ordering::SeqCst),
        provider.scan_table_called.load(Ordering::SeqCst)
    );
}

// =============================================================================
// Graph Source Creation API Tests
// =============================================================================

use fluree_db_api::{IcebergCreateConfig, R2rmlCreateConfig};

/// Test IcebergCreateConfig validation - valid configuration.
#[test]
fn test_iceberg_create_config_validation_valid() {
    let config = IcebergCreateConfig::new(
        "my-iceberg-gs",
        "https://polaris.example.com",
        "openflights.airlines",
    );

    // Validation should pass
    let result = config.validate();
    assert!(
        result.is_ok(),
        "Valid config should pass validation: {:?}",
        result.err()
    );

    // Check alias
    assert_eq!(config.graph_source_id(), "my-iceberg-gs:main");
}

/// Test IcebergCreateConfig validation - empty name.
#[test]
fn test_iceberg_create_config_validation_empty_name() {
    let config =
        IcebergCreateConfig::new("", "https://polaris.example.com", "openflights.airlines");

    let result = config.validate();
    assert!(result.is_err(), "Empty name should fail validation");
    assert!(
        result.unwrap_err().to_string().contains("name"),
        "Error should mention 'name'"
    );
}

/// Test IcebergCreateConfig validation - name with colon.
#[test]
fn test_iceberg_create_config_validation_name_with_colon() {
    let config = IcebergCreateConfig::new(
        "my:gs",
        "https://polaris.example.com",
        "openflights.airlines",
    );

    let result = config.validate();
    assert!(result.is_err(), "Name with colon should fail validation");
}

/// Test IcebergCreateConfig validation - empty catalog URI.
#[test]
fn test_iceberg_create_config_validation_empty_uri() {
    let config = IcebergCreateConfig::new("my-gs", "", "openflights.airlines");

    let result = config.validate();
    assert!(result.is_err(), "Empty catalog URI should fail validation");
}

/// Test IcebergCreateConfig validation - invalid table identifier.
#[test]
fn test_iceberg_create_config_validation_invalid_table() {
    let config = IcebergCreateConfig::new(
        "my-gs",
        "https://polaris.example.com",
        "invalid_table", // Missing namespace
    );

    let result = config.validate();
    assert!(
        result.is_err(),
        "Invalid table identifier should fail validation"
    );
}

/// Test IcebergCreateConfig builder methods.
#[test]
fn test_iceberg_create_config_builder() {
    let config = IcebergCreateConfig::new("my-gs", "https://polaris.example.com", "ns.table")
        .with_branch("dev")
        .with_warehouse("my-warehouse")
        .with_auth_bearer("my-token")
        .with_vended_credentials(false)
        .with_s3_region("us-west-2")
        .with_s3_endpoint("http://localhost:9000")
        .with_s3_path_style(true);

    assert_eq!(config.graph_source_id(), "my-gs:dev");
    match &config.catalog_mode {
        fluree_db_api::CatalogMode::Rest(rest) => {
            assert_eq!(rest.warehouse, Some("my-warehouse".to_string()));
            assert!(!rest.vended_credentials);
        }
        _ => panic!("Expected REST catalog mode"),
    }
    assert_eq!(config.s3_region, Some("us-west-2".to_string()));
    assert_eq!(
        config.s3_endpoint,
        Some("http://localhost:9000".to_string())
    );
    assert!(config.s3_path_style);

    // Validation should still pass
    assert!(config.validate().is_ok());
}

/// Test R2rmlCreateConfig validation - valid configuration.
#[test]
fn test_r2rml_create_config_validation_valid() {
    let config = R2rmlCreateConfig::new(
        "my-r2rml-gs",
        "https://polaris.example.com",
        "openflights.airlines",
        "fluree:file://mappings/airlines.ttl",
    );

    let result = config.validate();
    assert!(
        result.is_ok(),
        "Valid config should pass validation: {:?}",
        result.err()
    );

    assert_eq!(config.graph_source_id(), "my-r2rml-gs:main");
}

/// Test R2rmlCreateConfig validation - empty mapping source.
#[test]
fn test_r2rml_create_config_validation_empty_mapping() {
    let config = R2rmlCreateConfig::new(
        "my-gs",
        "https://polaris.example.com",
        "openflights.airlines",
        "", // Empty mapping source
    );

    let result = config.validate();
    assert!(
        result.is_err(),
        "Empty mapping source should fail validation"
    );
    assert!(
        result.unwrap_err().to_string().contains("mapping"),
        "Error should mention 'mapping'"
    );
}

/// Test R2rmlCreateConfig builder methods.
#[test]
fn test_r2rml_create_config_builder() {
    let config = R2rmlCreateConfig::new(
        "airlines-rdf",
        "https://polaris.example.com",
        "openflights.airlines",
        "s3://bucket/mappings/airlines.ttl",
    )
    .with_branch("staging")
    .with_mapping_media_type("text/turtle")
    .with_auth_bearer("token123")
    .with_warehouse("analytics");

    assert_eq!(config.graph_source_id(), "airlines-rdf:staging");
    assert!(
        matches!(&config.mapping, fluree_db_api::R2rmlMappingInput::Content(c) if c == "s3://bucket/mappings/airlines.ttl")
    );
    assert_eq!(config.mapping_media_type, Some("text/turtle".to_string()));
    match &config.iceberg.catalog_mode {
        fluree_db_api::CatalogMode::Rest(rest) => {
            assert_eq!(rest.warehouse, Some("analytics".to_string()));
        }
        _ => panic!("Expected REST catalog mode"),
    }

    // Validation should pass
    assert!(config.validate().is_ok());
}

/// Test IcebergGsConfig serialization roundtrip.
#[test]
fn test_iceberg_graph_source_config_serialization() {
    let config = IcebergCreateConfig::new("test-gs", "https://polaris.example.com", "ns.table")
        .with_warehouse("my-warehouse")
        .with_vended_credentials(true);

    // Convert to IcebergGsConfig for storage
    let iceberg_config = config.to_iceberg_gs_config();

    // Serialize to JSON
    let json = iceberg_config
        .to_json()
        .expect("serialization should succeed");

    // Parse back
    use fluree_db_iceberg::IcebergGsConfig;
    let parsed = IcebergGsConfig::from_json(&json).expect("parsing should succeed");

    match &parsed.catalog {
        fluree_db_iceberg::CatalogConfig::Rest { uri, warehouse, .. } => {
            assert_eq!(uri, "https://polaris.example.com");
            assert_eq!(warehouse, &Some("my-warehouse".to_string()));
        }
        other => panic!("Expected Rest variant, got {other:?}"),
    }
    assert_eq!(parsed.table.identifier(), "ns.table");
    assert!(parsed.io.vended_credentials);
    assert!(parsed.mapping.is_none());
}

/// Test R2rmlGsConfig serialization with mapping.
#[test]
fn test_r2rml_graph_source_config_serialization() {
    let config = R2rmlCreateConfig::new(
        "test-gs",
        "https://polaris.example.com",
        "ns.table",
        "fluree:file://mapping.ttl",
    )
    .with_mapping_media_type("text/turtle");

    // Convert to IcebergGsConfig for storage
    let iceberg_config = config.to_iceberg_gs_config("test-mapping-address");

    // Serialize to JSON
    let json = iceberg_config
        .to_json()
        .expect("serialization should succeed");

    // Parse back
    use fluree_db_iceberg::IcebergGsConfig;
    let parsed = IcebergGsConfig::from_json(&json).expect("parsing should succeed");

    // Should have mapping
    assert!(parsed.mapping.is_some(), "Mapping should be present");
    let mapping = parsed.mapping.unwrap();
    assert_eq!(mapping.source, "test-mapping-address");
    assert_eq!(mapping.media_type, Some("text/turtle".to_string()));
}

/// Integration test: Create Iceberg graph source via Fluree API.
///
/// This tests the full graph source creation flow with an in-memory nameservice.
/// The catalog connection test will fail (no real Polaris) but the
/// graph source record should still be created.
#[tokio::test]
async fn integration_create_iceberg_graph_source() {
    let fluree = FlureeBuilder::memory().build_memory();

    let config =
        IcebergCreateConfig::new("test-iceberg-gs", "https://polaris.example.com", "ns.table");

    // Create the graph source - connection test will fail but it should be registered
    let result = fluree.create_iceberg_graph_source(config).await;

    // Should succeed (connection_tested will be false due to no real catalog)
    assert!(
        result.is_ok(),
        "Graph source creation should succeed: {:?}",
        result.err()
    );

    let create_result = result.unwrap();
    assert_eq!(create_result.graph_source_id, "test-iceberg-gs:main");
    assert!(
        !create_result.connection_tested,
        "Connection test should fail without real catalog"
    );

    // Verify graph source is registered in nameservice
    let gs_record = fluree
        .nameservice()
        .lookup_graph_source("test-iceberg-gs:main")
        .await;
    assert!(gs_record.is_ok(), "Nameservice lookup should succeed");
    let record = gs_record.unwrap();
    assert!(record.is_some(), "Graph source record should exist");
    let record = record.unwrap();
    assert_eq!(
        record.source_type,
        fluree_db_nameservice::GraphSourceType::Iceberg
    );
}

/// Integration test: Create R2RML graph source with mapping validation.
///
/// This test stores a real mapping file and validates the creation flow.
#[tokio::test]
async fn integration_create_r2rml_graph_source_with_mapping() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create R2RML graph source config with inline mapping content
    let config = R2rmlCreateConfig::new(
        "airlines-rdf",
        "https://polaris.example.com",
        "openflights.airlines",
        AIRLINE_MAPPING_TTL,
    )
    .with_mapping_media_type("text/turtle");

    // Create the graph source — mapping content is stored to CAS internally
    let result = fluree.create_r2rml_graph_source(config).await;

    assert!(
        result.is_ok(),
        "Graph source creation should succeed: {:?}",
        result.err()
    );

    let create_result = result.unwrap();
    assert_eq!(create_result.graph_source_id, "airlines-rdf:main");
    // mapping_source should be a CID (content-addressed)
    assert!(!create_result.mapping_source.is_empty());
    assert!(
        create_result.mapping_validated,
        "Mapping should be validated"
    );
    assert_eq!(
        create_result.triples_map_count, 1,
        "Should have 1 TriplesMap"
    );
    assert!(
        !create_result.connection_tested,
        "Connection test should fail without real catalog"
    );

    // Verify graph source is registered in nameservice
    let gs_record = fluree
        .nameservice()
        .lookup_graph_source("airlines-rdf:main")
        .await
        .expect("Lookup should succeed");
    assert!(gs_record.is_some(), "Graph source record should exist");

    // The config JSON should contain the mapping
    let record = gs_record.unwrap();
    let config_json: serde_json::Value = serde_json::from_str(&record.config).unwrap();
    assert!(
        config_json.get("mapping").is_some(),
        "Config should contain mapping"
    );
}

// =============================================================================
// query_graph_source API Tests (GraphSourcePublisher impl)
// =============================================================================

/// Test that query_graph_source method properly wires FlureeR2rmlProvider.
///
/// This test verifies the query_graph_source API path uses the real R2RML provider
/// when the nameservice implements GraphSourcePublisher.
#[tokio::test]
async fn integration_query_graph_source_provider_wiring() {
    use serde_json::json;

    let fluree = FlureeBuilder::memory().build_memory();

    // Store the mapping file
    // Create R2RML graph source with inline mapping content
    let config = R2rmlCreateConfig::new(
        "airlines-query-test",
        "https://polaris.example.com",
        "openflights.airlines",
        AIRLINE_MAPPING_TTL,
    )
    .with_mapping_media_type("text/turtle");

    let gs_result = fluree.create_r2rml_graph_source(config).await;
    assert!(
        gs_result.is_ok(),
        "Graph source creation should succeed: {:?}",
        gs_result.err()
    );

    let graph_source_id = gs_result.unwrap().graph_source_id;

    // Create a basic ledger to query against
    let ledger = genesis_ledger(&fluree, "query-gs-test:main");

    // Build a simple query with GRAPH pattern targeting our graph source
    // SELECT ?s WHERE { GRAPH <airlines-query-test:main> { ?s a ex:Airline } }
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s"],
        "where": [
            {
                "graph": graph_source_id,
                "where": [
                    ["?s", "a", "ex:Airline"]
                ]
            }
        ]
    });

    // Execute query via query_graph_source - this exercises the FlureeR2rmlProvider path
    // The query will fail at scan_table level (no real Iceberg catalog) but
    // should get far enough to exercise the provider lookup
    let result = fluree.query_graph_source(&ledger, &query).await;

    // We expect an error because there's no real Iceberg catalog,
    // but the error should come from the R2RML provider trying to connect,
    // NOT from "R2RML not supported" (which would indicate wrong provider)
    match result {
        Ok(_) => panic!("Query should fail without real catalog"),
        Err(e) => {
            let error_msg = e.to_string();
            eprintln!("Expected error from query_graph_source: {error_msg}");

            // Verify it's NOT a "not supported" error from NoOpR2rmlProvider
            assert!(
                !error_msg.contains("R2RML graph sources are not supported"),
                "Should use FlureeR2rmlProvider, not NoOpR2rmlProvider. Got: {error_msg}"
            );
        }
    }
}

/// Streaming parity for `integration_query_graph_source_provider_wiring`: a
/// streaming query over a mapped graph source must resolve through the real
/// `FlureeR2rmlProvider`, not the no-op stand-in.
///
/// The no-op provider reports no mapping, so the GRAPH would read as an empty
/// native graph and the stream would end cleanly with zero rows. The real
/// provider instead rewrites to an R2RML scan and fails (there is no live
/// Iceberg catalog) — so an `error` terminal proves the real provider was the
/// one consulted on the streaming path.
#[tokio::test]
async fn streaming_graph_source_uses_real_r2rml_provider() {
    use fluree_db_api::{OwnedStreamQuery, QueryExecutionOptions};
    use support::graphdb_from_ledger;
    use tokio::sync::mpsc;

    let fluree = FlureeBuilder::memory().build_memory();

    let config = R2rmlCreateConfig::new(
        "airlines-stream-test",
        "https://polaris.example.com",
        "openflights.airlines",
        AIRLINE_MAPPING_TTL,
    )
    .with_mapping_media_type("text/turtle");
    let graph_source_id = fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("graph source creation should succeed")
        .graph_source_id;

    let ledger = genesis_ledger(&fluree, "stream-gs-test:main");

    let sparql = format!(
        "SELECT ?s WHERE {{ GRAPH <{graph_source_id}> {{ ?s a <http://example.org/Airline> }} }}"
    );

    let graph = graphdb_from_ledger(&ledger);
    let plan = fluree
        .plan_stream_query(&graph, &OwnedStreamQuery::Sparql(sparql))
        .await
        .expect("plan should succeed");
    drop(graph);

    let (tx, mut rx) = mpsc::channel(1024);
    fluree
        .run_stream_query(
            ledger,
            plan,
            Tracker::disabled(),
            QueryExecutionOptions::default(),
            tx,
        )
        .await;

    let mut bytes = Vec::new();
    while let Some(chunk) = rx.recv().await {
        bytes.extend_from_slice(&chunk);
    }
    let records: Vec<serde_json::Value> = String::from_utf8(bytes)
        .expect("ndjson is utf-8")
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).expect("each line is valid JSON"))
        .collect();

    let terminal = records.last().expect("a terminal record");
    assert_eq!(
        terminal["type"], "error",
        "real provider attempts the R2RML scan and fails without a catalog; \
         a clean `end` would mean the no-op provider was wired. Terminal: {terminal}"
    );
    assert!(
        !terminal
            .to_string()
            .contains("does not support graph source operations"),
        "must use FlureeR2rmlProvider, not the no-op. Terminal: {terminal}"
    );
}

// =============================================================================
// RefObjectMap Join Execution Tests
// =============================================================================

/// Create sample route data batch.
fn sample_routes_batch() -> ColumnBatch {
    let schema = BatchSchema::new(vec![
        FieldInfo {
            name: "airline_id".to_string(),
            field_type: FieldType::Int64,
            nullable: false,
            field_id: 1,
        },
        FieldInfo {
            name: "src_id".to_string(),
            field_type: FieldType::Int64,
            nullable: false,
            field_id: 2,
        },
        FieldInfo {
            name: "dst_id".to_string(),
            field_type: FieldType::Int64,
            nullable: false,
            field_id: 3,
        },
    ]);

    // Routes with airline_id matching airlines.id
    let columns = vec![
        Column::Int64(vec![Some(1), Some(1), Some(2), Some(3), Some(999)]), // airline_id - 999 has no match
        Column::Int64(vec![Some(100), Some(101), Some(100), Some(102), Some(100)]), // src_id
        Column::Int64(vec![Some(200), Some(201), Some(202), Some(200), Some(201)]), // dst_id
    ];

    ColumnBatch::new(Arc::new(schema), columns).unwrap()
}

/// Create sample airlines batch for join (simpler than main test).
fn sample_airlines_for_join() -> ColumnBatch {
    let schema = BatchSchema::new(vec![
        FieldInfo {
            name: "id".to_string(),
            field_type: FieldType::Int64,
            nullable: false,
            field_id: 1,
        },
        FieldInfo {
            name: "name".to_string(),
            field_type: FieldType::String,
            nullable: true,
            field_id: 2,
        },
    ]);

    let columns = vec![
        Column::Int64(vec![Some(1), Some(2), Some(3)]),
        Column::String(vec![
            Some("Delta".to_string()),
            Some("United".to_string()),
            Some("American".to_string()),
        ]),
    ];

    ColumnBatch::new(Arc::new(schema), columns).unwrap()
}

/// Mock provider that returns different batches based on table name.
#[derive(Debug)]
struct MultiTableMockProvider {
    mapping: Arc<CompiledR2rmlMapping>,
    airlines_batch: ColumnBatch,
    routes_batch: ColumnBatch,
}

#[async_trait]
impl R2rmlProvider for MultiTableMockProvider {
    async fn has_r2rml_mapping(&self, _graph_source_id: &str) -> bool {
        true
    }

    async fn compiled_mapping(
        &self,
        _graph_source_id: &str,
        _as_of_t: Option<i64>,
    ) -> QueryResult<Arc<CompiledR2rmlMapping>> {
        Ok(Arc::clone(&self.mapping))
    }
}

#[async_trait]
impl R2rmlTableProvider for MultiTableMockProvider {
    async fn scan_table(
        &self,
        _graph_source_id: &str,
        table_name: &str,
        _projection: &[String],
        _filters: &[ScanFilter],
        _as_of_t: Option<i64>,
    ) -> QueryResult<ColumnBatchStream> {
        eprintln!("MultiTableMockProvider.scan_table: {table_name}");
        // Return appropriate batch based on table name
        // Table names are normalized to dot notation
        match table_name {
            "openflights.airlines" => Ok(vec_batch_stream(vec![self.airlines_batch.clone()])),
            "openflights.routes" => Ok(vec_batch_stream(vec![self.routes_batch.clone()])),
            _ => {
                eprintln!("Unknown table: {table_name}");
                Ok(vec_batch_stream(vec![]))
            }
        }
    }
}

/// Test RefObjectMap join execution end-to-end.
///
/// This test verifies:
/// 1. Routes table is scanned
/// 2. Airlines table is scanned for parent lookup
/// 3. Join is performed correctly (routes.airline_id = airlines.id)
/// 4. Parent subject IRIs are correctly materialized as object values
/// 5. Orphan foreign keys (airline_id=999) produce no output
#[tokio::test]
async fn engine_e2e_ref_object_map_join_execution() {
    // Compile the routes mapping with RefObjectMap
    let mapping = R2rmlLoader::from_turtle(ROUTES_MAPPING_TTL)
        .expect("Failed to parse R2RML")
        .compile()
        .expect("Failed to compile");

    let provider = MultiTableMockProvider {
        mapping: Arc::new(mapping),
        airlines_batch: sample_airlines_for_join(),
        routes_batch: sample_routes_batch(),
    };

    // Create a minimal Fluree instance
    let fluree = FlureeBuilder::memory().build_memory();
    let mut ledger = genesis_ledger(&fluree, "ref-join-test:main");

    // Register example.org namespace
    std::sync::Arc::make_mut(&mut ledger.snapshot)
        .insert_namespace_code(9_999, "http://example.org/".to_string())
        .unwrap();

    // Build query: SELECT ?route ?airline WHERE { GRAPH <gs:main> { ?route ex:operatedBy ?airline } }
    let mut vars = VarRegistry::new();
    let route_var = vars.get_or_insert("?route");
    let airline_var = vars.get_or_insert("?airline");

    // Register predicate IRI
    let ex_operated_by_sid = ledger
        .snapshot
        .encode_iri("http://example.org/operatedBy")
        .expect("namespace should be registered");

    let inner_patterns = vec![Pattern::Triple(TriplePattern::new(
        Ref::Var(route_var),
        Ref::Sid(ex_operated_by_sid),
        Term::Var(airline_var),
    ))];

    let graph_pattern = Pattern::Graph {
        name: GraphName::Iri("routes-gs:main".into()),
        patterns: inner_patterns,
    };

    let mut parsed = Query::new(ParsedContext::default());
    parsed.patterns = vec![graph_pattern];
    parsed.output = QueryOutput::select_all(vec![route_var, airline_var]);

    let executable = ExecutableQuery::simple(parsed);
    let tracker = Tracker::disabled();

    // Execute query
    let batches = execute(
        GraphDbRef::new(&ledger.snapshot, 0, &NoOverlay, ledger.t()),
        &vars,
        &executable,
        r2rml_test_config(&tracker, &provider),
    )
    .await
    .expect("Query execution should succeed");

    // Count results
    let total_rows: usize = batches.iter().map(fluree_db_api::Batch::len).sum();
    eprintln!("RefObjectMap join query returned {total_rows} rows");

    // We have 5 routes, but airline_id=999 has no matching airline
    // So we expect 4 results (routes with airline_id 1, 1, 2, 3)
    assert_eq!(
        total_rows, 4,
        "Should have 4 results (5 routes minus 1 orphan foreign key)"
    );

    // Verify the bindings contain expected values
    // The airline var should be IRI like "http://example.org/airline/1"
    for batch in &batches {
        for row_idx in 0..batch.len() {
            // Get airline binding
            if let Some(col) = batch.column_by_idx(1) {
                let airline_binding = &col[row_idx];
                eprintln!("Row {row_idx}: airline = {airline_binding:?}");
                // Should be a Sid (encoded IRI), not Unbound
                assert!(
                    airline_binding.is_bound(),
                    "Airline binding should be bound for matched joins"
                );
            }
        }
    }
}

// =============================================================================
// Multi-table mapping in the idiomatic `@base` + `<#Name>` style (issue #1395)
// =============================================================================

/// A star-schema-style mapping written the way the W3C R2RML spec writes its
/// own examples: a document `@base` plus relative `<#Name>` TriplesMap subjects.
///
/// Before the parser fix every `<#Name>` resolved to the same fragment-stripped
/// IRI, collapsing all three TriplesMaps into one (single table, union of every
/// column). With RFC 3986 §5.3 fragment resolution, `<#DimDate>`,
/// `<#DimProduct>` and `<#FactSales>` resolve to three distinct IRIs and map
/// three distinct tables.
const BASE_FRAGMENT_MULTI_TABLE_TTL: &str = r#"
@base <http://ex/edw> .
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .

<#DimDate> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "DW.DIM_DATE" ] ;
    rr:subjectMap [ rr:template "http://example.org/date/{DATE_KEY}" ; rr:class ex:Date ] ;
    rr:predicateObjectMap [
        rr:predicate ex:year ;
        rr:objectMap [ rr:column "YEAR" ]
    ] .

<#DimProduct> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "DW.DIM_PRODUCT" ] ;
    rr:subjectMap [ rr:template "http://example.org/product/{PRODUCT_KEY}" ; rr:class ex:Product ] ;
    rr:predicateObjectMap [
        rr:predicate ex:productName ;
        rr:objectMap [ rr:column "NAME" ]
    ] .

<#FactSales> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "DW.FACT_SALES" ] ;
    rr:subjectMap [ rr:template "http://example.org/sale/{SALE_ID}" ; rr:class ex:Sale ] ;
    rr:predicateObjectMap [
        rr:predicate ex:amount ;
        rr:objectMap [ rr:column "AMOUNT" ]
    ] .
"#;

/// Two `rr:TriplesMap` subjects that resolve to the same IRI. This is the
/// post-collapse shape the (now-fixed) Turtle parser used to produce silently;
/// the hardening guard must reject it loudly rather than first-wins/union-merge.
const COLLIDING_TRIPLES_MAP_TTL: &str = r#"
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .

<http://example.org/mapping#Collide> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "DW.DIM_DATE" ] ;
    rr:subjectMap [ rr:template "http://example.org/date/{DATE_KEY}" ; rr:class ex:Date ] ;
    rr:predicateObjectMap [
        rr:predicate ex:year ;
        rr:objectMap [ rr:column "YEAR" ]
    ] .

<http://example.org/mapping#Collide> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "DW.DIM_PRODUCT" ] ;
    rr:subjectMap [ rr:template "http://example.org/product/{PRODUCT_KEY}" ; rr:class ex:Product ] ;
    rr:predicateObjectMap [
        rr:predicate ex:productName ;
        rr:objectMap [ rr:column "NAME" ]
    ] .
"#;

/// Sample batch for the `DW.DIM_PRODUCT` table (subject key + name column).
fn sample_product_batch() -> ColumnBatch {
    let schema = BatchSchema::new(vec![
        FieldInfo {
            name: "PRODUCT_KEY".to_string(),
            field_type: FieldType::Int64,
            nullable: false,
            field_id: 1,
        },
        FieldInfo {
            name: "NAME".to_string(),
            field_type: FieldType::String,
            nullable: true,
            field_id: 2,
        },
    ]);
    let columns = vec![
        Column::Int64(vec![Some(10), Some(20)]),
        Column::String(vec![Some("Widget".to_string()), Some("Gadget".to_string())]),
    ];
    ColumnBatch::new(Arc::new(schema), columns).unwrap()
}

/// Mock provider that records which tables `scan_table` is asked to read, so a
/// test can assert that a non-first TriplesMap scans its own table (and not the
/// first map's table, which was the collapse symptom).
#[derive(Debug)]
struct RecordingTableProvider {
    mapping: Arc<CompiledR2rmlMapping>,
    batches: HashMap<String, ColumnBatch>,
    scanned: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl R2rmlProvider for RecordingTableProvider {
    async fn has_r2rml_mapping(&self, _graph_source_id: &str) -> bool {
        true
    }

    async fn compiled_mapping(
        &self,
        _graph_source_id: &str,
        _as_of_t: Option<i64>,
    ) -> QueryResult<Arc<CompiledR2rmlMapping>> {
        Ok(Arc::clone(&self.mapping))
    }
}

#[async_trait]
impl R2rmlTableProvider for RecordingTableProvider {
    async fn scan_table(
        &self,
        _graph_source_id: &str,
        table_name: &str,
        _projection: &[String],
        _filters: &[ScanFilter],
        _as_of_t: Option<i64>,
    ) -> QueryResult<ColumnBatchStream> {
        self.scanned.lock().unwrap().push(table_name.to_string());
        match self.batches.get(table_name) {
            Some(batch) => Ok(vec_batch_stream(vec![batch.clone()])),
            None => Ok(vec_batch_stream(vec![])),
        }
    }
}

/// Offline regression guard for issue #1395: an idiomatic `@base` + `<#Name>`
/// multi-table mapping compiles to N distinct TriplesMaps over N distinct
/// tables — not one merged map.
#[test]
fn test_base_fragment_multi_table_compiles_to_distinct_tables() {
    let mapping = R2rmlLoader::from_turtle(BASE_FRAGMENT_MULTI_TABLE_TTL)
        .expect("Failed to parse R2RML Turtle")
        .compile()
        .expect("Failed to compile R2RML mapping");

    // Three TriplesMaps, not one collapsed map.
    assert_eq!(mapping.triples_maps.len(), 3);

    // Each `<#Name>` resolved to its own distinct IRI against `@base`.
    assert!(mapping.get("http://ex/edw#DimDate").is_some());
    assert!(mapping.get("http://ex/edw#DimProduct").is_some());
    assert!(mapping.get("http://ex/edw#FactSales").is_some());

    // Three distinct logical tables (the collapse produced exactly one).
    let mut tables = mapping.table_names();
    tables.sort_unstable();
    assert_eq!(
        tables,
        vec!["DW.DIM_DATE", "DW.DIM_PRODUCT", "DW.FACT_SALES"]
    );
}

/// Engine-level E2E for issue #1395: querying a predicate owned by a non-first
/// TriplesMap scans that map's own table (`DW.DIM_PRODUCT`) and never the first
/// map's table (`DW.DIM_DATE`). Pre-fix, every class scanned the first table.
#[tokio::test]
async fn engine_e2e_base_fragment_scans_non_first_table() {
    let mapping = R2rmlLoader::from_turtle(BASE_FRAGMENT_MULTI_TABLE_TTL)
        .expect("Failed to parse R2RML")
        .compile()
        .expect("Failed to compile");

    let mut batches = HashMap::new();
    batches.insert("DW.DIM_PRODUCT".to_string(), sample_product_batch());

    let scanned = Arc::new(Mutex::new(Vec::new()));
    let provider = RecordingTableProvider {
        mapping: Arc::new(mapping),
        batches,
        scanned: Arc::clone(&scanned),
    };

    let fluree = FlureeBuilder::memory().build_memory();
    let mut ledger = genesis_ledger(&fluree, "edw-multi:main");
    std::sync::Arc::make_mut(&mut ledger.snapshot)
        .insert_namespace_code(9_999, "http://example.org/".to_string())
        .unwrap();

    // Query: SELECT ?p ?n WHERE { GRAPH <edw-gs:main> { ?p ex:productName ?n } }
    // ex:productName is owned only by <#DimProduct> → only DW.DIM_PRODUCT.
    let mut vars = VarRegistry::new();
    let product_var = vars.get_or_insert("?p");
    let name_var = vars.get_or_insert("?n");

    let ex_product_name_sid = ledger
        .snapshot
        .encode_iri("http://example.org/productName")
        .expect("example.org namespace should be registered for Sid encoding");

    let inner_patterns = vec![Pattern::Triple(TriplePattern::new(
        Ref::Var(product_var),
        Ref::Sid(ex_product_name_sid),
        Term::Var(name_var),
    ))];

    let graph_pattern = Pattern::Graph {
        name: GraphName::Iri("edw-gs:main".into()),
        patterns: inner_patterns,
    };

    let mut parsed = Query::new(ParsedContext::default());
    parsed.patterns = vec![graph_pattern];
    parsed.output = QueryOutput::select_all(vec![product_var, name_var]);

    let executable = ExecutableQuery::simple(parsed);
    let tracker = Tracker::disabled();

    let result_batches = execute(
        GraphDbRef::new(&ledger.snapshot, 0, &NoOverlay, ledger.t()),
        &vars,
        &executable,
        r2rml_test_config(&tracker, &provider),
    )
    .await
    .expect("Query execution should succeed");

    let total_rows: usize = result_batches.iter().map(fluree_db_api::Batch::len).sum();
    assert_eq!(total_rows, 2, "Should return the 2 DW.DIM_PRODUCT rows");

    let scanned_tables = scanned.lock().unwrap().clone();
    assert_eq!(
        scanned_tables,
        vec!["DW.DIM_PRODUCT".to_string()],
        "Non-first TriplesMap must scan its own table only; scanned: {scanned_tables:?}"
    );
    assert!(
        !scanned_tables.contains(&"DW.DIM_DATE".to_string()),
        "The first map's table must not be scanned for a non-first predicate"
    );
}

/// Phase-2 hardening for issue #1395: two `rr:TriplesMap` subjects that resolve
/// to the same IRI must be a hard error, not a silent first-wins/union merge.
#[test]
fn test_colliding_triples_map_iris_error() {
    let result = R2rmlLoader::from_turtle(COLLIDING_TRIPLES_MAP_TTL)
        .expect("Turtle should parse")
        .compile();

    assert!(
        result.is_err(),
        "Two TriplesMap subjects colliding to one IRI should be rejected, got Ok"
    );

    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("Duplicate TriplesMap IRI"),
        "Error should be a DuplicateTriplesMap, got: {msg}"
    );
    assert!(
        msg.contains("http://example.org/mapping#Collide"),
        "Error should name the colliding IRI, got: {msg}"
    );
}

/// Test that composite join keys work correctly.
///
/// This test uses a mapping with multiple join conditions and verifies
/// all conditions are applied.
#[test]
fn test_ref_object_map_composite_key_parsing() {
    use fluree_db_r2rml::mapping::ObjectMap;

    const COMPOSITE_KEY_MAPPING: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#ParentMap> a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "parent_table" ] ;
        rr:subjectMap [
            rr:template "http://example.org/parent/{key1}/{key2}"
        ] .

    <http://example.org/mapping#ChildMap> a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "child_table" ] ;
        rr:subjectMap [
            rr:template "http://example.org/child/{id}"
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:parent ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#ParentMap> ;
                rr:joinCondition [
                    rr:child "child_key1" ;
                    rr:parent "key1"
                ] ;
                rr:joinCondition [
                    rr:child "child_key2" ;
                    rr:parent "key2"
                ]
            ]
        ] .
    "#;

    let mapping = R2rmlLoader::from_turtle(COMPOSITE_KEY_MAPPING)
        .expect("Failed to parse")
        .compile()
        .expect("Failed to compile");

    let child_map = mapping
        .get("http://example.org/mapping#ChildMap")
        .expect("ChildMap not found");

    let parent_pom = child_map
        .predicate_object_maps
        .iter()
        .find(|pom| pom.predicate_map.as_constant() == Some("http://example.org/parent"))
        .expect("parent POM not found");

    match &parent_pom.object_map {
        ObjectMap::RefObjectMap(rom) => {
            assert_eq!(
                rom.join_conditions.len(),
                2,
                "Should have 2 join conditions"
            );

            // Check first condition
            assert_eq!(rom.join_conditions[0].child_column, "child_key1");
            assert_eq!(rom.join_conditions[0].parent_column, "key1");

            // Check second condition
            assert_eq!(rom.join_conditions[1].child_column, "child_key2");
            assert_eq!(rom.join_conditions[1].parent_column, "key2");
        }
        _ => panic!("Expected RefObjectMap"),
    }
}

// =============================================================================
// Fused-aggregate correctness regressions (PR #1372 internal review)
//
// Run aggregate queries through the fused R2RML path (default) over crafted
// column batches and assert the result matches exact SPARQL semantics. Each
// test fails if the fused path diverges from the normal (fallback) pipeline.
// =============================================================================

/// R2RML mapping for `?s ex:val ?o`, with `val` typed as `datatype`
/// (e.g. `xsd:integer`, `xsd:decimal`) so the object materializes as numeric.
fn val_mapping(datatype: &str) -> String {
    format!(
        r#"
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<http://example.org/mapping#M> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "t" ] ;
    rr:subjectMap [ rr:template "http://example.org/r/{{id}}" ] ;
    rr:predicateObjectMap [ rr:predicate ex:val ;
        rr:objectMap [ rr:column "val" ; rr:datatype {datatype} ] ] .
"#
    )
}

/// Build an `id` (non-null subject key) + `val` (crafted aggregate input) batch.
fn id_val_batch(ids: Vec<Option<i64>>, val: Column, val_type: FieldType) -> ColumnBatch {
    let schema = BatchSchema::new(vec![
        FieldInfo {
            name: "id".into(),
            field_type: FieldType::Int64,
            nullable: false,
            field_id: 1,
        },
        FieldInfo {
            name: "val".into(),
            field_type: val_type,
            nullable: true,
            field_id: 2,
        },
    ]);
    ColumnBatch::new(Arc::new(schema), vec![Column::Int64(ids), val]).unwrap()
}

/// Run `SELECT (agg) WHERE { GRAPH gs { ?s ex:val ?o } <extra> }` (implicit
/// aggregation) over crafted batches via the fused path; return the single
/// scalar result formatted with `{:?}`. `build` receives the registry and the
/// object var `?o`, and returns any extra patterns (e.g. a `BIND`) plus the
/// aggregate function.
async fn run_val_agg(
    datatype: &str,
    batches: Vec<ColumnBatch>,
    build: impl FnOnce(&mut VarRegistry, VarId) -> (Vec<Pattern>, AggregateFn),
) -> String {
    let fluree = FlureeBuilder::memory().build_memory();
    let mut ledger = genesis_ledger(&fluree, "fa:main");
    Arc::make_mut(&mut ledger.snapshot)
        .insert_namespace_code(9_999, "http://example.org/".to_string())
        .unwrap();
    let mapping = R2rmlLoader::from_turtle(&val_mapping(datatype))
        .unwrap()
        .compile()
        .unwrap();
    let provider = MockR2rmlProvider::new(mapping, batches);

    let mut vars = VarRegistry::new();
    let s = vars.get_or_insert("?s");
    let o = vars.get_or_insert("?o");
    let out = vars.get_or_insert("?agg");
    let pred = ledger
        .snapshot
        .encode_iri("http://example.org/val")
        .expect("example.org namespace registered");

    let graph = Pattern::Graph {
        name: GraphName::Iri("fa-gs:main".into()),
        patterns: vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(s),
            Ref::Sid(pred),
            Term::Var(o),
        ))],
    };
    let (extra, agg_fn) = build(&mut vars, o);
    let mut patterns = vec![graph];
    patterns.extend(extra);

    let mut parsed = Query::new(ParsedContext::default());
    parsed.patterns = patterns;
    parsed.output = QueryOutput::select_all(vec![out]);
    parsed.grouping = Grouping::assemble(
        vec![],
        vec![AggregateSpec {
            function: agg_fn,
            output_var: out,
        }],
        vec![],
        None,
    );

    let executable = ExecutableQuery::simple(parsed);
    let tracker = Tracker::disabled();
    let result = execute(
        GraphDbRef::new(&ledger.snapshot, 0, &NoOverlay, ledger.t()),
        &vars,
        &executable,
        r2rml_test_config(&tracker, &provider),
    )
    .await
    .expect("query should execute");

    let batch = result
        .into_iter()
        .find(|b| !b.is_empty())
        .expect("one result row");
    let value = batch
        .column_by_idx(0)
        .and_then(|c| c.first())
        .expect("a scalar value");
    format!("{value:?}")
}

#[tokio::test]
async fn fused_sum_distinct_dedups() {
    // SUM(DISTINCT ?o) over [10, 10, 20] must dedup to 30, not 40.
    let batch = id_val_batch(
        vec![Some(1), Some(2), Some(3)],
        Column::Int64(vec![Some(10), Some(10), Some(20)]),
        FieldType::Int64,
    );
    let got = run_val_agg("xsd:integer", vec![batch], |_v, o| {
        (vec![], AggregateFn::Sum(o, InputSemantics::Set))
    })
    .await;
    assert!(
        got.contains("30") && !got.contains("40"),
        "SUM(DISTINCT) should dedup to 30, got {got}"
    );
}

#[tokio::test]
async fn fused_count_star_drops_null_object_rows() {
    // COUNT(*) over `?s ex:val ?o` where val = [10, null, 30]: the null-val row
    // produces no triple, so the count is 2, not 3.
    let batch = id_val_batch(
        vec![Some(1), Some(2), Some(3)],
        Column::Int64(vec![Some(10), None, Some(30)]),
        FieldType::Int64,
    );
    let got = run_val_agg("xsd:integer", vec![batch], |_v, _o| {
        (vec![], AggregateFn::CountAll)
    })
    .await;
    assert!(
        got.contains('2') && !got.contains('3'),
        "COUNT(*) should drop the null-object row → 2, got {got}"
    );
}

#[tokio::test]
async fn fused_sum_overflow_falls_back_to_exact() {
    // Two huge decimal values whose unscaled i128 sum overflows: the fused
    // accumulator must fall back to the BigDecimal pipeline, not wrap to a
    // wrong value. i128::MAX - 5 + 10 = i128::MAX + 5 (a 39-digit number).
    let batch = id_val_batch(
        vec![Some(1), Some(2)],
        Column::Decimal {
            values: vec![Some(i128::MAX - 5), Some(10)],
            precision: 38,
            scale: 0,
        },
        FieldType::Decimal {
            precision: 38,
            scale: 0,
        },
    );
    let got = run_val_agg("xsd:decimal", vec![batch], |_v, o| {
        (vec![], AggregateFn::Sum(o, InputSemantics::List))
    })
    .await;
    // Correct total is positive 2^127 + 4 (BigInt limbs [4, 2^63]); a wrapping
    // i128 add would instead produce a negative (sign=Minus) wrong value.
    assert!(
        got.contains("sign=Plus") && got.contains("9223372036854775808"),
        "overflowing SUM should fall back to the exact positive BigDecimal total, got {got}"
    );
}

#[tokio::test]
async fn fused_sum_integer_expr_keeps_integer_datatype() {
    // SUM(?o * 2) over integer `?o` is xsd:integer in the normal pipeline; the
    // native expr fold always finalizes as xsd:decimal, so it must fall back to
    // keep the datatype exact. o = [10, 20] → 2*(10+20) = 60.
    let batch = id_val_batch(
        vec![Some(1), Some(2)],
        Column::Int64(vec![Some(10), Some(20)]),
        FieldType::Int64,
    );
    let got = run_val_agg("xsd:integer", vec![batch], |vars, o| {
        let e = vars.get_or_insert("?e");
        let expr = Expression::Call {
            func: Function::Mul,
            args: vec![Expression::Var(o), Expression::Const(FlakeValue::Long(2))],
        };
        (
            vec![Pattern::Bind { var: e, expr }],
            AggregateFn::Sum(e, InputSemantics::List),
        )
    })
    .await;
    assert!(
        got.contains("60") && got.contains("integer") && !got.contains("decimal"),
        "SUM(integer expr) should stay xsd:integer (= 60), got {got}"
    );
}

#[tokio::test]
async fn fused_fallback_applies_offset_once() {
    // GROUP BY on an xsd:date column trips the fused operator's column-resolution
    // gate (date isn't a supported group key), so it falls back to the normal
    // pipeline at open. OFFSET must be applied exactly once: the fallback is built
    // with OFFSET stripped and the engine's OffsetOperator wraps the result. The
    // prior regression baked OFFSET into the fallback *and* re-applied it, which
    // would drop twice as many rows.
    let mapping = R2rmlLoader::from_turtle(&val_mapping("xsd:date"))
        .unwrap()
        .compile()
        .unwrap();
    // Three distinct date groups (days since epoch): {1: 2 rows, 2: 1, 3: 1}.
    let batch = id_val_batch(
        vec![Some(1), Some(2), Some(3), Some(4)],
        Column::Date(vec![Some(1), Some(1), Some(2), Some(3)]),
        FieldType::Date,
    );
    let provider = MockR2rmlProvider::new(mapping, vec![batch]);

    let fluree = FlureeBuilder::memory().build_memory();
    let mut ledger = genesis_ledger(&fluree, "fa:main");
    Arc::make_mut(&mut ledger.snapshot)
        .insert_namespace_code(9_999, "http://example.org/".to_string())
        .unwrap();

    let mut vars = VarRegistry::new();
    let s = vars.get_or_insert("?s");
    let o = vars.get_or_insert("?o");
    let c = vars.get_or_insert("?c");
    let pred = ledger
        .snapshot
        .encode_iri("http://example.org/val")
        .expect("example.org namespace registered");

    let graph = Pattern::Graph {
        name: GraphName::Iri("fa-gs:main".into()),
        patterns: vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(s),
            Ref::Sid(pred),
            Term::Var(o),
        ))],
    };

    let mut parsed = Query::new(ParsedContext::default());
    parsed.patterns = vec![graph];
    // Detection requires the projection to equal {group keys} ∪ {agg outputs}.
    parsed.output = QueryOutput::select_all(vec![o, c]);
    parsed.grouping = Grouping::assemble(
        vec![o],
        vec![AggregateSpec {
            function: AggregateFn::CountAll,
            output_var: c,
        }],
        vec![],
        None,
    );
    parsed.offset = Some(1);

    let executable = ExecutableQuery::simple(parsed);
    let tracker = Tracker::disabled();
    let result = execute(
        GraphDbRef::new(&ledger.snapshot, 0, &NoOverlay, ledger.t()),
        &vars,
        &executable,
        r2rml_test_config(&tracker, &provider),
    )
    .await
    .expect("query should execute");

    let rows: usize = result.iter().fold(0, |acc, b| acc + b.len());
    assert_eq!(
        rows, 2,
        "3 groups with OFFSET 1 must yield 2 rows (offset applied once, not twice)"
    );
}
