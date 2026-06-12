//! Decimal exactness integration tests
//!
//! xsd:decimal values must never round-trip through f64 anywhere between
//! ingestion and output: query constants, SPARQL UPDATE templates, and
//! stored values all carry exact BigDecimal representations.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::Value as JsonValue;
use support::{assert_index_defaults, genesis_ledger, MemoryFluree};

async fn run_sparql_update(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    sparql: &str,
) -> fluree_db_api::TransactResult {
    let parsed = fluree_db_sparql::parse_sparql(sparql);
    assert!(
        !parsed.has_errors(),
        "SPARQL parse errors: {:?}",
        parsed.diagnostics
    );
    let ast = parsed.ast.expect("SPARQL AST");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &ast,
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower SPARQL UPDATE to Txn IR");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect("stage SPARQL UPDATE")
}

/// Extract the literal values of a single-variable SPARQL JSON result.
fn binding_values(sparql_json: &JsonValue, var: &str) -> Vec<String> {
    sparql_json["results"]["bindings"]
        .as_array()
        .expect("bindings array")
        .iter()
        .map(|b| {
            b[var]["value"]
                .as_str()
                .expect("binding value string")
                .to_string()
        })
        .collect()
}

fn memory_fluree() -> MemoryFluree {
    assert_index_defaults();
    FlureeBuilder::memory().build_memory()
}

#[tokio::test]
async fn sparql_insert_data_decimal_roundtrip_is_exact() {
    let fluree = memory_fluree();
    let ledger = genesis_ledger(&fluree, "decimal/insert:main");

    // 19.99 has no exact f64 representation; an f64 round-trip surfaces as
    // 19.989999999999998... in exact-decimal output.
    let result = run_sparql_update(
        &fluree,
        ledger,
        r#"
        PREFIX ex: <http://example.org/>
        INSERT DATA { ex:item ex:price 19.99 . }
        "#,
    )
    .await;
    let ledger = result.ledger;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?price WHERE { ex:item ex:price ?price . }
    "#;
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "price"), vec!["19.99"]);
}

#[tokio::test]
async fn sparql_insert_data_high_precision_decimal_survives() {
    let fluree = memory_fluree();
    let ledger = genesis_ledger(&fluree, "decimal/precision:main");

    // More significant digits than f64 can hold (~17).
    let lexical = "1234567890123456789.0123456789";
    let result = run_sparql_update(
        &fluree,
        ledger,
        &format!(
            r#"
            PREFIX ex: <http://example.org/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            INSERT DATA {{ ex:big ex:amount "{lexical}"^^xsd:decimal . }}
            "#
        ),
    )
    .await;
    let ledger = result.ledger;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?amount WHERE { ex:big ex:amount ?amount . }
    "#;
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "amount"), vec![lexical]);
}

#[tokio::test]
async fn sparql_decimal_constant_matches_stored_decimal() {
    let fluree = memory_fluree();
    let ledger = genesis_ledger(&fluree, "decimal/constant:main");

    let result = run_sparql_update(
        &fluree,
        ledger,
        r#"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
            ex:a ex:price 19.99 .
            ex:b ex:price 20.00 .
        }
        "#,
    )
    .await;
    let ledger = result.ledger;

    // Constant in object position must exactly match the stored decimal.
    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ?s ex:price 19.99 . }
    "#;
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "s"), vec!["ex:a"]);

    // FILTER equality with a decimal constant.
    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ?s ex:price ?p . FILTER(?p = 20.00) }
    "#;
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "s"), vec!["ex:b"]);
}

#[tokio::test]
async fn sparql_delete_data_decimal_retracts_exactly() {
    let fluree = memory_fluree();
    let ledger = genesis_ledger(&fluree, "decimal/delete:main");

    let result = run_sparql_update(
        &fluree,
        ledger,
        r#"
        PREFIX ex: <http://example.org/>
        INSERT DATA { ex:item ex:price 19.99 . }
        "#,
    )
    .await;

    // DELETE DATA with the same lexical must hit the same stored fact —
    // an f64 round-trip on either side breaks retract identity.
    let result = run_sparql_update(
        &fluree,
        result.ledger,
        r#"
        PREFIX ex: <http://example.org/>
        DELETE DATA { ex:item ex:price 19.99 . }
        "#,
    )
    .await;
    let ledger = result.ledger;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?price WHERE { ex:item ex:price ?price . }
    "#;
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(
        binding_values(&sparql_json, "price"),
        Vec::<String>::new(),
        "deleted decimal fact must not survive"
    );
}
